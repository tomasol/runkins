use crate::cgroup::concepts::CGroupLimits;
use crate::cgroup::runtime::CGroupCommandError;
use crate::cgroup::runtime::CGroupCommandFactory;
use crate::cgroup::server_config::AutoCleanChildCGroup;
use crate::cgroup::server_config::CGroupConfig;
use crate::cgroup::server_config::ChildCGroup;
use log::*;
use std::ffi::OsStr;
use std::fmt::{Debug, Display};
use std::pin::Pin;
use std::process::ExitStatus;
use std::process::Stdio;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::process::Child;
use tokio::process::ChildStderr;
use tokio::process::ChildStdout;
use tokio::process::Command;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

#[derive(Error, Debug)]
pub enum StopError {
    #[error("main_actor is no longer running")] // TODO: panic instead?
    MainActorFinished,
}

#[derive(Error, Debug)]
pub enum AddClientError {
    #[error("main_actor is no longer running")] // TODO: panic instead?
    MainActorFinished,
}

#[derive(Error, Debug)]
pub enum StatusError {
    #[error("main_actor is no longer running")] // TODO: panic instead?
    MainActorFinished,
}

#[derive(Error, Debug)]
pub enum OutputError {
    #[error("main_actor is no longer running")] // TODO: panic instead?
    MainActorFinished,
}

#[derive(Error, Debug)]
pub enum ChildInfoCreationError {
    #[error("[{0}] cannot capture {1:?} of the child process")]
    CannotCaptureStream(Pid, StdStream),
    #[error("[{0}] cannot run process")]
    CannotRunProcess(Pid, #[source] std::io::Error),
}

#[derive(Error, Debug)]
pub enum ChildInfoCreationWithCGroupError {
    #[error("[{0}] error while child process creation")]
    ChildInfoCreationError(Pid, #[source] ChildInfoCreationError),
    #[error("[{0}] cannot run process in cgroup")]
    ProcessExecutionError(Pid, #[source] CGroupCommandError),
}

pub type Pid = u64;

#[derive(Debug)]
enum ActorEvent {
    // sent by std_forwarder
    ChunkAdded(Chunk),
    // sent by API call
    ClientAdded(String, oneshot::Sender<EventStream<Chunk>>),
    // internal to main_actor
    ProcessFinished(CompleteExitStatus),
    // sent by API call
    StatusRequest(oneshot::Sender<RunningState>),
    // TODO depreaete. sent by API call
    GetCurrentChunk(oneshot::Sender<Chunk>),
    // sent by API call
    KillRequest(oneshot::Sender<CompleteExitStatus>),
    // sent by API call
    NotifyWhenProcessFinishes(oneshot::Sender<CompleteExitStatus>),
}

#[derive(Debug)]
pub struct ChildInfo {
    pid: Pid,
    main_tx: mpsc::Sender<ActorEvent>,
    child_cgroup: Option<ChildCGroup>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub std_out: Vec<u8>,
    pub std_err: Vec<u8>,
}

impl Chunk {
    fn new(std_stream: &StdStream, content: Vec<u8>) -> Chunk {
        match std_stream {
            StdStream::StdOut => Chunk {
                std_out: content,
                ..Default::default()
            },
            StdStream::StdErr => Chunk {
                std_err: content,
                ..Default::default()
            },
        }
    }

    // TODO remove?
    pub fn std_stream(&self, std_stream: StdStream) -> &[u8] {
        match std_stream {
            StdStream::StdOut => &self.std_out,
            StdStream::StdErr => &self.std_err,
        }
    }

    // TODO remove?
    pub fn std_out(&self) -> &[u8] {
        self.std_stream(StdStream::StdOut)
    }

    //TODO remove?
    pub fn std_err(&self) -> &[u8] {
        self.std_stream(StdStream::StdErr)
    }
}

impl Default for Chunk {
    fn default() -> Self {
        Chunk {
            std_out: vec![],
            std_err: vec![],
        }
    }
}

impl std::ops::Add for Chunk {
    type Output = Chunk;

    fn add(mut self, mut rhs: Self) -> Self::Output {
        self.std_out.append(&mut rhs.std_out);
        self.std_err.append(&mut rhs.std_err);
        self
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum RunningState {
    // CompleteExitStatus + Running
    Running,
    Unknown(String),
    Finished(FinishedState),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FinishedState {
    WithExitCode(i32),
    WithSignal,
}

impl From<Option<i32>> for FinishedState {
    fn from(maybe_code: Option<i32>) -> Self {
        match maybe_code {
            Some(code) => FinishedState::WithExitCode(code),
            None => FinishedState::WithSignal,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StdStream {
    StdOut,
    StdErr,
}

// buffer capacity set arbitrarily
const CHUNK_BUF_CAPACITY: usize = 1024;

pub fn spawn_named<T>(
    _name: &str,
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> tokio::task::JoinHandle<T>
where
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    return tokio::task::Builder::new().name(_name).spawn(future);

    #[cfg(not(tokio_unstable))]
    tokio::spawn(future)
}

#[derive(Debug, Clone)]
pub enum CompleteExitStatus {
    Complete(FinishedState),
    Unknown(String),
}

impl<ERR: std::error::Error> From<Result<ExitStatus, ERR>> for CompleteExitStatus {
    fn from(src: Result<ExitStatus, ERR>) -> Self {
        match src {
            Ok(status) => CompleteExitStatus::Complete(status.code().into()),
            Err(err) => CompleteExitStatus::Unknown(err.to_string()),
        }
    }
}

enum ExitStatusOrListeners {
    ExitStatus(CompleteExitStatus),
    Listeners(Vec<oneshot::Sender<CompleteExitStatus>>),
}

impl ExitStatusOrListeners {
    fn as_running_state(&self) -> RunningState {
        match self {
            ExitStatusOrListeners::Listeners(_) => RunningState::Running,
            ExitStatusOrListeners::ExitStatus(CompleteExitStatus::Complete(exit_status)) => {
                RunningState::Finished(*exit_status)
            }
            ExitStatusOrListeners::ExitStatus(CompleteExitStatus::Unknown(reason)) => {
                RunningState::Unknown(reason.clone())
            }
        }
    }
}

impl Display for ExitStatusOrListeners {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self.as_running_state()))
    }
}

#[derive(Debug)]
pub enum EventStream<Item: Debug> {
    OpenForEvents(Item, BroadcastStream<Item>),
    ClosedForEvents(Item),
}

impl<Item: 'static + Debug + Clone + Send + Sync> EventStream<Item> {
    pub fn into_accumulated(self) -> Item {
        match self {
            EventStream::OpenForEvents(item, _) => item,
            EventStream::ClosedForEvents(item) => item,
        }
    }

    // FIXME: should not require map_fun
    pub fn into_stream<R, F>(self, map_fun: F) -> Pin<Box<dyn Stream<Item = R> + Send + Sync>>
    where
        R: 'static,
        F: Fn(Result<Item, BroadcastStreamRecvError>) -> R + Send + Sync + 'static,
    {
        match self {
            EventStream::OpenForEvents(item, stream) => Box::pin(
                tokio_stream::once(item)
                    .map(|item| Ok(item))
                    .chain(stream)
                    .map(map_fun),
            ),
            EventStream::ClosedForEvents(item) => {
                Box::pin(tokio_stream::once(item).map(|item| Ok(item)).map(map_fun))
            }
        }
    }
}

#[derive(Debug)]
struct EventStorage<Item: Debug> {
    accumulated: Item,
    sender: Option<broadcast::Sender<Item>>, // set to None when finished
}

#[derive(Debug, Error)]
enum AddEventError {
    #[error("Cannot add event after calling no_more_events")]
    AlreadyFinished,
}

impl<Item> EventStorage<Item>
where
    Item: Debug + Clone + Default + std::ops::Add<Output = Item> + Send + 'static,
{
    pub fn new(capacity: usize) -> Self {
        let (sender, _receiver) = broadcast::channel(capacity);
        EventStorage {
            accumulated: Default::default(),
            sender: Some(sender),
        }
    }

    pub fn add_event(&mut self, event: Item) -> Result<(), AddEventError> {
        // TODO low do not clone unless needed
        self.accumulated = std::mem::take(&mut self.accumulated) + event.clone();

        // Ignore possible sending errors when nobody is listening.
        let _ = self
            .sender
            .as_ref()
            .ok_or(AddEventError::AlreadyFinished)?
            .send(event);
        Ok(())
    }

    pub fn no_more_events(&mut self) {
        self.sender.take();
    }

    pub fn get_event_stream(&self, client_id: String) -> EventStream<Item> {
        debug!("{} Subscribing", client_id);
        if let Some(sender) = &self.sender {
            EventStream::OpenForEvents(
                self.accumulated.clone(),
                BroadcastStream::new(sender.subscribe()),
            )
        } else {
            EventStream::ClosedForEvents(self.accumulated.clone())
        }
    }
}

impl ChildInfo {
    async fn rpc<F: Fn(oneshot::Sender<RESP>) -> ActorEvent, RESP>(
        &self,
        event_fn: F,
    ) -> Result<RESP, ()> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        self.main_tx.send(event_fn(oneshot_tx)).await.map_err(|_| {
            // recreate the event for logging
            let (oneshot_tx, _) = oneshot::channel();
            let event = event_fn(oneshot_tx);
            error!("[{}] {:?} rpc failed to send request", self.pid, event);
        })?;
        oneshot_rx.await.map_err(|_| {
            // recreate the event for logging
            let (oneshot_tx, _) = oneshot::channel();
            let event = event_fn(oneshot_tx);
            error!("[{}] {:?} rpc failed to read response", self.pid, event);
        })
    }

    pub async fn stream_chunks<S: AsRef<str> + ?Sized>(
        &self,
        client_id: &S,
    ) -> Result<EventStream<Chunk>, AddClientError> {
        self.rpc(|tx| ActorEvent::ClientAdded(client_id.as_ref().to_string(), tx))
            .await
            .map_err(|_| AddClientError::MainActorFinished)
    }

    pub async fn wait_for_status(&self) -> Result<CompleteExitStatus, StatusError> {
        self.rpc(ActorEvent::NotifyWhenProcessFinishes)
            .await
            .map_err(|_| StatusError::MainActorFinished)
    }

    pub async fn output(&self) -> Result<(CompleteExitStatus, Chunk), OutputError> {
        let status = self
            .wait_for_status()
            .await
            .map_err(|_| OutputError::MainActorFinished)?;

        let chunk = self
            .rpc(ActorEvent::GetCurrentChunk)
            .await
            .map_err(|_| OutputError::MainActorFinished)?;

        Ok((status, chunk))
    }

    // Async task that reads StdStream and sends it in chunks
    // to the provided event sender of main_actor.
    async fn std_forwarder<T: AsyncRead + std::marker::Unpin>(
        pid: Pid,
        tx: mpsc::Sender<ActorEvent>,
        mut reader: T,
        std_stream: StdStream,
    ) {
        debug!("[{}] std_forwarder({:?}) started", pid, std_stream);
        loop {
            let mut buffer = Vec::with_capacity(CHUNK_BUF_CAPACITY);
            match reader.read_buf(&mut buffer).await {
                Ok(0) => {
                    debug!("[{}] std_forwarder({:?}) is done reading", pid, std_stream);
                    break;
                }
                Ok(_) => {
                    // send the line to the actor
                    trace!(
                        "[{}] std_forwarder({:?}) read_buf result {:?}",
                        pid,
                        std_stream,
                        buffer
                    );
                    let message = ActorEvent::ChunkAdded(Chunk::new(&std_stream, buffer));
                    let send_result = tx.send(message).await;
                    if let Err(err) = send_result {
                        // rx closed or dropped, so the main actor died
                        info!(
                            "[{}] std_forwarder({:?}) is terminating - Cannot send the line: {}",
                            pid, std_stream, err
                        );
                        break;
                    }
                }
                Err(err) => {
                    error!(
                        "[{}] std_forwarder({:?}) is terminating - {}",
                        pid, std_stream, err
                    );
                    break;
                }
            }
        }
    }

    // Run stdout,stderr forwarders, then monitor for exit status.
    // Leave only after forwarders are finished and exit status is obtained.
    async fn child_actor(
        pid: Pid,
        mut child: Child,
        child_actor_rx: oneshot::Receiver<()>,
        main_tx: mpsc::Sender<ActorEvent>,
        stdout: ChildStdout,
        stderr: ChildStderr,
    ) -> CompleteExitStatus {
        let out_handle = {
            let main_tx = main_tx.clone();
            spawn_named(&format!("[{}] stdout_forwarder", pid), async move {
                ChildInfo::std_forwarder(pid, main_tx, stdout, StdStream::StdOut).await
            })
        };
        let err_handle = {
            spawn_named(&format!("[{}] stderr_forwarder", pid), async move {
                ChildInfo::std_forwarder(pid, main_tx, stderr, StdStream::StdErr).await
            })
        };
        let status = tokio::select! {
                new_status = child.wait() => {
                    new_status
                }
                _ = child_actor_rx => {
                    let _ = child.start_kill(); // ignore error when process is already killed
                    child.wait().await
                }
        }
        .into();
        // Make sure forwarder tasks finish. Otherwise ChunkAdded events could be emitted after ProcessFinished.
        if let Err(err) = out_handle.await {
            debug!("[{}] out_handle exitted with panic {:?}", pid, err);
        }
        if let Err(err) = err_handle.await {
            debug!("[{}] err_handle exitted with panic {:?}", pid, err);
        }
        debug!("[{}] child_actor is returning with {:?}", pid, status);
        status
    }

    const EVENT_STORAGE_CAPACITY: usize = 32;

    async fn main_actor(
        pid: Pid,
        mut rx: mpsc::Receiver<ActorEvent>,
        child_actor_tx: oneshot::Sender<()>,
        mut child_handle: JoinHandle<CompleteExitStatus>,
    ) {
        fn send_back<T>(tx: oneshot::Sender<T>, reply: T, pid: Pid, event: &str) {
            let send_result = tx.send(reply);
            if send_result.is_err() {
                debug!("[{}] main_actor cannot respond to {}", pid, event);
            }
        }
        debug!("[{}] actor started", pid);
        let mut child_actor_tx = Some(child_actor_tx);
        let mut event_storage = EventStorage::new(Self::EVENT_STORAGE_CAPACITY);
        let mut status = ExitStatusOrListeners::Listeners(vec![]);

        loop {
            let event = tokio::select! {
                Some(event) = rx.recv() => {
                    event
                },
                status = &mut child_handle, if status.as_running_state() == RunningState::Running => {
                    match status {
                        Ok(status) => ActorEvent::ProcessFinished(status),
                        Err(err) => {
                            error!("[{}] child_actor exitted with panic {:?}", pid, err);
                            ActorEvent::ProcessFinished(Err(err).into())
                        }
                    }
                }
                else => {
                    debug!("[{}] main_actor is terminating", pid);
                    break;
                }
            };
            trace!("[{}] main_actor event={:?}, status={}", pid, event, status);
            // FIXME: cleanup resources when panicing in main_actor
            match event {
                ActorEvent::ProcessFinished(new_status) => {
                    info!(
                        "[{}] main_actor finished waiting for child process: {:?}",
                        pid, new_status
                    );
                    if let ExitStatusOrListeners::Listeners(exit_listeners) = &mut status {
                        exit_listeners.drain(..).for_each(|listener| {
                            send_back(
                                listener,
                                new_status.clone(),
                                pid,
                                "NotifyWhenProcessFinishes",
                            );
                        });
                        // disconnect clients
                        event_storage.no_more_events();
                    } else {
                        error!(
                            "[{}] main_actor got ProcessFinished with status {}",
                            pid, status
                        );
                    }
                    // update status
                    status = ExitStatusOrListeners::ExitStatus(new_status);
                }
                ActorEvent::ChunkAdded(chunk) => {
                    // notify all clients, removing disconnected
                    let result = event_storage.add_event(chunk);
                    if let Err(err) = result {
                        error!("[{}] main_actor got error on ChunkAdded: {}", pid, err);
                    }
                }
                ActorEvent::ClientAdded(client_id, response_tx) => {
                    let event_stream =
                        event_storage.get_event_stream(format!("[{}] {}", pid, client_id));
                    send_back(response_tx, event_stream, pid, "ClientAdded");
                }
                ActorEvent::GetCurrentChunk(response_tx) => {
                    // TODO: Deprecate?
                    let event_stream =
                        event_storage.get_event_stream(format!("[{}] GetCurrentChunks", pid));
                    let chunk = event_stream.into_accumulated();
                    send_back(response_tx, chunk, pid, "GetCurrentChunks");
                }
                ActorEvent::StatusRequest(status_tx) => {
                    send_back(status_tx, status.as_running_state(), pid, "StatusRequest");
                }
                ActorEvent::KillRequest(listener) => {
                    if let Some(child_actor_tx) = child_actor_tx.take() {
                        send_back(child_actor_tx, (), pid, "KillRequest");
                    }
                    // either send back the reply now or add to exit_listeners
                    match &mut status {
                        ExitStatusOrListeners::ExitStatus(ref status) => {
                            send_back(listener, status.clone(), pid, "KillRequest");
                        }
                        ExitStatusOrListeners::Listeners(exit_listeners) => {
                            exit_listeners.push(listener);
                        }
                    }
                }
                ActorEvent::NotifyWhenProcessFinishes(listener) => {
                    // either send back the reply now or add to exit_listeners
                    match &mut status {
                        ExitStatusOrListeners::ExitStatus(ref status) => {
                            send_back(listener, status.clone(), pid, "NotifyWhenProcessFinishes");
                        }
                        ExitStatusOrListeners::Listeners(exit_listeners) => {
                            exit_listeners.push(listener);
                        }
                    }
                }
            }
        }
    }

    pub fn new<STR, STR2, ITER>(
        pid: Pid,
        process_path: STR,
        process_args: ITER,
    ) -> Result<Self, ChildInfoCreationError>
    where
        ITER: IntoIterator<Item = STR2>,
        STR: AsRef<OsStr>,
        STR2: AsRef<OsStr>,
    {
        let mut command = Command::new(&process_path);
        command.args(process_args);
        Self::new_internal(pid, command, &process_path, None)
    }

    pub async fn new_with_cgroup<STR, STR2, ITER>(
        pid: Pid,
        process_path: STR,
        process_args: ITER,
        cgroup_config: &CGroupConfig,
        limits: CGroupLimits,
    ) -> Result<Self, ChildInfoCreationWithCGroupError>
    where
        ITER: ExactSizeIterator<Item = STR2>,
        STR: AsRef<OsStr>,
        STR2: AsRef<OsStr>,
    {
        // construct command based on path and args
        let (command, child_cgroup) = CGroupCommandFactory::create_command(
            cgroup_config,
            pid,
            &process_path,
            process_args,
            limits,
        )
        .await
        .map_err(|err| ChildInfoCreationWithCGroupError::ProcessExecutionError(pid, err))?;
        Self::new_internal(pid, command, &process_path.as_ref(), Some(child_cgroup))
            .map_err(|err| ChildInfoCreationWithCGroupError::ChildInfoCreationError(pid, err))
    }

    fn new_internal<STR>(
        pid: Pid,
        mut command: Command,
        process_path: STR,
        child_cgroup: Option<ChildCGroup>,
    ) -> Result<Self, ChildInfoCreationError>
    where
        STR: AsRef<OsStr>,
    {
        info!("[{}] Starting new process: {:?}", pid, command);
        // consider adding ability to control env.vars
        command
            .current_dir(".") // consider making this configurable
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|err| {
            info!("[{}] Cannot run process - {:?}", pid, process_path.as_ref());
            ChildInfoCreationError::CannotRunProcess(pid, err)
        })?;

        let stdout = child
            .stdout
            .take()
            .ok_or(ChildInfoCreationError::CannotCaptureStream(
                pid,
                StdStream::StdOut,
            ))?;

        let stderr = child
            .stderr
            .take()
            .ok_or(ChildInfoCreationError::CannotCaptureStream(
                pid,
                StdStream::StdErr,
            ))?;

        let (main_tx, main_rx) = mpsc::channel(1);
        let (child_tx, child_rx) = oneshot::channel();

        let child_handle = {
            let main_tx = main_tx.clone();
            spawn_named(&format!("[{}] child_actor", pid), async move {
                ChildInfo::child_actor(pid, child, child_rx, main_tx, stdout, stderr).await
            })
        };

        spawn_named(&format!("[{}] main_actor", pid), async move {
            ChildInfo::main_actor(pid, main_rx, child_tx, child_handle).await
        });

        Ok(ChildInfo {
            pid,
            main_tx,
            child_cgroup,
        })
    }

    pub async fn status(&self) -> Result<RunningState, StatusError> {
        self.rpc(ActorEvent::StatusRequest)
            .await
            .map_err(|_| StatusError::MainActorFinished)
    }

    pub async fn kill(&self) -> Result<CompleteExitStatus, StopError> {
        self.rpc(ActorEvent::KillRequest)
            .await
            .map_err(|_| StopError::MainActorFinished)
    }

    pub async fn clean_up(&self) -> std::io::Result<()> {
        if let Some(child_cgroup) = &self.child_cgroup {
            child_cgroup.clean_up().await
        } else {
            Ok(())
        }
    }

    pub fn as_auto_clean(&self) -> Option<AutoCleanChildCGroup> {
        self.child_cgroup
            .as_ref()
            .map(|child_cgroup| child_cgroup.as_auto_clean())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};
    use tokio_stream::StreamExt;

    impl CompleteExitStatus {
        fn is_success(&self) -> bool {
            matches!(
                self,
                CompleteExitStatus::Complete(FinishedState::WithExitCode(0))
            )
        }
    }

    const EXPECTED_PROC_SELF_CGROUP_PREFIX: &str = "0::/";

    fn parse_proc_self_cgroup(stdout: &str) -> &str {
        debug!("/proc/self/cgroup: {}", stdout);
        assert!(
            stdout.starts_with(EXPECTED_PROC_SELF_CGROUP_PREFIX),
            "Unexpected prefix: `{}`",
            stdout
        );
        assert!(stdout.ends_with('\n'), "Unexpected suffix: `{}`", stdout);
        &stdout[EXPECTED_PROC_SELF_CGROUP_PREFIX.len()..stdout.len() - 1]
    }

    async fn read_std(
        child_info: &ChildInfo,
        std_stream: StdStream,
    ) -> Result<String, anyhow::Error> {
        let (exit_status, chunk) = child_info.output().await?;
        assert!(
            exit_status.is_success(),
            "Running child process was not successful - {:?}",
            exit_status
        );
        let out_bytes: Vec<u8> = chunk.std_stream(std_stream).to_vec();
        Ok(String::from_utf8(out_bytes)?)
    }

    use std::sync::Once;
    use tokio::sync::mpsc::UnboundedReceiver;

    static INIT: Once = Once::new();
    static PID_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    fn before_all() {
        INIT.call_once(|| {
            env_logger::init();
        });
    }

    #[test]
    fn test_parse_proc_self_cgroup() {
        assert_eq!(parse_proc_self_cgroup("0::/bar/baz\n"), "bar/baz");
    }

    #[tokio::test]
    async fn test_streaming() -> Result<(), anyhow::Error> {
        before_all();
        let pid = PID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let slow = std::env::current_exe()?
            .parent()
            .expect("removing test filename failed")
            .parent()
            .expect("removing test subfolder failed")
            .join("examples")
            .join("slow");
        assert!(slow.exists(), "{:?} does not exist", slow);

        let child_info = ChildInfo::new(pid, slow, ["2"].iter())?;
        let start = Instant::now();
        debug!("Started at {:?}", start);
        let (tx, mut rx) = mpsc::unbounded_channel();

        let mut stream =
            child_info
                .stream_chunks("client")
                .await?
                .into_stream(move |chunk_result| {
                    let elapsed = start.elapsed();
                    debug!("Got {:?} after {:?}", chunk_result, elapsed);
                    tx.send((elapsed, chunk_result.unwrap())).map_err(|_| ())
                });
        let join_handle = tokio::spawn(async move {
            while let Some(v) = stream.next().await {
                debug!("GOT = {:?}", v);
            }
        });

        join_handle.await?;
        let complete_exit_status = child_info.wait_for_status().await?;
        assert!(complete_exit_status.is_success());
        let expected_first_stdout = "0\nclosing stdout\n".as_bytes();
        assert_eq!(
            read_std(&child_info, StdStream::StdOut).await?.as_bytes(),
            expected_first_stdout,
        );
        let expected_second_stderr = "1\n".as_bytes();
        assert_eq!(
            read_std(&child_info, StdStream::StdErr).await?.as_bytes(),
            expected_second_stderr,
        );

        async fn check_chunks(
            rx: &mut UnboundedReceiver<(Duration, Chunk)>,
            std_stream: StdStream,
            mut expected: &[u8],
        ) -> Duration {
            loop {
                let (elapsed1, chunk) = rx.recv().await.expect("message not sent");
                debug!("Got message {:?} after {:?}", chunk, elapsed1);
                let len = chunk.std_stream(std_stream).len();
                let expected_chunk = Chunk::new(&std_stream, expected[0..len].into());
                assert_eq!(chunk, expected_chunk);
                expected = &expected[len..];
                if expected.is_empty() {
                    return elapsed1;
                }
            }
        }

        // check timing
        let elapsed1 = check_chunks(&mut rx, StdStream::StdOut, expected_first_stdout).await;
        let elapsed2 = check_chunks(&mut rx, StdStream::StdErr, expected_second_stderr).await;

        let dur = elapsed2 - elapsed1;
        assert!(
            dur.as_millis() >= 900,
            "Duration between chunk2 {:?} and chunk1 {:?} should be around 1s, was {:?}ms",
            elapsed2,
            elapsed1,
            dur.as_millis()
        );
        Ok(())
    }

    #[cfg(test_systemd_run)]
    mod systemd_run {
        use super::*;
        use crate::cgroup::server_config::{CGroupConfig, CGroupConfigBuilder};
        use anyhow::{anyhow, bail, Context};
        use envconfig::Envconfig;
        use std::collections::HashSet;
        use std::path::PathBuf;

        #[derive(Debug, Envconfig)]
        struct EnvVarConfiguration {
            #[envconfig(from = "CGROUP_MOUNT_POINT", default = "/sys/fs/cgroup")]
            cgroup_mount_point: String,
            #[envconfig(from = "PARENT_CGROUP")]
            parent_cgroup: Option<String>,
            #[envconfig(from = "CGROUP_BLOCK_DEVICE_ID", default = "8:0")]
            block_device_id: String,
            #[envconfig(from = "SLICE_NAME", default = "jobexecutor_testing.slice")]
            slice_name: String,
        }

        impl EnvVarConfiguration {
            fn new() -> Result<EnvVarConfiguration, anyhow::Error> {
                let conf = Self::init_from_env();
                debug!("From env.vars: {:?}", conf);
                Ok(conf?)
            }
        }

        #[derive(Debug)]
        struct DetectedCgroupConfiguration {
            parent_cgroup: String,
            block_device_id: String,
        }

        impl DetectedCgroupConfiguration {
            async fn new(
                conf: &EnvVarConfiguration,
            ) -> Result<DetectedCgroupConfiguration, anyhow::Error> {
                let parent_cgroup = match &conf.parent_cgroup {
                    Some(parent_cgroup) => parent_cgroup.clone(),
                    None => {
                        DetectedCgroupConfiguration::find_parent_cgroup_using_systemd_slice(conf)
                            .await?
                    }
                };
                if !parent_cgroup.starts_with(&conf.cgroup_mount_point) {
                    bail!(
                        "parent_cgroup {} does not start with {}",
                        parent_cgroup,
                        conf.cgroup_mount_point
                    );
                }

                let conf = DetectedCgroupConfiguration {
                    parent_cgroup,
                    block_device_id: conf.block_device_id.clone(),
                };
                debug!("Detected: {:?}", conf);
                Ok(conf)
            }

            async fn find_parent_cgroup_using_systemd_slice(
                conf: &EnvVarConfiguration,
            ) -> Result<String, anyhow::Error> {
                let output = Command::new("systemd-run")
                    .args(&[
                        "--user",
                        "-p",
                        "Delegate=yes",
                        &format!("--slice={}", conf.slice_name),
                        "-P",
                        "--",
                        "cat",
                        "/proc/self/cgroup",
                    ])
                    .output()
                    .await?;

                if !output.status.success() {
                    bail!("Running systemd-run failed");
                }
                let stdout = String::from_utf8(output.stdout)?;
                let parent_cgroup =
                    get_parent_cgroup_from_proc_self_cgroup(&stdout, &conf.cgroup_mount_point)?;
                Ok(parent_cgroup.to_string_lossy().to_string())
            }
        }

        impl From<DetectedCgroupConfiguration> for CGroupConfigBuilder {
            fn from(conf: DetectedCgroupConfiguration) -> Self {
                CGroupConfigBuilder {
                    parent_cgroup: conf.parent_cgroup.into(),
                    move_current_pid_to_subfolder: false,
                    cgroup_block_device_id: conf.block_device_id,
                }
            }
        }

        fn get_parent_cgroup_from_proc_self_cgroup(
            stdout: &str,
            cgroup_mount_point: &str,
        ) -> Result<PathBuf, anyhow::Error> {
            let parsed_subpath = PathBuf::from(parse_proc_self_cgroup(stdout));
            let parsed_subpath = parsed_subpath
                .parent()
                .ok_or_else(|| anyhow!("Cannot get parent {}", parsed_subpath.display()))?;
            let abs_path = PathBuf::from(cgroup_mount_point).join(parsed_subpath);
            abs_path
                .canonicalize()
                .with_context(|| format!("Cannot canonicaize {:?}", abs_path))
        }

        // TODO: extract to a health check
        #[tokio::test]
        async fn test_cgroup() -> Result<(), anyhow::Error> {
            before_all();
            let pid = PID_COUNTER.fetch_add(1, Ordering::SeqCst);
            let env_conf = EnvVarConfiguration::new()?;
            let conf = DetectedCgroupConfiguration::new(&env_conf).await?;
            let cgroup_config_builder: CGroupConfigBuilder = conf.into();

            let expected_child_cgroup_path = cgroup_config_builder
                .parent_cgroup
                .as_path()
                .join(format!("{}", pid));
            // try to rmdir in case last execution failed to clean it up
            let _ = tokio::fs::remove_dir(&expected_child_cgroup_path).await;

            let cgroup_config = CGroupConfig::new(cgroup_config_builder).await?;

            // cat /proc/self/cgroup
            {
                let child_info = ChildInfo::new_with_cgroup(
                    pid,
                    "cat",
                    ["/proc/self/cgroup"].iter(),
                    &cgroup_config,
                    Default::default(),
                )
                .await?;
                let _cleanup_child_folder = child_info.as_auto_clean();
                let stdout = read_std(&child_info, StdStream::StdOut).await?;
                let parent_cgroup =
                    get_parent_cgroup_from_proc_self_cgroup(&stdout, &env_conf.cgroup_mount_point)?;
                let child_path = parent_cgroup.join(format!("{}", pid));

                assert_eq!(child_path, expected_child_cgroup_path);
            }
            // cat $expected_child_cgroup_path/cgroup.controllers
            // check that cgroup.controllers contains all of [cpu, memory, io]
            {
                let cgroup_controllers = expected_child_cgroup_path.join("cgroup.controllers");
                let child_info = ChildInfo::new_with_cgroup(
                    pid,
                    "cat",
                    [cgroup_controllers].iter(),
                    &cgroup_config,
                    Default::default(),
                )
                .await?;
                let _cleanup_child_folder = child_info.as_auto_clean();
                let stdout = read_std(&child_info, StdStream::StdOut).await?;
                let caps: HashSet<&str> = stdout.split(' ').filter(|x| !x.is_empty()).collect();
                debug!("Available controllers: `{}`", stdout);
                assert!(
                    caps.contains("cpu") && caps.contains("io") && caps.contains("memory"),
                    "Some required controllers not found: {:?}",
                    caps
                );
            }
            Ok(())
        }
    }
}
