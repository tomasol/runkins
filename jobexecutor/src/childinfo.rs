use log::*;
use std::ffi::OsStr;
use std::process::ExitStatus;
use std::process::Stdio;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

use crate::cgroup::concepts::CGroupLimits;
use crate::cgroup::runtime::CGroupCommandError;
use crate::cgroup::runtime::CGroupCommandFactory;
use crate::cgroup::server_config::AutoCleanChildCGroup;
use crate::cgroup::server_config::CGroupConfig;
use crate::cgroup::server_config::ChildCGroup;

#[derive(Error, Debug)]
pub enum StopError {
    #[error("[{0}] cannot stop the child process")]
    CannotStopProcess(Pid, #[source] std::io::Error),
    #[error("main_actor is no longer running")] // TODO: panic instead?
    MainActorFinished,
    #[error("main_actor failed to send the response")] // TODO: panic instead?
    FailedToReceiveResponse(#[source] RecvError),
}

#[derive(Error, Debug)]
pub enum AddClientError {
    #[error("main_actor is no longer running")] // TODO: panic instead?
    MainActorFinished,
}

#[derive(Error, Debug)]
pub enum StatusError {
    #[error("[{0}] cannot get the child process status")]
    UnknownProcessStatus(Pid),
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

/// Type of the writable part of a mpsc channel for streaming output chunks to a client.
// TODO low: add Display trait for showing IP:port of the client
type ClientTx<OUTPUT> = UnboundedSender<OUTPUT>;
pub type Pid = u64;

#[derive(Debug)]
enum ActorEvent<OUTPUT> {
    ChunkAdded(Chunk),                                 // sent by std_forwarder
    ClientAdded(ClientTx<OUTPUT>),                     // sent by API call
    ProcessFinished(std::io::Result<ExitStatus>),      // internal to main_actor
    StatusRequest(oneshot::Sender<RunningState>),      // sent by API call
    KillRequest(oneshot::Sender<std::io::Result<()>>), // sent by API call
    StreamFinished(StdStream),                         // sent by std_forwarder tasks
    GetCurrentChunks(oneshot::Sender<Vec<Chunk>>),     // sent by API call
}

#[derive(Debug)]
pub struct ChildInfo<OUTPUT> {
    pid: Pid,
    actor_tx: mpsc::Sender<ActorEvent<OUTPUT>>,
    child_cgroup: Option<ChildCGroup>,
}

#[derive(Debug, Clone)]
pub enum Chunk {
    StdOut(Vec<u8>),
    StdErr(Vec<u8>),
}

impl Chunk {
    fn new(std_stream: &StdStream, content: Vec<u8>) -> Chunk {
        match std_stream {
            StdStream::StdOut => Chunk::StdOut(content),
            StdStream::StdErr => Chunk::StdErr(content),
        }
    }

    pub fn std_out(&self) -> &[u8] {
        if let Chunk::StdOut(content) = self {
            content
        } else {
            &[]
        }
    }

    pub fn std_err(&self) -> &[u8] {
        if let Chunk::StdErr(content) = self {
            content
        } else {
            &[]
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
enum RunningState {
    Running,
    WaitFailed,
    Finished(ExitStatus),
}

#[derive(Debug)]
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

/// ChildInfo represents the started process, its state, output and all associated data.
/// It starts several async tasks to keep track of the output and running status.
/// Note about generic types:
/// OUTPUT is a type that represents the message written to a channel
/// when streaming the output, see ClientTx type.
impl<OUTPUT> ChildInfo<OUTPUT>
where
    OUTPUT: std::fmt::Debug + Send + 'static,
{
    pub async fn add_client(&self, client_tx: ClientTx<OUTPUT>) -> Result<(), AddClientError> {
        self.actor_tx
            .send(ActorEvent::ClientAdded(client_tx))
            .await
            .map_err(|err| {
                error!("[{}] Cannot add_client: {}", self.pid, err);
                AddClientError::MainActorFinished
            })
    }

    pub async fn output(&self) -> Result<(ExitStatus, Vec<Chunk>), StatusError> {
        let status = loop {
            match self.status().await? {
                Some(status) => {
                    break status;
                }
                // FIXME: receive a notification instead of polling
                None => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        };
        let (response_tx, response_rx) = oneshot::channel();
        self.actor_tx
            .send(ActorEvent::GetCurrentChunks(response_tx))
            .await
            .map_err(|err| {
                error!(
                    "[{}] output() failed to send to actor_tx: {}",
                    self.pid, err
                );
                StatusError::MainActorFinished
            })?;
        let chunks = response_rx.await.map_err(|err| {
            error!(
                "[{}] output() failed to read response_rx: {}",
                self.pid, err
            );
            StatusError::MainActorFinished
        })?;
        Ok((status, chunks))
    }

    // Async task that reads StdStream and sends it in chunks
    // to the provided event sender of main_actor.
    async fn std_forwarder<T: AsyncRead + std::marker::Unpin>(
        pid: Pid,
        tx: mpsc::Sender<ActorEvent<OUTPUT>>,
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
        if tx
            .send(ActorEvent::StreamFinished(std_stream))
            .await
            .is_err()
        {
            warn!("[{}] std_forwarder was unable to send StreamFinished", pid);
        }
    }

    async fn main_actor<F: Fn(Chunk) -> OUTPUT>(
        pid: Pid,
        mut rx: Receiver<ActorEvent<OUTPUT>>,
        mut child: Child,
        chunk_to_output: F,
    ) {
        debug!("[{}] actor started", pid);
        let mut chunks: Vec<Chunk> = vec![];
        let mut clients: Vec<ClientTx<OUTPUT>> = vec![];
        let mut running_state = RunningState::Running; // used for StatusRequest reporting
        let mut stream_finished_event_count = 0; // chunks is complete only when both events are received

        loop {
            let event = tokio::select! {
                exit_status_result = child.wait(), if running_state == RunningState::Running => {
                    debug!("[{}] main_actor finished waiting for child process: {:?}", pid, exit_status_result);
                    ActorEvent::ProcessFinished(exit_status_result)
                },
                Some(event) = rx.recv() => {
                    event
                },
                else => {
                    debug!("[{}] main_actor is terminating", pid);
                    break;
                }
            };
            trace!(
                    "[{}] main_actor event={:?}, chunks={}, clients={}, running_state={:?}, stream_finished_event_count={}",
                    pid,
                    event,
                    chunks.len(),
                    clients.len(),
                    running_state,
                    stream_finished_event_count
                );
            // FIXME: cleanup resources when panicing in main_actor
            match event {
                ActorEvent::ProcessFinished(running_status) => {
                    // created by child.wait arm of select! above
                    running_state = match running_status {
                        Ok(exit_status) => RunningState::Finished(exit_status),
                        Err(err) => {
                            warn!(
                                "[{}] main_actor failed waiting for child process - {}",
                                pid, err
                            );
                            RunningState::WaitFailed
                        }
                    };
                }
                ActorEvent::ChunkAdded(chunk) => {
                    assert!(
                            stream_finished_event_count < 2,
                            "[{}] main_actor in illegal state - ChunkAdded after receiving both StreamFinished events",
                            pid
                        );
                    // notify all clients, removing disconnected
                    clients.retain(|client_tx| {
                        ChildInfo::send_chunk(pid, client_tx, chunk.clone(), &chunk_to_output)
                            .map_err(|_| {
                                info!("[{}] main_actor removing client {:?}", pid, client_tx);
                            })
                            .is_ok()
                    });
                    chunks.push(chunk);
                    // memory improvement: detect when client closes the connection and drop the handle
                }
                ActorEvent::ClientAdded(client_tx) => {
                    let send_result =
                        ChildInfo::send_everything(pid, &client_tx, &chunks, &chunk_to_output);
                    if send_result.is_ok() {
                        if stream_finished_event_count < 2 {
                            // add to clients
                            clients.push(client_tx);
                        } // otherwise drop client_tx which will disconnect the client
                    } else {
                        info!(
                                "[{}] main_actor not adding the client {:?}, error while replaying the output",
                                pid, client_tx
                            );
                        // TODO test that in this state the RPC disconnects the client
                    }
                }
                ActorEvent::GetCurrentChunks(response_tx) => {
                    let send_result = response_tx.send(chunks.clone());
                    if send_result.is_err() {
                        debug!("[{}] main_actor cannot reply to GetCurrentChunks", pid);
                    }
                }
                ActorEvent::StatusRequest(status_tx) => {
                    let send_result = status_tx.send(running_state.clone());
                    if send_result.is_err() {
                        debug!("[{}] main_actor cannot reply to StatusRequest", pid);
                    }
                }
                ActorEvent::KillRequest(kill_tx) => {
                    let send_result = kill_tx.send(child.kill().await);
                    if send_result.is_err() {
                        debug!("[{}] main_actor cannot reply to KillRequest", pid);
                    }
                }
                ActorEvent::StreamFinished(_) => {
                    stream_finished_event_count += 1;
                    assert!(
                        stream_finished_event_count <= 2,
                        "[{}] Illegal StreamFinished count",
                        pid
                    );
                    if stream_finished_event_count == 2 {
                        // disconnect all clients, there will be no new chunks
                        clients.clear();
                    }
                }
            }
        }
    }

    // send everything to this client. This might be a bottleneck if many clients start connecting.
    fn send_everything<F: Fn(Chunk) -> OUTPUT>(
        pid: Pid,
        client_tx: &ClientTx<OUTPUT>,
        chunks: &[Chunk],
        chunk_to_output: &F,
    ) -> Result<(), ()> {
        for chunk in chunks {
            ChildInfo::send_chunk(pid, client_tx, chunk.clone(), chunk_to_output)?;
        }
        Ok(())
    }

    fn send_chunk<F: Fn(Chunk) -> OUTPUT>(
        pid: Pid,
        client_tx: &ClientTx<OUTPUT>,
        chunk: Chunk,
        chunk_to_output: &F,
    ) -> Result<(), ()> {
        trace!(
            "[{}] send_chunk chunk:{:?} client:{:?}",
            pid,
            chunk,
            client_tx
        );
        let output_response = chunk_to_output(chunk);
        client_tx.send(output_response).map_err(|err| {
            warn!(
                "[{}] Cannot send chunk to client {:?} - {}",
                pid, client_tx, err
            );
        })?;
        Ok(())
    }

    pub fn new<STR, ITER>(
        pid: Pid,
        process_path: STR,
        process_args: ITER,
        chunk_to_output: fn(Chunk) -> OUTPUT,
    ) -> Result<Self, ChildInfoCreationError>
    where
        ITER: IntoIterator<Item = STR>,
        STR: AsRef<OsStr>,
    {
        let mut command = Command::new(&process_path);
        command.args(process_args);
        Self::new_internal(pid, command, chunk_to_output, &process_path, None)
    }

    pub async fn new_with_cgroup<STR, ITER>(
        pid: Pid,
        process_path: STR,
        process_args: ITER,
        chunk_to_output: fn(Chunk) -> OUTPUT,
        cgroup_config: &CGroupConfig,
        limits: CGroupLimits,
    ) -> Result<Self, ChildInfoCreationWithCGroupError>
    where
        ITER: ExactSizeIterator<Item = STR>,
        STR: AsRef<OsStr>,
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
        Self::new_internal(
            pid,
            command,
            chunk_to_output,
            &process_path.as_ref(),
            Some(child_cgroup),
        )
        .map_err(|err| ChildInfoCreationWithCGroupError::ChildInfoCreationError(pid, err))
    }

    fn new_internal<STR: AsRef<OsStr>>(
        pid: Pid,
        mut command: Command,
        chunk_to_output: fn(Chunk) -> OUTPUT,
        process_path: STR,
        child_cgroup: Option<ChildCGroup>,
    ) -> Result<Self, ChildInfoCreationError> {
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

        let (tx, rx) = mpsc::channel(1);
        // TODO benchmark against one giant select!

        spawn_named(&format!("[{}] main_actor", pid), async move {
            ChildInfo::main_actor(pid, rx, child, chunk_to_output).await;
        });
        {
            let tx = tx.clone();
            spawn_named(&format!("[{}] stdout_forwarder", pid), async move {
                ChildInfo::std_forwarder(pid, tx, stdout, StdStream::StdOut).await;
            });
        }
        {
            let tx = tx.clone();
            spawn_named(&format!("[{}] stderr_forwarder", pid), async move {
                ChildInfo::std_forwarder(pid, tx, stderr, StdStream::StdErr).await;
            });
        }
        Ok(ChildInfo {
            pid,
            actor_tx: tx,
            child_cgroup,
        })
    }

    // If process is still running, return Ok(None)
    // If the process finished, return Ok(Some(ExitStatus))
    pub async fn status(&self) -> Result<Option<ExitStatus>, StatusError> {
        let (status_tx, status_rx) = oneshot::channel();
        self.actor_tx
            .send(ActorEvent::StatusRequest(status_tx))
            .await
            .map_err(|err| {
                error!(
                    "[{}] Cannot send StatusRequest to the main actor - {}",
                    self.pid, err
                );
                StatusError::MainActorFinished
            })?;
        match status_rx.await {
            Ok(RunningState::Running) => Ok(None),
            Ok(RunningState::Finished(exit_status)) => Ok(Some(exit_status)),
            _ => Err(StatusError::UnknownProcessStatus(self.pid)),
        }
    }

    pub async fn kill(&self) -> Result<(), StopError> {
        let (kill_tx, kill_rx) = oneshot::channel();
        self.actor_tx
            .send(ActorEvent::KillRequest(kill_tx))
            .await
            .map_err(|_| {
                error!("[{}] StopError::MainActorFinished", self.pid);
                StopError::MainActorFinished
            })?;
        match kill_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(StopError::CannotStopProcess(self.pid, err)),
            Err(err) => {
                error!("[{}] StopError::FailedToReceiveResponse", self.pid);
                Err(StopError::FailedToReceiveResponse(err))
            }
        }
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
    use std::path::PathBuf;

    use super::*;
    use crate::cgroup::server_config::{CGroupConfig, CGroupConfigBuilder};
    use anyhow::{anyhow, bail, Context};
    use envconfig::Envconfig;

    #[derive(Debug, Envconfig)]
    struct EnvVarConfiguration {
        #[envconfig(from = "CGEXEC_RS")]
        cgexec_rs: Option<String>,
        #[envconfig(from = "CGROUP_MOUNT_POINT", default = "/sys/fs/cgroup")]
        cgroup_mount_point: String,
        #[envconfig(from = "PARENT_CGROUP")]
        parent_cgroup: Option<String>,
        #[envconfig(from = "CGROUP_BLOCK_DEVICE_ID", default = "8:0")]
        block_device_id: String,
        #[envconfig(from = "SLICE_NAME", default = "my.slice")]
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
        cgexec_rs: PathBuf,
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
                    DetectedCgroupConfiguration::find_parent_cgroup_using_systemd_slice(&conf)
                        .await?
                }
            };
            let cgexec_rs = DetectedCgroupConfiguration::find_cgexec_rs(conf.cgexec_rs.as_ref())?;
            if !parent_cgroup.starts_with(&conf.cgroup_mount_point) {
                bail!(
                    "parent_cgroup {} does not start with {}",
                    parent_cgroup,
                    conf.cgroup_mount_point
                );
            }

            let conf = DetectedCgroupConfiguration {
                cgexec_rs,
                parent_cgroup,
                block_device_id: conf.block_device_id.clone(),
            };
            debug!("Detected: {:?}", conf);
            Ok(conf)
        }

        fn find_cgexec_rs(cgexec_rs: Option<&String>) -> Result<PathBuf, anyhow::Error> {
            Ok(if let Some(path) = cgexec_rs {
                path.into()
            } else {
                trace!("Guessing cgexec-rs location based on current_exe");
                let exe = std::env::current_exe()?;
                exe.parent()
                    .ok_or(anyhow!("Cannot "))?
                    .join("..")
                    .join("cgexec-rs")
            })
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

    impl Into<CGroupConfigBuilder> for DetectedCgroupConfiguration {
        fn into(self) -> CGroupConfigBuilder {
            CGroupConfigBuilder {
                parent_cgroup: self.parent_cgroup.into(),
                cgexec_rs: self.cgexec_rs,
                move_current_pid_to_subfolder: false,
                cgroup_block_device_id: self.block_device_id,
            }
        }
    }

    const EXPECTED_PROC_SELF_CGROUP_PREFIX: &str = "0::/";
    fn parse_proc_self_cgroup<'a>(stdout: &'a str) -> &'a str {
        debug!("/proc/self/cgroup: {}", stdout);
        assert!(
            stdout.starts_with(EXPECTED_PROC_SELF_CGROUP_PREFIX),
            "Unexpected prefix: {}",
            stdout
        );
        assert!(stdout.ends_with("\n"), "Unexpected suffix: {}", stdout);
        &stdout[EXPECTED_PROC_SELF_CGROUP_PREFIX.len()..stdout.len() - 1]
    }

    fn get_parent_cgroup_from_proc_self_cgroup(
        stdout: &str,
        cgroup_mount_point: &str,
    ) -> Result<PathBuf, anyhow::Error> {
        let parsed_subpath = PathBuf::from(parse_proc_self_cgroup(&stdout));
        let parsed_subpath = parsed_subpath
            .parent()
            .ok_or(anyhow!("Cannot get parent {}", parsed_subpath.display()))?;
        let abs_path = PathBuf::from(cgroup_mount_point).join(parsed_subpath);
        abs_path
            .canonicalize()
            .with_context(|| format!("Cannot canonicaize {:?}", abs_path))
    }

    #[test]
    fn test_parse_proc_self_cgroup() {
        assert_eq!(parse_proc_self_cgroup("0::/bar/baz\n"), "bar/baz");
    }

    #[cfg(test_systemd_run)]
    #[tokio::test]
    async fn test_cgroup() -> Result<(), anyhow::Error> {
        env_logger::init();
        let pid = 1;
        let env_conf = EnvVarConfiguration::new()?;
        let conf = DetectedCgroupConfiguration::new(&env_conf).await?;
        let cgroup_config_builder: CGroupConfigBuilder = conf.into();

        let expected_path = cgroup_config_builder
            .parent_cgroup
            .as_path()
            .join(format!("{}", pid));
        let cgroup_config = CGroupConfig::new(cgroup_config_builder).await?;
        let limits = Default::default();
        // FIXME: refactor, remove chunk_to_output from API
        let chunk_to_output = |chunk: Chunk| -> String { format!("{:?}", chunk) };
        let child_info = ChildInfo::new_with_cgroup(
            pid,
            "cat".to_string(),
            vec!["/proc/self/cgroup".to_string()].into_iter(),
            chunk_to_output,
            &cgroup_config,
            limits,
        )
        .await?;
        let _clean = child_info.as_auto_clean();
        let (exit_status, chunks) = child_info.output().await?;
        debug!("Child process status: {}", exit_status);
        assert!(
            exit_status.success(),
            "Running child process was not successful"
        );
        let out_bytes: Vec<u8> = chunks
            .iter()
            .map(|ch| ch.std_out())
            .flatten()
            .cloned()
            .collect();
        let stdout = String::from_utf8(out_bytes)?;
        let parent_cgroup =
            get_parent_cgroup_from_proc_self_cgroup(&stdout, &env_conf.cgroup_mount_point)?;
        let child_path = parent_cgroup.join(format!("{}", pid));

        assert_eq!(child_path, expected_path);
        Ok(())
    }
}
