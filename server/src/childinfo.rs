use log::*;
use std::process::ExitStatus;
use thiserror::Error;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::BufReader;
use tokio::process::Child;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

#[derive(Error, Debug)]
pub enum ChildInfoError {
    #[error("main_actor is no longer running - {0}")]
    MainActorFinished(String),
    #[error("cannot get the child process status")]
    UnknownProcessStatus,
    #[error("cannot stop the child process - {0}")]
    CannotStopProcess(#[from] std::io::Error),
    #[error("cannot capture {0} of the child process")]
    CannotCaptureStream(String),
}

// Type of the writable part of a mpsc channel for streaming output chunks to a client.
// consider adding Display trait for showing IP:port of the client
type ClientTx<RESP> =
    UnboundedSender<Result<RESP /* grpc generated OutputResponse */, tonic::Status>>;
pub type Pid = u64;

#[derive(Debug)]
enum ActorEvent<RESP> {
    ChunkAdded(Chunk),                            // sent by std_forwarder
    ClientAdded(ClientTx<RESP>),                  // send by API call
    ProcessFinished(std::io::Result<ExitStatus>), // internal to main_actor
    StatusRequest(Sender<RunningState>),          // send by API call
    KillRequest(Sender<std::io::Result<()>>),     // send by API call
    StreamFinished(StdStream),                    // Sent by std_forwarder tasks
}

#[derive(Debug)]
pub struct ChildInfo<RESP> {
    pid: Pid,
    actor_tx: UnboundedSender<ActorEvent<RESP>>,
}

#[derive(Debug, Clone)]
pub enum Chunk {
    StdOut(String),
    StdErr(String),
}

impl Chunk {
    fn new(std_stream: &StdStream, string: String) -> Chunk {
        match std_stream {
            StdStream::StdOut => Chunk::StdOut(string),
            StdStream::StdErr => Chunk::StdErr(string),
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
enum StdStream {
    StdOut,
    StdErr,
}

impl<RESP: std::fmt::Debug + Send + 'static> ChildInfo<RESP> {
    pub fn add_client(&self, client_tx: ClientTx<RESP>) -> Result<(), ChildInfoError> {
        self.actor_tx
            .send(ActorEvent::ClientAdded(client_tx))
            .map_err(|err| {
                error!("[{}] Cannot add_client: {}", self.pid, err);
                ChildInfoError::MainActorFinished("Cannot add client".to_string())
            })
    }

    // Async task that reads StdStream and sends it in chunks
    // to the provided event sender of main_actor.
    async fn std_forwarder<T: AsyncRead + std::marker::Unpin>(
        pid: Pid,
        tx: UnboundedSender<ActorEvent<RESP>>,
        mut buf_reader: BufReader<T>,
        std_stream: StdStream,
    ) {
        debug!("[{}] std_forwarder({:?}) started", pid, std_stream);
        loop {
            let mut line = String::new();
            // sending larger chunks than one line might increase performance
            match buf_reader.read_line(&mut line).await {
                Ok(0) => {
                    debug!("[{}] std_forwarder({:?}) is done reading", pid, std_stream);
                    break;
                }
                Ok(_) => {
                    // send the line to the actor
                    debug!(
                        "[{}] std_forwarder({:?}) got line - {}",
                        pid, std_stream, line
                    );
                    let message = ActorEvent::ChunkAdded(Chunk::new(&std_stream, line));
                    let send_result = tx.send(message);
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
                        "[{}] std_forwarder({:?}) is terminating - Error in read_line: {}",
                        pid, std_stream, err
                    );
                    // IO error or invalid UTF-8, consider recovery. For now just finish reading.
                    break;
                }
            }
        }
        if let Err(err) = tx.send(ActorEvent::StreamFinished(std_stream)) {
            warn!(
                "[{}] std_forwarder was unable to send StreamFinished - {}",
                pid, err
            );
        }
    }

    async fn main_actor<F: Fn(Chunk) -> RESP>(
        pid: Pid,
        mut rx: UnboundedReceiver<ActorEvent<RESP>>,
        mut child: Child,
        chunk_to_output: F,
    ) {
        debug!("[{}] actor started", pid);
        let mut chunks: Vec<Chunk> = vec![];
        let mut clients: Vec<ClientTx<RESP>> = vec![];
        let mut running_state = RunningState::Running; // used for StatusRequest reporting
        let mut stream_finished_event_count = 0; // chunks is complete only when both events are received

        loop {
            let event = tokio::select! {
                exit_status_result = child.wait(), if running_state == RunningState::Running => {
                    debug!("[{}] main_actor finished waiting for child process: {:?}", pid, exit_status_result);
                    Some(ActorEvent::ProcessFinished(exit_status_result))
                },
                Some(event) = rx.recv() => {
                    Some(event)
                },
                else => None
            };

            if let Some(event) = event {
                trace!(
                    "[{}] main_actor event={:?}, chunks={}, clients={}, running_state={:?}, stream_finished_event_count={}",
                    pid,
                    event,
                    chunks.len(),
                    clients.len(),
                    running_state,
                    stream_finished_event_count
                );
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
                            ChildInfo::send_line(pid, client_tx, chunk.clone(), &chunk_to_output)
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
            } else {
                debug!("[{}] main_actor is terminating", pid);
                break;
            }
        }
    }

    // send everything to this client. This might be a bottleneck if many clients start connecting.
    fn send_everything<F: Fn(Chunk) -> RESP>(
        pid: Pid,
        client_tx: &ClientTx<RESP>,
        chunks: &[Chunk],
        chunk_to_output: &F,
    ) -> Result<(), ()> {
        for chunk in chunks {
            ChildInfo::send_line(pid, client_tx, chunk.clone(), chunk_to_output)?;
        }
        Ok(())
    }

    fn send_line<F: Fn(Chunk) -> RESP>(
        pid: Pid,
        client_tx: &ClientTx<RESP>,
        chunk: Chunk,
        chunk_to_output: &F,
    ) -> Result<(), ()> {
        trace!(
            "[{}] send_line chunk:{:?} client:{:?}",
            pid,
            chunk,
            client_tx
        );
        let output_response = chunk_to_output(chunk);
        client_tx.send(Ok(output_response)).map_err(|err| {
            warn!(
                "[{}] Cannot send chunk to client {:?} - {}",
                pid, client_tx, err
            );
        })?;
        Ok(())
    }

    pub fn new<F: Fn(Chunk) -> RESP + Send + 'static>(
        mut child: Child,
        pid: Pid,
        chunk_to_output: F,
    ) -> Result<Self, ChildInfoError> {
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ChildInfoError::CannotCaptureStream("stdout".to_string()))?;

        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ChildInfoError::CannotCaptureStream("stderr".to_string()))?;

        let (tx, rx) = mpsc::unbounded_channel();
        // TODO benchmark against one giant select!
        tokio::spawn(async move {
            ChildInfo::main_actor(pid, rx, child, chunk_to_output).await;
        });
        {
            let tx = tx.clone();
            tokio::spawn(async move {
                ChildInfo::std_forwarder(pid, tx, BufReader::new(stdout), StdStream::StdOut).await;
            });
        }
        {
            let tx = tx.clone();
            tokio::spawn(async move {
                ChildInfo::std_forwarder(pid, tx, BufReader::new(stderr), StdStream::StdErr).await;
            });
        }
        Ok(ChildInfo { pid, actor_tx: tx })
    }

    pub async fn status(&self) -> Result<Option<ExitStatus>, ChildInfoError> {
        let (status_tx, status_rx) = oneshot::channel();
        self.actor_tx
            .send(ActorEvent::StatusRequest(status_tx))
            .map_err(|err| {
                error!(
                    "[{}] Cannot send StatusRequest to the main actor - {}",
                    self.pid, err
                );
                ChildInfoError::MainActorFinished("Error while getting the job status".to_string())
            })?;
        match status_rx.await {
            Ok(RunningState::Running) => Ok(None),
            Ok(RunningState::Finished(exit_status)) => Ok(Some(exit_status)),
            _ => Err(ChildInfoError::UnknownProcessStatus),
        }
    }

    pub async fn kill(&self) -> Result<(), ChildInfoError> {
        let (kill_tx, kill_rx) = oneshot::channel();
        self.actor_tx
            .send(ActorEvent::KillRequest(kill_tx))
            .map_err(|_| {
                error!("[{}] Unable to send kill message", self.pid);
                ChildInfoError::MainActorFinished("Unable to send kill message".to_string())
            })?;
        match kill_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(ChildInfoError::CannotStopProcess(err)),
            Err(_) => Err(ChildInfoError::MainActorFinished(
                "Error while stopping the job".to_string(),
            )),
        }
    }
}