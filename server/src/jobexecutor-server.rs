use log::*;
use rand::prelude::*;
use std::collections::HashMap;
use std::process::ExitStatus;
use std::process::Stdio;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::BufReader;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;

use anyhow::Context;
use tonic::{transport::Server, Response};

use job_executor::job_executor_server::*;
use job_executor::*;

pub mod job_executor {
    tonic::include_proto!("jobexecutor");
}

type ClientTx = UnboundedSender<Result<OutputResponse, tonic::Status>>; // consider adding Display trait for showing IP:port of client
type Pid = u64;

#[derive(Debug, Default)]
pub struct MyJobExecutor {
    child_storage: Mutex<HashMap<Pid, ChildInfo>>,
}

#[derive(Debug)]
struct ChildInfo {
    pid: Pid,
    actor_tx: UnboundedSender<ActorEvent>,
}

#[derive(Debug)]
enum ActorEvent {
    ChunkAdded(Chunk),                            // sent by std_forwarder
    ClientAdded(ClientTx),                        // send by API call
    ProcessFinished(std::io::Result<ExitStatus>), // internal to main_actor
    StatusRequest(Sender<RunningState>),          // send by API call
    KillRequest(Sender<std::io::Result<()>>),     // send by API call
    StreamFinished(StdStream),                    // Sent by std_forwarder tasks
}

#[derive(Debug)]
enum Chunk {
    StdOut(String),
    StdErr(String),
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

impl Chunk {
    fn new(std_stream: &StdStream, string: String) -> Chunk {
        match std_stream {
            StdStream::StdOut => Chunk::StdOut(string),
            StdStream::StdErr => Chunk::StdErr(string),
        }
    }
}

impl ChildInfo {
    pub fn add_client(&self, client_tx: ClientTx) -> Result<(), tonic::Status> {
        self.actor_tx
            .send(ActorEvent::ClientAdded(client_tx))
            .map_err(|err| {
                error!("[{}] Cannot add_client: {}", self.pid, err);
                tonic::Status::internal("Cannot subscribe")
            })
    }

    async fn std_forwarder<T: AsyncRead + std::marker::Unpin>(
        pid: Pid,
        tx: UnboundedSender<ActorEvent>,
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
                    // IO error or invalid UTF, consider recovery. For now just finish reading.
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

    async fn main_actor(pid: Pid, mut rx: UnboundedReceiver<ActorEvent>, mut child: Child) {
        debug!("[{}] actor started", pid);
        let mut chunks: Vec<Chunk> = vec![];
        let mut clients: Vec<ClientTx> = vec![];
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
                            ChildInfo::send_line(pid, client_tx, &chunk)
                                .map_err(|_| {
                                    info!("[{}] main_actor removing client {:?}", pid, client_tx);
                                })
                                .is_ok()
                        });
                        chunks.push(chunk);
                        // memory improvement: detect when client closes the connection and drop the handle
                    }
                    ActorEvent::ClientAdded(client_tx) => {
                        let send_result = ChildInfo::send_everything(pid, &client_tx, &chunks);
                        if let Ok(()) = send_result {
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
    fn send_everything(pid: Pid, client_tx: &ClientTx, chunks: &[Chunk]) -> Result<(), ()> {
        for chunk in chunks {
            ChildInfo::send_line(pid, client_tx, chunk)?;
        }
        Ok(())
    }

    fn send_line(pid: Pid, client_tx: &ClientTx, chunk: &Chunk) -> Result<(), ()> {
        trace!(
            "[{}] send_line chunk:{:?} client:{:?}",
            pid,
            chunk,
            client_tx
        );
        let output_response = match chunk {
            Chunk::StdOut(str) => OutputResponse {
                std_out_chunk: Some(OutputChunk {
                    chunk: str.to_string(),
                }),
                std_err_chunk: None,
            },
            Chunk::StdErr(str) => OutputResponse {
                std_out_chunk: None,
                std_err_chunk: Some(OutputChunk {
                    chunk: str.to_string(),
                }),
            },
        };
        client_tx.send(Ok(output_response)).map_err(|err| {
            warn!(
                "[{}] Cannot send chunk to client {:?} - {}",
                pid, client_tx, err
            );
        })?;
        Ok(())
    }

    pub fn new(mut child: Child, pid: Pid) -> Result<Self, tonic::Status> {
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| tonic::Status::internal("Cannot take stdout"))?;

        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| tonic::Status::internal("Cannot take stderr"))?;

        let (tx, rx) = mpsc::unbounded_channel();
        // TODO benchmark against one giant select!
        tokio::spawn(async move {
            ChildInfo::main_actor(pid, rx, child).await;
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

    pub async fn status(&self) -> Result<Option<ExitStatus>, tonic::Status> {
        let (status_tx, status_rx) = oneshot::channel();
        self.actor_tx
            .send(ActorEvent::StatusRequest(status_tx))
            .map_err(|err| {
                error!(
                    "[{}] Cannot send StatusRequest to the main actor - {}",
                    self.pid, err
                );
                tonic::Status::internal("Error while getting the job status")
            })?;
        match status_rx.await {
            Ok(RunningState::Running) => Ok(None),
            Ok(RunningState::Finished(exit_status)) => Ok(Some(exit_status)),
            _ => Err(tonic::Status::internal(
                "Error while getting the job status",
            )),
        }
    }

    pub async fn kill(&self) -> Result<(), tonic::Status> {
        let (kill_tx, kill_rx) = oneshot::channel();
        self.actor_tx
            .send(ActorEvent::KillRequest(kill_tx))
            .map_err(|_| {
                error!("[{}] Unable to send kill message", self.pid);
                tonic::Status::internal("Unable to send kill message")
            })?;
        match kill_rx.await {
            Ok(_) => Ok(()),
            _ => Err(tonic::Status::internal("Error while stopping the job")),
        }
    }
}

#[tonic::async_trait]
impl JobExecutor for MyJobExecutor {
    async fn start(
        &self,
        request: tonic::Request<job_executor::StartRequest>,
    ) -> Result<tonic::Response<job_executor::StartResponse>, tonic::Status> {
        debug!("Request: {:?}", request);
        let start_req = request.into_inner();

        let child = Command::new(start_req.path)
            .args(start_req.args)
            .current_dir(".") // consider making this configurable
            // consider adding ability to control env.vars
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Cannot run command")
            .map_err(|err| {
                tonic::Status::internal(format!("Cannot run command - {}", err))
                // consider adding tracing id
            })?;

        // obtain lock
        let mut store = self.child_storage.lock().await;
        let pid: Pid = loop {
            let random = thread_rng().gen();
            if !store.contains_key(&random) {
                break random;
            }
        };
        store.insert(pid, ChildInfo::new(child, pid)?);

        debug!("Assigned pid {} to the child process", pid);
        Ok(Response::new(StartResponse {
            id: Some(ExecutionId { id: pid }),
        }))
    }

    async fn job_status(
        &self,
        request: tonic::Request<job_executor::StatusRequest>,
    ) -> Result<tonic::Response<job_executor::StatusResponse>, tonic::Status> {
        debug!("Request: {:?}", request);
        let pid = request
            .into_inner()
            .id
            .ok_or_else(|| tonic::Status::invalid_argument("No executionId provided"))?
            .id;

        // Try to get child process from child_storage
        let child_storage = self.child_storage.lock().await;
        let child_info = child_storage
            .get(&pid)
            .ok_or_else(|| tonic::Status::not_found("Cannot find job"))?;

        let mut exit_code = None;
        let status = match child_info.status().await? {
            None => status_response::RunningStatus::Running,
            Some(exit_status) => match exit_status.code() {
                Some(code) => {
                    exit_code = Some(code);
                    status_response::RunningStatus::ExitedWithCode
                }
                None => status_response::RunningStatus::ExitedWithSignal,
            },
        };
        Ok(Response::new(job_executor::StatusResponse {
            status: status as i32,
            exit_code,
        }))
    }

    async fn stop(
        &self,
        request: tonic::Request<job_executor::StopRequest>,
    ) -> Result<tonic::Response<job_executor::StopResponse>, tonic::Status> {
        debug!("Request: {:?}", request);
        let inner_request = request.into_inner();
        let pid = inner_request
            .id
            .ok_or_else(|| tonic::Status::invalid_argument("No executionId provided"))?
            .id;

        // Try to get child process from child_storage
        let child_storage = self.child_storage.lock().await;
        let child_info = child_storage
            .get(&pid)
            .ok_or_else(|| tonic::Status::not_found("Cannot find job"))?;

        child_info.kill().await?;
        Ok(Response::new(job_executor::StopResponse {}))
    }

    type GetOutputStream = UnboundedReceiverStream<Result<OutputResponse, tonic::Status>>;

    async fn get_output(
        &self,
        request: tonic::Request<job_executor::OutputRequest>,
    ) -> Result<tonic::Response<Self::GetOutputStream>, tonic::Status> {
        debug!("Request: {:?}", request);
        let pid = request
            .into_inner()
            .id
            .ok_or_else(|| tonic::Status::invalid_argument("No executionId provided"))?
            .id;

        // Try to get child process from child_storage
        let child_storage = self.child_storage.lock().await;
        let child_info = child_storage
            .get(&pid)
            .ok_or_else(|| tonic::Status::not_found("Cannot find job"))?;

        let (tx, rx) = mpsc::unbounded_channel();
        child_info.add_client(tx)?;

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn remove(
        &self,
        request: tonic::Request<job_executor::RemoveRequest>,
    ) -> Result<tonic::Response<job_executor::RemoveResponse>, tonic::Status> {
        debug!("Request: {:?}", request);
        let pid = request
            .into_inner()
            .id
            .ok_or_else(|| tonic::Status::invalid_argument("No executionId provided"))?
            .id;

        // Try to get child process from child_storage
        let mut child_storage = self.child_storage.lock().await;
        let child_info = child_storage
            .get(&pid)
            .ok_or_else(|| tonic::Status::not_found("Cannot find job"))?;

        // Only allow removing finished processes
        match child_info.status().await {
            Ok(Some(_)) => {
                let removed = child_storage.remove(&pid);
                assert!(
                    removed.is_some(),
                    "HashMap contais a job that cannot be removed" // this should never happen
                );
                Ok(())
            }
            Ok(None) => {
                // still running, fail
                Err(tonic::Status::failed_precondition("Job is still running"))
            }
            Err(err) => {
                error!("[{}] Cannot get job status, not removing - {}", pid, err);
                Err(tonic::Status::internal(
                    "Cannot get job status, not removing",
                ))
            }
        }?;
        Ok(Response::new(RemoveResponse {}))
    }
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    // TODO make this configurable
    let addr = "[::1]:50051".parse()?;
    let exec = MyJobExecutor::default();

    Server::builder()
        .add_service(JobExecutorServer::new(exec))
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let result = run_server().await;
    if let Err(err) = result {
        eprintln!("Server failed: {}", err);
        let mut source = err.source();
        while let Some(source_err) = source {
            eprintln!(" Caused by: {}", source_err);
            source = source_err.source();
        }
        eprintln!("Details: {:?}", err);
        std::process::exit(1);
    } else {
        Ok(())
    }
}
