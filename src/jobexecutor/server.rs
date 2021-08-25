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
    child: Mutex<Child>,
    actor_tx: UnboundedSender<ActorEvent>,
}

#[derive(Debug)]
enum ActorEvent {
    ChunkAdded(Chunk),
    ClientAdded(ClientTx),
    ProcessFinished, // TODO send this event
}

#[derive(Debug)]
enum Chunk {
    StdOut(String),
    StdErr(String),
}

impl Chunk {
    fn new(is_stdout: bool, string: String) -> Chunk {
        if is_stdout {
            Chunk::StdOut(string)
        } else {
            Chunk::StdErr(string)
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
        is_stdout: bool,
    ) {
        debug!("[{}] std_forwarder({}) started", pid, is_stdout);
        loop {
            let mut line = String::new();
            // sending larger chunks than one line might increase performance
            match buf_reader.read_line(&mut line).await {
                Ok(0) => {
                    debug!("[{}] std_forwarder({}) is done reading", pid, is_stdout);
                    break;
                }
                Ok(_) => {
                    // send the line to the actor
                    debug!("[{}] std_forwarder({}) got line - {}", pid, is_stdout, line);
                    let message = ActorEvent::ChunkAdded(Chunk::new(is_stdout, line));
                    let send_result = tx.send(message);
                    if let Err(err) = send_result {
                        // rx closed or dropped, so the main actor died
                        info!(
                            "[{}] std_forwarder({}) - Cannot send the line: {}",
                            pid, is_stdout, err
                        );
                        break;
                    }
                }
                Err(err) => {
                    error!(
                        "[{}] std_forwarder({}) - Error in read_line: {}",
                        pid, is_stdout, err
                    );
                    // IO error or invalid UTF, consider recovery. For now just finish reading.
                    break;
                }
            }
        }
        debug!("[{}] std_forwarder({}) finished", pid, is_stdout);
    }

    async fn actor(pid: Pid, mut rx: UnboundedReceiver<ActorEvent>) {
        debug!("[{}] actor started", pid);
        let mut chunks: Vec<Chunk> = vec![];
        let mut clients: Vec<ClientTx> = vec![];
        let mut exited = false;
        while let Some(event) = rx.recv().await {
            debug!(
                "[{}] actor event={:?}, chunks={}, clients={}, exited={}",
                pid,
                event,
                chunks.len(),
                clients.len(),
                exited
            );
            match event {
                ActorEvent::ChunkAdded(chunk) => {
                    assert!(!exited, "Illegal state - StdOutLine after ProcessFinished");
                    // notify all clients
                    clients.retain(|client_tx| {
                        let res = ChildInfo::send_line(client_tx, &chunk);
                        if let Err(()) = res {
                            info!("[{}] Removing client {:?}", pid, client_tx);
                            false
                        } else {
                            true
                        }
                    });
                    chunks.push(chunk);
                    // memory improvement: detect when client closes the connection and drop the handle
                }
                ActorEvent::ClientAdded(client_tx) => {
                    let send_result = ChildInfo::send_everything(&client_tx, &chunks);
                    if let Ok(()) = send_result {
                        if !exited {
                            // add to clients
                            clients.push(client_tx);
                        } // otherwise drop client_tx which will disconnect the client
                    } else {
                        info!(
                            "[{}] Not adding the client {:?}, error while replaying the output",
                            pid, client_tx
                        );
                        // TODO test that in this state the RPC finished
                    }
                }
                ActorEvent::ProcessFinished => {
                    exited = true;
                    // disconnect all clients
                    clients.clear();
                }
            }
        }

        debug!("[{}] actor finished", pid);
    }

    // send everything to this client. This might be a bottleneck if many clients start connecting.
    fn send_everything(client_tx: &ClientTx, chunks: &[Chunk]) -> Result<(), ()> {
        for chunk in chunks {
            ChildInfo::send_line(client_tx, chunk)?;
        }
        Ok(())
    }

    fn send_line(client_tx: &ClientTx, chunk: &Chunk) -> Result<(), ()> {
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
        client_tx.send(Ok(output_response)).map_err(|_| ())?; // TODO do not hide the error
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

        tokio::spawn(async move {
            ChildInfo::actor(pid, rx).await;
        });
        {
            let tx = tx.clone();
            tokio::spawn(async move {
                ChildInfo::std_forwarder(pid, tx, BufReader::new(stdout), true).await;
            });
        }
        {
            let tx = tx.clone();
            tokio::spawn(async move {
                ChildInfo::std_forwarder(pid, tx, BufReader::new(stderr), false).await;
            });
        }

        Ok(ChildInfo {
            child: Mutex::new(child),
            pid,
            actor_tx: tx,
        })
    }

    pub async fn status(&self) -> Result<Option<ExitStatus>, tonic::Status> {
        self.child.lock().await.try_wait().map_err(|err| {
            error!("Child process {} got error on try_wait: {}", self.pid, err);
            tonic::Status::internal("Error while getting the job status")
        })
    }

    pub async fn kill(&self) -> Result<(), tonic::Status> {
        self.child.lock().await.kill().await.map_err(|err| {
            error!("[{}] Error while wait on child {}", self.pid, err);
            tonic::Status::internal("Error while killing the job")
        })
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr = "[::1]:50051".parse()?;
    let exec = MyJobExecutor::default();

    Server::builder()
        .add_service(JobExecutorServer::new(exec))
        .serve(addr)
        .await?;

    Ok(())
}
