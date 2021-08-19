use log::*;
use rand::prelude::*;
use std::collections::HashMap;
use std::process::ExitStatus;
use std::process::Stdio;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::ChildStdout;
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

#[derive(Debug, Default)]
pub struct MyJobExecutor {
    child_storage: Mutex<HashMap<u32, ChildInfo>>,
}

#[derive(Debug)]
struct ChildInfo {
    pid: u32,
    child: Mutex<Child>,
    actor_tx: UnboundedSender<ActorEvent>,
}

type ClientTx = UnboundedSender<Result<OutputResponse, tonic::Status>>; // consider adding Display trait for showing IP:port of client

#[derive(Debug)]
enum ActorEvent {
    StdOutLine(String),
    ClientAdded(ClientTx),
    StdOutFinished,
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

    async fn stdout_reader(pid: u32, stdout: ChildStdout, tx: UnboundedSender<ActorEvent>) {
        debug!("[{}] stdout_reader started", pid);
        let mut buf_stdout = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            // sending larger chunks than one line might increase performance
            match buf_stdout.read_line(&mut line).await {
                Ok(0) => {
                    debug!("[{}] stdout_reader is done reading", pid);
                    break;
                }
                Ok(_) => {
                    // send the line to the actor
                    debug!("[{}] stdout_reader got line - {}", pid, line);
                    let send_result = tx.send(ActorEvent::StdOutLine(line));
                    if let Err(err) = send_result {
                        // rx closed or dropped, so actor died
                        info!("[{}] stdout_reader - Cannot send the line: {}", pid, err);
                        break;
                    }
                }
                Err(err) => {
                    error!("[{}] stdout_reader - Error in read_line: {}", pid, err);
                    // IO error or invalid UTF, consider recovery. For now just finish reading.
                    break;
                }
            }
        }
        debug!("[{}] stdout_reader finished", pid);
        if let Err(err) = tx.send(ActorEvent::StdOutFinished) {
            error!(
                "[{}] stdout_reader could not send StdOutFinished - {}",
                pid, err
            );
        }
    }

    async fn actor(pid: u32, mut rx: UnboundedReceiver<ActorEvent>) {
        debug!("[{}] actor started", pid);
        let mut lines: Vec<String> = vec![];
        let mut clients: Vec<ClientTx> = vec![];
        let mut exited = false;
        while let Some(event) = rx.recv().await {
            debug!(
                "[{}] actor event={:?}, lines={}, clients={}, exited={}",
                pid,
                event,
                lines.len(),
                clients.len(),
                exited
            );
            match event {
                ActorEvent::StdOutLine(line) => {
                    assert!(!exited, "Illegal state - StdOutLine after StdOutFinished");
                    lines.push(line.clone());
                    // notify all clients
                    clients.retain(|client_tx| {
                        let res = ChildInfo::send_line(client_tx, &line);
                        if let Err(()) = res {
                            info!("[{}] Removing client {:?}", pid, client_tx);
                            false
                        } else {
                            true
                        }
                    });
                    // memory improvement: detect when client closes the connection and drop the handle
                }
                ActorEvent::ClientAdded(client_tx) => {
                    let send_result = ChildInfo::send_everything(&client_tx, &lines);
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
                ActorEvent::StdOutFinished => {
                    exited = true;
                    // disconnect all clients
                    clients.clear();
                }
            }
        }

        debug!("[{}] actor finished", pid);
    }

    // send everything to this client. This might be a bottleneck if many clients start connecting.
    fn send_everything(client_tx: &ClientTx, lines: &[String]) -> Result<(), ()> {
        for line in lines {
            ChildInfo::send_line(client_tx, line)?;
        }
        Ok(())
    }

    fn send_line(client_tx: &ClientTx, line: &str) -> Result<(), ()> {
        client_tx
            .send(Ok(ChildInfo::construct_output_response(line.to_string())))
            .map_err(|_| ())?;
        Ok(())
    }

    fn construct_output_response(stdout: String) -> OutputResponse {
        OutputResponse {
            std_out_chunk: Some(OutputChunk { chunk: stdout }),
            std_err_chunk: None,
        }
    }

    pub fn new(mut child: Child, pid: u32) -> Result<Self, tonic::Status> {
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| tonic::Status::internal("Cannot take stdout"))?;
        let (tx, rx) = mpsc::unbounded_channel();
        let actor_tx = tx.clone();

        tokio::spawn(async move {
            ChildInfo::actor(pid, rx).await;
        });

        tokio::spawn(async move {
            ChildInfo::stdout_reader(pid, stdout, tx).await;
        });

        Ok(ChildInfo {
            child: Mutex::new(child),
            pid,
            actor_tx,
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
        let pid: u32 = loop {
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
        let state = child_info
            .status()
            .await?
            .map(|s| s.to_string())
            .unwrap_or_else(|| "Running".to_string());
        Ok(Response::new(job_executor::StatusResponse { state }))
    }

    async fn stop(
        &self,
        request: tonic::Request<job_executor::StopRequest>,
    ) -> Result<tonic::Response<job_executor::StopResponse>, tonic::Status> {
        debug!("Request: {:?}", request);
        let inner_request = request.into_inner();
        let remove = inner_request.remove;
        let pid = inner_request
            .id
            .ok_or_else(|| tonic::Status::invalid_argument("No executionId provided"))?
            .id;

        // Try to get child process from child_storage
        let mut child_storage = self.child_storage.lock().await;
        let child_info = child_storage
            .get(&pid)
            .ok_or_else(|| tonic::Status::not_found("Cannot find job"))?;

        child_info.kill().await?;
        if remove {
            child_storage.remove(&pid);
        }
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
