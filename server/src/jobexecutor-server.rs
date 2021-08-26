mod childinfo;

use log::*;
use rand::prelude::*;
use std::collections::HashMap;

use std::process::Stdio;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;

use anyhow::Context;
use childinfo::*;
use job_executor::job_executor_server::*;
use job_executor::*;
use tonic::{transport::Server, Response};

pub mod job_executor {
    tonic::include_proto!("jobexecutor");
}

#[derive(Debug, Default)]
pub struct MyJobExecutor {
    child_storage: Mutex<HashMap<Pid, ChildInfo<OutputResponse, tonic::Status>>>,
}

impl MyJobExecutor {
    fn chunk_to_output(chunk: Chunk) -> OutputResponse {
        match chunk {
            Chunk::StdOut(str) => OutputResponse {
                std_out_chunk: Some(OutputChunk { chunk: str }),
                std_err_chunk: None,
            },
            Chunk::StdErr(str) => OutputResponse {
                std_out_chunk: None,
                std_err_chunk: Some(OutputChunk { chunk: str }),
            },
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
        store.insert(pid, ChildInfo::new(child, pid, Self::chunk_to_output)?);

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
    // TODO low: make this configurable
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

impl From<ChildInfoError> for tonic::Status {
    fn from(err: ChildInfoError) -> Self {
        tonic::Status::internal(err.to_string())
    }
}
