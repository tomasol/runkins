#![feature(map_try_insert)]

use log::*;
use rand::prelude::*;
use std::collections::hash_map::OccupiedError;
use std::collections::HashMap;
use std::process::Stdio;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
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
    child: Child,
}

impl ChildInfo {
    fn new(child: Child, pid: u32) -> Self {
        ChildInfo { child, pid }
    }

    fn status(&mut self) -> Result<String, tonic::Status> {
        // FIXME error type
        match self.child.try_wait() {
            Ok(Some(exit_status)) => Ok(exit_status.to_string()),
            Ok(None) => Ok("Running".to_string()),
            Err(err) => {
                error!("Child process {} got error on try_wait: {}", self.pid, err);
                Err(tonic::Status::internal("Error while wait on specified job"))
            }
        }
    }
}

impl Into<Child> for ChildInfo {
    fn into(self) -> Child {
        self.child
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

        let mut child = Command::new(start_req.path)
            .args(start_req.args)
            .current_dir("/") // TODO: make configurable
            // TODO add ability to control env.vars
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Cannot run command")
            .map_err(|err| {
                tonic::Status::internal(format!("Cannot run command - {}", err))
                // TODO add tracing id
            })?;
        let mut pid;
        loop {
            // generate new pid
            pid = thread_rng().gen();
            match self
                .child_storage
                .lock()
                .await
                .try_insert(pid, ChildInfo::new(child, pid))
            {
                Ok(_) => break,
                Err(OccupiedError { entry: _, value }) => {
                    child = value.into();
                }
            }
        }
        debug!("Assigned pid {} to child process", pid); // TODO Tracing ID
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
            .ok_or(tonic::Status::invalid_argument("No executionId provided"))?
            .id;

        // Try to get child process from child_storage
        let mut child_storage = self.child_storage.lock().await;
        let mut child_info = child_storage
            .remove(&pid)
            .ok_or(tonic::Status::not_found("Cannot find job"))?;

        Ok(Response::new(job_executor::StatusResponse {
            state: child_info.status()?,
        }))
    }

    async fn stop(
        &self,
        request: tonic::Request<job_executor::StopRequest>,
    ) -> Result<tonic::Response<job_executor::StopResponse>, tonic::Status> {
        debug!("Request: {:?}", request);
        let pid = request
            .into_inner()
            .id
            .ok_or(tonic::Status::invalid_argument("No executionId provided"))?
            .id;

        // Try to get child process from child_storage
        let mut child_storage = self.child_storage.lock().await;
        let mut child_info = child_storage
            .remove(&pid)
            .ok_or(tonic::Status::not_found("Cannot find job"))?;

        child_info.child.kill().await.map_err(|err| {
            error!("Error while wait on child {}: {}", pid, err);
            tonic::Status::internal("Error while killing the job")
        })?;
        Ok(Response::new(job_executor::StopResponse {}))
    }

    type GetOutputStream = UnboundedReceiverStream<Result<OutputResponse, tonic::Status>>;

    async fn get_output(
        &self,
        request: tonic::Request<job_executor::OutputRequest>,
    ) -> Result<tonic::Response<Self::GetOutputStream>, tonic::Status> {
        debug!("Request: {:?}", request);
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let msg: OutputResponse = OutputResponse {
                std_out_chunk: Some(OutputChunk {
                    chunk: "out".to_string(),
                }),
                std_err_chunk: None,
            };
            tx.send(Ok(msg)).unwrap();

            println!(" /// done sending");
        });

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
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
