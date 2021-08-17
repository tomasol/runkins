use log::*;
use std::collections::HashMap;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use tokio::sync::Mutex;

use anyhow::Context;
use tonic::{transport::Server, Response};

use job_executor::job_executor_server::*;
use job_executor::*;

pub mod job_executor {
    tonic::include_proto!("jobexecutor");
}

#[derive(Debug, Default)]
pub struct MyJobExecutor {
    child_storage: Mutex<HashMap<u32, Child>>,
}

// TODO: consider using tokio mutex

#[tonic::async_trait]
impl JobExecutor for MyJobExecutor {
    async fn start(
        &self,
        request: tonic::Request<job_executor::StartRequest>,
    ) -> Result<tonic::Response<job_executor::StartResponse>, tonic::Status> {
        debug!("Got a request: {:?}", request);
        let start_req = request.into_inner();

        let child = Command::new(start_req.path)
            .args(start_req.args)
            .current_dir("/") // TODO: make configurable
            // .env(key, val) TODO
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Cannot run command")
            .map_err(|err| {
                tonic::Status::internal(format!("Cannot run command - {}", err))
                // TODO add tracing id
            })?;
        debug!("Child process {:?}", child);
        let pid = child.id();
        self.child_storage.lock().await.insert(pid, child); //FIXME unwrap to status

        Ok(Response::new(StartResponse {
            id: Some(ExecutionId { id: pid }),
        }))
    }

    async fn job_status(
        &self,
        request: tonic::Request<job_executor::StatusRequest>,
    ) -> Result<tonic::Response<job_executor::StatusResponse>, tonic::Status> {
        debug!("Got a request: {:?}", request);
        let pid = request
            .into_inner()
            .id
            .ok_or(tonic::Status::invalid_argument("No executionId provided"))?
            .id;

        // Try to get child process from child_storage
        let mut child_storage = self.child_storage.lock().await;
        let mut child = child_storage
            .remove(&pid)
            .ok_or(tonic::Status::not_found("Cannot find job"))?;

        let wait_status = child.try_wait().map_err(|err| {
            error!("Error while wait on child {}: {}", child.id(), err);
            // child will be dropped
            tonic::Status::internal("Error while wait on specified job")
        })?;
        let name = child.id().to_string(); // TODO actual name

        let mut stdout = child
            .stdout
            .take()
            .ok_or(tonic::Status::internal("Cannot read stdout"))?;
        let mut stdout_buffer = String::new();
        stdout.read_to_string(&mut stdout_buffer)?;
        child.stdout = Some(stdout);

        let mut stderr = child
            .stderr
            .take()
            .ok_or(tonic::Status::internal("Cannot read stderr"))?;
        let mut stderr_buffer = String::new();
        stderr.read_to_string(&mut stderr_buffer)?;
        child.stderr = Some(stderr);

        let reply = if let Some(exit_status) = wait_status {
            job_executor::StatusResponse {
                name, // FIXME
                state: exit_status.to_string(),
                stdout: stdout_buffer,
                stderr: stderr_buffer,
            }
        } else {
            // reinsert child to child_storage
            child_storage.insert(pid, child);
            job_executor::StatusResponse {
                name,
                state: "Running".to_string(),
                stdout: stdout_buffer,
                stderr: stderr_buffer,
            }
        };

        Ok(Response::new(reply))
    }

    async fn stop(
        &self,
        request: tonic::Request<job_executor::StopRequest>,
    ) -> Result<tonic::Response<job_executor::StopResponse>, tonic::Status> {
        debug!("Got a request: {:?}", request);
        let pid = request
            .into_inner()
            .id
            .ok_or(tonic::Status::invalid_argument("No executionId provided"))?
            .id;

        // Try to get child process from child_storage
        let mut child_storage = self.child_storage.lock().await;
        let mut child = child_storage
            .remove(&pid)
            .ok_or(tonic::Status::not_found("Cannot find job"))?;

        child.kill().map_err(|err| {
            error!("Error while wait on child {}: {}", child.id(), err);
            // child is dropped
            tonic::Status::internal("Error while killing the job")
        })?;
        Ok(Response::new(job_executor::StopResponse {}))
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
