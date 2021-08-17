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
            match self.child_storage.lock().await.try_insert(pid, child) {
                Ok(_) => break,
                Err(OccupiedError { entry: _, value }) => {
                    child = value;
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
            error!("Error while wait on child {}: {}", pid, err);
            // child will be dropped
            tonic::Status::internal("Error while wait on specified job")
        })?;
        let name = pid.to_string(); // TODO actual name

        let stdout = child
            .stdout
            .take()
            .ok_or(tonic::Status::internal("Cannot read stdout"))?;

        let mut buf_stdout = BufReader::new(stdout);
        let mut stdout_buffer = String::new();

        buf_stdout.read_line(&mut stdout_buffer).await.unwrap();

        let stderr = child
            .stderr
            .take()
            .ok_or(tonic::Status::internal("Cannot read stderr"))?;
        let mut buf_stderr = BufReader::new(stderr);
        let mut stderr_buffer = String::new();
        buf_stderr.read_line(&mut stderr_buffer).await.unwrap();

        child.stderr = Some(buf_stderr.into_inner());

        let reply = if let Some(exit_status) = wait_status {
            job_executor::StatusResponse {
                name,
                state: exit_status.to_string(),
                stdout: stdout_buffer,
                stderr: stderr_buffer,
            }
        } else {
            // put it back for future, might not be needed
            child.stdout = Some(buf_stdout.into_inner());
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

        child.kill().await.map_err(|err| {
            error!("Error while wait on child {}: {}", pid, err);
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
