use log::*;
use std::collections::HashMap;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;

use anyhow::Context;
use tonic::{transport::Server, Response};

use job_executor::job_executor_server::*;
use job_executor::*;

pub mod job_executor {
    tonic::include_proto!("jobexecutor");
}

#[derive(Debug, Default)]
pub struct MyJobExecutor {
    child_storage: Mutex<HashMap<u32, Mutex<Child>>>,
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
        self.child_storage
            .lock()
            .unwrap()
            .insert(pid, Mutex::new(child)); //FIXME unwrap to status

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
        let mut child_storage = self.child_storage.lock().unwrap();
        let mut child = child_storage
            .get(&pid)
            .ok_or(tonic::Status::not_found("Cannot find job"))?
            .lock()
            .map_err(|err| {
                error!("Mutex acquiring failed - {}", err);
                tonic::Status::internal("Mutex acquiring failed")
            })?;

        let wait_status = child.try_wait().map_err(|err| {
            error!("Error while wait on child {}: {}", child.id(), err);
            tonic::Status::internal("Error while wait on specified job")
        })?;

        let reply = if let Some(status) = wait_status {
            let mut stdout = child.stdout.take().unwrap(); // will consume it
                                                           // TODO stderr
            let mut stdout_buffer = String::new();
            stdout.read_to_string(&mut stdout_buffer)?;

            let _name = child.id().to_string(); // TODO actual name

            drop(child);
            // on success: remove from store - !!! Remove inner mutex, explore removing and reinserting the child
            child_storage.remove(&pid);

            job_executor::StatusResponse {
                name: stdout_buffer, // FIXME
                state: status.to_string(),
            }
        } else {
            job_executor::StatusResponse {
                name: child.id().to_string(),
                state: "Running".to_string(),
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
        let mut child_storage = self.child_storage.lock().unwrap();
        let mut child = child_storage
            .get(&pid)
            .ok_or(tonic::Status::not_found("Cannot find job"))?
            .lock()
            .map_err(|err| {
                error!("Mutex acquiring failed - {}", err);
                tonic::Status::internal("Mutex acquiring failed")
            })?;

        child.kill().map_err(|err| {
            error!("Error while wait on child {}: {}", child.id(), err);
            tonic::Status::internal("Error while killing the job")
        })?;
        drop(child);
        // on success: remove from store - !!! Remove inner mutex, explore removing and reinserting the child
        child_storage.remove(&pid);
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
