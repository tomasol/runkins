use log::*;
use std::collections::HashMap;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use sysinfo::{ProcessExt, RefreshKind, Signal, SystemExt};

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
        // TODO: only allow managing child processes?
        debug!("Got a request: {:?}", request);
        let pid = request
            .into_inner()
            .id
            .ok_or(tonic::Status::invalid_argument("No executionId provided"))?
            .id; // https://github.com/GuillaumeGomez/sysinfo/issues/379

        // Try to get child process from child_storage
        let reply = if let Some(child) = self.child_storage.lock().unwrap().get(&pid) {
            //FIXME unwrap err to tonic::Status
            let mut child = child.lock().unwrap();
            match child.try_wait() {
                // FIXME make consistent
                Ok(Some(status)) => {
                    let mut stdout = child.stdout.take().unwrap(); // will consume it
                                                                   // TODO stderr
                    let mut stdout_buffer = String::new();
                    stdout.read_to_string(&mut stdout_buffer)?;

                    let _name = child.id().to_string();

                    job_executor::StatusResponse {
                        // TODO: remove from map?
                        name: stdout_buffer,
                        state: status.to_string(),
                    }
                }
                Ok(None) => job_executor::StatusResponse {
                    name: child.id().to_string(),
                    state: "Running".to_string(),
                },
                Err(e) => {
                    error!("Error while wait on child {}: {}", child.id(), e);
                    return Err(tonic::Status::internal("Error while wait on child"));
                }
            }
        } else {
            // FIXME make consistent
            let sys = sysinfo::System::new_with_specifics(RefreshKind::new().with_processes());
            let p = sys
                .process(pid as sysinfo::Pid)
                .ok_or(tonic::Status::not_found("Process not found"))?;
            job_executor::StatusResponse {
                name: p.name().to_string(),
                state: p.status().to_string(),
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
            .id as sysinfo::Pid; // https://github.com/GuillaumeGomez/sysinfo/issues/379

        let sys = sysinfo::System::new_with_specifics(RefreshKind::new().with_processes());
        let p = sys
            .process(pid)
            .ok_or(tonic::Status::not_found("Process not found"))?;

        let success = p.kill(Signal::Kill);

        let reply = job_executor::StopResponse { success };
        Ok(Response::new(reply))
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
