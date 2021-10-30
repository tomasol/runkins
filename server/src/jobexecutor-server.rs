#![feature(type_alias_impl_trait)]

use job_executor::job_executor_server::*;
use job_executor::*;
use jobexecutor::cgroup::concepts::CGroupLimits;
use jobexecutor::cgroup::server_config::CGroupConfig;
use jobexecutor::cgroup::server_config::CGroupConfigBuilder;
use jobexecutor::cgroup::server_config::CGroupConfigError;
use jobexecutor::childinfo::ChildInfo;
use jobexecutor::childinfo::Chunk;
use jobexecutor::childinfo::FinishedState;
use jobexecutor::childinfo::Pid;
use jobexecutor::childinfo::RunningState;
use log::*;
use rand::prelude::*;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;
use tonic::{transport::Server, Response};

pub mod job_executor {
    tonic::include_proto!("jobexecutor");
}

#[derive(Debug)]
pub struct MyJobExecutor {
    child_storage: Mutex<HashMap<Pid, ChildInfo>>,
    cgroup_config: Option<CGroupConfig>,
}

trait EmptyIsNone<S> {
    fn into_option(self) -> Option<S>;
}

impl<T> EmptyIsNone<Vec<T>> for Vec<T> {
    fn into_option(self) -> Option<Vec<T>> {
        if self.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

impl MyJobExecutor {
    fn new(cgroup_config: Option<CGroupConfig>) -> MyJobExecutor {
        MyJobExecutor {
            cgroup_config,
            child_storage: Default::default(),
        }
    }

    fn chunk_to_output(
        item: Result<Chunk, BroadcastStreamRecvError>,
    ) -> Result<OutputResponse, tonic::Status> {
        match item {
            Ok(Chunk { std_out, std_err }) => Ok(OutputResponse {
                std_out_chunk: std_out.into_option().map(|it| OutputChunk { chunk: it }),
                std_err_chunk: std_err.into_option().map(|it| OutputChunk { chunk: it }),
            }),
            Err(_) => Err(tonic::Status::data_loss("Please retry")),
        }
    }

    fn construct_limits(request_cgroup: CGroup) -> CGroupLimits {
        let mut limits = CGroupLimits {
            cpu_limit: request_cgroup.cpu_limit.map(|cpu| {
                jobexecutor::cgroup::concepts::CpuLimit {
                    cpu_max_quota_micros: cpu.cpu_max_quota_micros,
                    cpu_max_period_micros: cpu.cpu_max_period_micros,
                }
            }),
            block_device_limit: request_cgroup.block_device_limit.map(|io| {
                jobexecutor::cgroup::concepts::BlockDeviceLimit {
                    io_max_rbps: io.io_max_rbps,
                    io_max_riops: io.io_max_riops,
                    io_max_wbps: io.io_max_wbps,
                    io_max_wiops: io.io_max_wiops,
                }
            }),
            ..Default::default()
        };
        if let Some(memory) = request_cgroup.memory_limit {
            limits.memory_max = memory.memory_max;
            limits.memory_swap_max = memory.memory_swap_max;
        }
        limits
    }

    async fn create_child_info(
        &self,
        pid: Pid,
        path: String,
        args: Vec<String>,
        limits: Option<CGroupLimits>,
    ) -> Result<ChildInfo, tonic::Status> {
        Ok(match (limits, &self.cgroup_config) {
            (Some(limits), Some(cgroup_config)) => {
                ChildInfo::new_with_cgroup(pid, path, args.into_iter(), cgroup_config, limits)
                    .await?
            }
            (None, _) => ChildInfo::new(pid, path, args)?,
            _ => {
                // client requested limits but cgroup_config is not available
                return Err(tonic::Status::invalid_argument(
                    "cgroup support is not enabled",
                ));
            }
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

        // obtain lock to generate pid
        let store = self.child_storage.lock().await;
        let pid: Pid = loop {
            let random = thread_rng().gen();
            if !store.contains_key(&random) {
                break random;
            }
        };
        drop(store);

        debug!("Assigned pid {} to the child process", pid);
        let child_info = self
            .create_child_info(
                pid,
                start_req.path,
                start_req.args,
                start_req.cgroup.map(Self::construct_limits),
            )
            .await?; // can block

        let mut store = self.child_storage.lock().await;
        let old_value = store.insert(pid, child_info);
        drop(store);
        if let Some(old_value) = old_value {
            error!(
                "Collistion on pid {}, killing the old process {:?}",
                pid, old_value
            );
            old_value.kill().await?; // must not block
        }

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
            // must not block
            RunningState::Running => Ok(job_executor::status_response::RunningStatus::Running),
            RunningState::Finished(FinishedState::WithExitCode(code)) => {
                exit_code = Some(code);
                Ok(job_executor::status_response::RunningStatus::ExitedWithCode)
            }
            RunningState::Finished(FinishedState::WithSignal) => {
                Ok(job_executor::status_response::RunningStatus::ExitedWithSignal)
            }
            RunningState::Unknown(reason) => {
                error!("[{}] Cannot get job status - {}", pid, reason);
                Err(tonic::Status::internal(
                    "Cannot get job status, not removing",
                ))
            }
        }?;
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

        child_info.kill().await?; // must not block
        Ok(Response::new(job_executor::StopResponse {}))
    }

    type GetOutputStream = impl futures_core::Stream<Item = Result<OutputResponse, tonic::Status>>;

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

        let event_stream = child_info
            .stream_chunks("TODO:IP")
            .await?
            .map(MyJobExecutor::chunk_to_output);

        Ok(Response::new(event_stream))
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
            Ok(RunningState::Finished(_)) => {
                child_info.clean_up().await?;
                let removed = child_storage.remove(&pid);
                assert!(
                    removed.is_some(),
                    "HashMap contains a job that cannot be removed" // this should never happen
                );
                Ok(())
            }
            Ok(RunningState::Running) => {
                // still running, fail
                Err(tonic::Status::failed_precondition("Job is still running"))
            }
            Ok(RunningState::Unknown(reason)) => {
                error!("[{}] Cannot get job status, not removing - {}", pid, reason);
                Err(tonic::Status::internal(
                    "Cannot get job status, not removing",
                ))
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

// TODO externalize env vars
async fn guess_cgroup_config() -> Option<Result<CGroupConfig, CGroupConfigError>> {
    // when running as systemd service, this could be guessed using
    // cgroup2 mount point + /proc/self/cgroup
    let parent_cgroup = std::env::var("PARENT_CGROUP")
        .map_err(|_| debug!("PARENT_CGROUP not set"))
        .ok()?
        .into();
    trace!("Using parent_group {:?}", parent_cgroup);

    let cgroup_block_device_id = std::env::var("CGROUP_BLOCK_DEVICE_ID")
        .map_err(|_| debug!("CGROUP_BLOCK_DEVICE_ID not set"))
        .ok()?;
    trace!("Using block device {:?}", cgroup_block_device_id);

    let move_current_pid_to_subfolder =
        std::env::var("CGROUP_MOVE_CURRENT_PID_TO_SUBFOLDER_ENABLED").is_ok();
    Some(
        CGroupConfig::new(CGroupConfigBuilder {
            parent_cgroup,
            cgroup_block_device_id,
            move_current_pid_to_subfolder,
        })
        .await,
    )
}

async fn run_server() -> anyhow::Result<()> {
    // TODO low: make this configurable
    let addr = "[::1]:50051".parse()?;
    let cgroup_config = match guess_cgroup_config().await {
        Some(Ok(cgroup_config)) => {
            info!("cgroup support enabled: {:?}", cgroup_config);
            Some(cgroup_config)
        }
        None => {
            info!("cgroup config is not complete, cgroup functionality is disabled");
            None
        }
        Some(Err(err)) => {
            return Err(err.into());
        }
    };

    let exec = MyJobExecutor::new(cgroup_config);
    info!("Starting gRPC server at {}", addr);

    Server::builder()
        //TODO: add interceptor that logs errors
        .add_service(JobExecutorServer::new(exec))
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    {
        #[cfg(tokio_unstable)]
        console_subscriber::init();
    }
    env_logger::init();
    run_server().await
}

#[cfg(test)]
pub mod tests {
    use jobexecutor::childinfo::ChildInfoCreationError;

    use super::*;

    #[test]
    pub fn test_cannot_run_process() {
        match ChildInfo::new(1, "", [] as [&str; 0]) {
            Err(ChildInfoCreationError::CannotRunProcess(pid, _)) => {
                assert_eq!(pid, 1);
            }
            Ok(_) => {
                panic!("Unexpected Ok");
            }
            Err(e) => {
                panic!("Unexpected error {:?}", e);
            }
        }
    }
}
