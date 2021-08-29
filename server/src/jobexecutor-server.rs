use childinfo::*;
use job_executor::job_executor_server::*;
use job_executor::*;
use jobexecutor::cgroup::runtime::CGroupLimits;
use jobexecutor::cgroup::server_config::CGroupConfig;
use jobexecutor::cgroup::server_config::CGroupConfigBuilder;
use jobexecutor::cgroup::server_config::CGroupConfigError;
use jobexecutor::childinfo;
use log::*;
use rand::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server, Response};

pub mod job_executor {
    tonic::include_proto!("jobexecutor");
}

#[derive(Debug)]
pub struct MyJobExecutor {
    child_storage: Mutex<HashMap<Pid, ChildInfo<OutputResponse, tonic::Status>>>,
    cgroup_config: Option<CGroupConfig>,
}

impl MyJobExecutor {
    fn new(cgroup_config: Option<CGroupConfig>) -> MyJobExecutor {
        MyJobExecutor {
            cgroup_config,
            child_storage: Default::default(),
        }
    }

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

    fn construct_limits(request_cgroup: CGroup) -> CGroupLimits {
        let mut limits = CGroupLimits {
            cpu_limit: request_cgroup
                .cpu_limit
                .map(|cpu| jobexecutor::cgroup::runtime::CpuLimit {
                    cpu_max_quota_micros: cpu.cpu_max_quota_micros,
                    cpu_max_period_micros: cpu.cpu_max_period_micros,
                }),
            block_device_limit: request_cgroup.block_device_limit.map(|io| {
                jobexecutor::cgroup::runtime::BlockDeviceLimit {
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
}

#[tonic::async_trait]
impl JobExecutor for MyJobExecutor {
    async fn start(
        &self,
        request: tonic::Request<job_executor::StartRequest>,
    ) -> Result<tonic::Response<job_executor::StartResponse>, tonic::Status> {
        debug!("Request: {:?}", request);
        let start_req = request.into_inner();

        // obtain lock
        let mut store = self.child_storage.lock().await;
        let pid: Pid = loop {
            let random = thread_rng().gen();
            if !store.contains_key(&random) {
                break random;
            }
        };
        debug!("Assigned pid {} to the child process", pid);
        let child_info = if let Some(limits) = start_req.cgroup.map(Self::construct_limits) {
            if let Some(cgroup_config) = &self.cgroup_config {
                ChildInfo::new_with_cgroup(
                    pid,
                    start_req.path,
                    start_req.args.into_iter(),
                    Self::chunk_to_output,
                    cgroup_config,
                    limits,
                )
            } else {
                return Err(tonic::Status::invalid_argument(
                    "cgroup support is not enabled",
                ));
            }
        } else {
            ChildInfo::new(
                pid,
                start_req.path,
                start_req.args.into_iter(),
                Self::chunk_to_output,
            )
        }?;
        store.insert(pid, child_info);

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
                // clean up cgroup
                child_info.clean_up().await?;
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

fn guess_cgroup_config() -> Option<Result<CGroupConfig, CGroupConfigError>> {
    let cgexec_rs = if let Ok(path) = std::env::var("CGEXEC_RS") {
        path.into()
    } else {
        trace!("Guessing cgexec-rs location based on current_exe");
        let exe = std::env::current_exe().ok()?;
        exe.parent()?.join("cgexec-rs")
    };
    trace!("Using cgexec-rs {:?}", cgexec_rs);
    let parent_cgroup = std::env::var("PARENT_CGROUP")
        .map_err(|_| debug!("PARENT_CGROUP not set"))
        .ok()?
        .into();
    trace!("Using parent_group {:?}", parent_cgroup);

    let cgroup_block_device_id = std::env::var("CGROUP_BLOCK_DEVICE_ID")
        .map_err(|_| debug!("CGROUP_BLOCK_DEVICE_ID not set"))
        .ok()?;
    trace!("Using block device {:?}", cgroup_block_device_id);

    Some(CGroupConfig::new(CGroupConfigBuilder {
        cgexec_rs,
        parent_cgroup,
        cgroup_block_device_id,
    }))
}

async fn run_server() -> anyhow::Result<()> {
    // TODO low: make this configurable
    let addr = "[::1]:50051".parse()?;
    let cgroup_config = match guess_cgroup_config() {
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
    env_logger::init();
    run_server().await
}
