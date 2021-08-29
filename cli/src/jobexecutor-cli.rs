use std::io::Write;

use anyhow::bail;
use job_executor::job_executor_client::*;
use job_executor::*;
use log::*;
use structopt::StructOpt;

pub mod job_executor {
    tonic::include_proto!("jobexecutor");
}

type Pid = u64;

#[derive(StructOpt, Debug)]
#[structopt(about = "Job executor CLI")]
enum Subcommand {
    Start {
        // TODO low: allow setting limits in human writable form e.g. 10MB, 5ms, etc.
        #[structopt(short, long, help = "Enable cgroup limits")]
        limits: bool,
        // memory.max
        #[structopt(long, help = "Set memory.max in bytes", requires("limits"))]
        memory_max: Option<u64>,
        // memory.swap.max
        #[structopt(long, help = "Set memory.swap.max in bytes", requires("limits"))]
        memory_swap_max: Option<u64>,
        // cpu.max: none or both values must be set
        #[structopt(
            long,
            help = "Set cpu.max, quota part, both parts must be set together",
            requires("limits")
        )]
        cpu_max_quota_micros: Option<u64>,
        #[structopt(
            long,
            help = "Set cpu.max, period part, both parts must be set together",
            requires("limits")
        )]
        cpu_max_period_micros: Option<u64>,
        // io.max: not all values must be set
        #[structopt(long, help = "Set io.max, rbps value", requires("limits"))]
        io_max_rbps: Option<u64>,
        #[structopt(long, help = "Set io.max, riops value", requires("limits"))]
        io_max_riops: Option<u64>,
        #[structopt(long, help = "Set io.max, wbps value", requires("limits"))]
        io_max_wbps: Option<u64>,
        #[structopt(long, help = "Set io.max, wiops value", requires("limits"))]
        io_max_wiops: Option<u64>,

        path: String,
        #[structopt()]
        args: Vec<String>,
    },
    Status {
        pid: Pid,
    },
    Stop {
        pid: Pid,
    },
    Output {
        pid: Pid,
    },
    Remove {
        pid: Pid,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Subcommand::from_args();
    let result = exec_cli(opt).await;
    if let Err(err) = result {
        eprintln!("CLI failed: {}", err);
        // TODO unpack gRPC error, show only status and message
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

async fn exec_cli(opt: Subcommand) -> anyhow::Result<()> {
    // TODO low: make this configurable
    let mut client = JobExecutorClient::connect("http://[::1]:50051").await?;

    match opt {
        Subcommand::Start {
            path,
            args,
            limits,
            memory_max,
            memory_swap_max,
            cpu_max_quota_micros,
            cpu_max_period_micros,
            io_max_rbps,
            io_max_riops,
            io_max_wbps,
            io_max_wiops,
        } => {
            let cgroup = if limits {
                let cgroup = CGroup {
                    memory_limit: Some(MemoryLimit {
                        memory_max,
                        memory_swap_max,
                    }),
                    cpu_limit: match (cpu_max_quota_micros, cpu_max_period_micros) {
                        (Some(cpu_max_quota_micros), Some(cpu_max_period_micros)) => {
                            Some(CpuLimit {
                                cpu_max_quota_micros,
                                cpu_max_period_micros,
                            })
                        }
                        (None, None) => None,
                        _ => {
                            bail!(
                                "To control cpu.max, cpu_max_quota_micros and cpu_max_period_micros must be both set"
                            );
                        }
                    },
                    block_device_limit: Some(BlockDeviceLimit {
                        io_max_rbps,
                        io_max_riops,
                        io_max_wbps,
                        io_max_wiops,
                    }),
                };

                Some(cgroup)
            } else {
                None
            };

            let request = tonic::Request::new(StartRequest { path, args, cgroup });
            debug!("Request=${:?}", request);
            let response = client.start(request).await?;
            info!("Response={:?}", response);
            let id = response.into_inner().id.unwrap();
            println!("{}", id.id);
        }
        Subcommand::Status { pid } => {
            let request = tonic::Request::new(StatusRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let response = client.job_status(request).await?;
            info!("Response={:?}", response);
            let status_response = response.into_inner();
            let message = match status_response.status() {
                status_response::RunningStatus::Running => "Running".to_string(),
                status_response::RunningStatus::ExitedWithSignal => {
                    "Exited with signal".to_string()
                }
                status_response::RunningStatus::ExitedWithCode => {
                    format!("Exited with code {}", status_response.exit_code.unwrap())
                }
            };
            println!("{}", message);
        }
        Subcommand::Stop { pid } => {
            let request = tonic::Request::new(StopRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let response = client.stop(request).await?;
            info!("Response={:?}", response);
        }
        Subcommand::Output { pid } => {
            let request = tonic::Request::new(OutputRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let mut stream = client.get_output(request).await?.into_inner();
            let stdout = std::io::stdout();
            // consider wrapping with std::io::BufWriter once write performance becomes an issue
            // however buffer might add delays to streaming
            let mut stdout = stdout.lock();
            let stderr = std::io::stderr();
            let mut stderr = stderr.lock();
            while let Some(chunk) = stream.message().await? {
                if let Some(output_chunk) = chunk.std_out_chunk {
                    stdout.write_all(output_chunk.chunk.as_bytes())?;
                }
                if let Some(output_chunk) = chunk.std_err_chunk {
                    stderr.write_all(output_chunk.chunk.as_bytes())?;
                }
            }
        }
        Subcommand::Remove { pid } => {
            let request = tonic::Request::new(RemoveRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let response = client.remove(request).await?;
            info!("Response={:?}", response);
        }
    };
    Ok(())
}
