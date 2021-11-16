use anyhow::Context;
use log::*;
use runkins_proto::runkins::job_executor_client::*;
use runkins_proto::runkins::*;
use std::io::Write;
use std::time::Duration;
use structopt::StructOpt;
use tonic::transport::Channel;

type Pid = u64;

#[derive(StructOpt, Debug)]
#[structopt(about = "Job executor CLI")]
enum Subcommand {
    Start {
        // TODO: add --set-dot-env, pass current CWD, pass current env vars
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
        requires_all(& ["limits", "cpu-max-period-micros"])
        )]
        cpu_max_quota_micros: Option<u64>,
        #[structopt(
        long,
        help = "Set cpu.max, period part, both parts must be set together",
        requires_all(& ["limits", "cpu-max-quota-micros"])
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
        #[structopt(env = "RUNKINS_EID")]
        pid: Pid,
    },
    Stop {
        #[structopt(env = "RUNKINS_EID")]
        pid: Pid,
    },
    Logs {
        // TODO: rename to log(s)
        #[structopt(env = "RUNKINS_EID")]
        pid: Pid,
        #[structopt(short, long, help = "Exit after given timeout in seconds")]
        timeout_secs: Option<u64>,
    },
    Rm {
        // TODO implement --force
        #[structopt(env = "RUNKINS_EID")]
        pid: Pid,
    },
    WaitUntil {
        #[structopt(env = "RUNKINS_EID")]
        pid: Pid,
        #[structopt(short, long, help = "Exit when message is found in std out")]
        message: Option<String>,
        #[structopt(short, long, help = "Fail after given timeout in seconds")]
        timeout_secs: Option<u64>,
    },
    // TODO implement ps, ps -a
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    // TODO allow disabling this via env var
    dotenv::dotenv().ok();
    let opt = Subcommand::from_args();
    // TODO low: make this configurable
    let addr = "http://127.0.0.1:50051";
    let client = JobExecutorClient::connect(addr)
        .await
        .with_context(|| format!("Cannot connect to {}", addr))?;
    send_request(opt, client).await
}

async fn send_request(
    opt: Subcommand,
    mut client: JobExecutorClient<Channel>,
) -> Result<(), anyhow::Error> {
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
                        _ => None,
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
            Ok(())
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
            Ok(())
        }
        Subcommand::Stop { pid } => {
            let request = tonic::Request::new(StopRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let response = client.stop(request).await?;
            info!("Response={:?}", response);
            Ok(())
        }
        Subcommand::Logs { pid, timeout_secs } => {
            let request = tonic::Request::new(OutputRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let mut stream = client.get_output(request).await?.into_inner();
            let stderr = std::io::stderr();
            let mut stderr = stderr.lock();
            let stdout = std::io::stdout();
            let mut stdout = stdout.lock();
            let mut print_chunk = |chunk: OutputResponse| {
                if let Some(output_chunk) = chunk.std_err_chunk {
                    stderr.write_all(&output_chunk.chunk[..])?;
                }
                if let Some(output_chunk) = chunk.std_out_chunk {
                    stdout.write_all(&output_chunk.chunk[..])?;
                }
                Ok::<(), anyhow::Error>(())
            };
            let timeout = timeout_secs.map(Duration::from_secs);
            match timeout {
                Some(Duration::ZERO) => {
                    // just print first chunk
                    if let Some(chunk) = stream.message().await? {
                        print_chunk(chunk)?;
                    }
                    Ok(())
                }
                _ => {
                    let print_all = async {
                        while let Some(chunk) = stream.message().await? {
                            print_chunk(chunk)?;
                        }
                        Ok::<(), anyhow::Error>(())
                    };
                    match timeout {
                        Some(timeout) => {
                            tokio::select! { // print until stream finishes or timeout
                                result = print_all => {
                                    result
                                }
                                _ = tokio::time::sleep(timeout) => Ok(())
                            }
                        }
                        None => print_all.await, // print until stream finishes
                    }
                }
            }
        }
        Subcommand::Rm { pid } => {
            let request = tonic::Request::new(RemoveRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let response = client.remove(request).await?;
            info!("Response={:?}", response);
            Ok(())
        }
        Subcommand::WaitUntil {
            pid,
            message,
            timeout_secs,
        } => {
            let request = tonic::Request::new(OutputRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let stream = client.get_output(request).await?.into_inner();
            return wait_until(stream, message, timeout_secs.map(Duration::from_secs)).await;
        }
    }
}

async fn wait_until(
    stream: tonic::Streaming<OutputResponse>,
    message: Option<String>,
    timeout: Option<Duration>,
) -> Result<(), anyhow::Error> {
    let wait_until = wait_until_found(stream, message);
    // TODO if duration = 0, take just one chunk out of the stream
    match timeout {
        Some(timeout) => {
            tokio::select! {
                result = wait_until => {
                    result
                }
                _ = tokio::time::sleep(timeout) => {anyhow::bail!("Timeout")}
            }
        }
        None => wait_until.await,
    }
}

// Wait until message is found.
// If message is not set, wait until the stream exits.
async fn wait_until_found(
    mut stream: tonic::Streaming<OutputResponse>,
    message: Option<String>,
) -> Result<(), anyhow::Error> {
    while let Some(chunk) = stream.message().await? {
        if let Some(output_chunk) = chunk.std_out_chunk {
            if let Some(message) = &message {
                if String::from_utf8_lossy(&output_chunk.chunk).contains(message) {
                    return Ok(());
                }
            }
        }
    }
    if message.is_some() {
        anyhow::bail!("Not found")
    } else {
        Ok(())
    }
}
