use anyhow::Context;
use log::*;
use runkins_proto::runkins::job_executor_client::*;
use runkins_proto::runkins::*;
use std::fs::OpenOptions;
use std::io::{Seek, Write};
use std::path::Path;
use std::time::Duration;
use structopt::StructOpt;
use tonic::transport::Channel;

type Pid = u64;

const RUNKINS_EID: &str = "RUNKINS_EID";

#[derive(StructOpt, Debug)]
#[structopt(about = "Job executor CLI")]
enum Subcommand {
    Start {
        // TODO: pass current CWD, pass env vars via -e
        #[structopt(
            short,
            long,
            help = "Set RUNKINS_EID to .env file in current directory"
        )]
        set_dot_env: bool,
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

        program: String,
        #[structopt()]
        args: Vec<String>,
    },
    Status {
        #[structopt(env = RUNKINS_EID)]
        pid: Pid,
    },
    Stop {
        #[structopt(env = RUNKINS_EID)]
        pid: Pid,
    },
    Logs {
        // TODO: rename to log(s)
        #[structopt(env = RUNKINS_EID)]
        pid: Pid,
        #[structopt(short, long, help = "Exit after given timeout in seconds")]
        timeout_secs: Option<u64>,
    },
    Rm {
        #[structopt(
            short,
            long,
            help = "Remove RUNKINS_EID from .env file in current directory"
        )]
        cleanup_dot_env: bool,
        // TODO implement --force
        #[structopt(env = RUNKINS_EID)]
        pid: Pid,
    },
    WaitUntil {
        #[structopt(env = RUNKINS_EID)]
        pid: Pid,
        #[structopt(short, long, help = "Exit when message is found in std out")]
        message: Option<String>,
        #[structopt(short, long, help = "Fail after given timeout in seconds")]
        timeout_secs: Option<u64>,
    },
    Ps {},
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
            program,
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
            set_dot_env,
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

            let request = tonic::Request::new(StartRequest {
                program,
                args,
                cgroup,
            });
            debug!("Request=${:?}", request);
            let response = client.start(request).await?;
            info!("Response={:?}", response);
            let id = response.into_inner().id.unwrap();
            println!("{}", id.id);
            if set_dot_env {
                set_dot_env_file(id.id)?;
            }
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
            let message = status_to_string(status_response.status.expect("Status not found"));
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
        Subcommand::Rm {
            pid,
            cleanup_dot_env,
        } => {
            let request = tonic::Request::new(RemoveRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let response = client.remove(request).await?;
            info!("Response={:?}", response);
            if cleanup_dot_env {
                cleanup_dot_env_file()?;
            }
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
            wait_until(stream, message, timeout_secs.map(Duration::from_secs)).await
        }
        Subcommand::Ps {} => {
            let request = tonic::Request::new(ListExecutionsRequest {});
            debug!("Request=${:?}", request);
            let response = client.list_executions(request).await?;
            info!("Response={:?}", response);
            let mut table = tabular::Table::new("{:<} {:<} {:<} {:<}");
            table.add_row(
                tabular::Row::new()
                    .with_cell(RUNKINS_EID)
                    .with_cell("STATUS")
                    .with_cell("PROGRAM")
                    .with_cell("ARGS"),
            );
            for detail in response.into_inner().details {
                table.add_row(
                    tabular::Row::new()
                        .with_cell(detail.id.unwrap().id)
                        .with_cell(status_to_string(detail.status.unwrap().status.unwrap()))
                        .with_cell(detail.program)
                        .with_cell(format!("{:?}", detail.args)),
                );
            }
            print!("{}", table);
            Ok(())
        }
    }
}

fn status_to_string(status: status_response::Status) -> String {
    match status {
        status_response::Status::Running(_) => "Running".to_string(),
        status_response::Status::ExitedWithSignal(_) => "Exited with signal".to_string(),
        status_response::Status::ExitedWithCode(status_response::ExitedWithCode { code }) => {
            format!("Exited with code {}", code)
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

const DOT_ENV_PATH: &str = ".env";
fn set_dot_env_file(runkins_eid: u64) -> Result<(), anyhow::Error> {
    set_file(DOT_ENV_PATH, Some(runkins_eid))
}

fn cleanup_dot_env_file() -> Result<(), anyhow::Error> {
    set_file(DOT_ENV_PATH, None)
}

fn set_file<P: AsRef<Path>>(as_path: P, runkins_eid: Option<u64>) -> Result<(), anyhow::Error> {
    let path = as_path.as_ref();
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .context(format!("Cannot open {:?} file for writing", path))?;
    #[allow(deprecated)]
    for (key, val) in dotenv::from_path_iter(path)
        .context(format!("Cannot parse file {:?}", path))?
        .map(|it| {
            it.context(format!("Cannot parse key/value pair of {:?}", path))
                .unwrap()
        })
    {
        if key != RUNKINS_EID {
            file.write_all(format!("{}={}\n", key, val).as_bytes())?;
        }
    }
    if let Some(runkins_eid) = runkins_eid {
        file.write_all(format!("{}={}\n", RUNKINS_EID, runkins_eid).as_bytes())?;
    }
    file.flush().context(format!("Cannot flush {:?}", path))?;
    let pos = file.stream_position()?;
    file.set_len(pos)
        .context(format!("Cannot set length of file {:?} to {}", file, pos))
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::{io::Read, sync::Once};
    use tempfile::NamedTempFile;

    static INIT: Once = Once::new();

    fn before_all() {
        INIT.call_once(|| {
            env_logger::init();
        });
    }

    #[test]
    fn should_create_env_file() -> Result<(), anyhow::Error> {
        before_all();
        let runkins_eid = 1;
        let tmpfile = NamedTempFile::new()?;
        debug!("Using temp file {:?}", tmpfile);

        std::fs::remove_file(&tmpfile)?;
        set_file(&tmpfile, Some(runkins_eid))?;
        #[allow(deprecated)]
        let actual: Vec<(String, String)> = dotenv::from_path_iter(&tmpfile)
            .context(format!("Cannot parse file {:?}", tmpfile))?
            .map(|it| {
                it.context(format!("Cannot parse key/value pair of {:?}", tmpfile))
                    .unwrap()
            })
            .collect();
        let expected = vec![(RUNKINS_EID.to_string(), runkins_eid.to_string())];
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn should_rewrite_just_its_own_key() -> Result<(), anyhow::Error> {
        before_all();
        let runkins_eid = 1;
        let mut tmpfile = NamedTempFile::new()?;
        debug!("Using temp file {:?}", tmpfile);
        writeln!(tmpfile, "key1=val1")?;
        writeln!(
            tmpfile,
            "# comment comment comment comment comment comment comment comment"
        )?;
        writeln!(tmpfile, "{}={}", RUNKINS_EID, 10)?;
        writeln!(tmpfile, "key2=val2")?;
        writeln!(
            tmpfile,
            "# comment comment comment comment comment comment comment comment"
        )?;

        {
            let mut buf = String::new();
            tmpfile.reopen()?.read_to_string(&mut buf)?;
            debug!("before: '{}'", buf);
        }

        set_file(&tmpfile, Some(runkins_eid))?;
        {
            let mut buf = String::new();
            tmpfile.reopen()?.read_to_string(&mut buf)?;
            debug!("after: '{}'", buf);
        }
        #[allow(deprecated)]
        let actual: Vec<(String, String)> = dotenv::from_path_iter(&tmpfile)
            .context(format!("Cannot parse file {:?}", tmpfile))?
            .map(|it| {
                it.context(format!("Cannot parse key/value pair of {:?}", tmpfile))
                    .unwrap()
            })
            .collect();
        let expected = vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
            (RUNKINS_EID.to_string(), runkins_eid.to_string()),
        ];
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn should_remove_just_its_own_key() -> Result<(), anyhow::Error> {
        before_all();
        let mut tmpfile = NamedTempFile::new()?;
        debug!("Using temp file {:?}", tmpfile);
        writeln!(tmpfile, "key1=val1")?;
        writeln!(
            tmpfile,
            "# comment comment comment comment comment comment comment comment"
        )?;
        writeln!(tmpfile, "{}={}", RUNKINS_EID, 10)?;
        writeln!(tmpfile, "key2=val2")?;
        writeln!(
            tmpfile,
            "# comment comment comment comment comment comment comment comment"
        )?;

        {
            let mut buf = String::new();
            tmpfile.reopen()?.read_to_string(&mut buf)?;
            debug!("before: '{}'", buf);
        }

        set_file(&tmpfile, None)?;
        {
            let mut buf = String::new();
            tmpfile.reopen()?.read_to_string(&mut buf)?;
            debug!("after: '{}'", buf);
        }
        #[allow(deprecated)]
        let actual: Vec<(String, String)> = dotenv::from_path_iter(&tmpfile)
            .context(format!("Cannot parse file {:?}", tmpfile))?
            .map(|it| {
                it.context(format!("Cannot parse key/value pair of {:?}", tmpfile))
                    .unwrap()
            })
            .collect();
        let expected = vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ];
        assert_eq!(actual, expected);
        Ok(())
    }
}
