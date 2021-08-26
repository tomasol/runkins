use std::io::Write;

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

async fn exec_cli(opt: Subcommand) -> Result<(), Box<dyn std::error::Error>> {
    // TODO low: make this configurable
    let mut client = JobExecutorClient::connect("http://[::1]:50051").await?;

    match opt {
        Subcommand::Start { path, args } => {
            let request = tonic::Request::new(StartRequest {
                path,
                args,
                cgroup: None,
            });
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
            println!("ok");
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
            println!("ok");
        }
    };
    Ok(())
}
