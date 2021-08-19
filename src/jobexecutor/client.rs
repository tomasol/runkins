use job_executor::job_executor_client::*;
use job_executor::*;
use log::*;
use structopt::StructOpt;

pub mod job_executor {
    tonic::include_proto!("jobexecutor");
}

#[derive(StructOpt, Debug)]
#[structopt(about = "Job executor CLI")]
enum Subcommand {
    Start {
        // #[structopt(short, long)]
        path: String,
        #[structopt()]
        args: Vec<String>,
    },
    Status {
        pid: u32,
    },
    Stop {
        #[structopt(short, long)]
        remove: bool,
        pid: u32,
    },
    Output {
        pid: u32,
    },
    Remove {
        pid: u32,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Subcommand::from_args();
    debug!("{:#?}", opt);

    let mut client = JobExecutorClient::connect("http://[::1]:50051").await?;

    match opt {
        Subcommand::Start { path, args } => {
            let request = tonic::Request::new(StartRequest { path, args });
            debug!("Request=${:?}", request);
            let response = client.start(request).await?;
            info!("Response={:?}", response);
            if let Some(id) = response.into_inner().id {
                println!("{}", id.id);
            }
        }
        Subcommand::Status { pid } => {
            let request = tonic::Request::new(StatusRequest {
                id: Some(ExecutionId { id: pid }),
            });
            debug!("Request=${:?}", request);
            let response = client.job_status(request).await?;
            info!("Response={:?}", response);
        }
        Subcommand::Stop { pid, remove } => {
            let request = tonic::Request::new(StopRequest {
                id: Some(ExecutionId { id: pid }),
                remove,
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

            while let Some(chunk) = stream.message().await? {
                println!("chunk = {:?}", chunk);
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
