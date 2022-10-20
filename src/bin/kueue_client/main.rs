mod print;

use clap::{Parser, Subcommand};
use kueue::{
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::JobInfo, config::{Config, default_path},
};
use simple_logger::SimpleLogger;
use std::{net::Ipv4Addr, str::FromStr};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to config file.
    #[arg(short, long, default_value_t = default_path().to_string_lossy().into())]
    config: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Issue command to be off-loaded to remote workers.
    #[command(external_subcommand)]
    Cmd(Vec<String>), // TODO: missing in help!
    /// Query information about scheduled and running jobs.
    ListJobs,
    /// Query infromation about available workers.
    ListWorkers,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger.
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    // Read command line arguments.
    let args = Args::parse();
    log::debug!("{:?}", args);

    // Read configuration from file or defaults.
    let config = Config::new()?;
    // If there is no config file, create template.
    config.create_default_config();

    // Connect to server.
    let server_addr = (
        Ipv4Addr::from_str(&config.server_address)?,
        config.server_port);
    let stream = TcpStream::connect(server_addr).await?;
    let mut stream = MessageStream::new(stream);

    // Send hello from client.
    stream.send(&HelloMessage::HelloFromClient).await?;

    // Await welcoming response from server.
    match stream.receive::<ServerToClientMessage>().await? {
        ServerToClientMessage::WelcomeClient => log::trace!("Established connection to server!"), // continue
        other => return Err(format!("Expected WelcomeClient, received: {:?}", other).into()),
    }

    // TODO: Implement encryption & authentification

    // Process subcommand
    match args.command {
        Command::Cmd(cmd) => {
            // Issue job
            assert!(!cmd.is_empty());
            let cwd = std::env::current_dir()?;
            let message = ClientToServerMessage::IssueJob(JobInfo::new(cmd, cwd));
            stream.send(&message).await?;

            // Await acceptance
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::AcceptJob(job_info) => {
                    log::debug!("Job submitted successfully!");
                    log::info!("Job ID: {}", job_info.id);
                }
                other => {
                    return Err(format!("Expected AcceptJob, received: {:?}", other).into());
                }
            }
        }
        Command::ListJobs => {
            // Query jobs
            let message = ClientToServerMessage::ListJobs;
            stream.send(&message).await?;

            // Await results
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::JobList(job_list) => {
                    print::job_list(job_list);
                }
                other => {
                    return Err(format!("Expected JobList, received: {:?}", other).into());
                }
            }
        }
        Command::ListWorkers => {
            // Query workers
            let message = ClientToServerMessage::ListWorkers;
            stream.send(&message).await?;

            // Await results
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::WorkerList(worker_list) => {
                    print::worker_list(worker_list);
                }
                other => {
                    return Err(format!("Expected WorkerList, received: {:?}", other).into());
                }
            }
        }
    }

    // Say bye to gracefully shut down connection.
    stream.send(&ClientToServerMessage::Bye).await?;

    Ok(())
}
