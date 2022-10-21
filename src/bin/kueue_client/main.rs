mod print;

use clap::{Parser, Subcommand};
use kueue::{
    config::{default_path, Config},
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::JobInfo,
};
use sha2::{Digest, Sha256};
use simple_logger::SimpleLogger;
use std::{error::Error, net::Ipv4Addr, str::FromStr};
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
async fn main() -> Result<(), Box<dyn Error>> {
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
    if let Err(e) = config.create_default_config() {
        log::error!("Could not create default config: {}", e);
    }

    // Connect to server.
    let server_addr = (
        Ipv4Addr::from_str(&config.server_address)?,
        config.server_port,
    );
    let stream = TcpStream::connect(server_addr).await?;
    let mut stream = MessageStream::new(stream);

    // Send hello from client.
    stream.send(&HelloMessage::HelloFromClient).await?;

    // Await welcoming response from server.
    match stream.receive::<ServerToClientMessage>().await? {
        ServerToClientMessage::WelcomeClient => log::trace!("Established connection to server!"), // continue
        other => return Err(format!("Expected WelcomeClient, received: {:?}", other).into()),
    }

    // Process subcommand
    match args.command {
        Command::Cmd(cmd) => {
            // This command requires authentification.
            authenticate(&mut stream, &config).await?;

            // Issue job.
            assert!(!cmd.is_empty());
            let cwd = std::env::current_dir()?;
            let message = ClientToServerMessage::IssueJob(JobInfo::new(cmd, cwd));
            stream.send(&message).await?;

            // Await acceptance.
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
            // Query jobs.
            stream.send(&ClientToServerMessage::ListJobs).await?;

            // Await results.
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
            stream.send(&ClientToServerMessage::ListWorkers).await?;

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

async fn authenticate(stream: &mut MessageStream, config: &Config) -> Result<(), Box<dyn Error>> {
    // Request authentification.
    stream.send(&ClientToServerMessage::AuthRequest).await?;

    // Await authentification challenge.
    match stream.receive::<ServerToClientMessage>().await? {
        ServerToClientMessage::AuthChallenge(salt) => {
            // Calculate response.
            let salted_secret = config.shared_secret.clone() + &salt;
            let salted_secret = salted_secret.into_bytes();
            let mut hasher = Sha256::new();
            hasher.update(salted_secret);
            let response = hasher.finalize().to_vec();
            let response = base64::encode(response);

            // Send response back to server.
            let message = ClientToServerMessage::AuthResponse(response);
            stream.send(&message).await?;
        }
        other => {
            return Err(format!("Expected AuthChallenge, received: {:?}", other).into());
        }
    }

    // Await authentification confirmation.
    match stream.receive::<ServerToClientMessage>().await? {
        ServerToClientMessage::AuthAccepted(accepted) => {
            if accepted {
                Ok(())
            } else {
                Err("Authentification failed!".into())
            }
        }
        other => Err(format!("Expected AuthAccepted, received: {:?}", other).into()),
    }
}
