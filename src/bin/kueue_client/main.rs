mod cli;
mod print;

use crate::cli::{Args, Command};
use anyhow::{anyhow, Result};
use clap::Parser;
use kueue::{
    config::Config,
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::JobInfo,
};
use sha2::{Digest, Sha256};
use simple_logger::SimpleLogger;
use std::fs::canonicalize;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<()> {
    // Read command line arguments.
    let args = Args::parse();
    log::debug!("{:?}", args);

    // Read configuration from file or defaults.
    let config = Config::new().map_err(|e| anyhow!("Failed to load config: {}", e))?;
    // If there is no config file, create template.
    if let Err(e) = config.create_default_config() {
        log::error!("Could not create default config: {}", e);
    }

    // Initialize logger.
    SimpleLogger::new()
        .with_level(config.get_log_level().to_level_filter())
        .init()
        .unwrap();

    // Connect to server.
    let server_addr = config.get_server_address().await?;
    let stream = TcpStream::connect(server_addr).await?;
    let mut stream = MessageStream::new(stream);

    // Send hello from client.
    stream.send(&HelloMessage::HelloFromClient).await?;

    // Await welcoming response from server.
    match stream.receive::<ServerToClientMessage>().await? {
        ServerToClientMessage::WelcomeClient => log::trace!("Established connection to server!"), // continue
        other => return Err(anyhow!("Expected WelcomeClient, received: {:?}", other)),
    }

    // Process subcommand
    match args.command {
        Command::Cmd(cmd) => {
            // This command requires authentification.
            authenticate(&mut stream, &config).await?;

            // Issue job.
            assert!(!cmd.is_empty());
            let cwd = std::env::current_dir()?;
            let cwd = canonicalize(cwd)?;
            let message = ClientToServerMessage::IssueJob(JobInfo::new(cmd, cwd));
            stream.send(&message).await?;

            // Await acceptance.
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::AcceptJob(job_info) => {
                    log::debug!("Job submitted successfully!");
                    log::info!("Job ID: {}", job_info.id);
                }
                other => {
                    return Err(anyhow!("Expected AcceptJob, received: {:?}", other));
                }
            }
        }
        Command::ListJobs {
            num_jobs,
            pending,
            offered,
            running,
            finished,
            failed,
        } => {
            // Query jobs.
            let message = ClientToServerMessage::ListJobs {
                num_jobs,
                pending,
                offered,
                running,
                finished,
                failed,
            };
            stream.send(&message).await?;

            // Await results.
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::JobList {
                    jobs_pending,
                    jobs_offered,
                    jobs_running,
                    jobs_finished,
                    any_job_failed,
                    job_infos,
                } => {
                    print::job_list(
                        jobs_pending,
                        jobs_offered,
                        jobs_running,
                        jobs_finished,
                        any_job_failed,
                        job_infos,
                    );
                }
                other => {
                    return Err(anyhow!("Expected JobList, received: {:?}", other));
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
                    return Err(anyhow!("Expected WorkerList, received: {:?}", other));
                }
            }
        }
        Command::ShowJob { id } => {
            // Query jobs.
            let message = ClientToServerMessage::ShowJob { id };
            stream.send(&message).await?;

            // Await results.
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::JobInfo {
                    job_info,
                    stdout,
                    stderr,
                } => {
                    print::job_info(job_info, stdout, stderr);
                }
                other => {
                    return Err(anyhow!("Expected JobInfo, received: {:?}", other));
                }
            }
        }
    }

    // Say bye to gracefully shut down connection.
    stream.send(&ClientToServerMessage::Bye).await?;

    Ok(())
}

async fn authenticate(stream: &mut MessageStream, config: &Config) -> Result<()> {
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
            return Err(anyhow!("Expected AuthChallenge, received: {:?}", other));
        }
    }

    // Await authentification confirmation.
    match stream.receive::<ServerToClientMessage>().await? {
        ServerToClientMessage::AuthAccepted(accepted) => {
            if accepted {
                Ok(())
            } else {
                Err(anyhow!("Authentification failed!"))
            }
        }
        other => Err(anyhow!("Expected AuthAccepted, received: {:?}", other)),
    }
}
