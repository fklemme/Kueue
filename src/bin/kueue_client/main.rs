mod cli;
mod print;

use crate::{
    cli::{Cli, CmdArgs, Command},
    print::term_size,
};
use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose, Engine as _};
use clap::Parser;
use kueue::{
    config::Config,
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::{JobInfo, Resources},
};
use sha2::{Digest, Sha256};
use simple_logger::SimpleLogger;
use std::fs::canonicalize;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<()> {
    // Read command line arguments.
    let args = Cli::parse();

    // Read configuration from file or defaults.
    let config =
        Config::new(args.config.clone()).map_err(|e| anyhow!("Failed to load config: {}", e))?;
    // If there is no config file, create template.
    if let Err(e) = config.create_default_config(args.config) {
        bail!("Could not create default config: {}", e);
    }

    // Initialize logger.
    SimpleLogger::new()
        .with_level(config.get_log_level().to_level_filter())
        .init()
        .unwrap();

    // Run client.
    // TODO: Wrap client into own struct.

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

    // Process subcommands.
    match args.command {
        Command::Cmd {
            cpus,
            ram_mb,
            stdout,
            stderr,
            args,
        } => {
            let CmdArgs::Args(cmd) = args;

            if cmd.is_empty() {
                return Err(anyhow!("Empty command!"));
            }

            // This command requires authentification.
            authenticate(&mut stream, &config).await?;

            // Issue job.
            let cwd = std::env::current_dir()?;
            let cwd = canonicalize(cwd)?;
            let resources = Resources::new(cpus, ram_mb);
            let job_info = JobInfo::new(cmd, cwd, resources, stdout, stderr);
            let message = ClientToServerMessage::IssueJob(job_info);
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
            succeeded,
            failed,
            canceled,
        } => {
            // Query jobs.
            let message = ClientToServerMessage::ListJobs {
                // Current space (height) in the terminal to show jobs.
                num_jobs: num_jobs.unwrap_or(term_size().1 - 4),
                pending,
                offered,
                running,
                succeeded,
                failed,
                canceled,
            };
            stream.send(&message).await?;

            // Await results.
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::JobList {
                    jobs_pending,
                    jobs_offered,
                    jobs_running,
                    jobs_succeeded,
                    jobs_failed,
                    jobs_canceled,
                    job_infos,
                } => {
                    print::job_list(
                        jobs_pending,
                        jobs_offered,
                        jobs_running,
                        jobs_succeeded,
                        jobs_failed,
                        jobs_canceled,
                        job_infos,
                    );
                }
                other => {
                    return Err(anyhow!("Expected JobList, received: {:?}", other));
                }
            }
        }
        Command::ShowJob { id } => {
            // Query job.
            let message = ClientToServerMessage::ShowJob { id };
            stream.send(&message).await?;

            // Await results.
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::JobInfo {
                    job_info,
                    stdout_text,
                    stderr_text,
                } => print::job_info(job_info, stdout_text, stderr_text),
                ServerToClientMessage::RequestResponse { success, text } if !success => {
                    println!("{}", text);
                }
                other => {
                    return Err(anyhow!("Expected JobInfo, received: {:?}", other));
                }
            }
        }
        Command::RemoveJob { id, kill } => {
            // This command requires authentification.
            authenticate(&mut stream, &config).await?;

            // Remove job from queue.
            let message = ClientToServerMessage::RemoveJob { id, kill };
            stream.send(&message).await?;

            // Await results.
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::RequestResponse { success: _, text } => println!("{}", text),
                other => {
                    return Err(anyhow!("Expected RequestResponse, received: {:?}", other));
                }
            }
        }
        Command::ListWorkers => {
            // Query workers.
            stream.send(&ClientToServerMessage::ListWorkers).await?;

            // Await results.
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::WorkerList(worker_list) => {
                    print::worker_list(worker_list);
                }
                other => {
                    return Err(anyhow!("Expected WorkerList, received: {:?}", other));
                }
            }
        }
        Command::ShowWorker { id } => {
            // Query worker.
            let message = ClientToServerMessage::ShowWorker { id };
            stream.send(&message).await?;

            // Await results.
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::WorkerInfo(worker_info) => print::worker_info(worker_info),
                ServerToClientMessage::RequestResponse { success, text } if !success => {
                    println!("{}", text);
                }
                other => {
                    return Err(anyhow!("Expected WorkerInfo, received: {:?}", other));
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
            let salted_secret = config.common_settings.shared_secret.clone() + &salt;
            let salted_secret = salted_secret.into_bytes();
            let mut hasher = Sha256::new();
            hasher.update(salted_secret);
            let response = hasher.finalize().to_vec();
            let response = general_purpose::STANDARD_NO_PAD.encode(response);

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
