use crate::{
    cli::{Cli, CmdArgs, Command},
    print::{self, term_size},
};
use anyhow::{anyhow, Result, bail};
use base64::{engine::general_purpose, Engine as _};
use kueue_lib::{
    config::Config,
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::{JobInfo, Resources},
};
use sha2::{Digest, Sha256};
use std::fs::canonicalize;
use tokio::net::TcpStream;

pub struct Client {
    args: Cli,
    config: Config,
    stream: MessageStream,
}

impl Client {
    /// Set up a new client instance and connect to the server.
    pub async fn new(args: Cli, config: Config) -> Result<Self> {
        // Connect to server.
        let server_addr = config.get_server_address().await?;
        let stream = TcpStream::connect(server_addr).await?;
        let stream = MessageStream::new(stream);

        Ok(Client {
            args,
            config,
            stream,
        })
    }

    /// Perform request and handle messages.
    pub async fn run(&mut self) -> Result<()> {
        // Send hello from client.
        self.stream.send(&HelloMessage::HelloFromClient).await?;

        // Await welcoming response from server.
        match self.stream.receive::<ServerToClientMessage>().await? {
            ServerToClientMessage::WelcomeClient => {
                log::trace!("Established connection to server!")
            } // continue
            other => bail!("Expected WelcomeClient, received: {:?}", other),
        }

        // Process subcommands.
        match self.args.command.clone() {
            Command::Cmd {
                cpus,
                ram_mb,
                stdout,
                stderr,
                args,
            } => {
                let CmdArgs::Args(cmd) = args;

                if cmd.is_empty() {
                    bail!("Empty command!");
                }

                // This command requires authentification.
                self.authenticate().await?;

                // Issue job.
                let cwd = std::env::current_dir()?;
                let cwd = canonicalize(cwd)?;
                let resources = Resources::new(cpus, ram_mb);
                let job_info = JobInfo::new(cmd, cwd, resources, stdout, stderr);
                let message = ClientToServerMessage::IssueJob(job_info);
                self.stream.send(&message).await?;

                // Await acceptance.
                match self.stream.receive::<ServerToClientMessage>().await? {
                    ServerToClientMessage::AcceptJob(job_info) => {
                        log::debug!("Job submitted successfully!");
                        log::info!("Job ID: {}", job_info.id);
                    }
                    other => {
                        bail!("Expected AcceptJob, received: {:?}", other);
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
                self.stream.send(&message).await?;

                // Await results.
                match self.stream.receive::<ServerToClientMessage>().await? {
                    ServerToClientMessage::JobList {
                        job_infos,
                        jobs_pending,
                        jobs_offered,
                        jobs_running,
                        jobs_succeeded,
                        jobs_failed,
                        jobs_canceled,
                        job_avg_run_time_seconds,
                        remaining_jobs_eta_seconds,
                    } => {
                        print::job_list(
                            job_infos,
                            jobs_pending,
                            jobs_offered,
                            jobs_running,
                            jobs_succeeded,
                            jobs_failed,
                            jobs_canceled,
                            job_avg_run_time_seconds,
                            remaining_jobs_eta_seconds,
                        );
                    }
                    other => {
                        bail!("Expected JobList, received: {:?}", other);
                    }
                }
            }
            Command::ShowJob { id } => {
                // Query job.
                let message = ClientToServerMessage::ShowJob { id };
                self.stream.send(&message).await?;

                // Await results.
                match self.stream.receive::<ServerToClientMessage>().await? {
                    ServerToClientMessage::JobInfo {
                        job_info,
                        stdout_text,
                        stderr_text,
                    } => print::job_info(job_info, stdout_text, stderr_text),
                    ServerToClientMessage::RequestResponse { success, text } if !success => {
                        println!("{}", text);
                    }
                    other => {
                        bail!("Expected JobInfo, received: {:?}", other);
                    }
                }
            }
            Command::RemoveJob { id, kill } => {
                // This command requires authentification.
                self.authenticate().await?;

                // Remove job from queue.
                let message = ClientToServerMessage::RemoveJob { id, kill };
                self.stream.send(&message).await?;

                // Await results.
                match self.stream.receive::<ServerToClientMessage>().await? {
                    ServerToClientMessage::RequestResponse { success: _, text } => {
                        println!("{}", text)
                    }
                    other => {
                        bail!("Expected RequestResponse, received: {:?}", other);
                    }
                }
            }
            Command::ListWorkers => {
                // Query workers.
                self.stream
                    .send(&ClientToServerMessage::ListWorkers)
                    .await?;

                // Await results.
                match self.stream.receive::<ServerToClientMessage>().await? {
                    ServerToClientMessage::WorkerList(worker_list) => {
                        print::worker_list(worker_list);
                    }
                    other => {
                        bail!("Expected WorkerList, received: {:?}", other);
                    }
                }
            }
            Command::ShowWorker { id } => {
                // Query worker.
                let message = ClientToServerMessage::ShowWorker { id };
                self.stream.send(&message).await?;

                // Await results.
                match self.stream.receive::<ServerToClientMessage>().await? {
                    ServerToClientMessage::WorkerInfo(worker_info) => {
                        print::worker_info(worker_info)
                    }
                    ServerToClientMessage::RequestResponse { success, text } if !success => {
                        println!("{}", text);
                    }
                    other => {
                        bail!("Expected WorkerInfo, received: {:?}", other);
                    }
                }
            }
        }

        // Say bye to gracefully shut down connection.
        self.stream.send(&ClientToServerMessage::Bye).await?;

        Ok(())
    }

    async fn authenticate(&mut self) -> Result<()> {
        // Request authentification.
        self.stream
            .send(&ClientToServerMessage::AuthRequest)
            .await?;

        // Await authentification challenge.
        match self.stream.receive::<ServerToClientMessage>().await? {
            ServerToClientMessage::AuthChallenge(salt) => {
                // Calculate response.
                let salted_secret = self.config.common_settings.shared_secret.clone() + &salt;
                let salted_secret = salted_secret.into_bytes();
                let mut hasher = Sha256::new();
                hasher.update(salted_secret);
                let response = hasher.finalize().to_vec();
                let response = general_purpose::STANDARD_NO_PAD.encode(response);

                // Send response back to server.
                let message = ClientToServerMessage::AuthResponse(response);
                self.stream.send(&message).await?;
            }
            other => {
                bail!("Expected AuthChallenge, received: {:?}", other);
            }
        }

        // Await authentification confirmation.
        match self.stream.receive::<ServerToClientMessage>().await? {
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
}
