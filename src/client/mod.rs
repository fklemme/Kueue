pub mod cli;
mod print;

use crate::{
    config::Config,
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::{JobInfo, JobStatus, Resources},
};
use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose, Engine};
use cli::{Cli, CmdArgs, Command};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs::canonicalize};
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
                log::debug!("Established connection to server!")
            } // continue
            other => bail!("Expected WelcomeClient, received: {:?}", other),
        }

        // Process subcommands.
        match self.args.command.clone() {
            Command::Cmd {
                job_slots,
                cpus,
                ram_mb,
                resources,
                stdout,
                stderr,
                wait,
                args,
            } => {
                let CmdArgs::Args(cmd) = args;

                if cmd.is_empty() {
                    bail!("Empty command!");
                }

                // This command requires authentication.
                self.authenticate().await?;

                // Collect job parameters.
                let cwd = std::env::current_dir()?;
                let cwd = canonicalize(cwd)?;
                let worker_resources = Resources::new(
                    job_slots,
                    cpus.unwrap_or(self.config.client_settings.job_default_cpus),
                    ram_mb.unwrap_or(self.config.client_settings.job_default_ram_mb),
                );

                // Parse resource parameters into map.
                let mut global_resources: BTreeMap<String, u64> = BTreeMap::new();
                for resource in resources {
                    let parts: Vec<_> = resource.split('=').collect();
                    if parts.len() == 1 {
                        global_resources.insert(resource, 1);
                    } else if parts.len() == 2 {
                        let res_key = parts.first().unwrap().to_string();
                        let amount: u64 = parts.last().unwrap().parse().map_err(|err| {
                            anyhow!("Failed to parse resource: '{}', {}", resource, err)
                        })?;
                        global_resources.insert(res_key, amount);
                    } else {
                        bail!("Failed to parse resource: {}", resource);
                    }
                }
                let global_resources = if global_resources.is_empty() {
                    None
                } else {
                    Some(global_resources)
                };

                // Issue new job.
                let job_info =
                    JobInfo::new(cmd, cwd, worker_resources, global_resources, stdout, stderr);
                let message = ClientToServerMessage::IssueJob(job_info);
                self.stream.send(&message).await?;

                // Await acceptance.
                let job_id = match self.stream.receive::<ServerToClientMessage>().await? {
                    ServerToClientMessage::AcceptJob(job_info) => {
                        log::debug!("Job submitted successfully!");
                        job_info.job_id
                    }
                    ServerToClientMessage::RejectJob {
                        job_info: _,
                        reason,
                    } => {
                        bail!("Job rejected by server: {reason}");
                    }
                    other => {
                        bail!("Expected AcceptJob or RejectJob, received: {other:?}");
                    }
                };

                // Block until the job has been finished or canceled.
                if wait {
                    // Get notified when job is updated.
                    let message = ClientToServerMessage::ObserveJob { job_id };
                    self.stream.send(&message).await?;

                    // Await results.
                    loop {
                        match self.stream.receive::<ServerToClientMessage>().await? {
                            ServerToClientMessage::JobUpdated(job_info) => {
                                log::debug!("Job updated: {:?}", job_info.status);
                                match job_info.status {
                                    JobStatus::Finished { return_code, .. } => {
                                        // Print return code to stdout.
                                        println!("{}", return_code);
                                        break;
                                    }
                                    JobStatus::Canceled { .. } => break,
                                    _ => {}
                                }
                            }
                            other => {
                                bail!("Expected NotifyJob, received: {other:?}");
                            }
                        }
                    }
                } else {
                    // Print job ID to stdout.
                    println!("{}", job_id);
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
                    num_jobs: num_jobs.unwrap_or(print::term_size().1 as u64 - 4),
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
                        bail!("Expected JobList, received: {other:?}");
                    }
                }
            }
            Command::ShowJob { job_id } => {
                // Query job.
                let message = ClientToServerMessage::ShowJob { job_id };
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
            Command::WaitJob { job_id } => {
                // Get notified when job is updated.
                let message = ClientToServerMessage::ObserveJob { job_id };
                self.stream.send(&message).await?;

                // Await results.
                loop {
                    match self.stream.receive::<ServerToClientMessage>().await? {
                        ServerToClientMessage::JobUpdated(job_info) => {
                            log::debug!("Job updated: {:?}", job_info.status);
                            match job_info.status {
                                JobStatus::Finished { .. } => break,
                                JobStatus::Canceled { .. } => break,
                                _ => {}
                            }
                        }
                        other => {
                            bail!("Expected NotifyJob, received: {other:?}");
                        }
                    }
                }
            }
            Command::RemoveJob { job_id, kill } => {
                // This command requires authentication.
                self.authenticate().await?;

                // Remove job from queue.
                let message = ClientToServerMessage::RemoveJob { job_id, kill };
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
            Command::CleanJobs { all } => {
                // This command requires authentication.
                self.authenticate().await?;

                // Remove finished and canceled jobs.
                let message = ClientToServerMessage::CleanJobs { all };
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
            Command::ShowWorker { worker_id } => {
                // Query worker.
                let message = ClientToServerMessage::ShowWorker { worker_id };
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
            Command::ListResources => {
                // Query resources.
                self.stream
                    .send(&ClientToServerMessage::ListResources)
                    .await?;

                // Await results.
                match self.stream.receive::<ServerToClientMessage>().await? {
                    ServerToClientMessage::ResourceList {
                        used_resources,
                        total_resources,
                    } => {
                        print::resource_list(used_resources, total_resources);
                    }
                    other => {
                        bail!("Expected ResourceList, received: {:?}", other);
                    }
                }
            }
            // Shell completion is already handled in main function.
            Command::Complete { .. } => unreachable!(),
        }

        // Say bye to gracefully shut down connection.
        self.stream.send(&ClientToServerMessage::Bye).await?;

        Ok(())
    }

    async fn authenticate(&mut self) -> Result<()> {
        // Request authentication.
        self.stream
            .send(&ClientToServerMessage::AuthRequest)
            .await?;

        // Await authentication challenge.
        match self.stream.receive::<ServerToClientMessage>().await? {
            ServerToClientMessage::AuthChallenge { salt } => {
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

        // Await authentication confirmation.
        match self.stream.receive::<ServerToClientMessage>().await? {
            ServerToClientMessage::AuthAccepted(accepted) => {
                if accepted {
                    Ok(())
                } else {
                    bail!("Authentication failed!")
                }
            }
            other => bail!("Expected AuthAccepted, received: {:?}", other),
        }
    }
}
