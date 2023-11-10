use crate::{
    config::Config,
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, ServerToClientMessage},
    server::job_manager::Manager,
    structs::{JobInfo, JobStatus},
};
use anyhow::{bail, Result};
use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use sha2::{Digest, Sha256};
use std::{
    cmp::max,
    sync::{Arc, Mutex, RwLock},
};
use tokio::sync::mpsc;

pub struct ClientConnection {
    stream: MessageStream,
    config: Arc<RwLock<Config>>,
    manager: Arc<Mutex<Manager>>,
    job_updated_tx: mpsc::Sender<u64>,
    job_updated_rx: mpsc::Receiver<u64>,
    authenticated: bool,
    salt: String,
    connection_closed: bool,
}

impl ClientConnection {
    /// Construct a new ClientConnection.
    pub fn new(
        stream: MessageStream,
        config: Arc<RwLock<Config>>,
        manager: Arc<Mutex<Manager>>,
    ) -> Self {
        let (job_updated_tx, job_updated_rx) = mpsc::channel::<u64>(100);

        // Salt is generated for each client connection.
        let salt: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(64)
            .map(char::from)
            .collect();

        ClientConnection {
            stream,
            config,
            manager,
            job_updated_tx,
            job_updated_rx,
            authenticated: false,
            salt,
            connection_closed: false,
        }
    }

    /// Run client connection, handling communication with the remote client.
    pub async fn run(&mut self) {
        // Hello/Welcome messages are already exchanged at this point.

        let mut shutdown_rx = self.manager.lock().unwrap().shutdown_rx.clone();

        while !self.connection_closed {
            tokio::select! {
                // Read and handle incoming messages.
                message = self.stream.receive::<ClientToServerMessage>() => {
                    match message {
                        Ok(message) => {
                            if let Err(e) = self.handle_message(message).await {
                                log::error!("Failed to handle message: {}", e);
                                self.connection_closed = true; // end client session
                            }
                        }
                        Err(e) => {
                            log::error!("Error while receiving message: {}", e);
                            self.connection_closed = true; // end client session
                        }
                    }
                }
                // Or, get active when observed job is updated.
                Some(job_id) = self.job_updated_rx.recv() => {
                    // Get job.
                    let job = self.manager.lock().unwrap().get_job(job_id);

                    if let Some(job) = job {
                        // Send job update to client.
                        let message = ServerToClientMessage::JobUpdated(job.lock().unwrap().info.clone());
                        if let Err(e) = self.stream.send(&message).await {
                            log::error!("Failed to send job notification: {}", e);
                            self.connection_closed = true; // end client session
                        }
                    } else {
                        log::error!("Notifying job not found: {}", job_id);
                        self.connection_closed = true; // end client session
                    };
                }
                // Or, close the connection if server is shutting down.
                _ = shutdown_rx.changed() => {
                    log::info!("Closing connection to client!");
                    if let Err(e) = self.stream.send(&ServerToClientMessage::Bye).await {
                        log::error!("Failed to send bye: {}", e);
                    }
                    self.connection_closed = true; // end client session
                }
            }
        }
    }

    /// Dispatch incoming message based on variant.
    async fn handle_message(&mut self, message: ClientToServerMessage) -> Result<()> {
        match message {
            ClientToServerMessage::AuthRequest => self.on_auth_request().await,
            ClientToServerMessage::AuthResponse(response) => self.on_auth_response(response).await,
            ClientToServerMessage::IssueJob(job_info) => self.on_issue_job(job_info).await,
            ClientToServerMessage::ListJobs {
                num_jobs,
                pending,
                offered,
                running,
                succeeded,
                failed,
                canceled,
            } => {
                self.on_list_jobs(
                    num_jobs, pending, offered, running, succeeded, failed, canceled,
                )
                .await
            }
            ClientToServerMessage::ShowJob { job_id } => self.on_show_job(job_id).await,
            ClientToServerMessage::ObserveJob { job_id } => self.on_observe_job(job_id).await,
            ClientToServerMessage::RemoveJob { job_id, kill } => {
                self.on_remove_job(job_id, kill).await
            }
            ClientToServerMessage::CleanJobs { all } => self.on_clean_jobs(all).await,
            ClientToServerMessage::ListWorkers => self.on_list_workers().await,
            ClientToServerMessage::ShowWorker { worker_id } => self.on_show_worker(worker_id).await,
            ClientToServerMessage::ListResources => self.on_list_resources().await,
            ClientToServerMessage::Bye => {
                log::trace!("Connection closed by client!");
                self.connection_closed = true;
                Ok(())
            }
        }
    }

    /// Returns error if client is not authenticated.
    async fn is_authenticated(&mut self) -> Result<()> {
        if self.authenticated {
            Ok(())
        } else {
            // We are nice to the client and let
            // them know why their request failed.
            let message = ServerToClientMessage::RequestResponse {
                success: false,
                text: "Not authenticated!".into(),
            };
            self.stream.send(&message).await?;
            // Close connection with an error message.
            bail!("Client is not authenticated!")
        }
    }

    /// Called upon receiving ClientToServerMessage::AuthRequest.
    async fn on_auth_request(&mut self) -> Result<()> {
        // Send salt to client.
        let message = ServerToClientMessage::AuthChallenge {
            salt: self.salt.clone(),
        };
        self.stream.send(&message).await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::AuthResponse.
    async fn on_auth_response(&mut self, response: String) -> Result<()> {
        // Calculate baseline result.
        let salted_secret = self
            .config
            .read()
            .unwrap()
            .common_settings
            .shared_secret
            .clone()
            + &self.salt;
        let salted_secret = salted_secret.into_bytes();
        let mut hasher = Sha256::new();
        hasher.update(salted_secret);
        let baseline = hasher.finalize().to_vec();
        let baseline = general_purpose::STANDARD_NO_PAD.encode(baseline);

        // Update status and send reply.
        if response == baseline {
            self.authenticated = true;
        } else {
            // After failed attempt, change the salt.
            self.salt = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(64)
                .map(char::from)
                .collect();
        }

        let message = ServerToClientMessage::AuthAccepted(self.authenticated);
        self.stream.send(&message).await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::IssueJob.
    async fn on_issue_job(&mut self, job_info: JobInfo) -> Result<()> {
        self.is_authenticated().await?;

        // Check if job can ever be processed. (job slots)
        if job_info.worker_resources.job_slots
            > self
                .config
                .read()
                .unwrap()
                .server_settings
                .global_max_parallel_jobs
        {
            // Send reject to client.
            let message = ServerToClientMessage::RejectJob {
                job_info,
                reason: "Job requires more slots than the server will allow at once! See global_max_parallel_jobs setting in config.".to_string()
            };
            self.stream.send(&message).await?;
            return Ok(());
        }

        // Check if job can ever be processed. (global resources)
        let mut reject_reason = None;
        if let Some(job_global_resources) = &job_info.global_resources {
            if let Some(config_global_resources) = &self.config.read().unwrap().global_resources {
                for (resource, required_amount) in job_global_resources {
                    match config_global_resources.get(resource) {
                        Some(available_amount) if available_amount < required_amount => {
                            reject_reason = Some(format!(
                                "Required resource exceeds limits configured on the server: max. {resource}={available_amount}"
                            ));
                            break;
                        }
                        None => {
                            reject_reason = Some(format!(
                                "Required resource not configured on server: {resource}"
                            ));
                            break;
                        }
                        _ => {} // else, required resource is fine
                    }
                }
                // If we haven't set a reject_reason at this point, everything
                // is fine and global resources fit into the server limits.
            } else {
                reject_reason = Some(format!(
                    "No global resources configured on the server! Requested resources: {job_global_resources:?}"
                ));
            }
        };
        if let Some(reason) = reject_reason {
            // Send reject to client.
            let message = ServerToClientMessage::RejectJob { job_info, reason };
            self.stream.send(&message).await?;
            return Ok(());
        }

        // If we passed all the checks, we can process the job request.

        // Add new job.
        let job_info = {
            let mut manager = self.manager.lock().unwrap();
            let job = manager.add_new_job(job_info);
            let job_info = job.lock().unwrap().info.clone();

            // Notify workers.
            manager.notify_new_jobs.notify_waiters();

            job_info
        };

        log::debug!("New job {} received from client!", job_info.job_id);

        // Send response to client.
        self.stream
            .send(&ServerToClientMessage::AcceptJob(job_info))
            .await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::ListJobs.
    async fn on_list_jobs(
        &mut self,
        num_jobs: u64,
        pending: bool,
        offered: bool,
        running: bool,
        succeeded: bool,
        failed: bool,
        canceled: bool,
    ) -> Result<()> {
        // Get job and worker lists.
        let mut job_infos = self.manager.lock().unwrap().get_all_job_infos();

        // Count total number of pending/offered/running/etc jobs.
        let jobs_pending = job_infos
            .iter()
            .filter(|job_info| job_info.status.is_pending())
            .count() as u64;
        let jobs_offered = job_infos
            .iter()
            .filter(|job_info| job_info.status.is_offered())
            .count() as u64;
        let jobs_running = job_infos
            .iter()
            .filter(|job_info| job_info.status.is_running())
            .count() as u64;
        let jobs_succeeded = job_infos
            .iter()
            .filter(|job_info| job_info.status.has_succeeded())
            .count() as u64;
        let jobs_failed = job_infos
            .iter()
            .filter(|job_info| job_info.status.has_failed())
            .count() as u64;
        let jobs_canceled = job_infos
            .iter()
            .filter(|job_info| job_info.status.is_canceled())
            .count() as u64;

        // Gather some information for metrics.
        let now = Utc::now();
        let running_jobs_run_times: Vec<i64> = job_infos
            .iter()
            .filter(|job_info| job_info.status.is_running())
            .map(|job_info| match job_info.status {
                JobStatus::Running { started, .. } => (now - started).num_seconds(),
                _ => 0,
            })
            .collect();
        let finished_jobs_run_times: Vec<i64> = job_infos
            .iter()
            .filter(|job_info| job_info.status.is_finished())
            .map(|job_info| match job_info.status {
                JobStatus::Finished {
                    run_time_seconds, ..
                } => run_time_seconds,
                _ => 0,
            })
            .collect();

        // Calculate average job runtime.
        let job_avg_run_time_seconds = if !finished_jobs_run_times.is_empty() {
            let finished_jobs_avg =
                finished_jobs_run_times.iter().sum::<i64>() / finished_jobs_run_times.len() as i64;
            let all_jobs_avg = running_jobs_run_times
                .iter()
                .chain(finished_jobs_run_times.iter())
                .sum::<i64>()
                / (running_jobs_run_times.len() + finished_jobs_run_times.len()) as i64;
            max(finished_jobs_avg, all_jobs_avg)
        } else if !running_jobs_run_times.is_empty() {
            *running_jobs_run_times.iter().max().unwrap()
        } else {
            0
        };

        // Calculate remaining jobs ETA. This includes a fair bit of simplifications.
        let running_job_eta_seconds = if jobs_running > 0 {
            let most_recent_job_run_time = *running_jobs_run_times.iter().min().unwrap();
            // We assume that the most recent job needs the avg job runtime to finish.
            max(0, job_avg_run_time_seconds - most_recent_job_run_time)
        } else {
            0
        };
        // As simplification, we assume that all future jobs will run with
        // the same degree of parallelization as the currently running jobs.
        let pending_jobs_eta_seconds = if jobs_pending > jobs_running {
            if jobs_running > 0 {
                jobs_pending as i64 * job_avg_run_time_seconds / jobs_running as i64
            } else {
                jobs_pending as i64 * job_avg_run_time_seconds
            }
        } else if jobs_pending > 0 {
            job_avg_run_time_seconds
        } else {
            0
        };
        let remaining_jobs_eta_seconds = running_job_eta_seconds + pending_jobs_eta_seconds;

        // Filter job list based on status.
        if pending || offered || running || succeeded || failed || canceled {
            job_infos.retain(|job_info| match job_info.status {
                JobStatus::Pending { .. } => pending,
                JobStatus::Offered { .. } => offered,
                JobStatus::Running { .. } => running,
                JobStatus::Finished { return_code, .. } => {
                    (return_code == 0 && succeeded) || (return_code != 0 && failed)
                }
                JobStatus::Canceled { .. } => canceled,
            });
        }

        // Trim potentially long job list.
        if job_infos.len() > num_jobs as usize {
            let start = job_infos.len() - num_jobs as usize;
            job_infos.drain(..start);
        }

        // Send response to client.
        let message = ServerToClientMessage::JobList {
            job_infos,
            jobs_pending,
            jobs_offered,
            jobs_running,
            jobs_succeeded,
            jobs_failed,
            jobs_canceled,
            job_avg_run_time_seconds,
            remaining_jobs_eta_seconds,
        };
        self.stream.send(&message).await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::ShowJob.
    async fn on_show_job(&mut self, job_id: u64) -> Result<()> {
        // Get job.
        let job = self.manager.lock().unwrap().get_job(job_id);

        let message = if let Some(job) = job {
            let job_lock = job.lock().unwrap();
            ServerToClientMessage::JobInfo {
                job_info: job_lock.info.clone(),
                stdout_text: job_lock.stdout_text.clone(),
                stderr_text: job_lock.stderr_text.clone(),
            }
        } else {
            ServerToClientMessage::RequestResponse {
                success: false,
                text: "Job not found!".into(),
            }
        };
        self.stream.send(&message).await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::ObserveJob.
    async fn on_observe_job(&mut self, job_id: u64) -> Result<()> {
        // Get job.
        let job = self.manager.lock().unwrap().get_job(job_id);

        let message = if let Some(job) = job {
            // Register as an observer.
            let mut job_lock = job.lock().unwrap();
            job_lock.observers.push(self.job_updated_tx.clone());

            // Send first update immediately (also as confirmation).
            ServerToClientMessage::JobUpdated(job_lock.info.clone())
        } else {
            ServerToClientMessage::RequestResponse {
                success: false,
                text: "Job not found!".into(),
            }
        };
        self.stream.send(&message).await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::RemoveJob.
    async fn on_remove_job(&mut self, job_id: u64, kill: bool) -> Result<()> {
        self.is_authenticated().await?;

        // Cancel job and send message back to client.
        let result = self.manager.lock().unwrap().cancel_job(job_id, kill);
        let message = match result {
            Ok(Some(tx)) => {
                // Signal kill to the worker.
                if kill {
                    tx.send(job_id).await?;
                }
                ServerToClientMessage::RequestResponse {
                    success: true,
                    text: format!("Canceled job ID={}!", job_id),
                }
            }
            Ok(None) => ServerToClientMessage::RequestResponse {
                success: true,
                text: format!("Canceled job ID={}!", job_id),
            },
            Err(e) => ServerToClientMessage::RequestResponse {
                success: false,
                text: e.to_string(),
            },
        };
        self.stream.send(&message).await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::CleanJobs.
    async fn on_clean_jobs(&mut self, all: bool) -> Result<()> {
        self.is_authenticated().await?;

        self.manager.lock().unwrap().clean_jobs(all);
        let message = ServerToClientMessage::RequestResponse {
            success: true,
            text: "Removed finished and canceled jobs!".to_string(),
        };
        self.stream.send(&message).await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::ListWorkers.
    async fn on_list_workers(&mut self) -> Result<()> {
        // Get worker list.
        let worker_list = self.manager.lock().unwrap().get_all_worker_infos();

        // Send response to client.
        self.stream
            .send(&ServerToClientMessage::WorkerList(worker_list))
            .await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::ShowWorker.
    async fn on_show_worker(&mut self, worker_id: u64) -> Result<()> {
        // Get worker.
        let worker = self.manager.lock().unwrap().get_worker(worker_id);

        let message = if let Some(worker) = worker {
            if let Some(worker) = worker.upgrade() {
                let worker_lock = worker.lock().unwrap();
                ServerToClientMessage::WorkerInfo(worker_lock.info.clone())
            } else {
                ServerToClientMessage::RequestResponse {
                    success: false,
                    text: "Worker no longer available!".into(),
                }
            }
        } else {
            ServerToClientMessage::RequestResponse {
                success: false,
                text: "Worker not found!".into(),
            }
        };
        self.stream.send(&message).await?;
        Ok(())
    }

    /// Called upon receiving ClientToServerMessage::ListResources.
    async fn on_list_resources(&mut self) -> Result<()> {
        // Get global resources.
        let used_resources = self.manager.lock().unwrap().get_used_global_resources();
        let total_resources = self.config.read().unwrap().global_resources.clone();

        // Send response to client.
        self.stream
            .send(&ServerToClientMessage::ResourceList {
                used_resources,
                total_resources,
            })
            .await?;
        Ok(())
    }
}
