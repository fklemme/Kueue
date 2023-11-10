use crate::{
    config::Config,
    messages::{
        stream::{MessageError, MessageStream},
        ServerToWorkerMessage, WorkerToServerMessage,
    },
    server::job_manager::{Manager, Worker},
    structs::{JobInfo, JobStatus, Resources, SystemInfo},
};
use anyhow::{bail, Result};
use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, RwLock},
};
use tokio::{sync::mpsc, task::yield_now};

pub struct WorkerConnection {
    worker_id: u64,
    worker_name: String,
    stream: MessageStream,
    config: Arc<RwLock<Config>>,
    manager: Arc<Mutex<Manager>>,
    /// Shared state in the job_manager, storing information about the worker.
    worker: Arc<Mutex<Worker>>,
    free_resources: Resources,
    rejected_jobs: BTreeSet<u64>,
    deferred_jobs: BTreeSet<u64>,
    kill_job_rx: mpsc::Receiver<u64>,
    authenticated: bool,
    salt: String,
    connection_closed: bool,
}

impl WorkerConnection {
    /// Construct a new WorkerConnection.
    pub fn new(
        worker_name: String,
        stream: MessageStream,
        config: Arc<RwLock<Config>>,
        manager: Arc<Mutex<Manager>>,
    ) -> Self {
        let (kill_job_tx, kill_job_rx) = mpsc::channel::<u64>(10);
        let worker = manager
            .lock()
            .unwrap()
            .add_new_worker(worker_name.clone(), kill_job_tx);
        let worker_id = worker.lock().unwrap().info.worker_id;

        // Salt is generated for each worker connection.
        let salt: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(64)
            .map(char::from)
            .collect();

        WorkerConnection {
            worker_id,
            worker_name,
            stream,
            config,
            manager,
            worker,
            free_resources: Resources::new(0, 0, 0),
            rejected_jobs: BTreeSet::new(),
            deferred_jobs: BTreeSet::new(),
            kill_job_rx,
            authenticated: false,
            salt,
            connection_closed: false,
        }
    }

    /// Run worker connection, handling communication with the remote worker.
    pub async fn run(&mut self) {
        // Hello/Welcome messages are already exchanged at this point.

        // Send authentication challenge.
        let message = ServerToWorkerMessage::AuthChallenge {
            salt: self.salt.clone(),
        };
        if let Err(e) = self.stream.send(&message).await {
            log::error!("Failed to send AuthChallenge: {}", e);
            return;
        }

        log::info!("Established connection to worker '{}'!", self.worker_name);

        // Notify for newly available jobs.
        let (notify_new_jobs, mut shutdown_rx) = {
            let manager = self.manager.lock().unwrap();
            (manager.notify_new_jobs.clone(), manager.shutdown_rx.clone())
        };

        while !self.connection_closed {
            tokio::select! {
                // Read and handle incoming messages.
                message = self.stream.receive::<WorkerToServerMessage>() => {
                    match message {
                        Ok(message) => {
                            if let Err(e) = self.handle_message(message).await {
                                log::error!("Failed to handle message: {}", e);
                                self.connection_closed = true; // end worker session
                            }
                        }
                        Err(e) => {
                            log::error!("{}", e);
                            self.connection_closed = true; // end worker session
                        }
                    }
                }
                // Or, get active when notified about new jobs.
                _ = notify_new_jobs.notified() => {
                    // First, check if this worker is still alive.
                    if self.worker.lock().unwrap().info.timed_out(self.config.read().unwrap().server_settings.worker_timeout_seconds) {
                        self.connection_closed = true; // end worker session
                    } else {
                        // Offer new job, if no job is currently offered.
                        if self.worker.lock().unwrap().info.jobs_offered.is_empty() {
                            // Give less-busy workers the chance to pick up the job.
                            self.yield_if_busy().await;
                            // Now, check for available jobs and pick up the next one.
                            if let Err(e) = self.offer_pending_job().await {
                                log::error!("Failed to offer new job: {}", e);
                                self.connection_closed = true; // end worker session
                            }
                        }
                    }
                }
                // Or, when signaled to kill a job on the worker.
                Some(job_id) = self.kill_job_rx.recv() => {
                    // Kill job.
                    let job = self.manager.lock().unwrap().get_job(job_id);
                    if let Some(job) = job {
                        let job_info = job.lock().unwrap().info.clone();
                        let message = ServerToWorkerMessage::KillJob(job_info);
                        if let Err(e) = self.stream.send(&message).await{
                            log::error!("Failed to send kill instruction: {}", e);
                            self.connection_closed = true; // end worker session
                        }

                        // We wait for "update_job_status" to clean up the job
                        // and send new offers to the worker.
                    } else {
                        log::error!("Job to be killed with ID={} not found!", job_id);
                    }
                }
                // Or, close the connection if server is shutting down.
                _ = shutdown_rx.changed() => {
                    log::info!("Closing connection to worker!");
                    if let Err(e) = self.stream.send(&ServerToWorkerMessage::Bye).await {
                        log::error!("Failed to send bye: {}", e);
                    }
                    self.connection_closed = true; // end worker session
                }
            }
        }
    }

    /// Dispatch incoming message based on variant.
    async fn handle_message(&mut self, message: WorkerToServerMessage) -> Result<()> {
        match message {
            WorkerToServerMessage::AuthResponse(response) => self.on_auth_response(response).await,
            WorkerToServerMessage::UpdateSystemInfo(system_info) => {
                self.on_update_system_info(system_info)
            }
            WorkerToServerMessage::UpdateJobStatus(job_info) => self.on_update_job_status(job_info),
            WorkerToServerMessage::UpdateJobResults {
                job_id,
                stdout_text,
                stderr_text,
            } => self.on_update_job_results(job_id, stdout_text, stderr_text),
            WorkerToServerMessage::UpdateResources(resources) => {
                self.on_update_resources(resources).await
            }
            WorkerToServerMessage::AcceptJobOffer(job_info) => {
                self.on_accept_job_offer(job_info).await
            }
            WorkerToServerMessage::DeferJobOffer(job_info) => {
                self.on_defer_job_offer(job_info).await
            }
            WorkerToServerMessage::RejectJobOffer(job_info) => {
                self.on_reject_job_offer(job_info).await
            }
            WorkerToServerMessage::Bye => {
                log::trace!("Connection closed by worker!");
                self.connection_closed = true;
                Ok(())
            }
        }
    }

    /// Returns error if worker is not authenticated.
    fn check_authenticated(&self) -> Result<()> {
        if self.authenticated {
            Ok(())
        } else {
            // Close connection with an error message.
            bail!("Worker {} is not authenticated!", self.worker_name)
        }
    }

    /// Called upon receiving WorkerToServerMessage::AuthResponse.
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
        }
        let message = ServerToWorkerMessage::AuthAccepted(self.authenticated);
        self.stream.send(&message).await?;

        if self.authenticated {
            Ok(())
        } else {
            // Close connection with an error. No second chance for workers.
            bail!("Worker {} failed to authenticate!", self.worker_name);
        }
    }

    /// Called upon receiving WorkerToServerMessage::UpdateSystemInfo.
    fn on_update_system_info(&self, system_info: SystemInfo) -> Result<()> {
        let mut worker_lock = self.worker.lock().unwrap();
        // Update information in shared worker object.
        worker_lock.info.system_info = system_info;
        // This happens regularly, indicating that the worker is still alive.
        worker_lock.info.last_updated = Utc::now();
        Ok(()) // No response to worker needed.
    }

    /// Called upon receiving WorkerToServerMessage::UpdateJobStatus.
    fn on_update_job_status(&self, job_info: JobInfo) -> Result<()> {
        self.check_authenticated()?;

        // Update job information from the worker.
        let job = self.manager.lock().unwrap().get_job(job_info.job_id);
        if let Some(job) = job {
            // See if job is associated with worker.
            let worker_id = job.lock().unwrap().worker_id;
            match worker_id {
                Some(worker_id) if worker_id == self.worker_id => {
                    // Update job and worker if the job has finished.
                    if job_info.status.is_finished() || job_info.status.is_canceled() {
                        log::debug!("Job {} finished on {}!", job_info.job_id, self.worker_name);
                        self.worker
                            .lock()
                            .unwrap()
                            .info
                            .jobs_running
                            .remove(&job_info.job_id);

                        let mut job = job.lock().unwrap();
                        job.info.status = job_info.status.clone();

                        // Notify observers of the job
                        job.notify_observers();
                    } else {
                        // At the moment, the worker will only send updates on completed jobs.
                        log::error!("Expected updated job to be finished: {:?}", job_info);
                    }
                }
                _ => {
                    log::error!(
                        "Job not associated with worker {}: {:?}",
                        self.worker_name,
                        job_info
                    );
                }
            }
        } else {
            log::error!("Updated job not found: {:?}", job_info);
        }
        Ok(())
    }

    /// Called upon receiving WorkerToServerMessage::UpdateJobResults.
    fn on_update_job_results(
        &self,
        job_id: u64,
        stdout_text: Option<String>,
        stderr_text: Option<String>,
    ) -> Result<()> {
        self.check_authenticated()?;

        // Update job results with whatever the worker sends us.
        let job = self.manager.lock().unwrap().get_job(job_id);
        if let Some(job) = job {
            let mut job_lock = job.lock().unwrap();

            // Just a small check: See if job is associated with worker.
            match job_lock.worker_id {
                Some(worker_id) if worker_id == self.worker_id => {
                    // Update results.
                    job_lock.stdout_text = stdout_text;
                    job_lock.stderr_text = stderr_text;
                }
                _ => {
                    log::error!(
                        "Job not associated with worker {}: {:?}",
                        self.worker_name,
                        job_lock.info
                    );
                }
            }
        } else {
            log::error!("Updated job not found: ID={}", job_id);
        }
        Ok(())
    }

    /// Called upon receiving WorkerToServerMessage::UpdateResources.
    async fn on_update_resources(&mut self, resources: Resources) -> Result<()> {
        self.check_authenticated()?;

        // Update resources.
        self.free_resources = resources.clone();

        // Forget all deferred jobs.
        self.deferred_jobs.clear();

        let no_job_offered = {
            let mut worker_lock = self.worker.lock().unwrap();

            // Keep copy of free resources in info.
            worker_lock.info.free_resources = resources;

            worker_lock.info.jobs_offered.is_empty()
        };

        // Offer new job.
        if no_job_offered {
            self.offer_pending_job().await?;
        }
        Ok(())
    }

    /// Called upon receiving WorkerToServerMessage::AcceptJobOffer.
    async fn on_accept_job_offer(&mut self, job_info: JobInfo) -> Result<()> {
        self.check_authenticated()?;

        let job = self.manager.lock().unwrap().get_job(job_info.job_id);
        if let Some(job) = job {
            let job_info = {
                // Perform small check and update job status.
                let mut job_lock = job.lock().unwrap();
                match &job_lock.info.status {
                    JobStatus::Offered {
                        issued,
                        offered: _,
                        worker,
                    } if worker == &self.worker_name => {
                        job_lock.info.status = JobStatus::Running {
                            issued: *issued,
                            started: Utc::now(),
                            worker: self.worker_name.clone(),
                        };
                    }
                    JobStatus::Canceled { .. } => {
                        log::debug!("Offered job has been canceled in the meantime!")
                        // TODO: Withdraw!
                    }
                    _ => bail!(
                        "Accepted job was not offered to worker {}: {:?}",
                        self.worker_name,
                        job_lock.info.status
                    ),
                }

                // Notify observers of the job
                job_lock.notify_observers();

                job_lock.info.clone()
            };

            log::debug!("Job {} accepted by {}!", job_info.job_id, self.worker_name);

            // Confirm job -> Worker will start execution
            let job_id = job_info.job_id; // copy before move
            let message = ServerToWorkerMessage::ConfirmJobOffer(job_info);
            self.stream.send(&message).await?;

            // Update worker.
            let mut worker_lock = self.worker.lock().unwrap();
            worker_lock.info.jobs_offered.remove(&job_id);
            worker_lock.info.jobs_running.insert(job_id);
        } else {
            bail!("Accepted job not found: {:?}", job_info);
        }
        Ok(())
    }

    /// Called upon receiving WorkerToServerMessage::DeferJobOffer.
    async fn on_defer_job_offer(&mut self, job_info: JobInfo) -> Result<()> {
        self.check_authenticated()?;

        let job = self.manager.lock().unwrap().get_job(job_info.job_id);
        if let Some(job) = job {
            // Perform small check and update job status.
            {
                let mut job_lock = job.lock().unwrap();
                match &job_lock.info.status {
                    JobStatus::Offered {
                        issued,
                        offered: _,
                        worker,
                    } if worker == &self.worker_name => {
                        job_lock.info.status = JobStatus::Pending { issued: *issued };
                        job_lock.worker_id = None;
                        // TODO: The job should also be made available again!

                        // Remember defer and avoid fetching the same job again soon.
                        self.deferred_jobs.insert(job_lock.info.job_id);
                    }
                    _ => bail!(
                        "Deferred job was not offered to worker {}: {:?}",
                        self.worker_name,
                        job_lock.info.status
                    ),
                }

                // Notify observers of the job
                job_lock.notify_observers();
            };

            log::debug!("Job {} deferred by {}!", job_info.job_id, self.worker_name);

            // Update worker.
            let no_jobs_offered = {
                let mut worker_lock = self.worker.lock().unwrap();
                worker_lock.info.jobs_offered.remove(&job_info.job_id);
                worker_lock.info.jobs_offered.is_empty()
            };

            // Offer new job.
            if no_jobs_offered {
                self.offer_pending_job().await?;
            }
        } else {
            bail!("Deferred job not found: {:?}", job_info);
        }
        Ok(())
    }

    /// Called upon receiving WorkerToServerMessage::RejectJobOffer.
    async fn on_reject_job_offer(&mut self, job_info: JobInfo) -> Result<()> {
        self.check_authenticated()?;

        let job = self.manager.lock().unwrap().get_job(job_info.job_id);
        if let Some(job) = job {
            // Perform small check and update job status.
            {
                let mut job_lock = job.lock().unwrap();
                match &job_lock.info.status {
                    JobStatus::Offered {
                        issued,
                        offered: _,
                        worker,
                    } if worker == &self.worker_name => {
                        job_lock.info.status = JobStatus::Pending { issued: *issued };
                        job_lock.worker_id = None;
                        // TODO: The job should also be made available again!

                        // Remember reject and avoid fetching the same job again.
                        self.rejected_jobs.insert(job_lock.info.job_id);
                    }
                    _ => bail!(
                        "Rejected job was not offered to worker {}: {:?}",
                        self.worker_name,
                        job_lock.info.status
                    ),
                }

                // Notify observers of the job
                job_lock.notify_observers();
            };

            log::debug!("Job {} rejected by {}!", job_info.job_id, self.worker_name);

            // Update worker.
            let no_jobs_offered = {
                let mut worker_lock = self.worker.lock().unwrap();
                worker_lock.info.jobs_offered.remove(&job_info.job_id);
                worker_lock.info.jobs_offered.is_empty()
            };

            // Offer new job.
            if no_jobs_offered {
                self.offer_pending_job().await?;
            }
        } else {
            bail!("Rejected job not found: {:?}", job_info);
        }
        Ok(())
    }

    async fn yield_if_busy(&self) {
        let info = self.worker.lock().unwrap().info.clone();

        // FIXME: Not sure how well this will work...
        if info.resource_load() > 0.1 {
            yield_now().await
        }
        if info.resource_load() > 0.25 {
            yield_now().await
        }
        if info.resource_load() > 0.5 {
            yield_now().await
        }
        if info.resource_load() > 0.75 {
            yield_now().await
        }
    }

    async fn offer_pending_job(&mut self) -> Result<(), MessageError> {
        let excluded_jobs: BTreeSet<u64> = self
            .rejected_jobs
            .iter()
            .chain(self.deferred_jobs.iter())
            .cloned()
            .collect();

        let available_job = self.manager.lock().unwrap().get_job_waiting_for_assignment(
            self.worker_id,
            &self.worker_name,
            &excluded_jobs,
            &self.free_resources,
        );

        if let Some(job) = available_job {
            let job_info = job.lock().unwrap().info.clone();

            // Worker has one more job reserved. Prevents over-offering.
            self.worker
                .lock()
                .unwrap()
                .info
                .jobs_offered
                .insert(job_info.job_id);

            // Finally, send offer to worker.
            let job_offer = ServerToWorkerMessage::OfferJob(job_info);
            self.stream.send(&job_offer).await
        } else {
            Ok(()) // no job offered
        }
    }
}
