use crate::job_manager::{Manager, Worker};
use anyhow::{anyhow, Result};
use chrono::Utc;
use kueue::{
    config::Config,
    messages::{
        stream::{MessageError, MessageStream},
        ServerToWorkerMessage, WorkerToServerMessage,
    },
    structs::{JobInfo, JobStatus, Resources},
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;

pub struct WorkerConnection {
    id: usize,
    name: String,
    stream: MessageStream,
    manager: Arc<Mutex<Manager>>,
    config: Config,
    worker: Arc<Mutex<Worker>>,
    free_resources: Resources,
    rejected_jobs: BTreeSet<usize>,
    defered_jobs: BTreeSet<usize>,
    kill_job_rx: mpsc::Receiver<usize>,
    connection_closed: bool,
    authenticated: bool,
    salt: String,
}

impl WorkerConnection {
    pub fn new(
        name: String,
        stream: MessageStream,
        manager: Arc<Mutex<Manager>>,
        config: Config,
    ) -> Self {
        let (kill_job_tx, kill_job_rx) = mpsc::channel::<usize>(10);
        let worker = manager
            .lock()
            .unwrap()
            .add_new_worker(name.clone(), kill_job_tx);
        let id = worker.lock().unwrap().info.id;

        // Salt is generated for each worker.
        let salt: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        WorkerConnection {
            id,
            name,
            stream,
            manager,
            config,
            worker,
            free_resources: Resources::new(0, 0),
            rejected_jobs: BTreeSet::new(),
            defered_jobs: BTreeSet::new(),
            kill_job_rx,
            connection_closed: false,
            authenticated: false,
            salt,
        }
    }

    pub async fn run(&mut self) {
        // Hello/Welcome messages are already exchanged at this point.

        // Send authentification challenge.
        let message = ServerToWorkerMessage::AuthChallenge(self.salt.clone());
        if let Err(e) = self.stream.send(&message).await {
            log::error!("Failed to send AuthChallenge: {}", e);
            return;
        }

        // Notify for newly available jobs.
        let new_jobs = self.manager.lock().unwrap().new_jobs();

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
                // Or, get active when notified.
                _ = new_jobs.notified() => {
                    // First, check if this worker is still alive.
                    if self.worker.lock().unwrap().info.timed_out() {
                        self.connection_closed = true; // end worker session
                    } else {
                        // Offer new job, if no job is currently offered.
                        if self .worker.lock().unwrap() .info.jobs_offered.is_empty() {
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
            }
        }
    }

    /// Dispatch incoming message based on variant.
    async fn handle_message(&mut self, message: WorkerToServerMessage) -> Result<()> {
        match message {
            WorkerToServerMessage::AuthResponse(response) => self.on_auth_response(response),
            WorkerToServerMessage::UpdateHwInfo(hw_info) => {
                let mut worker_lock = self.worker.lock().unwrap();
                // Update information in shared worker object.
                worker_lock.info.hw = hw_info;
                // This happens regularily, indicating that the worker is still alive.
                worker_lock.info.last_updated = Utc::now();
                Ok(()) // No response to worker needed.
            }
            WorkerToServerMessage::UpdateJobStatus(job_info) => self.on_update_job_status(job_info),
            WorkerToServerMessage::UpdateJobResults {
                job_id,
                stdout_text,
                stderr_text,
            } => self.on_update_job_results(job_id, stdout_text, stderr_text),
            WorkerToServerMessage::UpdateResources(resources) => {
                self.check_authenticated()?;

                // Update resources.
                self.free_resources = resources.clone();

                // Forget all defered jobs.
                self.defered_jobs.clear();

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
                log::trace!("Bye worker!");
                self.connection_closed = true;
                Ok(())
            }
        }
    }

    /// Called upon receiving WorkerToServerMessage::AuthResponse.
    fn on_auth_response(&mut self, response: String) -> Result<()> {
        // Calculate baseline result.
        let salted_secret = self.config.common_settings.shared_secret.clone() + &self.salt;
        let salted_secret = salted_secret.into_bytes();
        let mut hasher = Sha256::new();
        hasher.update(salted_secret);
        let baseline = hasher.finalize().to_vec();
        let baseline = base64::encode(baseline);

        // Pass or die!
        if response == baseline {
            self.authenticated = true;
            Ok(())
        } else {
            Err(anyhow!("Worker {} failed authentification!", self.name))
        }
    }

    /// Returns error if worker is not authenticated.
    fn check_authenticated(&self) -> Result<()> {
        if self.authenticated {
            Ok(())
        } else {
            Err(anyhow!("Worker {} is not authenticated!", self.name))
        }
    }

    /// Called upon receiving WorkerToServerMessage::UpdateJobStatus.
    fn on_update_job_status(&self, job_info: JobInfo) -> Result<()> {
        self.check_authenticated()?;

        // Update job information from the worker.
        let job = self.manager.lock().unwrap().get_job(job_info.id);
        if let Some(job) = job {
            // See if job is associated with worker.
            let worker_id = job.lock().unwrap().worker_id;
            match worker_id {
                Some(id) if id == self.id => {
                    // Update job and worker if the job has finished.
                    if job_info.status.is_finished() || job_info.status.is_canceled() {
                        job.lock().unwrap().info.status = job_info.status.clone();
                        self.worker
                            .lock()
                            .unwrap()
                            .info
                            .jobs_running
                            .remove(&job_info.id);
                    } else {
                        log::error!("Expected updated job to be finished: {:?}", job_info);
                    }
                }
                _ => {
                    log::error!(
                        "Job not associated with worker {}: {:?}",
                        self.name,
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
        job_id: usize,
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
                Some(id) if id == self.id => {
                    // Update results.
                    job_lock.stdout_text = stdout_text;
                    job_lock.stderr_text = stderr_text;
                }
                _ => {
                    log::error!(
                        "Job not associated with worker {}: {:?}",
                        self.name,
                        job_lock.info
                    );
                }
            }
        } else {
            log::error!("Updated job not found: ID={}", job_id);
        }
        Ok(())
    }

    /// Called upon receiving WorkerToServerMessage::AcceptJobOffer.
    async fn on_accept_job_offer(&mut self, job_info: JobInfo) -> Result<()> {
        self.check_authenticated()?;

        let job = self.manager.lock().unwrap().get_job(job_info.id);
        if let Some(job) = job {
            let job_info = {
                // Perform small check and update job status.
                let mut job_lock = job.lock().unwrap();
                match &job_lock.info.status {
                    JobStatus::Offered {
                        issued,
                        offered: _,
                        to,
                    } if to == &self.name => {
                        job_lock.info.status = JobStatus::Running {
                            issued: issued.clone(),
                            started: Utc::now(),
                            on: self.name.clone(),
                        };
                    }
                    JobStatus::Canceled { .. } => {
                        log::debug!("Offered job has been canceled in the meantime!")
                        // TODO: Withdraw!
                    }
                    _ => {
                        return Err(anyhow!(
                            "Accepted job was not offered to worker {}: {:?}",
                            self.name,
                            job_lock.info.status
                        ));
                    }
                }
                job_lock.info.clone()
            };

            // Confirm job -> Worker will start execution
            let job_id = job_info.id; // copy before move
            let message = ServerToWorkerMessage::ConfirmJobOffer(job_info);
            self.stream.send(&message).await?;

            // Update worker.
            let mut worker_lock = self.worker.lock().unwrap();
            worker_lock.info.jobs_offered.remove(&job_id);
            worker_lock.info.jobs_running.insert(job_id);
        } else {
            return Err(anyhow!("Accepted job not found: {:?}", job_info));
        }
        Ok(())
    }

    /// Called upon receiving WorkerToServerMessage::DeferJobOffer.
    async fn on_defer_job_offer(&mut self, job_info: JobInfo) -> Result<()> {
        self.check_authenticated()?;

        let job = self.manager.lock().unwrap().get_job(job_info.id);
        if let Some(job) = job {
            // Perform small check and update job status.
            {
                let mut job_lock = job.lock().unwrap();
                match &job_lock.info.status {
                    JobStatus::Offered {
                        issued,
                        offered: _,
                        to,
                    } if to == &self.name => {
                        job_lock.info.status = JobStatus::Pending {
                            issued: issued.clone(),
                        };
                        job_lock.worker_id = None;
                        // TODO: The job should also be made available again!

                        // Remember defer and avoid fetching the same job again soon.
                        self.defered_jobs.insert(job_lock.info.id);
                    }
                    _ => {
                        return Err(anyhow!(
                            "Defered job was not offered to worker {}: {:?}",
                            self.name,
                            job_lock.info.status
                        ));
                    }
                }
            };

            // Update worker.
            let no_jobs_offered = {
                let mut worker_lock = self.worker.lock().unwrap();
                worker_lock.info.jobs_offered.remove(&job_info.id);
                worker_lock.info.jobs_offered.is_empty()
            };

            // Offer new job.
            if no_jobs_offered {
                self.offer_pending_job().await?;
            }
        } else {
            return Err(anyhow!("Defered job not found: {:?}", job_info));
        }
        Ok(())
    }

    /// Called upon receiving WorkerToServerMessage::RejectJobOffer.
    async fn on_reject_job_offer(&mut self, job_info: JobInfo) -> Result<()> {
        self.check_authenticated()?;

        let job = self.manager.lock().unwrap().get_job(job_info.id);
        if let Some(job) = job {
            // Perform small check and update job status.
            {
                let mut job_lock = job.lock().unwrap();
                match &job_lock.info.status {
                    JobStatus::Offered {
                        issued,
                        offered: _,
                        to,
                    } if to == &self.name => {
                        job_lock.info.status = JobStatus::Pending {
                            issued: issued.clone(),
                        };
                        job_lock.worker_id = None;
                        // TODO: The job should also be made available again!

                        // Remember reject and avoid fetching the same job again.
                        self.rejected_jobs.insert(job_lock.info.id);
                    }
                    _ => {
                        return Err(anyhow!(
                            "Rejected job was not offered to worker {}: {:?}",
                            self.name,
                            job_lock.info.status
                        ));
                    }
                }
            };

            // Update worker.
            let no_jobs_offered = {
                let mut worker_lock = self.worker.lock().unwrap();
                worker_lock.info.jobs_offered.remove(&job_info.id);
                worker_lock.info.jobs_offered.is_empty()
            };

            // Offer new job.
            if no_jobs_offered {
                self.offer_pending_job().await?;
            }
        } else {
            return Err(anyhow!("Rejected job not found: {:?}", job_info));
        }
        Ok(())
    }

    async fn offer_pending_job(&mut self) -> Result<(), MessageError> {
        let excluded_jobs: BTreeSet<usize> = self
            .rejected_jobs
            .iter()
            .chain(self.defered_jobs.iter())
            .cloned()
            .collect();

        let available_job = self
            .manager
            .lock()
            .unwrap()
            .get_job_waiting_for_assignment(&excluded_jobs, &self.free_resources);

        if let Some(job) = available_job {
            let job_info = {
                let mut job_lock = job.lock().unwrap();

                // Update job status.
                job_lock.info.status = if let JobStatus::Pending { issued } = job_lock.info.status {
                    JobStatus::Offered {
                        issued,
                        offered: Utc::now(),
                        to: self.name.clone(),
                    }
                } else {
                    log::warn!(
                        "Job waiting for assignment was not set to pending: {:?}",
                        job_lock.info.status
                    );
                    JobStatus::Offered {
                        issued: Utc::now(),
                        offered: Utc::now(),
                        to: self.name.clone(),
                    }
                };

                // Set worker reference.
                job_lock.worker_id = Some(self.id);

                job_lock.info.clone()
            };

            // Worker has one more job reserved. Prevents over-offering.
            self.worker
                .lock()
                .unwrap()
                .info
                .jobs_offered
                .insert(job_info.id);

            // Finally, send offer to worker.
            let job_offer = ServerToWorkerMessage::OfferJob(job_info);
            self.stream.send(&job_offer).await
        } else {
            Ok(()) // no job offered
        }
    }
}
