use crate::job_manager::{Manager, Worker};
use anyhow::{anyhow, Result};
use chrono::Utc;
use kueue::{
    config::Config,
    messages::{
        stream::{MessageError, MessageStream},
        ServerToWorkerMessage, WorkerToServerMessage,
    },
    structs::JobStatus,
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
    rejected_jobs: BTreeSet<usize>,
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
        let id = worker.lock().unwrap().id;

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
            rejected_jobs: BTreeSet::new(),
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
        match self.stream.send(&message).await {
            Ok(()) => {
                log::trace!("Send authentification challenge!");
            }
            Err(e) => {
                log::error!("Failed to send AuthChallenge: {}", e);
                return;
            }
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
                        // Offer jobs, if slots are free.
                        let free_slots = self.worker.lock().unwrap().info.free_slots();
                        if free_slots {
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

    fn is_authenticated(&self) -> Result<()> {
        if self.authenticated {
            Ok(())
        } else {
            Err(anyhow!("Worker {} is not authenticated!", self.name))
        }
    }

    async fn handle_message(&mut self, message: WorkerToServerMessage) -> Result<()> {
        match message {
            WorkerToServerMessage::AuthResponse(response) => {
                // Calculate baseline result.
                let salted_secret = self.config.shared_secret.clone() + &self.salt;
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
            WorkerToServerMessage::UpdateHwInfo(hw_info) => {
                // Update information in shared worker object.
                self.worker.lock().unwrap().info.hw = hw_info;
                Ok(()) // No response to worker needed.
            }
            WorkerToServerMessage::UpdateLoadInfo(load_info) => {
                let mut worker_lock = self.worker.lock().unwrap();
                // Update information in shared worker object.
                worker_lock.info.load = load_info;
                // This happens regularily, indicating that the worker is still alive.
                worker_lock.info.last_updated = Utc::now();
                Ok(()) // No response to worker needed.
            }
            WorkerToServerMessage::UpdateJobStatus(job_info) => {
                self.is_authenticated()?;

                // Update job info with whatever the worker sends us.
                // TODO: Probably handle more specifically?
                let job = self.manager.lock().unwrap().get_job(job_info.id);
                if let Some(job) = job {
                    // Just a small check: See if job is associated with worker.
                    let worker_id = job.lock().unwrap().worker_id;
                    match worker_id {
                        Some(id) if id == self.id => {} // all good.
                        _ => {
                            return Err(anyhow!(
                                "Job not associated with worker {}: {:?}",
                                self.name,
                                job_info
                            ));
                        }
                    }

                    // Update status.
                    job.lock().unwrap().info.status = job_info.status.clone();

                    // Update worker if the job has finished.
                    if job_info.status.is_finished() || job_info.status.is_canceled() {
                        let free_slots = {
                            let mut worker_lock = self.worker.lock().unwrap();
                            worker_lock.info.jobs_running.remove(&job_info.id);
                            worker_lock.info.free_slots()
                        };

                        // Offer further jobs.
                        if free_slots {
                            self.offer_pending_job().await?;
                        }
                    }
                } else {
                    // Unexpected but we can continue running.
                    log::error!("Updated job not found: {:?}", job_info);
                }
                Ok(())
            }
            WorkerToServerMessage::UpdateJobResults {
                job_id,
                stdout_text,
                stderr_text,
            } => {
                self.is_authenticated()?;

                // Update job results with whatever the worker sends us.
                let option_job = self.manager.lock().unwrap().get_job(job_id);
                if let Some(job) = option_job {
                    let mut job_lock = job.lock().unwrap();

                    // Just a small check: See if job is associated with worker.
                    match job_lock.worker_id {
                        Some(id) if id == self.id => {} // all good.
                        _ => {
                            return Err(anyhow!(
                                "Job not associated with worker {}: {:?}",
                                self.name,
                                job_lock.info
                            ));
                        }
                    }

                    // Update results.
                    job_lock.stdout_text = stdout_text;
                    job_lock.stderr_text = stderr_text;
                } else {
                    // Unexpected but we can continue running.
                    log::error!("Updated job not found: ID={}", job_id);
                }
                Ok(())
            }
            // This will also trigger the first jobs offered to the worker
            WorkerToServerMessage::AcceptParallelJobs(num) => {
                self.is_authenticated()?;

                let free_slots = {
                    let mut worker_lock = self.worker.lock().unwrap();
                    worker_lock.info.max_parallel_jobs = num;
                    worker_lock.info.free_slots()
                };

                // Start offering jobs.
                if free_slots {
                    self.offer_pending_job().await?;
                }
                Ok(())
            }
            WorkerToServerMessage::AcceptJobOffer(job_info) => {
                self.is_authenticated()?;

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
                                // TODO!
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
                    let free_slots = {
                        let mut worker_lock = self.worker.lock().unwrap();
                        worker_lock.info.jobs_reserved.remove(&job_id);
                        worker_lock.info.jobs_running.insert(job_id);
                        worker_lock.info.free_slots()
                    };

                    // Offer further jobs.
                    if free_slots {
                        self.offer_pending_job().await?;
                    }
                } else {
                    return Err(anyhow!("Accepted job not found: {:?}", job_info));
                }
                Ok(())
            }
            WorkerToServerMessage::RejectJobOffer(job_info) => {
                self.is_authenticated()?;

                let option_job = self.manager.lock().unwrap().get_job(job_info.id);
                if let Some(job) = option_job {
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
                    let free_slots = {
                        let mut worker_lock = self.worker.lock().unwrap();
                        worker_lock.info.jobs_reserved.remove(&job_info.id);
                        worker_lock.info.free_slots()
                    };

                    // Offer further jobs.
                    if free_slots {
                        self.offer_pending_job().await?;
                    }
                } else {
                    return Err(anyhow!("Rejected job not found: {:?}", job_info));
                }
                Ok(())
            }
            WorkerToServerMessage::Bye => {
                log::trace!("Bye worker!");
                self.connection_closed = true;
                Ok(())
            }
        }
    }

    async fn offer_pending_job(&mut self) -> Result<(), MessageError> {
        let available_job = self
            .manager
            .lock()
            .unwrap()
            .get_job_waiting_for_assignment(&self.rejected_jobs);

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
                .jobs_reserved
                .insert(job_info.id);

            // Finally, send offer to worker.
            let job_offer = ServerToWorkerMessage::OfferJob(job_info);
            self.stream.send(&job_offer).await
        } else {
            Ok(()) // no job offered
        }
    }
}
