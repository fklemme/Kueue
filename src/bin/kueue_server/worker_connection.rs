use crate::job_manager::{Manager, Worker};
use chrono::Utc;
use kueue::{
    messages::{
        stream::{MessageError, MessageStream},
        ServerToWorkerMessage, WorkerToServerMessage,
    },
    structs::JobStatus,
};
use std::{
    error::Error,
    sync::{Arc, Mutex}, collections::BTreeSet,
};

pub struct WorkerConnection {
    id: u64,
    name: String,
    stream: MessageStream,
    ss: Arc<Mutex<Manager>>,
    worker: Arc<Mutex<Worker>>,
    rejected_jobs: BTreeSet<u64>,
    connection_closed: bool,
}

impl WorkerConnection {
    pub fn new(name: String, stream: MessageStream, ss: Arc<Mutex<Manager>>) -> Self {
        let worker = ss.lock().unwrap().add_new_worker(name.clone());
        let id = worker.lock().unwrap().id;
        WorkerConnection {
            id,
            name,
            stream,
            ss,
            worker,
            rejected_jobs: BTreeSet::new(),
            connection_closed: false,
        }
    }

    pub async fn run(&mut self) {
        // Hello/Welcome messages are already exchanged at this point.

        // Notify for newly available jobs
        let new_jobs = self.ss.lock().unwrap().new_jobs();

        while !self.connection_closed {
            tokio::select! {
                // Read and handle incoming messages
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
                // Or, get active when notified
                _ = new_jobs.notified() => {
                    let free_slots = self.worker.lock().unwrap().info.free_slots();

                    if free_slots {
                        if let Err(e) = self.offer_pending_job().await {
                            log::error!("Failed to offer new job: {}", e);
                            self.connection_closed = true; // end worker session
                        }
                    }
                }
            }
        }

        // Before we leave, update shared worker
        // TODO: Was this needed anywhere?
        //self.worker.lock().unwrap().connected = false;
    }

    async fn handle_message(
        &mut self,
        message: WorkerToServerMessage,
    ) -> Result<(), Box<dyn Error>> {
        match message {
            WorkerToServerMessage::UpdateHwInfo(hw_info) => {
                // Update information in shared worker object.
                self.worker.lock().unwrap().info.hw = hw_info;
                Ok(()) // No response to worker needed.
            }
            WorkerToServerMessage::UpdateLoadInfo(load_info) => {
                // Update information in shared worker object.
                self.worker.lock().unwrap().info.load = load_info;
                Ok(()) // No response to worker needed.
            }
            WorkerToServerMessage::UpdateJobStatus(job_info) => {
                // Update job info with whatever the worker sends us.
                let option_job = self.ss.lock().unwrap().get_job(job_info.id);
                if let Some(job) = option_job {
                    let (old_status, new_status_finished) = {
                        let mut job_lock = job.lock().unwrap();

                        // Just a small check: See if job is associated with worker.
                        match job_lock.worker_id {
                            Some(id) if id == self.id => {} // all good.
                            _ => {
                                return Err(format!(
                                    "Job not associated with worker {}: {:?}",
                                    self.name, job_lock.info
                                )
                                .into());
                            }
                        }

                        // Update status.
                        let old_status = job_lock.info.status.clone();
                        job_lock.info.status = job_info.status;

                        (old_status, job_lock.info.status.is_finished())
                    };

                    // Update worker if the job has finished.
                    if old_status.is_running() && new_status_finished {
                        let free_slots = {
                            let mut worker_lock = self.worker.lock().unwrap();
                            worker_lock.info.jobs_running -= 1;
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
            // This will also trigger the first jobs offered to the worker
            WorkerToServerMessage::AcceptParallelJobs(num) => {
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
                let option_job = self.ss.lock().unwrap().get_job(job_info.id);
                if let Some(job) = option_job {
                    let job_info = {
                        // Perform small check and update job status.
                        let mut job_lock = job.lock().unwrap();
                        match &job_lock.info.status {
                            JobStatus::Offered { issued, to } if to == &self.name => {
                                job_lock.info.status = JobStatus::Running {
                                    issued: issued.clone(),
                                    started: Utc::now(),
                                    on: self.name.clone(),
                                };
                            }
                            _ => {
                                return Err(format!(
                                    "Accepted job was not offered to worker {}: {:?}",
                                    self.name, job_lock.info.status
                                )
                                .into());
                            }
                        }
                        job_lock.info.clone()
                    };

                    // Update worker.
                    let free_slots = {
                        let mut worker_lock = self.worker.lock().unwrap();
                        worker_lock.info.jobs_reserved -= 1;
                        worker_lock.info.jobs_running += 1;
                        worker_lock.info.free_slots()
                    };

                    // Confirm job -> Worker will start execution
                    let message = ServerToWorkerMessage::ConfirmJobOffer(job_info);
                    self.stream.send(&message).await?;

                    // Offer further jobs.
                    if free_slots {
                        self.offer_pending_job().await?;
                    }
                } else {
                    return Err(format!("Accepted job not found: {:?}", job_info).into());
                }
                Ok(())
            }
            WorkerToServerMessage::RejectJobOffer(job_info) => {
                let option_job = self.ss.lock().unwrap().get_job(job_info.id);
                if let Some(job) = option_job {
                    // Perform small check and update job status.
                    {
                        let mut job_lock = job.lock().unwrap();
                        match &job_lock.info.status {
                            JobStatus::Offered { issued, to } if to == &self.name => {
                                job_lock.info.status = JobStatus::Pending {
                                    issued: issued.clone(),
                                };
                                // Remember reject and avoid fetching the same job again.
                                self.rejected_jobs.insert(job_lock.info.id);
                            }
                            _ => {
                                return Err(format!(
                                    "Rejected job was not offered to worker {}: {:?}",
                                    self.name, job_lock.info.status
                                )
                                .into());
                            }
                        }
                    };

                    // Update worker.
                    let free_slots = {
                        let mut worker_lock = self.worker.lock().unwrap();
                        worker_lock.info.jobs_reserved -= 1;
                        worker_lock.info.free_slots()
                    };

                    // Offer further jobs.
                    if free_slots {
                        self.offer_pending_job().await?;
                    }
                } else {
                    return Err(format!("Rejected job not found: {:?}", job_info).into());
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
            .ss
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
                        to: self.name.clone(),
                    }
                } else {
                    log::warn!(
                        "Job waiting for assignment was not set to pending: {:?}",
                        job_lock.info.status
                    );
                    JobStatus::Offered {
                        issued: Utc::now(),
                        to: self.name.clone(),
                    }
                };

                // Set worker reference.
                job_lock.worker_id = Some(self.id);

                job_lock.info.clone()
            };

            // Worker has one more job reserved. Prevents over-offering.
            self.worker.lock().unwrap().info.jobs_reserved += 1;

            // Finally, send offer to worker.
            let job_offer = ServerToWorkerMessage::OfferJob(job_info);
            self.stream.send(&job_offer).await
        } else {
            Ok(()) // no job offered
        }
    }
}
