use crate::shared_state::{SharedState, Worker};
use chrono::Utc;
use kueue::{
    messages::{
        stream::{MessageError, MessageStream},
        ServerToWorkerMessage, WorkerToServerMessage,
    },
    structs::JobStatus,
};
use std::sync::{Arc, Mutex};

pub struct WorkerConnection {
    stream: MessageStream,
    ss: Arc<Mutex<SharedState>>,
    worker: Arc<Mutex<Worker>>,
    connection_closed: bool,
}

impl WorkerConnection {
    pub fn new(name: String, stream: MessageStream, ss: Arc<Mutex<SharedState>>) -> Self {
        let worker = ss.lock().unwrap().add_worker(name);
        WorkerConnection {
            stream,
            ss,
            worker,
            connection_closed: false,
        }
    }

    pub async fn run(&mut self) {
        // Hello/Welcome messages are already exchanged at this point.

        while !self.connection_closed {
            match self.stream.receive::<WorkerToServerMessage>().await {
                Ok(message) => {
                    log::debug!("Received message: {:?}", message);
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

        // Before we leave, update shared worker
        self.worker.lock().unwrap().connected = false;
    }

    async fn handle_message(&mut self, message: WorkerToServerMessage) -> Result<(), MessageError> {
        match message {
            WorkerToServerMessage::UpdateHwInfo(hw_info) => {
                // Update information in shared worker object
                self.worker.lock().unwrap().info.hw = hw_info;

                // No response to worker needed
                Ok(())
            }
            WorkerToServerMessage::UpdateLoadInfo(load_info) => {
                // Update information in shared worker object
                self.worker.lock().unwrap().info.load = load_info;

                // No response to worker needed
                Ok(())
            }
            WorkerToServerMessage::UpdateJobStatus => {
                // TODO!
                Ok(())
            }
            // This will also trigger the first jobs offered to the worker
            WorkerToServerMessage::AcceptParallelJobs(num) => {
                let free_slots = {
                    let mut worker = self.worker.lock().unwrap();
                    worker.info.max_parallel_jobs = num;
                    worker.info.current_jobs < num
                };

                if free_slots {
                    self.offer_pending_job_if_any().await
                } else {
                    Ok(()) // Worker busy. Nothing to do.
                }
            }
            WorkerToServerMessage::AcceptJobOffer(job_info) => {
                {
                    let mut worker_lock = self.worker.lock().unwrap();
                    let offered_job_index = worker_lock
                        .offered_jobs
                        .iter()
                        .position(|job| job.lock().unwrap().info.id == job_info.id);
                    match offered_job_index {
                        Some(index) => {
                            // Remove job from offered list
                            let job = worker_lock.offered_jobs.remove(index);
                            let mut job_lock = job.lock().unwrap();
                            // Check job status and move to running list
                            match &job_lock.info.status {
                                JobStatus::Offered { issued: _, to } if to == &worker_lock.info.name => {
                                    // All looks good. Continue.
                                    job_lock.info.status = JobStatus::Running {
                                        started: Utc::now(),
                                    };
                                    drop(job_lock); // unlock job
                                    worker_lock.assigned_jobs.push(Arc::clone(&job));
                                    worker_lock.info.current_jobs = worker_lock.assigned_jobs.len() as u32;
                                    drop(worker_lock); // unlock worker
                                    self.ss.lock().unwrap().move_accepted_job_to_running(job);
                                }
                                _ => {
                                    log::warn!("Accepted job was no longer offered to worker! Not changing state!");
                                }
                            }
                        }
                        None => {
                            log::error!(
                                "Worker accepted job with ID={} that has not been offered previously!",
                                job_info.id
                            );
                            // Error but we can continue running
                        }
                    }
                }

                let free_slots = {
                    let worker_lock = self.worker.lock().unwrap();
                    worker_lock.info.current_jobs < worker_lock.info.max_parallel_jobs
                };

                if free_slots {
                    self.offer_pending_job_if_any().await
                } else {
                    Ok(()) // Worker busy. Nothing to do.
                }
            }
            WorkerToServerMessage::RejectJobOffer(job_info) => {
                let mut worker_lock = self.worker.lock().unwrap();
                let offered_job_index = worker_lock
                    .offered_jobs
                    .iter()
                    .position(|job| job.lock().unwrap().info.id == job_info.id);
                match offered_job_index {
                    Some(index) => {
                        // Remove job from offered list
                        let job = worker_lock.offered_jobs.remove(index);
                        // Check job status and restore pending state
                        let mut job_lock = job.lock().unwrap();
                        match &job_lock.info.status {
                            JobStatus::Offered { issued, to } if to == &worker_lock.info.name => {
                                // All looks good. Restore.
                                job_lock.info.status = JobStatus::Pending {
                                    issued: issued.clone(),
                                };
                            }
                            _ => {
                                log::warn!("Rejected job was no longer offered to worker! Not restoring state!");
                            }
                        };
                        Ok(())
                    }
                    None => {
                        log::error!(
                            "Workder rejected job with ID={} that has not been offered previously!",
                            job_info.id
                        );
                        Ok(()) // Error but we can continue running
                    }
                }

                // TODO: Should we offer a new job right after reject?
            }
            WorkerToServerMessage::Bye => {
                log::trace!("Bye worker!");
                self.connection_closed = true;
                Ok(())
            }
        }
    }

    async fn offer_pending_job_if_any(&mut self) -> Result<(), MessageError> {
        let pending_job = self
            .ss
            .lock()
            .unwrap()
            .get_pending_job_to_offer(Arc::clone(&self.worker));
        match pending_job {
            Some(job) => {
                let job_info = job.lock().unwrap().info.clone();
                let job_offer = ServerToWorkerMessage::OfferJob(job_info);
                self.stream.send(&job_offer).await // job offered
            }
            None => Ok(()), // no job offered
        }
    }
}
