use crate::shared_state::{SharedState, Worker};
use kueue::messages::{
    stream::{MessageError, MessageStream},
    ServerToWorkerMessage, WorkerToServerMessage,
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
        let worker = ss.lock().unwrap().register_worker(name);
        WorkerConnection {
            stream,
            ss,
            worker,
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
                    let free_slots = {
                        let worker_lock = self.worker.lock().unwrap();
                        worker_lock.info.current_jobs < worker_lock.info.max_parallel_jobs
                    };

                    if free_slots {
                        if let Err(e) = self.offer_pending_job_if_any().await {
                            log::error!("Failed to offer new job: {}", e);
                            self.connection_closed = true; // end worker session
                        }
                    }
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

                Ok(()) // No response to worker needed.
            }
            WorkerToServerMessage::UpdateLoadInfo(load_info) => {
                // Update information in shared worker object
                self.worker.lock().unwrap().info.load = load_info;

                Ok(()) // No response to worker needed.
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
                let job = self.ss.lock().unwrap().get_job_from_info(job_info.clone());
                match job {
                    Some(job) => {
                        self.ss.lock().unwrap().move_accepted_job_to_running(job);

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
                    None => {
                        log::error!("Accepted job not found: {:?}", job_info);
                        Ok(()) // Error but we can still continue.
                    }
                }
            }
            WorkerToServerMessage::RejectJobOffer(job_info) => {
                let job = self.ss.lock().unwrap().get_job_from_info(job_info.clone());
                match job {
                    Some(job) => {
                        self.ss.lock().unwrap().move_rejected_job_to_pending(job);

                        // Let other workers know that the job is available again.
                        let new_jobs = self.ss.lock().unwrap().new_jobs();
                        new_jobs.notify_waiters();

                        // TODO: Should we offere more jobs right after reject?
                        Ok(())
                    }
                    None => {
                        log::error!("Rejected job not found: {:?}", job_info);
                        Ok(()) // Error but we can still continue.
                    }
                }
            }
            WorkerToServerMessage::Bye => {
                log::trace!("Bye worker!");
                self.connection_closed = true;
                Ok(())
            }
        }
    }

    async fn offer_pending_job_if_any(&mut self) -> Result<(), MessageError> {
        let offered_job = self
            .ss
            .lock()
            .unwrap()
            .get_pending_job_and_make_offered(Arc::clone(&self.worker));
        match offered_job {
            Some(job) => {
                let job_info = job.lock().unwrap().info.clone();
                let job_offer = ServerToWorkerMessage::OfferJob(job_info);
                self.stream.send(&job_offer).await // job offered
            }
            None => Ok(()), // no job offered
        }
    }
}
