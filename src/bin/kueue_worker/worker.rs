use crate::job::Job;
use chrono::Utc;
use kueue::{
    messages::stream::{MessageError, MessageStream},
    messages::{HelloMessage, ServerToWorkerMessage, WorkerToServerMessage},
    structs::{HwInfo, JobStatus, LoadInfo},
};
use std::{
    cmp::{max, min},
    sync::Arc,
};
use sysinfo::{System, SystemExt};
use tokio::{
    net::TcpStream,
    sync::Notify,
    time::{sleep, Duration},
};

pub struct Worker {
    name: String,
    stream: MessageStream,
    notify_update: Arc<Notify>,
    notify_job_status: Arc<Notify>,
    system: System,
    offered_jobs: Vec<Job>,
    running_jobs: Vec<Job>,
    finished_jobs: Vec<Job>,
    max_parallel_jobs: u32,
    connection_closed: bool,
}

impl Worker {
    pub async fn new<T: tokio::net::ToSocketAddrs>(
        name: String,
        addr: T,
    ) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Worker {
            name,
            stream: MessageStream::new(stream),
            notify_update: Arc::new(Notify::new()),
            notify_job_status: Arc::new(Notify::new()),
            system: System::new_all(),
            offered_jobs: Vec::new(),
            running_jobs: Vec::new(),
            finished_jobs: Vec::new(),
            max_parallel_jobs: 0,
            connection_closed: false,
        })
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.connect_to_server().await?;

        // TODO: Implement encryption & authentification

        // Send hardware information to server (once)
        self.update_hw_status().await?;

        // Notify worker regularly to send other updates
        let notify_update = Arc::clone(&self.notify_update);
        tokio::spawn(async move {
            loop {
                notify_update.notify_one();
                sleep(Duration::from_secs(60)).await;
            }
        });

        // Send job capacity to server and get ready to accept jobs
        self.accept_jobs_based_on_hw().await?;

        // Main loop
        while !self.connection_closed {
            tokio::select! {
                // Read and handle incoming messages
                message = self.stream.receive::<ServerToWorkerMessage>() => {
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
                _ = self.notify_update.notified() => {
                    self.update_load_status().await?;
                }
                _ = self.notify_job_status.notified() => {
                    self.update_job_status().await?;
                }
            }
        }
        Ok(()) // end of worker run
    }

    async fn connect_to_server(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Send hello from worker
        let hello = HelloMessage::HelloFromWorker {
            name: self.name.clone(),
        };
        self.stream.send(&hello).await?;

        // Await welcoming response from server
        match self.stream.receive::<ServerToWorkerMessage>().await? {
            ServerToWorkerMessage::WelcomeWorker => {
                log::trace!("Established connection to server...");
                Ok(()) // continue
            } 
            other => Err(format!("Expected WelcomeWorker, received: {:?}", other).into()),
        }
    }

    async fn update_hw_status(&mut self) -> Result<(), MessageError> {
        // Read hardware information
        let hw_info = HwInfo {
            kernel: self.system.kernel_version().unwrap_or("unknown".into()),
            distribution: self.system.long_os_version().unwrap_or("unknown".into()),
            cpu_cores: self.system.cpus().len() as u32,
            total_memory: self.system.total_memory(),
        };

        // Send to server
        self.stream
            .send(&WorkerToServerMessage::UpdateHwInfo(hw_info))
            .await
    }

    async fn update_load_status(&mut self) -> Result<(), MessageError> {
        // Read load
        let load_avg = self.system.load_average();
        let load_info = LoadInfo {
            one: load_avg.one,
            five: load_avg.five,
            fifteen: load_avg.fifteen,
        };

        // Send to server
        self.stream
            .send(&WorkerToServerMessage::UpdateLoadInfo(load_info))
            .await
    }

    async fn update_job_status(&mut self) -> Result<(), MessageError> {
        // We check all running processes for exit codes
        let mut index = 0;
        while index < self.running_jobs.len() {
            let option_status = self.running_jobs[index].exit_status.lock().unwrap().clone();
            if let Some(status) = option_status {
                // Job has finished. Remove from list.
                let mut job = self.running_jobs.remove(index);

                // Update info
                job.info.status = JobStatus::Finished {
                    finished: Utc::now(),
                    return_code: status,
                    on: self.name.clone(),
                };

                // Send update to server
                let job_update = WorkerToServerMessage::UpdateJobStatus(job.info.clone());
                self.stream.send(&job_update).await?;

                // Put job into finsished list
                self.finished_jobs.push(job);
            } else {
                // Job still running... Next!
                index += 1;
            }
        }
        Ok(())
    }

    async fn accept_jobs_based_on_hw(&mut self) -> Result<(), MessageError> {
        let cpu_cores = self.system.cpus().len() as u32;
        let total_memory = self.system.total_memory();
        let total_memory_gb = (total_memory / 1024 / 1024 / 1024) as u32;

        // TODO: Do something smart to set the hw-based default.
        self.max_parallel_jobs = max(1, min(cpu_cores / 8, total_memory_gb / 8));

        // Send to server
        self.stream
            .send(&WorkerToServerMessage::AcceptParallelJobs(
                self.max_parallel_jobs,
            ))
            .await
    }

    async fn handle_message(&mut self, message: ServerToWorkerMessage) -> Result<(), MessageError> {
        match message {
            ServerToWorkerMessage::WelcomeWorker => {
                log::warn!("Received duplicate welcome message");
                Ok(())
            }
            ServerToWorkerMessage::OfferJob(job_info) => {
                // TODO: Make some smart checks whether or not to accept the job offer
                if ((self.running_jobs.len() + self.offered_jobs.len()) as u32)
                    < self.max_parallel_jobs
                {
                    // Accept job offer
                    self.offered_jobs.push(Job::new(
                        job_info.clone(),
                        Arc::clone(&self.notify_job_status),
                    )); // remember
                    self.stream
                        .send(&WorkerToServerMessage::AcceptJobOffer(job_info))
                        .await?;
                } else {
                    // Reject job offer
                    self.stream
                        .send(&WorkerToServerMessage::RejectJobOffer(job_info))
                        .await?;
                }

                Ok(())
            }
            ServerToWorkerMessage::ConfirmJobOffer(job_info) => {
                let offered_job_index = self
                    .offered_jobs
                    .iter()
                    .position(|job| job.info.id == job_info.id);
                match offered_job_index {
                    Some(index) => {
                        // Move job to running processes
                        let job = self.offered_jobs.remove(index);
                        self.running_jobs.push(job);

                        // Run job as child process
                        match self.running_jobs.last_mut().unwrap().run() {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                log::error!("Failed to run job: {}", e);
                                let job = self.running_jobs.last().unwrap();
                                let mut status_lock = job.exit_status.lock().unwrap();
                                *status_lock = Some(-42); // failed
                                drop(status_lock); // unlock
                                self.update_job_status().await
                            }
                        }

                        // TODO: There is some checking we could do here. We do
                        // not want jobs to remain in offered_jobs indefinitly!
                    }
                    None => {
                        log::error!(
                            "Confirmed job with ID={} that has not been offered previously!",
                            job_info.id
                        );
                        Ok(()) // Error but we can continue running
                    }
                }
            }
            ServerToWorkerMessage::WithdrawJobOffer(job_info) => {
                // Remove job from offered list
                let offered_job_index = self
                    .offered_jobs
                    .iter()
                    .position(|job| job.info.id == job_info.id);
                match offered_job_index {
                    Some(index) => {
                        // Remove job from list
                        let job = self.offered_jobs.remove(index);
                        log::debug!("Withdrawn job: {:?}", job);
                        Ok(())
                    }
                    None => {
                        log::error!(
                            "Withdrawn job with ID={} that has not been offered previously!",
                            job_info.id
                        );
                        Ok(()) // Error but we can continue running
                    }
                }
            }
        }
    }
}
