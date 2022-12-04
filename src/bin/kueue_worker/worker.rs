use crate::job::Job;
use anyhow::{anyhow, Result};
use chrono::Utc;
use kueue::{
    config::Config,
    messages::stream::{MessageError, MessageStream},
    messages::{HelloMessage, ServerToWorkerMessage, WorkerToServerMessage},
    structs::{HwInfo, JobStatus, LoadInfo},
};
use sha2::{Digest, Sha256};
use std::{
    cmp::{max, min},
    sync::Arc,
};
use sysinfo::{CpuExt, CpuRefreshKind, System, SystemExt};
use tokio::{
    net::TcpStream,
    sync::Notify,
    time::{sleep, Duration},
};

pub struct Worker {
    name: String,
    config: Config,
    stream: MessageStream,
    notify_update_hw: Arc<Notify>,
    notify_job_status: Arc<Notify>,
    system_info: System,
    offered_jobs: Vec<Job>,
    running_jobs: Vec<Job>,
    max_parallel_jobs: usize,
}

impl Worker {
    pub async fn new<T: tokio::net::ToSocketAddrs>(
        name: String,
        config: Config,
        addr: T,
    ) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Worker {
            name,
            config,
            stream: MessageStream::new(stream),
            notify_update_hw: Arc::new(Notify::new()),
            notify_job_status: Arc::new(Notify::new()),
            system_info: System::new_all(),
            offered_jobs: Vec::new(),
            running_jobs: Vec::new(),
            max_parallel_jobs: 0,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        // Do hello/welcome handshake.
        self.connect_to_server().await?;

        // Do challenge-response authentification.
        self.authenticate().await?;

        // Notify worker regularly to send updates.
        let notify_update_hw = Arc::clone(&self.notify_update_hw);
        tokio::spawn(async move {
            loop {
                notify_update_hw.notify_one();
                sleep(Duration::from_secs(60)).await;
            }
        });

        // Send job capacity to server and get ready to accept jobs
        self.accept_jobs_based_on_hw().await?;

        // Main loop
        loop {
            tokio::select! {
                // Read and handle incoming messages.
                message = self.stream.receive::<ServerToWorkerMessage>() => {
                    self.handle_message(message?).await?;
                }
                // Or, get active when notified.
                _ = self.notify_update_hw.notified() => {
                    self.update_hw_status().await?;
                    self.update_load_status().await?;
                }
                _ = self.notify_job_status.notified() => {
                    self.update_job_status().await?;
                }
            }
        }
    }

    async fn connect_to_server(&mut self) -> Result<()> {
        // Send hello from worker.
        let hello = HelloMessage::HelloFromWorker {
            name: self.name.clone(),
        };
        self.stream.send(&hello).await?;

        // Await welcoming response from server.
        match self.stream.receive::<ServerToWorkerMessage>().await? {
            ServerToWorkerMessage::WelcomeWorker => {
                log::trace!("Established connection to server...");
                Ok(()) // continue
            }
            other => Err(anyhow!("Expected WelcomeWorker, received: {:?}", other)),
        }
    }

    async fn authenticate(&mut self) -> Result<()> {
        // Authentification challenge is sent automatically after welcome.
        match self.stream.receive::<ServerToWorkerMessage>().await? {
            ServerToWorkerMessage::AuthChallenge(salt) => {
                // Calculate response.
                let salted_secret = self.config.shared_secret.clone() + &salt;
                let salted_secret = salted_secret.into_bytes();
                let mut hasher = Sha256::new();
                hasher.update(salted_secret);
                let response = hasher.finalize().to_vec();
                let response = base64::encode(response);

                // Send response back to server.
                let message = WorkerToServerMessage::AuthResponse(response);
                self.stream.send(&message).await?;

                Ok(()) // done
            }
            other => Err(anyhow!("Expected AuthChallenge, received: {:?}", other)),
        }
    }

    async fn accept_jobs_based_on_hw(&mut self) -> Result<(), MessageError> {
        let cpu_cores = self.system_info.cpus().len();
        let total_memory = self.system_info.total_memory() as usize;
        let total_memory_gb = (total_memory / 1024 / 1024 / 1024) as usize;

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
                // This is already handled before the main loop begins.
                log::warn!("Received duplicate welcome message!");
                Ok(())
            }
            ServerToWorkerMessage::AuthChallenge(_challenge) => {
                // This is already handled before the main loop begins.
                log::warn!("Received duplicate authentification challenge!");
                Ok(())
            }
            ServerToWorkerMessage::OfferJob(job_info) => {
                // Make some smart checks whether or not to accept the job offer.
                let free_slots =
                    (self.running_jobs.len() + self.offered_jobs.len()) < self.max_parallel_jobs;
                let cwd_available = job_info.cwd.is_dir();

                if free_slots && cwd_available {
                    // Accept job offer
                    self.offered_jobs.push(Job::new(
                        job_info.clone(),
                        Arc::clone(&self.notify_job_status),
                    )); // remember
                    self.stream
                        .send(&WorkerToServerMessage::AcceptJobOffer(job_info))
                        .await
                } else {
                    // Reject job offer
                    self.stream
                        .send(&WorkerToServerMessage::RejectJobOffer(job_info))
                        .await
                }
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
                                let job = self.running_jobs.last_mut().unwrap();
                                let mut result_lock = job.result.lock().unwrap();
                                result_lock.finished = true;
                                result_lock.exit_code = -43;
                                result_lock.run_time = chrono::Duration::seconds(0);
                                result_lock.comment = format!("Failed to run job: {}", e);
                                drop(result_lock); // unlock
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

    async fn update_hw_status(&mut self) -> Result<(), MessageError> {
        // Refresh relevant system information.
        self.system_info
            .refresh_cpu_specifics(CpuRefreshKind::new().with_frequency());

        // Get CPU cores and frequency.
        let cpu_cores = self.system_info.cpus().len();
        let cpu_frequency = if cpu_cores > 0 {
            self.system_info
                .cpus()
                .iter()
                .map(|cpu| cpu.frequency())
                .sum::<u64>()
                / cpu_cores as u64
        } else {
            0u64
        };

        // Collect hardware information.
        let hw_info = HwInfo {
            kernel: self.system_info.kernel_version().unwrap_or("unknown".into()),
            distribution: self.system_info.long_os_version().unwrap_or("unknown".into()),
            cpu_cores,
            cpu_frequency,
            total_memory: self.system_info.total_memory(),
        };

        // Send to server.
        self.stream
            .send(&WorkerToServerMessage::UpdateHwInfo(hw_info))
            .await
    }

    async fn update_load_status(&mut self) -> Result<(), MessageError> {
        // Read system load.
        let load_avg = self.system_info.load_average();
        let load_info = LoadInfo {
            one: load_avg.one,
            five: load_avg.five,
            fifteen: load_avg.fifteen,
        };

        // Send to server.
        self.stream
            .send(&WorkerToServerMessage::UpdateLoadInfo(load_info))
            .await
    }

    async fn update_job_status(&mut self) -> Result<(), MessageError> {
        // We check all running processes for exit codes
        let mut index = 0;
        while index < self.running_jobs.len() {
            let finished = self.running_jobs[index].result.lock().unwrap().finished;
            if finished {
                // Job has finished. Remove from list.
                let mut job = self.running_jobs.remove(index);
                let mut stdout = None;
                let mut stderr = None;

                {
                    // Update info
                    let result_lock = job.result.lock().unwrap();
                    job.info.status = JobStatus::Finished {
                        finished: Utc::now(),
                        return_code: result_lock.exit_code,
                        on: self.name.clone(),
                        run_time_seconds: result_lock.run_time.num_seconds(),
                    };
                    if !result_lock.stdout.is_empty() {
                        stdout = Some(result_lock.stdout.clone());
                    }
                    if !result_lock.stderr.is_empty() {
                        stderr = Some(result_lock.stderr.clone());
                    }
                }

                // Send update to server
                let job_status = WorkerToServerMessage::UpdateJobStatus(job.info.clone());
                self.stream.send(&job_status).await?;

                // Send stdout/stderr to server
                let job_results = WorkerToServerMessage::UpdateJobResults {
                    job_id: job.info.id,
                    stdout,
                    stderr,
                };
                self.stream.send(&job_results).await?;

                // TODO: Store/remember finished jobs?
            } else {
                // Job still running... Next!
                index += 1;
            }
        }
        Ok(())
    }
}
