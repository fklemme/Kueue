use crate::job::Job;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};
use chrono::Utc;
use kueue_lib::{
    config::Config,
    messages::stream::{MessageError, MessageStream},
    messages::{HelloMessage, ServerToWorkerMessage, WorkerToServerMessage},
    structs::{HwInfo, JobStatus, LoadInfo, Resources},
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
    config: Config,
    name: String,
    stream: MessageStream,
    notify_update_hw: Arc<Notify>,
    notify_job_status: Arc<Notify>,
    system_info: System,
    /// Jobs offered to the worker but not yet confirmed or started.
    offered_jobs: Vec<Job>,
    /// Currently running jobs on the worker.
    running_jobs: Vec<Job>,
}

impl Worker {
    /// Set up a new Worker instance and connect to the server.
    pub async fn new(config: Config) -> Result<Self> {
        // Generate unique name from hostname and random suffix.
        let fqdn: String = gethostname::gethostname().to_string_lossy().into();
        let hostname = fqdn.split(|c| c == '.').next().unwrap().to_string();
        let mut generator = names::Generator::default();
        let name_suffix = generator.next().unwrap_or("default".into());
        let name = format!("{}-{}", hostname, name_suffix);
        log::info!("Worker name: {}", name);

        // Connect to the server.
        let server_addr = config.get_server_address().await?;
        let stream = TcpStream::connect(server_addr).await?;

        // Initialize system resources.
        let system_info = System::new_all();

        Ok(Worker {
            config,
            name,
            stream: MessageStream::new(stream),
            notify_update_hw: Arc::new(Notify::new()),
            notify_job_status: Arc::new(Notify::new()),
            system_info,
            offered_jobs: Vec::new(),
            running_jobs: Vec::new(),
        })
    }

    /// Perform message and job handling. This function will run indefinitly.
    pub async fn run(&mut self) -> Result<()> {
        // Perform hello/welcome handshake.
        self.connect_to_server().await?;

        // Perform challenge-response authentification.
        self.authenticate().await?;

        // Send regular updates about hardware and load to the server.
        let notify_update_hw = Arc::clone(&self.notify_update_hw);
        tokio::spawn(async move {
            loop {
                notify_update_hw.notify_one();
                sleep(Duration::from_secs(60)).await;
            }
        });

        // Inform server about available resources. This information
        // triggers the server to send new job offers to the worker.
        let message = WorkerToServerMessage::UpdateResources(self.get_available_resources());
        self.stream.send(&message).await?;

        // Main loop
        loop {
            tokio::select! {
                // Read and handle incoming messages.
                message = self.stream.receive::<ServerToWorkerMessage>() => {
                    self.handle_message(message?).await?;
                }
                // Or, get active when notified by timer.
                _ = self.notify_update_hw.notified() => {
                    self.update_hw_status().await?;
                }
                // Or, get active when a job finishes.
                _ = self.notify_job_status.notified() => {
                    self.update_job_status().await?;
                }
            }
        }
    }

    /// Perform hello/welcome handshake with the server.
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

    /// Perform challenge-response authentification.
    async fn authenticate(&mut self) -> Result<()> {
        // Authentification challenge is sent automatically after welcome.
        match self.stream.receive::<ServerToWorkerMessage>().await? {
            ServerToWorkerMessage::AuthChallenge(salt) => {
                // Calculate response.
                let salted_secret = self.config.common_settings.shared_secret.clone() + &salt;
                let salted_secret = salted_secret.into_bytes();
                let mut hasher = Sha256::new();
                hasher.update(salted_secret);
                let response = hasher.finalize().to_vec();
                let response = general_purpose::STANDARD_NO_PAD.encode(response);

                // Send response back to server.
                let message = WorkerToServerMessage::AuthResponse(response);
                self.stream.send(&message).await?;

                Ok(()) // done
            }
            other => Err(anyhow!("Expected AuthChallenge, received: {:?}", other)),
        }
    }

    /// Called in the main loop to handle different incoming messages from the server.
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
                // Reject job when the worker cannot see the working directory.
                if !job_info.cwd.is_dir() {
                    // Reject job offer.
                    return self
                        .stream
                        .send(&WorkerToServerMessage::RejectJobOffer(job_info))
                        .await;
                }

                // Accept job if required resources can be acquired.
                if self.resources_available(&job_info.resources) {
                    // Remember accepted job for later confirmation.
                    self.offered_jobs.push(Job::new(
                        job_info.clone(),
                        Arc::clone(&self.notify_job_status),
                    ));
                    // Notify server about accepted job offer.
                    self.stream
                        .send(&WorkerToServerMessage::AcceptJobOffer(job_info))
                        .await
                } else {
                    // Defer job offer (until resources become available).
                    self.stream
                        .send(&WorkerToServerMessage::DeferJobOffer(job_info))
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
                        // Move job to running processes.
                        let mut job = self.offered_jobs.remove(index);
                        // Also update job status. (Should now be "running".)
                        job.info.status = job_info.status;
                        self.running_jobs.push(job);

                        // TODO: Also compare entire "job_info"s for consistency?

                        // Run job as child process
                        match self.running_jobs.last_mut().unwrap().run().await {
                            Ok(()) => {
                                // Inform server about available resources.
                                // This information triggers the server to
                                // send new job offers to this worker.
                                let message = WorkerToServerMessage::UpdateResources(
                                    self.get_available_resources(),
                                );
                                self.stream.send(&message).await
                            }
                            Err(e) => {
                                log::error!("Failed to start job: {}", e);
                                let job = self.running_jobs.last_mut().unwrap();
                                {
                                    let mut job_result = job.result.lock().unwrap();
                                    job_result.finished = true;
                                    job_result.exit_code = -43;
                                    job_result.run_time = chrono::Duration::seconds(0);
                                    job_result.comment = format!("Failed to start job: {}", e);
                                }

                                // Update server. This will also send an undate on
                                // available resources and thus trigger new job offers.
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
                        Ok(()) // Error occured, but we can continue running.
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
                        Ok(()) // Error occured, but we can continue running.
                    }
                }
            }
            ServerToWorkerMessage::KillJob(job_info) => {
                let running_job_index = self
                    .running_jobs
                    .iter()
                    .position(|job| job.info.id == job_info.id);
                match running_job_index {
                    Some(index) => {
                        // Signal kill.
                        let job = self.running_jobs.get_mut(index).unwrap();
                        job.notify_kill_job.notify_one();
                        // Also update job status. (Should now be "canceled".)
                        job.info.status = job_info.status;
                        // After the job has been killed, "notify_job_status"
                        // is notified, causing "update_job_status" to remove
                        // the job from "running_jobs" and inform the server.
                        Ok(())
                    }
                    None => {
                        log::error!(
                            "Job to be killed with ID={} is not running on this worker!",
                            job_info.id
                        );
                        Ok(()) // Error occured, but we can continue running.
                    }
                }
            }
        }
    }

    /// Returns available, unused resources of the worker.
    fn get_available_resources(&self) -> Resources {
        let allocated_cpus: i64 = self
            .offered_jobs
            .iter()
            .chain(self.running_jobs.iter())
            .map(|job| job.info.resources.cpus as i64)
            .sum();

        let allocated_ram_mb: i64 = self
            .offered_jobs
            .iter()
            .chain(self.running_jobs.iter())
            .map(|job| job.info.resources.ram_mb as i64)
            .sum();

        let total_cpus = self.system_info.cpus().len() as i64;
        let available_cpus = if self.config.worker_settings.dynamic_check_free_resources {
            let busy_cpus = self.system_info.load_average().one.ceil() as i64;
            max(0, total_cpus - max(allocated_cpus, busy_cpus))
        } else {
            max(0, total_cpus - allocated_cpus)
        };

        let total_ram_mb = (self.system_info.total_memory() / 1024 / 1024) as i64;
        let available_ram_mb = if self.config.worker_settings.dynamic_check_free_resources {
            let available_ram_mb = (self.system_info.available_memory() / 1024 / 1024) as i64;
            max(0, min(total_ram_mb - allocated_ram_mb, available_ram_mb))
        } else {
            max(0, total_ram_mb - allocated_ram_mb)
        };

        Resources::new(available_cpus as usize, available_ram_mb as usize)
    }

    /// Returns "true" if there are enough resources free to
    /// fit the demand of the given "required" resources.
    fn resources_available(&self, required: &Resources) -> bool {
        let available = self.get_available_resources();

        (available.cpus >= required.cpus) && (available.ram_mb >= required.ram_mb)
    }

    /// Update and report hardware information and system load.
    async fn update_hw_status(&mut self) -> Result<(), MessageError> {
        // Refresh relevant system information.
        self.system_info
            .refresh_cpu_specifics(CpuRefreshKind::new().with_frequency());

        // Get CPU cores, frequency, and RAM.
        let cpu_cores = self.system_info.cpus().len();
        let cpu_frequency = if cpu_cores > 0 {
            self.system_info
                .cpus()
                .iter()
                .map(|cpu| cpu.frequency() as usize)
                .sum::<usize>()
                / cpu_cores
        } else {
            0
        };
        let total_ram_mb = (self.system_info.total_memory() / 1024 / 1024) as usize;

        // Read system load.
        let load_avg = self.system_info.load_average();
        let load_info = LoadInfo {
            one: load_avg.one,
            five: load_avg.five,
            fifteen: load_avg.fifteen,
        };

        // Collect hardware information.
        let hw_info = HwInfo {
            kernel: self
                .system_info
                .kernel_version()
                .unwrap_or("unknown".into()),
            distribution: self
                .system_info
                .long_os_version()
                .unwrap_or("unknown".into()),
            cpu_cores,
            cpu_frequency,
            total_ram_mb,
            load_info,
        };

        // Send to server.
        self.stream
            .send(&WorkerToServerMessage::UpdateHwInfo(hw_info))
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
                let mut stdout_text = None;
                let mut stderr_text = None;

                {
                    // Update info
                    let result_lock = job.result.lock().unwrap();
                    match job.info.status {
                        JobStatus::Running {
                            issued, started, ..
                        } => {
                            job.info.status = JobStatus::Finished {
                                issued,
                                started,
                                finished: Utc::now(),
                                return_code: result_lock.exit_code,
                                on: self.name.clone(),
                                run_time_seconds: result_lock.run_time.num_seconds(),
                                comment: result_lock.comment.clone(),
                            };
                        }
                        JobStatus::Canceled { .. } => {} // leave status as it is (canceled)
                        _ => log::error!(
                            "Expected job status to be running or canceled. Found: {:?}",
                            job.info.status
                        ),
                    }
                    if !result_lock.stdout_text.is_empty() {
                        stdout_text = Some(result_lock.stdout_text.clone());
                    }
                    if !result_lock.stderr_text.is_empty() {
                        stderr_text = Some(result_lock.stderr_text.clone());
                    }
                }

                // Send update to server
                let job_status = WorkerToServerMessage::UpdateJobStatus(job.info.clone());
                self.stream.send(&job_status).await?;

                // Send stdout/stderr to server
                let job_results = WorkerToServerMessage::UpdateJobResults {
                    job_id: job.info.id,
                    stdout_text,
                    stderr_text,
                };
                self.stream.send(&job_results).await?;

                // Inform server about available resources. This information
                // triggers the server to send new job offers to the worker.
                let message =
                    WorkerToServerMessage::UpdateResources(self.get_available_resources());
                self.stream.send(&message).await?;

                // TODO: Store/remember finished jobs?
            } else {
                // Job still running... Next!
                index += 1;
            }
        }
        Ok(())
    }
}
