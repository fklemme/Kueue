//! This module handles the communication with the server.

use crate::job::Job;
use anyhow::{bail, Result};
use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use kueue_lib::{
    config::Config,
    messages::stream::{MessageError, MessageStream},
    messages::{HelloMessage, ServerToWorkerMessage, WorkerToServerMessage},
    structs::{JobStatus, LoadInfo, Resources, SystemInfo},
};
use sha2::{Digest, Sha256};
use std::{
    cmp::{max, min},
    sync::Arc,
};
use sysinfo::{CpuExt, System, SystemExt};
use tokio::{
    net::TcpStream,
    sync::Notify,
    time::{sleep, Duration},
};

/// Main struct, holding all information related to this worker instance.
pub struct Worker {
    /// Settings from the parsed config file.
    config: Config,
    /// Name of the worker. Used as an identifier for the user.
    worker_name: String,
    /// Message stream, connected to the server.
    stream: MessageStream,
    /// Regularly notified by a timer to trigger sending a message
    /// about updated system and hardware information to the server.
    notify_system_update: Arc<Notify>,
    /// Notified, whenever any job threads concludes.
    /// The job's status has already been updated by this time.
    notify_job_status: Arc<Notify>,
    /// Handle to query system information.
    system_info: System,
    /// Jobs offered to the worker but not yet confirmed or started.
    offered_jobs: Vec<Job>,
    /// Jobs currently running on the worker.
    running_jobs: Vec<Job>,
}

impl Worker {
    /// Set up a new Worker instance and connect to the server.
    pub async fn new(config: Config) -> Result<Self> {
        // Use hostname as worker name.
        let fqdn: String = gethostname::gethostname().to_string_lossy().into();
        let hostname = fqdn.split(|c| c == '.').next().unwrap().to_string();

        // Connect to the server.
        let server_addr = config.get_server_address().await?;
        let stream = TcpStream::connect(server_addr).await?;

        // Initialize system resources.
        let system_info = System::new_all();

        Ok(Worker {
            config,
            worker_name: hostname,
            stream: MessageStream::new(stream),
            notify_system_update: Arc::new(Notify::new()),
            notify_job_status: Arc::new(Notify::new()),
            system_info,
            offered_jobs: Vec::new(),
            running_jobs: Vec::new(),
        })
    }

    /// Perform message and job handling. This function will run indefinitely.
    pub async fn run(&mut self) -> Result<()> {
        // Perform hello/welcome handshake.
        self.connect_to_server().await?;

        // Perform challenge-response authentication.
        self.authenticate().await?;

        // Send regular updates about system, load, and resources to the server.
        let notify_system_update = Arc::clone(&self.notify_system_update);
        let system_update_interval_seconds =
            self.config.worker_settings.system_update_interval_seconds;
        tokio::spawn(async move {
            loop {
                notify_system_update.notify_one();
                sleep(Duration::from_secs(system_update_interval_seconds)).await;
            }
        });

        log::info!("Successfully connected to server!");

        // Main loop
        loop {
            tokio::select! {
                // Read and handle incoming messages.
                message = self.stream.receive::<ServerToWorkerMessage>() => {
                    self.handle_message(message?).await?;
                }
                // Or, get active when notified by timer.
                _ = self.notify_system_update.notified() => {
                    // Update server about system and load.
                    // Also used as "keep alive" signal by the server.
                    self.update_system_info().await?;
                    // Also send an update on available resources. This is
                    // important when "dynamic resources" are used. Otherwise,
                    // the server might see an outdated, full-loaded worker
                    // with no running jobs and will never offer any new jobs.
                    let message = WorkerToServerMessage::UpdateResources(self.get_available_resources());
                    self.stream.send(&message).await?;
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
            worker_name: self.worker_name.clone(),
        };
        self.stream.send(&hello).await?;

        // Await welcoming response from server.
        match self.stream.receive::<ServerToWorkerMessage>().await? {
            ServerToWorkerMessage::WelcomeWorker => {
                log::trace!("Established connection to server...");
                Ok(()) // continue
            }
            other => bail!("Expected WelcomeWorker, received: {:?}", other),
        }
    }

    /// Perform challenge-response authentication. We only need to answer the
    /// challenge without waiting for a response. If the authentication fails,
    /// the server closes the connection.
    async fn authenticate(&mut self) -> Result<()> {
        // Authentication challenge is sent automatically after welcome.
        match self.stream.receive::<ServerToWorkerMessage>().await? {
            ServerToWorkerMessage::AuthChallenge { salt } => {
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
            }
            other => {
                bail!("Expected AuthChallenge, received: {:?}", other);
            }
        }

        // Await authentication confirmation.
        match self.stream.receive::<ServerToWorkerMessage>().await? {
            ServerToWorkerMessage::AuthAccepted(accepted) => {
                if accepted {
                    Ok(())
                } else {
                    bail!("Authentication failed!")
                }
            }
            other => bail!("Expected AuthAccepted, received: {:?}", other),
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
            ServerToWorkerMessage::AuthChallenge { salt: _ } => {
                // This is already handled before the main loop begins.
                log::warn!("Received duplicate authentication challenge!");
                Ok(())
            }
            ServerToWorkerMessage::AuthAccepted(_accepted) => {
                // This is already handled before the main loop begins.
                log::warn!("Received duplicate authentication acceptance!");
                Ok(())
            }
            ServerToWorkerMessage::OfferJob(job_info) => {
                // Reject job when the worker cannot see the working directory.
                if !job_info.cwd.is_dir() {
                    log::debug!("Rejected job {}!", job_info.job_id);

                    // Reject job offer.
                    return self
                        .stream
                        .send(&WorkerToServerMessage::RejectJobOffer(job_info))
                        .await;
                }

                // Accept job if required resources can be acquired.
                if self.resources_available(&job_info.local_resources) {
                    log::debug!("Accepted job {}!", job_info.job_id);

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
                    log::debug!("Deferred job {}!", job_info.job_id);

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
                    .position(|job| job.info.job_id == job_info.job_id);
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
                                log::debug!("Started job {}!", job_info.job_id);

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

                                // Update server. This will also send an update on
                                // available resources and thus trigger new job offers.
                                self.update_job_status().await
                            }
                        }

                        // TODO: There is some checking we could do here. We do
                        // not want jobs to remain in offered_jobs indefinitely!
                    }
                    None => {
                        log::error!(
                            "Confirmed job with ID={} that has not been offered previously!",
                            job_info.job_id
                        );
                        Ok(()) // Error occurred, but we can continue running.
                    }
                }
            }
            ServerToWorkerMessage::WithdrawJobOffer(job_info) => {
                // Remove job from offered list
                let offered_job_index = self
                    .offered_jobs
                    .iter()
                    .position(|job| job.info.job_id == job_info.job_id);
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
                            job_info.job_id
                        );
                        Ok(()) // Error occurred, but we can continue running.
                    }
                }
            }
            ServerToWorkerMessage::KillJob(job_info) => {
                let running_job_index = self
                    .running_jobs
                    .iter()
                    .position(|job| job.info.job_id == job_info.job_id);
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
                            job_info.job_id
                        );
                        Ok(()) // Error occurred, but we can continue running.
                    }
                }
            }
        }
    }

    /// Returns available, unused resources of the worker.
    fn get_available_resources(&mut self) -> Resources {
        // Refresh relevant system information.
        self.system_info.refresh_cpu();
        self.system_info.refresh_memory();

        // Calculate available job slots.
        let allocated_job_slots: u64 = self
            .offered_jobs
            .iter()
            .chain(self.running_jobs.iter())
            .map(|job| job.info.local_resources.job_slots)
            .sum();
        assert!(self.config.worker_settings.worker_max_parallel_jobs >= allocated_job_slots);
        let available_job_slots =
            self.config.worker_settings.worker_max_parallel_jobs - allocated_job_slots;

        // Calculate available cpus.
        let total_cpus = self.system_info.cpus().len() as i64;

        let allocated_cpus: i64 = self
            .offered_jobs
            .iter()
            .chain(self.running_jobs.iter())
            .map(|job| job.info.local_resources.cpus as i64)
            .sum();

        let available_cpus = if self.config.worker_settings.dynamic_check_free_resources {
            let busy_cpus = self.system_info.load_average().one.ceil() as i64;

            // TODO: Is this calculation better? Maybe working on Windows as well?
            // let busy_cpus = self
            //     .system_info
            //     .cpus()
            //     .iter()
            //     .map(|cpu| cpu.cpu_usage())
            //     .sum::<f32>();
            // let busy_cpus = (busy_cpus / 100.0 / total_cpus as f32).ceil() as i64;

            let busy_cpus = (busy_cpus as f64
                * self.config.worker_settings.dynamic_cpu_load_scale_factor)
                .ceil() as i64;
            max(0, total_cpus - max(allocated_cpus, busy_cpus))
        } else {
            max(0, total_cpus - allocated_cpus)
        };

        // Calculate available memory.
        let total_ram_mb = (self.system_info.total_memory() / 1024 / 1024) as i64;

        let allocated_ram_mb: i64 = self
            .offered_jobs
            .iter()
            .chain(self.running_jobs.iter())
            .map(|job| job.info.local_resources.ram_mb as i64)
            .sum();

        let available_ram_mb = if self.config.worker_settings.dynamic_check_free_resources {
            let available_ram_mb = (self.system_info.available_memory() / 1024 / 1024) as i64;
            max(0, min(total_ram_mb - allocated_ram_mb, available_ram_mb))
        } else {
            max(0, total_ram_mb - allocated_ram_mb)
        };

        Resources::new(
            available_job_slots,
            available_cpus as u64,
            available_ram_mb as u64,
        )
    }

    /// Returns "true" if there are enough resources free to
    /// fit the demand of the given "required" resources.
    fn resources_available(&mut self, required: &Resources) -> bool {
        let available = self.get_available_resources();
        required.fit_into(&available)
    }

    /// Update and report hardware information and system load.
    async fn update_system_info(&mut self) -> Result<(), MessageError> {
        // Refresh relevant system information.
        self.system_info.refresh_cpu();
        self.system_info.refresh_memory();

        // Get CPU cores, frequency, and RAM.
        let cpu_cores = self.system_info.cpus().len() as u64;
        let cpu_frequency = if cpu_cores > 0 {
            self.system_info
                .cpus()
                .iter()
                .map(|cpu| cpu.frequency())
                .sum::<u64>()
                / cpu_cores
        } else {
            0
        };
        let total_ram_mb = self.system_info.total_memory() / 1024 / 1024;

        // Read system load.
        let load_avg = self.system_info.load_average();
        let load_info = LoadInfo {
            one: load_avg.one,
            five: load_avg.five,
            fifteen: load_avg.fifteen,
        };

        // Collect hardware information.
        let hw_info = SystemInfo {
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
            .send(&WorkerToServerMessage::UpdateSystemInfo(hw_info))
            .await
    }

    /// Find jobs that have concluded and update the server about the new status.
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
                                worker: self.worker_name.clone(),
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
                    job_id: job.info.job_id,
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
