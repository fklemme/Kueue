use chrono::Utc;
use kueue::structs::{JobInfo, JobStatus};
use std::{
    error::Error,
    sync::{Arc, Mutex},
};
use tokio::{process::Command, sync::Notify};

#[derive(Debug)]
pub struct Job {
    pub info: JobInfo,
    pub notify_job_status: Arc<Notify>,
    pub exit_status: Arc<Mutex<JobExitStatus>>
}

#[derive(Clone, Debug)]
pub struct JobExitStatus {
    pub finished: bool,
    pub exit_code: i32,
}

impl Job {
    pub fn new(info: JobInfo, notify_job_status: Arc<Notify>) -> Self {
        Job {
            info,
            notify_job_status,
            exit_status: Arc::new(Mutex::new(JobExitStatus{finished: false, exit_code: -42}))
        }
    }

    pub fn run(&mut self) -> Result<(), Box<dyn Error>> {
        // Update job status
        let job_status = &self.info.status;
        if let JobStatus::Offered { issued, to } = job_status {
            self.info.status = JobStatus::Running {
                issued: issued.clone(),
                started: Utc::now(),
                on: to.clone(),
            };
        } else {
            // Can this happen?
            log::error!("Expected job state to be offered, found: {:?}", job_status);
        }

        // Run command as sub-process
        assert!(!self.info.cmd.is_empty()); // TODO
        log::trace!("Running command: {}", self.info.cmd.join(" "));
        let mut cmd = Command::new(self.info.cmd.first().unwrap());
        cmd.current_dir(self.info.cwd.clone());
        cmd.args(&self.info.cmd[1..]);

        let mut child = cmd.spawn()?;
        let notify = Arc::clone(&self.notify_job_status);
        let status = Arc::clone(&self.exit_status);

        tokio::spawn(async move {
            log::trace!("Waiting for job to finish...");
            let result = child.wait().await;
            log::trace!("Job finished!");

            // When done, set exit status
            match result {
                Ok(exit_status) => {
                    let mut status_lock = status.lock().unwrap();
                    status_lock.finished = true;
                    status_lock.exit_code = exit_status.code().unwrap_or(-44);
                }
                Err(e) => {
                    log::error!("Error while waiting for child process: {}", e);
                    let mut status_lock = status.lock().unwrap();
                    status_lock.finished = true;
                    status_lock.exit_code = -45;
                }
            }

            // Notify main thread
            notify.notify_one();
        });

        Ok(())
    }
}
