use anyhow::Result;
use chrono::{Duration, Utc};
use kueue::structs::{JobInfo, JobStatus};
use std::{
    process::Stdio,
    sync::{Arc, Mutex},
};
use tokio::{process::Command, sync::Notify};

#[derive(Debug)]
pub struct Job {
    pub info: JobInfo,
    pub notify_job_status: Arc<Notify>,
    pub result: Arc<Mutex<JobResult>>,
}

#[derive(Clone, Debug)]
pub struct JobResult {
    pub finished: bool,
    pub exit_code: i32,
    pub run_time: Duration,
    pub stdout: String,
    pub stderr: String,
}

impl Job {
    pub fn new(info: JobInfo, notify_job_status: Arc<Notify>) -> Self {
        Job {
            info,
            notify_job_status,
            result: Arc::new(Mutex::new(JobResult {
                finished: false,
                exit_code: -42,
                run_time: Duration::min_value(),
                stdout: String::new(),
                stderr: String::new(),
            })),
        }
    }

    pub fn run(&mut self) -> Result<()> {
        // Update job status
        let job_status = &self.info.status;
        if let JobStatus::Offered {
            issued,
            offered: _,
            to,
        } = job_status
        {
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
        let start_time = Utc::now();
        let mut cmd = Command::new(self.info.cmd.first().unwrap());
        cmd.current_dir(self.info.cwd.clone());
        cmd.args(&self.info.cmd[1..]);

        // Spawn child process and capture output
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let child = cmd.spawn()?;

        let notify = Arc::clone(&self.notify_job_status);
        let result = Arc::clone(&self.result);

        tokio::spawn(async move {
            log::trace!("Waiting for job to finish...");
            let output = child.wait_with_output().await;
            log::trace!("Job finished!");
            let finish_time = Utc::now();

            // When done, set exit status
            match output {
                Ok(output) => {
                    let mut result_lock = result.lock().unwrap();
                    result_lock.finished = true;
                    result_lock.exit_code = output.status.code().unwrap_or(-44);
                    result_lock.run_time = finish_time - start_time;
                    result_lock.stdout = String::from_utf8(output.stdout)
                        .unwrap_or("failed to parse stdout into utf-8 string".into());
                    result_lock.stderr = String::from_utf8(output.stderr)
                        .unwrap_or("failed to parse stderr into utf-8 string".into());
                }
                Err(e) => {
                    log::error!("Error while waiting for child process: {}", e);
                    let mut result_lock = result.lock().unwrap();
                    result_lock.finished = true;
                    result_lock.exit_code = -45;
                    result_lock.run_time = finish_time - start_time;
                }
            }

            // Notify main thread
            notify.notify_one();
        });

        Ok(())
    }
}
