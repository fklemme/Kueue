use anyhow::Result;
use chrono::{Duration, Utc};
use kueue::structs::JobInfo;
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
    pub notify_kill_job: Arc<Notify>,
}

#[derive(Clone, Debug)]
pub struct JobResult {
    pub finished: bool,
    pub exit_code: i32,
    pub run_time: Duration,
    pub comment: String,
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
                comment: String::new(),
                stdout: String::new(),
                stderr: String::new(),
            })),
            notify_kill_job: Arc::new(Notify::new()),
        }
    }

    pub fn run(&mut self) -> Result<()> {
        // Run command as sub-process
        assert!(!self.info.cmd.is_empty()); // TODO
        log::trace!("Running command: {}", self.info.cmd.join(" "));
        let start_time = Utc::now();
        let mut cmd = Command::new(self.info.cmd.first().unwrap());
        cmd.current_dir(self.info.cwd.clone());
        cmd.args(&self.info.cmd[1..]);

        // Spawn child process and capture output
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let mut child = cmd.spawn()?;

        let notify_job_status = Arc::clone(&self.notify_job_status);
        let result = Arc::clone(&self.result);
        let notify_kill_job = Arc::clone(&self.notify_kill_job);

        tokio::spawn(async move {
            log::trace!("Waiting for job to finish...");
            tokio::select! {
                _ = child.wait() => {
                    log::trace!("Job finished orderly!");
                    let finish_time = Utc::now();

                    // When done, set exit status
                    match cmd.output().await {
                        Ok(output) => {
                            let mut result_lock = result.lock().unwrap();
                            result_lock.finished = true;
                            result_lock.exit_code = output.status.code().unwrap_or(-44);
                            result_lock.run_time = finish_time - start_time;
                            result_lock.comment = "Job finished orderly.".into();
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
                            result_lock.comment = format!("Error while waiting for child process: {}", e);
                            result_lock.run_time = finish_time - start_time;
                        }
                    }
                }
                _ = notify_kill_job.notified() => {
                    log::trace!("Kill job!");
                    if let Err(e) = child.kill().await {
                        log::error!("Failed to kill job: {}", e);
                    }

                    // Update job result
                    let finish_time = Utc::now();
                    let mut result_lock = result.lock().unwrap();
                    result_lock.finished = true;
                    result_lock.exit_code = -46;
                    result_lock.comment = format!("Job killed!");
                    result_lock.run_time = finish_time - start_time;
                }
            }

            // Notify main thread
            notify_job_status.notify_one();
        });

        Ok(())
    }
}
