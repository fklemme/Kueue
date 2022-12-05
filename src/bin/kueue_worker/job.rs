use anyhow::Result;
use chrono::{Duration, Utc};
use futures::future::try_join3;
use kueue::structs::JobInfo;
use std::{
    process::Stdio,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::Notify,
};

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
        let job_result = Arc::clone(&self.result);
        let notify_kill_job = Arc::clone(&self.notify_kill_job);
        let job_id = self.info.id;

        tokio::spawn(async move {
            log::trace!("Waiting for job {} to finish...", job_id);

            // This is basically the implementation of wait_with_output from
            // https://docs.rs/tokio/1.22.0/src/tokio/process/mod.rs.html#1213-1241
            // The problem with calling that function directly is that it
            // _moves_ the child into the fuction, making it impossible to
            // borrow it later for killing, if needed.
            async fn read_to_end<A: AsyncRead + Unpin>(
                io: &mut Option<A>,
            ) -> std::io::Result<Vec<u8>> {
                let mut vec = Vec::new();
                if let Some(io) = io.as_mut() {
                    io.read_to_end(&mut vec).await?;
                }
                Ok(vec)
            }

            let mut stdout_pipe = child.stdout.take();
            let mut stderr_pipe = child.stderr.take();

            let stdout_fut = read_to_end(&mut stdout_pipe);
            let stderr_fut = read_to_end(&mut stderr_pipe);

            let combined_fut = try_join3(child.wait(), stdout_fut, stderr_fut);

            tokio::select! {
                combined_result = combined_fut => {
                    log::trace!("Job {} finished orderly!",job_id);
                    let finish_time = Utc::now();

                    // When done, set exit status
                    match combined_result {
                        Ok((status, stdout, stderr)) => {
                            let mut result_lock = job_result.lock().unwrap();
                            result_lock.finished = true;
                            result_lock.exit_code = status.code().unwrap_or(-44);
                            result_lock.run_time = finish_time - start_time;
                            result_lock.comment = "Job finished orderly.".into();
                            result_lock.stdout = String::from_utf8(stdout)
                                .unwrap_or("failed to parse stdout into utf-8 string".into());
                            result_lock.stderr = String::from_utf8(stderr)
                                .unwrap_or("failed to parse stderr into utf-8 string".into());
                        }
                        Err(e) => {
                            log::error!("Error while waiting for child process: {}", e);
                            let mut result_lock = job_result.lock().unwrap();
                            result_lock.finished = true;
                            result_lock.exit_code = -45;
                            result_lock.comment = format!("Error while waiting for child process: {}", e);
                            result_lock.run_time = finish_time - start_time;
                        }
                    }
                }
                _ = notify_kill_job.notified() => {
                    log::trace!("Kill job {}!", job_id);
                    if let Err(e) = child.kill().await {
                        log::error!("Failed to kill job {}: {}", job_id, e);
                    }

                    // Update job result
                    let finish_time = Utc::now();
                    let mut result_lock = job_result.lock().unwrap();
                    result_lock.finished = true;
                    result_lock.exit_code = -46;
                    result_lock.comment = format!("Job killed!");
                    result_lock.run_time = finish_time - start_time;
                }
            }

            // Notify main thread
            log::trace!("Notify job {} done!", job_id);
            notify_job_status.notify_one();
        });

        Ok(())
    }
}
