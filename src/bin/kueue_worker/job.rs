//! This module takes care of executing the jobs on the worker.

use anyhow::{bail, Result};
use chrono::{Duration, Utc};
use futures::future::try_join3;
use kueue_lib::structs::JobInfo;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
};
use tokio::{
    fs::File,
    io::{copy, AsyncRead, AsyncReadExt, AsyncWrite},
    process::Command,
    sync::Notify,
};

/// Struct, representing a job on the worker.
#[derive(Debug)]
pub struct Job {
    /// All information about the job.
    pub info: JobInfo,
    /// The job-execution thread will notify when the job has concluded.
    pub notify_job_status: Arc<Notify>,
    /// Status and output of the job after concluding.
    pub result: Arc<Mutex<JobResult>>,
    /// Will be notified by the worker thread when job should be killed.
    pub notify_kill_job: Arc<Notify>,
}

/// Status and outputs of the job after execution is concluded.
#[derive(Clone, Debug)]
pub struct JobResult {
    /// Set to `true` if the job's execution has concluded.
    pub finished: bool,
    /// Return code from the underlying process.
    pub exit_code: i32,
    /// Runtime of the underlying process.
    pub run_time: Duration,
    /// Additional information about the status of the job.
    /// This will be send back to the user and can help with debugging.
    pub comment: String,
    /// All text that has been sent to stdout by the underlying process.
    pub stdout_text: String,
    /// All text that has been sent to stderr by the underlying process.
    pub stderr_text: String,
}

impl Job {
    /// Setup a new job for execution.
    pub fn new(info: JobInfo, notify_job_status: Arc<Notify>) -> Self {
        Job {
            info,
            notify_job_status,
            result: Arc::new(Mutex::new(JobResult {
                finished: false,
                exit_code: -42,
                run_time: Duration::min_value(),
                comment: String::new(),
                stdout_text: String::new(),
                stderr_text: String::new(),
            })),
            notify_kill_job: Arc::new(Notify::new()),
        }
    }

    /// Start executing the job.
    pub async fn run(&mut self) -> Result<()> {
        if self.info.cmd.is_empty() {
            bail!("Empty command!");
        }

        // Set up command as subprocess.
        let mut cmd = Command::new(self.info.cmd.first().unwrap());
        cmd.current_dir(self.info.cwd.clone());
        cmd.args(&self.info.cmd[1..]);

        /// Pipe output or redirect to files.
        async fn get_path_and_file(
            path: &Option<String>,
            cwd: &Path,
        ) -> Result<(Stdio, Option<PathBuf>, Option<File>)> {
            match path {
                Some(path) if path.to_lowercase().trim() == "null" => {
                    log::trace!("Redirect to null!");
                    Ok((Stdio::null(), None, None))
                }
                Some(path) => {
                    log::trace!("Redirect to file!");

                    // Check if path is absolute, otherwise use cwd.
                    let mut full_path = Path::new(path).to_owned();
                    if full_path.is_relative() {
                        full_path = cwd.join(full_path);
                    }

                    let file = match File::create(&full_path).await {
                        Ok(file) => file,
                        Err(e) => bail!(
                            "Failed to create file {}: {}",
                            full_path.to_string_lossy(),
                            e
                        ),
                    };

                    Ok((Stdio::piped(), Some(full_path), Some(file)))
                }
                None => Ok((Stdio::piped(), None, None)),
            }
        }

        // FIXME: What happens if stdout_path == stderr_path?
        cmd.stdin(Stdio::null());
        let (cfg, stdout_path, mut stdout_file) =
            get_path_and_file(&self.info.stdout_path, &self.info.cwd).await?;
        cmd.stdout(cfg);
        let (cfg, stderr_path, mut stderr_file) =
            get_path_and_file(&self.info.stderr_path, &self.info.cwd).await?;
        cmd.stderr(cfg);

        // Spawn child process.
        log::trace!("Running command: {}", self.info.cmd.join(" "));
        let start_time = Utc::now();
        let mut child = cmd.spawn()?;

        let notify_job_status = Arc::clone(&self.notify_job_status);
        let job_result = Arc::clone(&self.result);
        let notify_kill_job = Arc::clone(&self.notify_kill_job);
        let job_id = self.info.job_id;

        tokio::spawn(async move {
            /// This is based on the implementation of wait_with_output from
            /// https://docs.rs/tokio/1.22.0/src/tokio/process/mod.rs.html#1213-1241
            /// The problem with calling that function directly is that it
            /// _moves_ the child into the function, making it impossible to
            /// borrow it later for killing, if needed.
            async fn read_or_copy<A: AsyncRead + Unpin, B: AsyncWrite + Unpin>(
                io: &mut Option<A>,
                file: &mut Option<B>,
                path: Option<PathBuf>,
            ) -> std::io::Result<Vec<u8>> {
                let mut vec = Vec::new();
                if let Some(io) = io.as_mut() {
                    // If input is available, read and...
                    if let Some(file) = file.as_mut() {
                        // ...copy it to redirect file.
                        copy(io, file).await?;
                        if let Some(path) = path {
                            // Leave a hint that input has been redirected.
                            let hint = format!("Redirected to {}", path.to_string_lossy());
                            vec.extend(hint.as_bytes());
                        }
                    } else {
                        // ...or append input to buffer to send later.
                        io.read_to_end(&mut vec).await?;
                    }
                }
                Ok(vec)
            }

            let mut stdout_pipe = child.stdout.take();
            let mut stderr_pipe = child.stderr.take();

            let stdout_fut = read_or_copy(&mut stdout_pipe, &mut stdout_file, stdout_path);
            let stderr_fut = read_or_copy(&mut stderr_pipe, &mut stderr_file, stderr_path);

            let combined_fut = try_join3(child.wait(), stdout_fut, stderr_fut);

            log::trace!("Waiting for job {} to finish...", job_id);
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
                            result_lock.stdout_text = String::from_utf8(stdout)
                                .unwrap_or("failed to parse stdout into utf-8 string".into());
                            result_lock.stderr_text = String::from_utf8(stderr)
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
                    result_lock.comment = "Job killed!".to_string();
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
