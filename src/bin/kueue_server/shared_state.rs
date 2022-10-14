use std::path::PathBuf;

use kueue::message::JobInfo;

pub struct SharedState {
    jobs: Vec<Job>,
}

impl SharedState {
    pub fn new() -> Self {
        SharedState { jobs: Vec::new() }
    }

    pub fn add_job(&mut self, cmd: String, cwd: PathBuf) {
        let job = Job {
            cmd,
            cwd,
            state: JobState::Pending,
        };
        self.jobs.push(job);
    }

    pub fn get_job_list(&self) -> Vec<JobInfo> {
        let mut job_list: Vec<JobInfo> = Vec::new();
        for job in &self.jobs {
            job_list.push(JobInfo {
                cmd: job.cmd.clone(),
                status: format!("{:?}", job.state),
            });
        }
        job_list
    }
}

#[derive(Debug)]
pub struct Job {
    cmd: String,
    cwd: PathBuf,
    state: JobState,
}

#[derive(Debug)]
pub enum JobState {
    Pending,
    // TODO...
}
