use chrono::Utc;
use kueue::structs::{JobInfo, JobStatus};

#[derive(Debug)]
pub struct Job {
    pub info: JobInfo,
}

impl Job {
    pub fn new(info: JobInfo) -> Self {
        Job { info }
    }

    pub fn run(&mut self) {
        self.info.status = JobStatus::Running {
            started: Utc::now(),
        }

        // TODO: Do smart stuff here!
    }
}
