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
        let job_status = &self.info.status;
        if let JobStatus::Offered { issued, to } = job_status {
            self.info.status = JobStatus::Running {
                issued: issued.clone(),
                started: Utc::now(),
                on: to.clone(),
            };
        } else {
            // Can this happen?
        }

        // TODO: Do smart stuff here!
    }
}
