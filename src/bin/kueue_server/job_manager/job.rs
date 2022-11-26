use std::path::PathBuf;

use kueue::structs::JobInfo;

pub struct Job {
    pub info: JobInfo,
    pub worker_id: Option<usize>,
}

impl Job {
    pub fn new(cmd: Vec<String>, cwd: PathBuf) -> Self {
        Job {
            info: JobInfo::new(cmd, cwd),
            worker_id: None,
        }
    }
}
