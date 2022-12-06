use std::path::PathBuf;

use kueue::structs::JobInfo;

pub struct Job {
    pub info: JobInfo,
    pub worker_id: Option<usize>,
    pub stdout_text: Option<String>,
    pub stderr_text: Option<String>,
}

impl Job {
    pub fn new(
        cmd: Vec<String>,
        cwd: PathBuf,
        stdout_path: Option<String>,
        stderr_path: Option<String>,
    ) -> Self {
        Job {
            info: JobInfo::new(cmd, cwd, stdout_path, stderr_path),
            worker_id: None,
            stdout_text: None,
            stderr_text: None,
        }
    }
}
