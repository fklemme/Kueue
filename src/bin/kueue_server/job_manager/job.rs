use kueue_lib::structs::JobInfo;

pub struct Job {
    pub info: JobInfo,
    pub worker_id: Option<usize>,
    pub stdout_text: Option<String>,
    pub stderr_text: Option<String>,
}

impl Job {
    pub fn from(job_info: JobInfo) -> Self {
        Job {
            info: JobInfo::from(job_info),
            worker_id: None,
            stdout_text: None,
            stderr_text: None,
        }
    }
}
