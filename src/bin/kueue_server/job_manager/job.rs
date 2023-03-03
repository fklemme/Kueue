use kueue_lib::structs::JobInfo;
use tokio::sync::mpsc;

pub struct Job {
    pub info: JobInfo,
    pub worker_id: Option<u64>,
    pub stdout_text: Option<String>,
    pub stderr_text: Option<String>,
    pub observers: Vec<mpsc::Sender<u64>>,
}

impl Job {
    pub fn from(job_info: JobInfo) -> Self {
        Job {
            info: JobInfo::from(job_info),
            worker_id: None,
            stdout_text: None,
            stderr_text: None,
            observers: Vec::new(),
        }
    }

    pub fn notify_observers(&self) {
        for observer in &self.observers {
            if let Err(err) = observer.try_send(self.info.job_id) {
                log::error!("Failed to notify job: {err}");
            }
        }
    }
}
