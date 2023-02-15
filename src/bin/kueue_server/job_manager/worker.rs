use kueue::structs::WorkerInfo;
use tokio::sync::mpsc;

pub struct Worker {
    pub info: WorkerInfo,
    pub kill_job_tx: mpsc::Sender<usize>,
}

impl Worker {
    pub fn new(name: String, kill_job_tx: mpsc::Sender<usize>) -> Self {
        Worker {
            info: WorkerInfo::new(name),
            kill_job_tx,
        }
    }
}
