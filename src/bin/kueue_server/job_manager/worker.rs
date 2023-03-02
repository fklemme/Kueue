use kueue_lib::structs::WorkerInfo;
use tokio::sync::mpsc;

pub struct Worker {
    pub info: WorkerInfo,
    pub kill_job_tx: mpsc::Sender<u64>,
}

impl Worker {
    pub fn new(name: String, kill_job_tx: mpsc::Sender<u64>) -> Self {
        Worker {
            info: WorkerInfo::new(name),
            kill_job_tx,
        }
    }
}
