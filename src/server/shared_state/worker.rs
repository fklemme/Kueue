use crate::structs::WorkerInfo;
use tokio::sync::mpsc;

/// Representation of the connected worker in the `job_manager`.
pub struct Worker {
    pub info: WorkerInfo,
    /// Channel can be used to issue the termination of
    /// the job with the submitted id on the remote worker.
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
