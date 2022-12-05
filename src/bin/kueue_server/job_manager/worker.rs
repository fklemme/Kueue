use kueue::structs::WorkerInfo;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

pub struct Worker {
    pub id: usize,
    pub info: WorkerInfo,
    pub kill_job_tx: mpsc::Sender<usize>,
}

impl Worker {
    pub fn new(name: String, kill_job_tx: mpsc::Sender<usize>) -> Self {
        static WORKER_COUNTER: AtomicUsize = AtomicUsize::new(0);
        Worker {
            id: WORKER_COUNTER.fetch_add(1, Ordering::Relaxed),
            info: WorkerInfo::new(name),
            kill_job_tx,
        }
    }
}
