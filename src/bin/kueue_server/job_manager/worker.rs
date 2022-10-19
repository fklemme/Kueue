use kueue::structs::WorkerInfo;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Worker {
    pub id: u64,
    pub info: WorkerInfo,
}

impl Worker {
    pub fn new(name: String) -> Self {
        static WORKER_COUNTER: AtomicU64 = AtomicU64::new(0);
        Worker {
            id: WORKER_COUNTER.fetch_add(1, Ordering::Relaxed),
            info: WorkerInfo::new(name),
        }
    }
}
