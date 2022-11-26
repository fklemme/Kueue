use kueue::structs::WorkerInfo;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Worker {
    pub id: usize,
    pub info: WorkerInfo,
}

impl Worker {
    pub fn new(name: String) -> Self {
        static WORKER_COUNTER: AtomicUsize = AtomicUsize::new(0);
        Worker {
            id: WORKER_COUNTER.fetch_add(1, Ordering::Relaxed),
            info: WorkerInfo::new(name),
        }
    }
}
