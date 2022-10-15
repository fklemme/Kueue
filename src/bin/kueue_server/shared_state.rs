use kueue::structs::{JobInfo, JobStatus, WorkerInfo};
use std::sync::{Arc, Mutex, Weak};

// Hold all shared information in the server process
#[derive(Debug)]
pub struct SharedState {
    pending_jobs: Vec<Arc<Mutex<Job>>>,
    running_jobs: Vec<Arc<Mutex<Job>>>,
    workers: Vec<Weak<Mutex<Worker>>>,
}

#[derive(Debug)]
pub struct Job {
    pub info: JobInfo,
}

#[derive(Debug)]
pub struct Worker {
    pub info: WorkerInfo,
    pub offered_jobs: Vec<Arc<Mutex<Job>>>,
    pub assigned_jobs: Vec<Arc<Mutex<Job>>>,
    pub connected: bool,
}

impl SharedState {
    pub fn new() -> Self {
        SharedState {
            pending_jobs: Vec::new(),
            running_jobs: Vec::new(),
            workers: Vec::new(),
        }
    }

    pub fn add_job(&mut self, job_info: JobInfo) -> Arc<Mutex<Job>> {
        let job = Arc::new(Mutex::new(Job { info: job_info }));
        self.pending_jobs.push(Arc::clone(&job));
        job
    }

    pub fn add_worker(&mut self, name: String) -> Arc<Mutex<Worker>> {
        let worker = Arc::new(Mutex::new(Worker {
            info: WorkerInfo::new(name),
            offered_jobs: Vec::new(),
            assigned_jobs: Vec::new(),
            connected: true,
        }));
        self.workers.push(Arc::downgrade(&worker));
        worker
    }

    // Get all job information to send to the client upon request
    pub fn get_all_job_infos(&self) -> Vec<JobInfo> {
        let mut job_list: Vec<JobInfo> = Vec::new();
        for job in &self.pending_jobs {
            let job_info = job.lock().unwrap().info.clone();
            job_list.push(job_info);
        }
        job_list
    }

    // Get all worker information to send to the client upon request
    pub fn get_all_worker_infos(&self) -> Vec<WorkerInfo> {
        let mut worker_list: Vec<WorkerInfo> = Vec::new();
        for worker in &self.workers {
            if let Some(worker) = worker.upgrade() {
                let worker_info = worker.lock().unwrap().info.clone();
                worker_list.push(worker_info);
            }
        }
        worker_list
    }

    // Finds pending job, marks it as "offered to worker", and returns it.
    pub fn get_pending_job_to_offer(&self, worker: Arc<Mutex<Worker>>) -> Option<Arc<Mutex<Job>>> {
        for job in &self.pending_jobs {
            // Explicitly check the status of each job.
            let mut job_lock = job.lock().unwrap();
            if let JobStatus::Pending { issued } = job_lock.info.status {
                // Found a pending job. Mark and return it.
                let mut worker_lock = worker.lock().unwrap();
                job_lock.info.status = JobStatus::Offered {
                    issued,
                    to: worker_lock.info.name.clone(),
                };
                worker_lock.offered_jobs.push(Arc::clone(job));
                return Some(Arc::clone(job));
            }
        }
        None // no pending jobs found
    }

    pub fn move_accepted_job_to_running(&mut self, job: Arc<Mutex<Job>>) {
        let job_index = self
            .pending_jobs
            .iter()
            .position(|j| Arc::as_ptr(j) == Arc::as_ptr(&job));
        match job_index {
            Some(index) => {
                // Move job
                let job = self.pending_jobs.remove(index);
                self.running_jobs.push(job);
            }
            None => {
                log::warn!("Job not found in pending_jobs list!");
            }
        }
    }
}
