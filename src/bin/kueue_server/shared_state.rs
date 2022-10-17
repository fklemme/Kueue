use chrono::Utc;
use kueue::structs::{JobInfo, JobStatus, WorkerInfo};
use tokio::sync::Notify;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};

type JobList = Vec<Arc<Mutex<Job>>>;
type WorkerList = Vec<Weak<Mutex<Worker>>>;

// Hold all shared information in the server process
#[derive(Debug)]
pub struct SharedState {
    pending_jobs: JobList,
    offered_jobs: JobList,
    running_jobs: JobList,
    finished_jobs: JobList,
    workers: WorkerList,
    new_jobs: Arc<Notify>
}

#[derive(Debug)]
pub struct Job {
    pub info: JobInfo,
    pub worker: Option<Weak<Mutex<Worker>>>,
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
            offered_jobs: Vec::new(),
            running_jobs: Vec::new(),
            finished_jobs: Vec::new(),
            workers: Vec::new(),
            new_jobs: Arc::new(Notify::new()),
        }
    }

    pub fn notify_new_jobs(&self) -> Arc<Notify> {
        Arc::clone(&self.new_jobs)
    }

    // Add newly submitted job to list of pending jobs.
    pub fn add_new_job(&mut self, cmd: String, cwd: PathBuf) -> Arc<Mutex<Job>> {
        // Add new job. We create a new JobInfo instance to make sure to
        // not adopt remote (non-unique) job ids or inconsistent states.
        let job = Arc::new(Mutex::new(Job {
            info: JobInfo::new(cmd, cwd),
            worker: None,
        }));
        self.pending_jobs.push(Arc::clone(&job));
        job
    }

    // Register a new worker where jobs can be assigned to.
    pub fn register_worker(&mut self, name: String) -> Arc<Mutex<Worker>> {
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
        // TODO: update for all lists
        let mut job_info_list: Vec<JobInfo> = Vec::new();
        for list in [
            &self.pending_jobs,
            &self.offered_jobs,
            &self.running_jobs,
            &self.finished_jobs,
        ] {
            for job in list {
                let job_info = job.lock().unwrap().info.clone();
                job_info_list.push(job_info);
            }
        }
        job_info_list
    }

    // Get all worker information to send to the client upon request
    pub fn get_all_worker_infos(&self) -> Vec<WorkerInfo> {
        let mut worker_info_list: Vec<WorkerInfo> = Vec::new();
        for worker in &self.workers {
            if let Some(worker) = worker.upgrade() {
                let worker_info = worker.lock().unwrap().info.clone();
                worker_info_list.push(worker_info);
            }
        }
        worker_info_list
    }

    // Finds a pending job, moves it to "offered" lists, and returns it.
    pub fn get_pending_job_and_make_offered(
        &mut self,
        worker: Arc<Mutex<Worker>>,
    ) -> Option<Arc<Mutex<Job>>> {
        if self.pending_jobs.is_empty() {
            None // no pending jobs
        } else {
            let job = self.pending_jobs.remove(0);
            let mut worker_lock = worker.lock().unwrap();
            let mut job_lock = job.lock().unwrap();
            // Update job status
            match job_lock.info.status {
                JobStatus::Pending { issued } => {
                    job_lock.info.status = JobStatus::Offered {
                        issued,
                        to: worker_lock.info.name.clone(),
                    };
                    job_lock.worker = Some(Arc::downgrade(&worker));
                    // Add to offered lists
                    self.offered_jobs.push(Arc::clone(&job));
                    worker_lock.offered_jobs.push(Arc::clone(&job));
                    worker_lock.info.current_jobs += 1;
                    drop(job_lock); // unlock job to move
                    Some(job)
                }
                _ => {
                    log::error!("Expected status 'pending' for job: {:?}", job_lock.info);
                    None // TODO: what to do here?
                }
            }
        }
    }

    // Finds the job in the shared state based on the job's ID.
    pub fn get_job_from_info(&self, job_info: JobInfo) -> Option<Arc<Mutex<Job>>> {
        let target_list = match job_info.status {
            JobStatus::Pending { .. } => &self.pending_jobs,
            JobStatus::Offered { .. } => &self.offered_jobs,
            JobStatus::Running { .. } => &self.running_jobs,
            JobStatus::Finished { .. } => &self.finished_jobs,
        };
        let index = target_list
            .iter()
            .position(|job| job.lock().unwrap().info.id == job_info.id);
        match index {
            Some(index) => Some(Arc::clone(&target_list[index])),
            None => None,
        }
    }

    // Moves job from offered to running/assigned lists and updates info.
    pub fn move_accepted_job_to_running(&mut self, job: Arc<Mutex<Job>>) {
        let option_worker = job.lock().unwrap().worker.clone();
        if let Some(weak_worker) = option_worker {
            if let Some(worker) = weak_worker.upgrade() {
                log::debug!("lock worker");
                let mut worker_lock = worker.lock().unwrap();
                log::debug!("lock job");
                let mut job_lock = job.lock().unwrap();
                log::debug!("all locks aquired");

                // Update job status
                if let JobStatus::Offered { issued, to } = &job_lock.info.status {
                    job_lock.info.status = JobStatus::Running {
                        issued: issued.clone(),
                        started: Utc::now(),
                        on: worker_lock.info.name.clone(),
                    };
                } else {
                    log::error!("Expected status 'offered' for job: {:?}", job_lock.info);
                    // TODO: what to do here?
                }

                // Move from offered to assigned in worker
                let job_index = worker_lock
                    .offered_jobs
                    .iter()
                    .position(|j| Arc::as_ptr(j) == Arc::as_ptr(&job));
                match job_index {
                    Some(index) => {
                        // Move job
                        let job = worker_lock.offered_jobs.remove(index);
                        worker_lock.assigned_jobs.push(job);
                    }
                    None => {
                        log::error!(
                            "Job not found in worker's offered list: {:?}",
                            job_lock.info
                        );
                    }
                }

                // Move from offered to running in shared state
                let job_index = self
                    .offered_jobs
                    .iter()
                    .position(|j| Arc::as_ptr(j) == Arc::as_ptr(&job));
                match job_index {
                    Some(index) => {
                        // Move job
                        let job = self.offered_jobs.remove(index);
                        self.running_jobs.push(job);
                    }
                    None => {
                        log::error!(
                            "Job not found in pending list: {:?}",
                            job.lock().unwrap().info
                        );
                    }
                }
            } else {
                log::error!(
                    "Worker of job no longer valid: {:?}",
                    job.lock().unwrap().info
                );
                // TODO: Can this happen? We could recover from this, sending the job back to pending list!
            }
        } else {
            log::error!("No worker assigned to job: {:?}", job.lock().unwrap().info);
        }
    }

    // Moves job from offered to pending list and updates info.
    pub fn move_rejected_job_to_pending(&self, job: Arc<Mutex<Job>>) {
        // TODO!
    }
}
