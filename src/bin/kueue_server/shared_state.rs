use chrono::Utc;
use kueue::structs::{JobInfo, JobStatus, WorkerInfo};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;

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
    new_jobs: Arc<Notify>,
}

#[derive(Debug)]
pub struct Job {
    pub info: JobInfo,
    pub worker: Option<Weak<Mutex<Worker>>>,
}

#[derive(Debug)]
pub struct Worker {
    pub info: WorkerInfo,
    pub offered_jobs: JobList,
    pub accepted_jobs: JobList,
    pub rejected_jobs: Vec<Weak<Mutex<Job>>>,
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

    pub fn new_jobs(&self) -> Arc<Notify> {
        Arc::clone(&self.new_jobs)
    }

    // Add newly submitted job to list of pending jobs.
    pub fn add_new_job(&mut self, cmd: Vec<String>, cwd: PathBuf) -> Arc<Mutex<Job>> {
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
            accepted_jobs: Vec::new(),
            rejected_jobs: Vec::new(),
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

    // Update job status and assigns the job to the corresponding job list.
    fn update_job_status(
        &mut self,
        job: Arc<Mutex<Job>>,
        new_status: JobStatus,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let old_status = job.lock().unwrap().info.status.clone();

        // Remove from old list
        let removed_job = {
            let source_list = self.get_job_list(old_status.clone());
            let index = source_list.iter().position(|j| Arc::ptr_eq(j, &job));
            if let Some(index) = index {
                source_list.remove(index)
            } else {
                return Err(format!(
                    "Job not found in corresponding source list: {:?}",
                    old_status
                )
                .into());
            }
        };

        // Update status
        removed_job.lock().unwrap().info.status = new_status.clone();

        // Move to new list
        let target_list = self.get_job_list(new_status);
        target_list.push(removed_job);

        Ok(())
    }

    // Get job list based on the job's status.
    fn get_job_list(&mut self, job_status: JobStatus) -> &mut JobList {
        match job_status {
            JobStatus::Pending { .. } => &mut self.pending_jobs,
            JobStatus::Offered { .. } => &mut self.offered_jobs,
            JobStatus::Running { .. } => &mut self.running_jobs,
            JobStatus::Finished { .. } => &mut self.finished_jobs,
        }
    }

    // Finds a pending job, moves it to "offered" lists, and returns it.
    pub fn get_pending_job_and_make_offered(
        &mut self,
        worker: Arc<Mutex<Worker>>,
    ) -> Result<Option<Arc<Mutex<Job>>>, Box<dyn std::error::Error>> {
        if self.pending_jobs.is_empty() {
            Ok(None) // no pending jobs
        } else {
            // TODO: Check worker's rejected list!
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
                    drop(job_lock); // unlock job for move
                    Ok(Some(job))
                }
                _ => Err(format!(
                    "Expected status 'pending' for job in pending list: {:?}",
                    job_lock.info
                )
                .into()),
            }
        }
    }

    // Moves job from offered to running/assigned lists and updates info.
    pub fn move_accepted_job_to_running(
        &mut self,
        job: Arc<Mutex<Job>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (old_status, option_worker) = {
            let job_lock = job.lock().unwrap();
            (job_lock.info.status.clone(), job_lock.worker.clone())
        };

        if let JobStatus::Offered { issued, to } = old_status {
            // Update job in shared state
            let new_status = JobStatus::Running {
                issued,
                started: Utc::now(),
                on: to,
            };
            self.update_job_status(Arc::clone(&job), new_status)?;

            // Move to new list inside worker
            if let Some(weak_worker) = option_worker {
                if let Some(worker) = weak_worker.upgrade() {
                    let mut worker_lock = worker.lock().unwrap();
                    let index = worker_lock
                        .offered_jobs
                        .iter()
                        .position(|j| Arc::ptr_eq(j, &job));
                    if let Some(index) = index {
                        let job = worker_lock.offered_jobs.remove(index);
                        worker_lock.accepted_jobs.push(job);
                        Ok(())
                    } else {
                        Err("Job not found in worker's offered list!".into())
                    }
                } else {
                    Err("Associated worker does not exists anymore!".into())
                }
            } else {
                Err("Did not find associated worker!".into())
            }
        } else {
            Err(format!("Expected job status to be offered, found: {:?}", old_status).into())
        }
    }

    // Moves job from offered to pending list and updates info.
    pub fn move_rejected_job_to_pending(
        &mut self,
        job: Arc<Mutex<Job>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (old_status, option_worker) = {
            let job_lock = job.lock().unwrap();
            (job_lock.info.status.clone(), job_lock.worker.clone())
        };

        if let JobStatus::Offered { issued, to: _ } = old_status {
            // Update job in shared state
            let new_status = JobStatus::Pending { issued };
            self.update_job_status(Arc::clone(&job), new_status)?;
            // Remove associated worker
            job.lock().unwrap().worker = None;

            // Move to rejected list on old worker
            if let Some(weak_worker) = option_worker {
                if let Some(worker) = weak_worker.upgrade() {
                    let mut worker_lock = worker.lock().unwrap();
                    let index = worker_lock
                        .offered_jobs
                        .iter()
                        .position(|j| Arc::ptr_eq(j, &job));
                    if let Some(index) = index {
                        let job = worker_lock.offered_jobs.remove(index);
                        worker_lock.rejected_jobs.push(Arc::downgrade(&job));
                        Ok(())
                    } else {
                        Err("Job not found in worker's offered list!".into())
                    }
                } else {
                    Err("Associated worker does not exists anymore!".into())
                }
            } else {
                Err("Did not find associated worker!".into())
            }
        } else {
            Err(format!("Expected job status to be offered, found: {:?}", old_status).into())
        }
    }
}
