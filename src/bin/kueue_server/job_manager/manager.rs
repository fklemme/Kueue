use crate::job_manager::{Job, Worker};
use chrono::{Duration, Utc};
use kueue::structs::{JobInfo, JobStatus, WorkerInfo};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;

pub struct Manager {
    jobs: BTreeMap<u64, Arc<Mutex<Job>>>,
    jobs_waiting_for_assignment: BTreeSet<u64>,
    workers: BTreeMap<u64, Weak<Mutex<Worker>>>,
    new_jobs: Arc<Notify>,
}

impl Manager {
    pub fn new() -> Self {
        Manager {
            jobs: BTreeMap::new(),
            jobs_waiting_for_assignment: BTreeSet::new(),
            workers: BTreeMap::new(),
            new_jobs: Arc::new(Notify::new()),
        }
    }

    /// Registers a new worker to process jobs.
    pub fn add_new_worker(&mut self, name: String) -> Arc<Mutex<Worker>> {
        let worker = Worker::new(name);
        let worker_id = worker.id;
        let worker = Arc::new(Mutex::new(worker));
        self.workers.insert(worker_id, Arc::downgrade(&worker));
        worker
    }

    /// Adds a new job to be processed.
    pub fn add_new_job(&mut self, cmd: Vec<String>, cwd: PathBuf) -> Arc<Mutex<Job>> {
        let job = Job::new(cmd, cwd);
        let job_id = job.info.id;
        let job = Arc::new(Mutex::new(job));
        self.jobs.insert(job_id, Arc::clone(&job));
        self.jobs_waiting_for_assignment.insert(job_id);
        job
    }

    /// Get a handle to the "new jobs" notifier.
    pub fn new_jobs(&self) -> Arc<Notify> {
        Arc::clone(&self.new_jobs)
    }

    /// Get job by ID.
    pub fn get_job(&self, id: u64) -> Option<Arc<Mutex<Job>>> {
        match self.jobs.get(&id) {
            Some(job) => Some(Arc::clone(job)),
            None => None,
        }
    }

    /// Collect job informations about all jobs.
    pub fn get_all_job_infos(&self) -> Vec<JobInfo> {
        let mut job_infos = Vec::new();
        for (_id, job) in &self.jobs {
            job_infos.push(job.lock().unwrap().info.clone());
        }
        job_infos
    }

    /// Cellect worker information about all workers.
    pub fn get_all_worker_infos(&self) -> Vec<WorkerInfo> {
        let mut worker_infos = Vec::new();
        for (_id, worker) in &self.workers {
            if let Some(worker) = worker.upgrade() {
                worker_infos.push(worker.lock().unwrap().info.clone());
            }
        }
        worker_infos
    }

    /// Get a job to be assigned to a worker.
    pub fn get_job_waiting_for_assignment(
        &mut self,
        exclude: &BTreeSet<u64>,
    ) -> Option<Arc<Mutex<Job>>> {
        if self.jobs_waiting_for_assignment.is_empty() {
            None // no jobs marked waiting for assignment
        } else {
            let option_job_id = self
                .jobs_waiting_for_assignment
                .iter()
                .find(|job_id| !exclude.contains(job_id))
                .cloned();
            if let Some(job_id) = option_job_id {
                // Found matching job
                self.jobs_waiting_for_assignment.remove(&job_id);
                return Some(Arc::clone(self.jobs.get(&job_id).unwrap()));
            }
            None // no job fulfilling requirements
        }
    }

    /// Inspect every job and "repair" if needed.
    pub fn run_maintenance(&mut self) {
        for (job_id, job) in &self.jobs {
            let info = job.lock().unwrap().info.clone();
            match &info.status {
                JobStatus::Pending { .. } => {
                    // Pending jobs should be available for workers
                    let newly_inserted = self.jobs_waiting_for_assignment.insert(job_id.clone());
                    if newly_inserted {
                        log::warn!("Job {} was pending but not available for workers!", job_id);
                    }
                }
                JobStatus::Offered {
                    issued,
                    offered,
                    to,
                } => {
                    // A job should only be briefly in this state
                    let offer_timed_out = (Utc::now() - offered.clone()).num_minutes() > 5;
                    let worker_id = job.lock().unwrap().worker_id;
                    let worker_alive = match worker_id {
                        Some(id) => match self.workers.get(&id) {
                            Some(weak_worker) => {
                                // Worker is still with us.
                                weak_worker.upgrade().is_some()
                            }
                            None => false,
                        },
                        None => false,
                    };

                    // Recover if offer timed out of worker died
                    if offer_timed_out || !worker_alive {
                        log::warn!("Job {:?} got stuck in offered state. Recover...", info);
                        let mut job_lock = job.lock().unwrap();
                        job_lock.info.status = JobStatus::Pending {
                            issued: issued.clone(),
                        };
                        job_lock.worker_id = None;
                        self.jobs_waiting_for_assignment.insert(job_id.clone());
                    }
                }
                // TODO: Finish implementation...
                JobStatus::Running {
                    issued,
                    started,
                    on,
                } => {}
                JobStatus::Finished {
                    finished,
                    return_code,
                    on,
                    run_time_seconds,
                } => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_new_worker() {
        let mut manager = Manager::new();
        let worker = manager.add_new_worker("test_worker".into());
        assert_eq!(worker.lock().unwrap().id, 0);
    }

    #[test]
    fn add_new_job() {
        let mut manager = Manager::new();
        let cmd = vec!["ls".to_string(), "-la".to_string()];
        let cwd: PathBuf = "/tmp".into();
        manager.add_new_job(cmd, cwd);
        assert_eq!(manager.get_all_job_infos().len(), 1);
    }

    #[test]
    fn get_job_waiting_for_assignment() {
        let mut manager = Manager::new();
        let cmd = vec!["ls".to_string(), "-la".to_string()];
        let cwd: PathBuf = "/tmp".into();
        let job = manager.add_new_job(cmd, cwd);

        // Put job on exclude list.
        let mut exclude = BTreeSet::new();
        exclude.insert(job.lock().unwrap().info.id);

        // Now, we should not get it.
        let job = manager.get_job_waiting_for_assignment(&exclude);
        assert!(job.is_none());

        // Now we want any job. One is waiting to be assigned.
        exclude.clear();
        let job = manager.get_job_waiting_for_assignment(&exclude);
        assert!(job.is_some());

        // We want any job, again. But none are left.
        let job = manager.get_job_waiting_for_assignment(&exclude);
        assert!(job.is_none());
    }
}
