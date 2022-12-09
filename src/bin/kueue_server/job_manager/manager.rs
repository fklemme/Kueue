use crate::job_manager::{Job, Worker};
use anyhow::{anyhow, Result};
use chrono::Utc;
use kueue::{
    constants::{CLEANUP_JOB_AFTER_HOURS, OFFER_TIMEOUT_MINUTES},
    structs::{JobInfo, JobStatus, WorkerInfo},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{mpsc, Notify};

pub struct Manager {
    jobs: BTreeMap<usize, Arc<Mutex<Job>>>,
    jobs_waiting_for_assignment: BTreeSet<usize>,
    workers: BTreeMap<usize, Weak<Mutex<Worker>>>,
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
    pub fn add_new_worker(
        &mut self,
        name: String,
        kill_job_tx: mpsc::Sender<usize>,
    ) -> Arc<Mutex<Worker>> {
        let worker = Worker::new(name, kill_job_tx);
        let worker_id = worker.id;
        let worker = Arc::new(Mutex::new(worker));
        self.workers.insert(worker_id, Arc::downgrade(&worker));
        worker
    }

    /// Adds a new job to be processed.
    pub fn add_new_job(&mut self, job_info: JobInfo) -> Arc<Mutex<Job>> {
        // Add new job. We create a new JobInfo instance to make sure to
        // not adopt remote (non-unique) job ids or inconsistent states.
        let job = Job::new(
            job_info.cmd,
            job_info.cwd,
            job_info.cpus,
            job_info.ram_mb,
            job_info.stdout_path,
            job_info.stderr_path,
        );
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
    pub fn get_job(&self, id: usize) -> Option<Arc<Mutex<Job>>> {
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

    /// Get worker by ID.
    // pub fn get_worker(&self, id:usize) -> Option<Weak<Mutex<Worker>>> {
    //     match self.workers.get(&id) {
    //         Some(worker) => Some(Weak::clone(worker)),
    //         None => None,
    //     }
    // }

    /// Collect worker information about all workers.
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
    /// TODO: Consider resources!
    pub fn get_job_waiting_for_assignment(
        &mut self,
        exclude: &BTreeSet<usize>,
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

    /// Cancel and remove a job from the queue. If the job is running and a
    /// worker is associated with the job, a sender is returned that can be
    /// used to signal a kill instruction to the worker. The job_id sent over
    /// the returned sender indicates the job to be killed on the worker.
    pub fn cancel_job(&mut self, id: usize, kill: bool) -> Result<Option<mpsc::Sender<usize>>> {
        match self.get_job(id) {
            Some(job) => {
                let job_info = job.lock().unwrap().info.clone();
                match job_info.status {
                    JobStatus::Pending { .. } => {
                        // Do not attempt to offer the job to workers.
                        self.jobs_waiting_for_assignment.remove(&id);
                        job.lock().unwrap().info.status = JobStatus::Canceled {
                            canceled: Utc::now(),
                        };
                        Ok(None)
                    }
                    JobStatus::Offered { .. } => {
                        // Offer will be withdrawn by the server on worker's response.
                        job.lock().unwrap().info.status = JobStatus::Canceled {
                            canceled: Utc::now(),
                        };
                        Ok(None)
                    }
                    JobStatus::Running { .. } => {
                        if !kill {
                            // Makes no sense to set the job to canceled if the
                            // worker proceeds anyway.
                            return Err(anyhow!("Job ID={} has already started!", id));
                        }
                        // Update job status
                        job.lock().unwrap().info.status = JobStatus::Canceled {
                            canceled: Utc::now(),
                        };
                        // If worker is assigned and alive, get the kill job sender.
                        let worker_id = job.lock().unwrap().worker_id;
                        if let Some(worker_id) = worker_id {
                            if let Some(worker) = self.workers.get(&worker_id) {
                                if let Some(worker) = worker.upgrade() {
                                    let tx = worker.lock().unwrap().kill_job_tx.clone();
                                    return Ok(Some(tx));
                                }
                            }
                        }
                        Err(anyhow!(
                            "Job with ID={} was running but worker could not be aquired!",
                            id
                        ))
                    }
                    JobStatus::Finished { .. } => {
                        Err(anyhow!("Job ID={} has already finished!", id))
                    }
                    JobStatus::Canceled { .. } => {
                        Err(anyhow!("Job ID={} is already canceled!", id))
                    }
                }
            }
            None => Err(anyhow!("Job with ID={} not found!", id)),
        }
    }

    /// Inspect every job and "repair" if needed.
    pub fn run_maintenance(&mut self) {
        let mut jobs_to_be_removed: Vec<usize> = Vec::new();
        let mut new_jobs_pending = false;

        for (job_id, job) in &self.jobs {
            let info = job.lock().unwrap().info.clone();
            match &info.status {
                JobStatus::Pending { .. } => {
                    // Pending jobs should be available for workers.
                    let newly_inserted = self.jobs_waiting_for_assignment.insert(job_id.clone());
                    if newly_inserted {
                        log::warn!("Job {} was pending but not available for workers!", job_id);
                    }
                }
                JobStatus::Offered {
                    issued,
                    offered,
                    to: _,
                } => {
                    // A job should only be briefly in this state.
                    let offer_timed_out =
                        (Utc::now() - offered.clone()).num_minutes() > OFFER_TIMEOUT_MINUTES;
                    let worker_id = job.lock().unwrap().worker_id;
                    let worker_alive = match worker_id {
                        Some(id) => match self.workers.get(&id) {
                            Some(weak_worker) => match weak_worker.upgrade() {
                                Some(worker) => {
                                    // Worker is still with us.
                                    !worker.lock().unwrap().info.timed_out()
                                }
                                None => false,
                            },
                            None => false,
                        },
                        None => false,
                    };

                    // Recover if offer timed out or worker died.
                    if offer_timed_out || !worker_alive {
                        log::warn!("Job {:?} got stuck in offered state. Recover...", info);
                        let mut job_lock = job.lock().unwrap();
                        job_lock.info.status = JobStatus::Pending {
                            issued: issued.clone(),
                        };
                        job_lock.worker_id = None;
                        self.jobs_waiting_for_assignment.insert(job_id.clone());
                        new_jobs_pending = true; // notify at the end
                    }
                }
                JobStatus::Running {
                    issued,
                    started: _,
                    on,
                } => {
                    // If the job is running, the worker should still be alive.
                    let worker_id = job.lock().unwrap().worker_id;
                    let worker_alive = match worker_id {
                        Some(id) => match self.workers.get(&id) {
                            Some(weak_worker) => match weak_worker.upgrade() {
                                Some(worker) => {
                                    // Worker is still with us.
                                    !worker.lock().unwrap().info.timed_out()
                                }
                                None => false,
                            },
                            None => false,
                        },
                        None => false,
                    };

                    // Recover if worker died while the job was still running.
                    if !worker_alive {
                        log::warn!(
                            "Worker {} died while job {:?} was still running. Recover...",
                            on,
                            info
                        );
                        let mut job_lock = job.lock().unwrap();
                        job_lock.info.status = JobStatus::Pending {
                            issued: issued.clone(),
                        };
                        job_lock.worker_id = None;
                        self.jobs_waiting_for_assignment.insert(job_id.clone());
                        new_jobs_pending = true; // notify at the end
                    }
                }
                JobStatus::Finished { finished, .. } => {
                    // Finished jobs should be cleaned up after some time.
                    let cleanup_job =
                        (Utc::now() - finished.clone()).num_hours() > CLEANUP_JOB_AFTER_HOURS;
                    if cleanup_job {
                        log::debug!("Clean up old finished job: {:?}", info);
                        jobs_to_be_removed.push(info.id);
                    }
                }
                JobStatus::Canceled { canceled } => {
                    // Canceled jobs should be cleaned up after some time.
                    let cleanup_job =
                        (Utc::now() - canceled.clone()).num_hours() > CLEANUP_JOB_AFTER_HOURS;
                    if cleanup_job {
                        log::debug!("Clean up old canceled job: {:?}", info);
                        jobs_to_be_removed.push(info.id);
                    }
                }
            }
        }

        // Clean up jobs.
        for id in jobs_to_be_removed {
            self.jobs.remove(&id);
        }

        let mut workers_to_be_removed: Vec<usize> = Vec::new();

        // Remove workers that are no longer alive.
        for (worker_id, weak_worker) in &self.workers {
            let worker_alive = match weak_worker.upgrade() {
                Some(worker) => {
                    // Worker is still with us.
                    !worker.lock().unwrap().info.timed_out()
                }
                None => false,
            };
            if !worker_alive {
                workers_to_be_removed.push(worker_id.clone());
            }
        }

        // Clean up workers.
        for id in workers_to_be_removed {
            self.workers.remove(&id);
        }

        // If jobs have been marked as pending, notify workers.
        if new_jobs_pending {
            self.new_jobs.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn add_new_job() {
        let mut manager = Manager::new();
        let cmd = vec!["ls".to_string(), "-la".to_string()];
        let cwd: PathBuf = "/tmp".into();
        let job_info = JobInfo::new(cmd, cwd, 8, 8 * 1024, None, None);
        manager.add_new_job(job_info);
        assert_eq!(manager.get_all_job_infos().len(), 1);
    }

    #[test]
    fn get_job_waiting_for_assignment() {
        let mut manager = Manager::new();
        let cmd = vec!["ls".to_string(), "-la".to_string()];
        let cwd: PathBuf = "/tmp".into();
        let job_info = JobInfo::new(cmd, cwd, 8, 8 * 1024, None, None);
        let job = manager.add_new_job(job_info);

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
