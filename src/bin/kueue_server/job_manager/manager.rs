use crate::job_manager::{Job, Worker};
use anyhow::{bail, Result};
use chrono::Utc;
use kueue_lib::{
    config::Config,
    structs::{JobInfo, JobStatus, Resources, WorkerInfo},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{mpsc, Notify};

pub struct Manager {
    config: Config,
    jobs: BTreeMap<u64, Arc<Mutex<Job>>>,
    jobs_waiting_for_assignment: BTreeSet<u64>,
    workers: BTreeMap<u64, Weak<Mutex<Worker>>>,
    notify_new_jobs: Arc<Notify>,
}

impl Manager {
    pub fn new(config: Config) -> Self {
        Manager {
            config,
            jobs: BTreeMap::new(),
            jobs_waiting_for_assignment: BTreeSet::new(),
            workers: BTreeMap::new(),
            notify_new_jobs: Arc::new(Notify::new()),
        }
    }

    /// Registers a new worker to process jobs.
    pub fn add_new_worker(
        &mut self,
        name: String,
        kill_job_tx: mpsc::Sender<u64>,
    ) -> Arc<Mutex<Worker>> {
        let worker = Worker::new(name, kill_job_tx);
        let worker_id = worker.info.worker_id;
        let worker = Arc::new(Mutex::new(worker));
        self.workers.insert(worker_id, Arc::downgrade(&worker));
        worker
    }

    /// Adds a new job to be processed.
    pub fn add_new_job(&mut self, job_info: JobInfo) -> Arc<Mutex<Job>> {
        // Add new job. We create a new JobInfo instance to make sure to
        // not adopt remote (non-unique) job ids or inconsistent states.
        let job = Job::from(job_info);
        let job_id = job.info.job_id;
        let job = Arc::new(Mutex::new(job));
        self.jobs.insert(job_id, Arc::clone(&job));
        self.jobs_waiting_for_assignment.insert(job_id);
        job
    }

    /// Get a handle to the "new jobs" notifier.
    pub fn notify_new_jobs(&self) -> Arc<Notify> {
        Arc::clone(&self.notify_new_jobs)
    }

    /// Get job by ID.
    pub fn get_job(&self, job_id: u64) -> Option<Arc<Mutex<Job>>> {
        self.jobs.get(&job_id).map(Arc::clone)
    }

    /// Collect job information about all jobs.
    pub fn get_all_job_infos(&self) -> Vec<JobInfo> {
        let mut job_infos = Vec::new();
        for job in self.jobs.values() {
            job_infos.push(job.lock().unwrap().info.clone());
        }
        job_infos
    }

    /// Get worker by ID.
    pub fn get_worker(&self, worker_id: u64) -> Option<Weak<Mutex<Worker>>> {
        self.workers.get(&worker_id).map(Weak::clone)
    }

    /// Collect worker information about all workers.
    pub fn get_all_worker_infos(&self) -> Vec<WorkerInfo> {
        let mut worker_infos = Vec::new();
        for worker in self.workers.values() {
            if let Some(worker) = worker.upgrade() {
                worker_infos.push(worker.lock().unwrap().info.clone());
            }
        }
        worker_infos
    }

    fn get_available_global_resources(&self) {
        let job_infos = self.get_all_job_infos();
    }

    /// Get a job to be assigned to a worker.
    pub fn get_job_waiting_for_assignment(
        &mut self,
        exclude: &BTreeSet<u64>,
        resource_limit: &Resources,
    ) -> Option<Arc<Mutex<Job>>> {
        if self.jobs_waiting_for_assignment.is_empty() {
            // No jobs marked waiting for assignment.
            None
        } else {
            let job_ids: Vec<u64> = self
                .jobs_waiting_for_assignment
                .iter()
                .filter(|job_id| !exclude.contains(job_id))
                .cloned()
                .collect();
            for job_id in job_ids {
                if let Some(job) = self.jobs.get(&job_id) {
                    let job_resources = job.lock().unwrap().info.local_resources.clone();
                    if job_resources.fit_into(resource_limit) {
                        // Found matching job.
                        self.jobs_waiting_for_assignment.remove(&job_id);
                        return Some(Arc::clone(job));
                    }
                }
            }
            // No job fulfilling requirements.
            None
        }
    }

    /// Cancel and remove a job from the queue. If the job is running and a
    /// worker is associated with the job, a sender is returned that can be
    /// used to signal a kill instruction to the worker. The job_id sent over
    /// the returned sender indicates the job to be killed on the worker.
    pub fn cancel_job(&mut self, job_id: u64, kill: bool) -> Result<Option<mpsc::Sender<u64>>> {
        match self.get_job(job_id) {
            Some(job) => {
                let job_info = job.lock().unwrap().info.clone();
                match job_info.status {
                    JobStatus::Pending { issued } => {
                        // Do not attempt to offer the job to workers.
                        self.jobs_waiting_for_assignment.remove(&job_id);
                        job.lock().unwrap().info.status = JobStatus::Canceled {
                            issued,
                            canceled: Utc::now(),
                        };
                        Ok(None)
                    }
                    JobStatus::Offered { issued, .. } => {
                        // Offer will be withdrawn by the server on worker's response.
                        job.lock().unwrap().info.status = JobStatus::Canceled {
                            issued,
                            canceled: Utc::now(),
                        };
                        Ok(None)
                    }
                    JobStatus::Running { issued, .. } => {
                        if !kill {
                            // Makes no sense to set the job to canceled if the
                            // worker proceeds anyway.
                            bail!("Job ID={} has already started!", job_id);
                        }
                        // Update job status
                        job.lock().unwrap().info.status = JobStatus::Canceled {
                            issued,
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
                        bail!(
                            "Job with ID={} was running but worker could not be acquired!",
                            job_id
                        )
                    }
                    JobStatus::Finished { .. } => bail!("Job ID={} has already finished!", job_id),
                    JobStatus::Canceled { .. } => bail!("Job ID={} is already canceled!", job_id),
                }
            }
            None => bail!("Job with ID={} not found!", job_id),
        }
    }

    /// Remove jobs that have been finished or canceled.
    /// If `all` is true, also remove failed jobs.
    pub fn clean_jobs(&mut self, all: bool) {
        let clean_pred = if all {
            |status: &JobStatus| status.is_finished() || status.is_canceled()
        } else {
            |status: &JobStatus| status.has_succeeded() || status.is_canceled()
        };

        self.jobs
            .retain(|_, job| !clean_pred(&job.lock().unwrap().info.status));
    }

    /// Inspect every job and "repair" if needed.
    pub fn run_maintenance(&mut self) {
        let mut jobs_to_be_removed: Vec<u64> = Vec::new();
        let mut new_jobs_pending = false;

        for (job_id, job) in &self.jobs {
            let info = job.lock().unwrap().info.clone();
            match &info.status {
                JobStatus::Pending { .. } => {
                    // Pending jobs should be available for workers.
                    let newly_inserted = self.jobs_waiting_for_assignment.insert(*job_id);
                    if newly_inserted {
                        log::warn!("Job {} was pending but not available for workers!", job_id);
                    }
                }
                JobStatus::Offered {
                    issued,
                    offered,
                    worker: _,
                } => {
                    // A job should only be briefly in this state.
                    let offer_timed_out = (Utc::now() - *offered).num_seconds()
                        > self.config.server_settings.job_offer_timeout_seconds as i64;
                    let worker_id = job.lock().unwrap().worker_id;
                    let worker_alive = match worker_id {
                        Some(id) => match self.workers.get(&id) {
                            Some(weak_worker) => match weak_worker.upgrade() {
                                Some(worker) => {
                                    // Worker is still with us.
                                    !worker.lock().unwrap().info.timed_out(
                                        self.config.server_settings.worker_timeout_seconds,
                                    )
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
                        job_lock.info.status = JobStatus::Pending { issued: *issued };
                        job_lock.worker_id = None;
                        self.jobs_waiting_for_assignment.insert(*job_id);
                        new_jobs_pending = true; // notify at the end
                    }
                }
                JobStatus::Running {
                    issued,
                    started: _,
                    worker,
                } => {
                    // If the job is running, the worker should still be alive.
                    let worker_id = job.lock().unwrap().worker_id;
                    let worker_alive = match worker_id {
                        Some(id) => match self.workers.get(&id) {
                            Some(weak_worker) => match weak_worker.upgrade() {
                                Some(worker) => {
                                    // Worker is still with us.
                                    !worker.lock().unwrap().info.timed_out(
                                        self.config.server_settings.worker_timeout_seconds,
                                    )
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
                            worker,
                            info
                        );
                        let mut job_lock = job.lock().unwrap();
                        job_lock.info.status = JobStatus::Pending { issued: *issued };
                        job_lock.worker_id = None;
                        self.jobs_waiting_for_assignment.insert(*job_id);
                        new_jobs_pending = true; // notify at the end
                    }
                }
                JobStatus::Finished { finished, .. } => {
                    // Finished jobs should be cleaned up after some time.
                    let cleanup_job = (Utc::now() - *finished).num_minutes()
                        > self.config.server_settings.job_cleanup_after_minutes as i64;
                    if cleanup_job {
                        log::debug!("Clean up old finished job: {:?}", info);
                        jobs_to_be_removed.push(info.job_id);
                    }
                }
                JobStatus::Canceled { canceled, .. } => {
                    // Canceled jobs should be cleaned up after some time.
                    let cleanup_job = (Utc::now() - *canceled).num_hours()
                        > self.config.server_settings.job_cleanup_after_minutes as i64;
                    if cleanup_job {
                        log::debug!("Clean up old canceled job: {:?}", info);
                        jobs_to_be_removed.push(info.job_id);
                    }
                }
            }
        }

        // Clean up jobs.
        for job_id in jobs_to_be_removed {
            self.jobs.remove(&job_id);
        }

        let mut workers_to_be_removed: Vec<u64> = Vec::new();

        // Remove workers that are no longer alive.
        for (worker_id, weak_worker) in &self.workers {
            let worker_alive = match weak_worker.upgrade() {
                Some(worker) => {
                    // Worker is still with us.
                    !worker
                        .lock()
                        .unwrap()
                        .info
                        .timed_out(self.config.server_settings.worker_timeout_seconds)
                }
                None => false,
            };
            if !worker_alive {
                workers_to_be_removed.push(*worker_id);
            }
        }

        // Clean up workers.
        for id in workers_to_be_removed {
            self.workers.remove(&id);
        }

        // If jobs have been marked as pending, notify workers.
        if new_jobs_pending {
            self.notify_new_jobs.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use kueue_lib::structs::Resources;

    use super::*;
    use std::path::PathBuf;

    #[test]
    fn add_new_job() {
        let config = Config::new(Some("no-config".into())).unwrap();
        let mut manager = Manager::new(config);
        let cmd = vec!["ls".to_string(), "-la".to_string()];
        let cwd: PathBuf = "/tmp".into();
        let resources = Resources::new(1, 8, 8 * 1024);
        let job_info = JobInfo::new(cmd, cwd, resources, None, None, None);
        manager.add_new_job(job_info);
        assert_eq!(manager.get_all_job_infos().len(), 1);
    }

    #[test]
    fn get_job_waiting_for_assignment() {
        let config = Config::new(Some("no-config".into())).unwrap();
        let mut manager = Manager::new(config);
        let cmd = vec!["ls".to_string(), "-la".to_string()];
        let cwd: PathBuf = "/tmp".into();
        let resources = Resources::new(1, 8, 8 * 1024);
        let job_info = JobInfo::new(cmd, cwd, resources.clone(), None, None, None);
        let job = manager.add_new_job(job_info);

        // Put job on exclude list.
        let mut exclude = BTreeSet::new();
        exclude.insert(job.lock().unwrap().info.job_id);

        // Now, we should not get it.
        let job = manager.get_job_waiting_for_assignment(&exclude, &resources);
        assert!(job.is_none());

        // Now we want any job. One is waiting to be assigned.
        exclude.clear();
        let job = manager.get_job_waiting_for_assignment(&exclude, &resources);
        assert!(job.is_some());

        // We want any job, again. But none are left.
        let job = manager.get_job_waiting_for_assignment(&exclude, &resources);
        assert!(job.is_none());
    }
}
