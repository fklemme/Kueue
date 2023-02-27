//! Structs that are shared among binary crates and part of messages.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

/// All information resembling a job. Only the outputs
/// of stdout and stderr are stored separately.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobInfo {
    /// Unique job ID, assigned by the server.
    pub id: usize,
    /// Command to be executed. First element is the name of the
    /// program. Further elements are arguments to the program.
    pub cmd: Vec<String>,
    /// Working directory for the job to be executed in.
    pub cwd: PathBuf,
    /// Required/reserved resources to run the command.
    pub resources: Resources,
    /// Current status of the job, e.g., running, finished, etc.
    pub status: JobStatus,
    /// If Some(path), redirect stdout to given file path.
    pub stdout_path: Option<String>,
    /// If Some(path), redirect stderr to given file path.
    pub stderr_path: Option<String>,
}

/// Generate a unique job ID.
fn next_job_id() -> usize {
    /// Keeps track of generated job IDs.
    static JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);
    JOB_COUNTER.fetch_add(1, Ordering::Relaxed)
}

impl JobInfo {
    /// Creates a new job.
    pub fn new(
        cmd: Vec<String>,
        cwd: PathBuf,
        resources: Resources,
        stdout_path: Option<String>,
        stderr_path: Option<String>,
    ) -> Self {
        JobInfo {
            id: next_job_id(),
            cmd,
            cwd,
            resources,
            status: JobStatus::Pending { issued: Utc::now() },
            stdout_path,
            stderr_path,
        }
    }

    /// Creates a new job based on the given job information.
    pub fn from(job_info: JobInfo) -> Self {
        JobInfo {
            id: next_job_id(),
            cmd: job_info.cmd,
            cwd: job_info.cwd,
            resources: job_info.resources,
            status: JobStatus::Pending { issued: Utc::now() },
            stdout_path: job_info.stdout_path,
            stderr_path: job_info.stderr_path,
        }
    }
}

/// Represents a combination of resources, either available on a worker or required by a job.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Resources {
    /// Available job slots on the worker. For jobs, this should be always `1`.
    pub job_slots: usize,
    /// CPU cores, available on worker or required to run the command.
    pub cpus: usize,
    /// RAM (in megabytes), available on worker or required to run the command.
    pub ram_mb: usize,
}

impl Resources {
    /// Creates a new resources instance.
    pub fn new(job_slots: usize, cpus: usize, ram_mb: usize) -> Self {
        Resources {
            job_slots,
            cpus,
            ram_mb,
        }
    }

    /// Returns `true` if all components of this resource are smaller or equal than the `required` resource.
    pub fn fit_into(&self, required: &Resources) -> bool {
        (self.job_slots <= required.job_slots)
            && (self.cpus <= required.cpus)
            && (self.ram_mb <= required.ram_mb)
    }
}

/// Represents the state of a job.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum JobStatus {
    /// The job is waiting to be assigned to a worker.
    Pending {
        /// Point in time when the job has been posted to the server.
        issued: DateTime<Utc>,
    },
    /// The job has been offered to a worker but not yet accepted.
    Offered {
        /// Point in time when the job has been posted to the server.
        issued: DateTime<Utc>,
        /// Point in time when the job has been offered to the worker.
        offered: DateTime<Utc>,
        /// Name of the worker the job has been offered to.
        to: String,
    },
    /// The job is currently running on a worker.
    Running {
        /// Point in time when the job has been posted to the server.
        issued: DateTime<Utc>,
        /// Point in time when the job has been started executing on the worker.
        started: DateTime<Utc>,
        /// Name of the worker the job is running on.
        on: String,
    },
    /// The job has concluded, either successful or failing.
    Finished {
        /// Point in time when the job has been posted to the server.
        issued: DateTime<Utc>,
        /// Point in time when the job has been started executing on the worker.
        started: DateTime<Utc>,
        /// Point in time when the job has concluded on the worker.
        finished: DateTime<Utc>,
        /// Exit code returned by the executed command on the worker.
        return_code: i32,
        /// Name of the worker the job has been executed on.
        on: String,
        /// Total run time of the executed command in seconds.
        run_time_seconds: i64,
        /// Additional information about the execution of the job. This
        /// comment might include helpful information to debug failed jobs.
        comment: String,
    },
    /// The job has been canceled.
    Canceled {
        /// Point in time when the job has been posted to the server.
        issued: DateTime<Utc>,
        /// Point in time when the job has been canceled on the server.
        canceled: DateTime<Utc>,
    },
}

impl JobStatus {
    /// Returns true is the job is in "pending" state.
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending { .. })
    }

    /// Returns true is the job is in "offered" state.
    pub fn is_offered(&self) -> bool {
        matches!(self, Self::Offered { .. })
    }

    /// Returns true is the job is in "running" state.
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    /// Returns true is the job is in "finished" state.
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Finished { .. })
    }

    /// Returns true is the job is in "finished" state with exit code 0.
    pub fn has_succeeded(&self) -> bool {
        matches!(self, Self::Finished { return_code, .. } if *return_code == 0)
    }

    /// Returns true is the job is in "finished" state with exit code not 0.
    pub fn has_failed(&self) -> bool {
        matches!(self, Self::Finished { return_code, .. } if *return_code != 0)
    }

    /// Returns true is the job is in "canceled" state.
    pub fn is_canceled(&self) -> bool {
        matches!(self, Self::Canceled { .. })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerInfo {
    /// Unique worker ID, assigned by the server.
    pub id: usize,
    /// Name of the worker, usually including host name.
    pub name: String,
    pub connected_since: DateTime<Utc>,
    pub hw: HwInfo,
    pub last_updated: DateTime<Utc>,
    pub jobs_offered: BTreeSet<usize>,
    pub jobs_running: BTreeSet<usize>,
    pub free_resources: Resources,
}

/// Generate a unique worker ID.
fn next_worker_id() -> usize {
    /// Keeps track of generated worker IDs.
    static JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);
    JOB_COUNTER.fetch_add(1, Ordering::Relaxed)
}

impl WorkerInfo {
    pub fn new(name: String) -> Self {
        WorkerInfo {
            id: next_worker_id(),
            name,
            connected_since: Utc::now(),
            hw: HwInfo::default(),
            last_updated: Utc::now(),
            jobs_offered: BTreeSet::new(),
            jobs_running: BTreeSet::new(),
            free_resources: Resources::new(0, 0, 0),
        }
    }

    pub fn jobs_total(&self) -> usize {
        self.jobs_offered.len() + self.jobs_running.len()
    }

    /// Returns true if the worker timed out, given timeout_seconds as constraint.
    pub fn timed_out(&self, timeout_seconds: i64) -> bool {
        (Utc::now() - self.last_updated).num_seconds() > timeout_seconds
    }

    /// Returns the percentage of resources occupied on the worker. For multiple
    /// resources, the maximum occupation is returned. E.g., if some memory is
    /// still available but all cpus are take, 100% occupation is returned.
    pub fn resource_load(&self) -> f64 {
        // TODO: Should job slots be considered in this calculation?
        if self.free_resources.job_slots == 0 {
            return 1.0; // fully busy!
        }

        let cpu_busy = if self.hw.cpu_cores == 0 {
            1.0
        } else {
            1.0 - (self.free_resources.cpus as f64 / self.hw.cpu_cores as f64)
        };

        let ram_busy = if self.hw.total_ram_mb == 0 {
            1.0
        } else {
            1.0 - (self.free_resources.ram_mb as f64 / self.hw.total_ram_mb as f64)
        };

        f64::max(cpu_busy, ram_busy)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct HwInfo {
    pub kernel: String,
    pub distribution: String,
    pub cpu_cores: usize,
    pub cpu_frequency: usize,
    pub total_ram_mb: usize,
    pub load_info: LoadInfo,
}

impl Default for HwInfo {
    fn default() -> Self {
        HwInfo {
            kernel: "unknown".into(),
            distribution: "unknown".into(),
            cpu_cores: 0,
            cpu_frequency: 0,
            total_ram_mb: 0,
            load_info: LoadInfo::default(),
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
pub struct LoadInfo {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}
