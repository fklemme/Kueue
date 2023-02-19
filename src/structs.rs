use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

// Struct that are shared among crates and parts of messages.
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
    /// Current status of the job. E.g., running, finished, etc.
    pub status: JobStatus,
    /// If Some(), redirect stdout to given file path.
    pub stdout_path: Option<String>,
    /// If Some(), redirect stderr to given file path.
    pub stderr_path: Option<String>,
}

fn next_job_id() -> usize {
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Resources {
    /// Required/reserved CPU cores to run the command.
    pub cpus: usize,
    /// Required/reserved RAM (in megabytes) to run the command.
    pub ram_mb: usize,
}

impl Resources {
    pub fn new(cpus: usize, ram_mb: usize) -> Self {
        Resources { cpus, ram_mb }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum JobStatus {
    Pending {
        issued: DateTime<Utc>,
    },
    Offered {
        issued: DateTime<Utc>,
        offered: DateTime<Utc>,
        to: String,
    },
    Running {
        issued: DateTime<Utc>,
        started: DateTime<Utc>,
        on: String,
    },
    Finished {
        issued: DateTime<Utc>,
        started: DateTime<Utc>,
        finished: DateTime<Utc>,
        return_code: i32,
        on: String,
        run_time_seconds: i64,
        comment: String,
    },
    Canceled {
        issued: DateTime<Utc>,
        canceled: DateTime<Utc>,
    },
}

impl JobStatus {
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending { .. })
    }

    pub fn is_offered(&self) -> bool {
        matches!(self, Self::Offered { .. })
    }

    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Finished { .. })
    }

    pub fn has_succeeded(&self) -> bool {
        matches!(self, Self::Finished { return_code, .. } if *return_code == 0)
    }

    pub fn has_failed(&self) -> bool {
        matches!(self, Self::Finished { return_code, .. } if *return_code != 0)
    }

    pub fn is_canceled(&self) -> bool {
        matches!(self, Self::Canceled { .. })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerInfo {
    pub id: usize,
    pub name: String,
    pub connected_since: DateTime<Utc>,
    pub hw: HwInfo,
    pub last_updated: DateTime<Utc>,
    pub jobs_offered: BTreeSet<usize>,
    pub jobs_running: BTreeSet<usize>,
    pub free_resources: Resources,
}

fn next_worker_id() -> usize {
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
            free_resources: Resources::new(0, 0),
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

impl HwInfo {
    pub fn default() -> Self {
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct LoadInfo {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

impl LoadInfo {
    pub fn default() -> Self {
        LoadInfo {
            one: 0.0,
            five: 0.0,
            fifteen: 0.0,
        }
    }
}
