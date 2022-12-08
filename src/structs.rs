use crate::constants::WORKER_TIMEOUT_MINUTES;
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
    pub id: usize,
    pub cmd: Vec<String>,
    pub cwd: PathBuf,
    pub status: JobStatus,
    /// If Some(), redirect stdout to given file path.
    pub stdout_path: Option<String>,
    /// If Some(), redirect stderr to given file path.
    pub stderr_path: Option<String>,
}

impl JobInfo {
    pub fn new(
        cmd: Vec<String>,
        cwd: PathBuf,
        stdout_path: Option<String>,
        stderr_path: Option<String>,
    ) -> Self {
        static JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);
        JobInfo {
            id: JOB_COUNTER.fetch_add(1, Ordering::Relaxed),
            cmd,
            cwd,
            status: JobStatus::Pending { issued: Utc::now() },
            stdout_path,
            stderr_path,
        }
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
        finished: DateTime<Utc>,
        return_code: i32,
        on: String,
        run_time_seconds: i64,
        comment: String,
    },
    Canceled {
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
    pub name: String,
    pub connected_since: DateTime<Utc>,
    pub hw: HwInfo,
    pub load: LoadInfo,
    pub last_updated: DateTime<Utc>,
    pub jobs_running: BTreeSet<usize>,
    pub jobs_reserved: BTreeSet<usize>,
    pub max_parallel_jobs: usize,
}

impl WorkerInfo {
    pub fn new(name: String) -> Self {
        WorkerInfo {
            name,
            connected_since: Utc::now(),
            hw: HwInfo::default(),
            load: LoadInfo::default(),
            last_updated: Utc::now(),
            jobs_running: BTreeSet::new(),
            jobs_reserved: BTreeSet::new(),
            max_parallel_jobs: 0,
        }
    }

    pub fn jobs_total(&self) -> usize {
        self.jobs_running.len() + self.jobs_reserved.len()
    }

    pub fn free_slots(&self) -> bool {
        self.jobs_total() < self.max_parallel_jobs
    }

    pub fn timed_out(&self) -> bool {
        (Utc::now() - self.last_updated).num_minutes() > WORKER_TIMEOUT_MINUTES
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct HwInfo {
    pub kernel: String,
    pub distribution: String,
    pub cpu_cores: usize,
    pub cpu_frequency: u64,
    pub total_memory: u64,
}

impl HwInfo {
    pub fn default() -> Self {
        HwInfo {
            kernel: "unknown".into(),
            distribution: "unknown".into(),
            cpu_cores: 0,
            cpu_frequency: 0,
            total_memory: 0,
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
