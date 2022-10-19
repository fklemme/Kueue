use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

// Struct that are shared among crates and parts of messages.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobInfo {
    pub id: u64,
    pub cmd: Vec<String>,
    pub cwd: PathBuf,
    pub status: JobStatus,
}

impl JobInfo {
    pub fn new(cmd: Vec<String>, cwd: PathBuf) -> Self {
        static JOB_COUNTER: AtomicU64 = AtomicU64::new(0);
        JobInfo {
            id: JOB_COUNTER.fetch_add(1, Ordering::Relaxed),
            cmd,
            cwd,
            status: JobStatus::Pending { issued: Utc::now() },
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
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerInfo {
    pub name: String,
    pub hw: HwInfo,
    pub load: LoadInfo,
    pub jobs_running: u32,
    pub jobs_reserved: u32,
    pub max_parallel_jobs: u32,
}

impl WorkerInfo {
    pub fn new(name: String) -> Self {
        WorkerInfo {
            name,
            hw: HwInfo::default(),
            load: LoadInfo::default(),
            jobs_running: 0,
            jobs_reserved: 0,
            max_parallel_jobs: 0,
        }
    }

    pub fn free_slots(&self) -> bool {
        let total = self.jobs_running + self.jobs_reserved;
        total < self.max_parallel_jobs
    }

    pub fn jobs_total(&self) -> u32 {
        self.jobs_running + self.jobs_reserved
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct HwInfo {
    pub kernel: String,
    pub distribution: String,
    pub cpu_cores: u32,
    pub total_memory: u64,
}

impl HwInfo {
    pub fn default() -> Self {
        HwInfo {
            kernel: "unknown".into(),
            distribution: "unknown".into(),
            cpu_cores: 0,
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
