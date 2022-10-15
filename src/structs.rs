use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};

// Struct that are shared among crates and parts of messages.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobInfo {
    pub id: u64,
    pub cmd: String,
    pub cwd: PathBuf,
    pub status: JobStatus,
}

impl JobInfo {
    pub fn new(cmd: String, cwd: PathBuf) -> Self {
        static JOB_COUNTER: AtomicU64 = AtomicU64::new(0);
        JobInfo {
            id: JOB_COUNTER.fetch_add(1, Ordering::Relaxed),
            cmd,
            cwd,
            status: JobStatus::Pending{issued: Utc::now()},
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum JobStatus {
    Pending {issued: DateTime<Utc>},
    Offered {issued: DateTime<Utc>, to:String},
    Running {started: DateTime<Utc>},
    Finished{finished: DateTime<Utc>, return_code: i32}
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerInfo {
    pub name: String,
    pub hw: HwInfo,
    pub load: LoadInfo,
    pub current_jobs: u32,
    pub max_parallel_jobs: u32,
}

impl WorkerInfo {
    pub fn new(name: String) -> Self {
        WorkerInfo {
            name,
            hw: HwInfo::default(),
            load: LoadInfo::default(),
            current_jobs: 0,
            max_parallel_jobs: 0,
        }
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
