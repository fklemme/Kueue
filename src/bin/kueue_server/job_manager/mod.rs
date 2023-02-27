//! To avoid potential deadlocks, the convention is to acquire locks in the
//! following order: manager, worker, job.

pub mod job;
pub mod manager;
pub mod worker;

pub use crate::job_manager::{job::Job, manager::Manager, worker::Worker};
