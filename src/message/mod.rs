pub mod error;
pub mod stream;

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// HelloMessage helps the server to distinguish between client and worker
#[derive(Serialize, Deserialize, Debug)]
pub enum HelloMessage {
    // Initiate client connection with this message, expect WelcomeClient
    HelloFromClient,
    // Initiate worker connection with this message, expect WelcomeWorker
    HelloFromWorker { name: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    IssueJob { cmd: String, cwd: PathBuf },
    ListJobs,
    Bye,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerMessage {
    // Respond with WelcomeClient after HelloFromClient
    WelcomeClient,
    AcceptJob,
    //RejectJob,
    JobList(Vec<JobInfo>),
    // Respond with WelcomeWorker after HelloFromWorker
    WelcomeWorker,
    OfferJob,
    ConfirmJobOffer,
    WithdrawJobOffer,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerMessage {
    UpdateHwStatus {
        kernel: String,
        cpu_cores: usize,
        total_memory: u64,
    },
    UpdateLoadStatus {
        one: f64,
        five: f64,
        fifteen: f64,
    },
    UpdateJobStatus,
    AcceptJobOffer,
    RejectJobOffer,
    Bye,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfo {
    pub cmd: String,
    pub status: String,
}
