//! Contains a collection of structs that are transfered as messages between
//! client and server, and worker and server.

pub mod stream;

use crate::structs::{HwInfo, JobInfo, LoadInfo, WorkerInfo};
use serde::{Deserialize, Serialize};

/// Communication to the server is initialized with HelloFromClient or
/// HelloFromWorker. The variants help the server to distinguish between client
/// and worker connections. The server will repond with the corresponding
/// "welcome" message.
#[derive(Serialize, Deserialize, Debug)]
pub enum HelloMessage {
    /// Initiate a new client connection with the HelloFromClient message.
    /// The server confirms the connection with the WelcomeClient message.
    HelloFromClient,
    /// Initiate a new worker connection with the HelloFromWorker message.
    /// The server confirms the connection with the WelcomeWorker message.
    HelloFromWorker {
        /// Name of the worker. Can be helpful for the user to identify where
        /// their jobs are running.
        name: String,
    },
}

/// Contains all messages sent by the client to the server.
#[derive(Serialize, Deserialize, Debug)]
pub enum ClientToServerMessage {
    // Request authentification challenge.
    AuthRequest,
    // Send Sha256(secret + salt) back to server.
    AuthResponse(String),
    IssueJob(JobInfo),
    ListJobs {
        num_jobs: usize,
        pending: bool,
        offered: bool,
        running: bool,
        finished: bool,
        failed: bool,
    },
    ListWorkers,
    ShowJob {
        id: usize,
    },
    Bye,
}

/// Contains all messages sent by the server to a client.
#[derive(Serialize, Deserialize, Debug)]
pub enum ServerToClientMessage {
    // Respond with WelcomeClient after HelloFromClient.
    WelcomeClient,
    // AuthChallenge sends a random salt to the client.
    AuthChallenge(String),
    // Let client know if authentification succeeded.
    AuthAccepted(bool),
    AcceptJob(JobInfo),
    //RejectJob,
    JobList {
        jobs_pending: usize,
        jobs_offered: usize,
        jobs_running: usize,
        jobs_finished: usize,
        any_job_failed: bool,
        job_infos: Vec<JobInfo>,
    },
    JobInfo {
        job_info: Option<JobInfo>,
        stdout: Option<String>,
        stderr: Option<String>,
    },
    WorkerList(Vec<WorkerInfo>),
}

/// Contains all messages sent by the server to a worker.
#[derive(Serialize, Deserialize, Debug)]
pub enum ServerToWorkerMessage {
    // Respond with WelcomeWorker after HelloFromWorker
    WelcomeWorker,
    // AuthChallenge sends a random salt to the client.
    AuthChallenge(String),
    OfferJob(JobInfo),
    ConfirmJobOffer(JobInfo),
    WithdrawJobOffer(JobInfo),
}

/// Contains all messages sent by the worker to the server.
#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerToServerMessage {
    // Send Sha256(secret + salt) back to server.
    AuthResponse(String),
    UpdateHwInfo(HwInfo),
    UpdateLoadInfo(LoadInfo),
    UpdateJobStatus(JobInfo),
    UpdateJobResults {
        job_id: usize,
        stdout: Option<String>,
        stderr: Option<String>,
    },
    AcceptParallelJobs(usize),
    AcceptJobOffer(JobInfo),
    RejectJobOffer(JobInfo),
    Bye,
}
