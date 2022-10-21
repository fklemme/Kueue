pub mod stream;

use crate::structs::{HwInfo, JobInfo, LoadInfo, WorkerInfo};
use serde::{Deserialize, Serialize};

// HelloMessage helps the server to distinguish between client and worker
#[derive(Serialize, Deserialize, Debug)]
pub enum HelloMessage {
    // Initiate client connection with this message, expect WelcomeClient
    HelloFromClient,
    // Initiate worker connection with this message, expect WelcomeWorker
    HelloFromWorker { name: String },
}

// All messages sent by the client to the server
#[derive(Serialize, Deserialize, Debug)]
pub enum ClientToServerMessage {
    // Request authentification challenge.
    AuthRequest,
    // Send Sha256(secret + salt) back to server.
    AuthResponse(String),
    IssueJob(JobInfo),
    ListJobs,
    ListWorkers,
    Bye,
}

// All messages sent by the server to a client
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
    JobList(Vec<JobInfo>),
    WorkerList(Vec<WorkerInfo>),
}

// All messages sent by the server to a worker
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

// All messages sent by the worker to the server
#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerToServerMessage {
    // Send Sha256(secret + salt) back to server.
    AuthResponse(String),
    UpdateHwInfo(HwInfo),
    UpdateLoadInfo(LoadInfo),
    UpdateJobStatus(JobInfo),
    AcceptParallelJobs(u32),
    AcceptJobOffer(JobInfo),
    RejectJobOffer(JobInfo),
    Bye,
}
