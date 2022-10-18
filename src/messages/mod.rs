pub mod stream;

use serde::{Deserialize, Serialize};
use crate::structs::{JobInfo, WorkerInfo, HwInfo, LoadInfo};

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
    IssueJob(JobInfo),
    ListJobs,
    ListWorkers,
    Bye,
}

// All messages sent by the server to a client
#[derive(Serialize, Deserialize, Debug)]
pub enum ServerToClientMessage {
    // Respond with WelcomeClient after HelloFromClient
    WelcomeClient,
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
    OfferJob(JobInfo),
    ConfirmJobOffer(JobInfo),
    WithdrawJobOffer(JobInfo),
}

// All messages sent by the worker to the server
#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerToServerMessage {
    UpdateHwInfo(HwInfo),
    UpdateLoadInfo(LoadInfo),
    UpdateJobStatus(JobInfo),
    AcceptParallelJobs(u32),
    AcceptJobOffer(JobInfo),
    RejectJobOffer(JobInfo),
    Bye,
}
