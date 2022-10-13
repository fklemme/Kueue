pub mod error;
pub mod stream;

use serde::{Deserialize, Serialize};

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
    Bye,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerMessage {
    // Respond with WelcomeClient after HelloFromClient
    WelcomeClient,
    // Respond with WelcomeWorker after HelloFromWorker
    WelcomeWorker,
    OfferJob,
    ConfirmJobOffer,
    WithdrawJobOffer,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerMessage {
    UpdateHwStatus,
    UpdateLoadStatus,
    UpdateJobStatus,
    AcceptJobOffer,
    RejectJobOffer,
    Bye,
}