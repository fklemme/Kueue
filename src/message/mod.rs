pub mod error;
pub mod stream;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    HelloFromClient,
    Bye,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerMessage {
    Todo,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerMessage {
    HelloFromWorker { worker_name: String },
    Bye,
}
