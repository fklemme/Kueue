pub mod error;
pub mod stream;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    Hello,
    Bye,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerMessage {
    Todo,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerMessage {
    Hello { worker_name: String },
    Bye,
}
