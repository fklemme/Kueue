use crate::shared_state::SharedState;
use kueue::message::error::MessageError;
use kueue::message::stream::MessageStream;
use kueue::message::{ServerMessage, WorkerMessage};
use std::sync::{Arc, Mutex};

pub struct Worker {
    name: String,
    stream: MessageStream,
    ss: Arc<Mutex<SharedState>>,
}

impl Worker {
    pub fn new(name: String, stream: MessageStream, ss: Arc<Mutex<SharedState>>) -> Self {
        Worker { name, stream, ss }
    }

    pub async fn run(&mut self) {
        // Hello/Welcome messages are already exchanged at this point.

        // for testing: read WorkerMessages and print out!
        loop {
            match self.stream.receive::<WorkerMessage>().await {
                Ok(message) => log::debug!("Received message: {:?}", message),
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            }
        }
    }
}
