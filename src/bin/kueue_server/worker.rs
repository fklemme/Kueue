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
        // for testing: read WorkerMessages and print out!
        loop {
            match self.stream.receive::<WorkerMessage>().await {
                Ok(message) => println!("Received message: {:?}", message),
                Err(MessageError::ConnectionClosed) => {
                    println!("Connection closed!");
                    return;
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return;
                }
            }
        }
    }
}
