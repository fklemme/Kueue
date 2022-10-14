use crate::shared_state::SharedState;
use kueue::message::error::MessageError;
use kueue::message::stream::MessageStream;
use kueue::message::{ClientMessage, ServerMessage};
use std::sync::{Arc, Mutex};

pub struct Client {
    stream: MessageStream,
    ss: Arc<Mutex<SharedState>>,
}

impl Client {
    pub fn new(stream: MessageStream, ss: Arc<Mutex<SharedState>>) -> Self {
        Client { stream, ss }
    }

    pub async fn run(&mut self) {
        // for testing: read ClientMessages and print out!
        loop {
            match self.stream.receive::<ClientMessage>().await {
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
