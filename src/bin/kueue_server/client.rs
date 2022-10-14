use crate::shared_state::SharedState;
use kueue::message::error::MessageError;
use kueue::message::stream::MessageStream;
use kueue::message::{ClientMessage, ServerMessage};
use std::sync::{Arc, Mutex};

pub struct Client {
    stream: MessageStream,
    ss: Arc<Mutex<SharedState>>,
    connection_closed: bool,
}

impl Client {
    pub fn new(stream: MessageStream, ss: Arc<Mutex<SharedState>>) -> Self {
        Client {
            stream,
            ss,
            connection_closed: false,
        }
    }

    pub async fn run(&mut self) {
        // for testing: read ClientMessages and print out!
        while !self.connection_closed {
            match self.stream.receive::<ClientMessage>().await {
                Ok(message) => {
                    println!("Received message: {:?}", message); // debug
                    self.handle_message(message).await
                }
                Err(MessageError::ConnectionClosed) => {
                    eprintln!("Connection closed unexpectedly!");
                    return; // end client session
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return; // end client session
                }
            }
        }
    }

    async fn handle_message(&mut self, message: ClientMessage) {
        match message {
            ClientMessage::IssueJob { cmd, cwd } => {
                println!("TODO: Add job {}", cmd);

                // Send response to client
                if let Err(e) = self.stream.send(&ServerMessage::AcceptJob).await {
                    eprintln!("Failed to respond to client after accepting job: {}", e);
                }
            }
            ClientMessage::Bye => {
                println!("Bye client!");
                self.connection_closed = true;
            }
        }
    }
}
