use crate::shared_state::SharedState;
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
        // Hello/Welcome messages are already exchanged at this point.

        while !self.connection_closed {
            match self.stream.receive::<ClientMessage>().await {
                Ok(message) => {
                    log::debug!("Received message: {:?}", message);
                    if let Err(e) = self.handle_message(message).await {
                        log::error!("Failed to handle message: {}", e);
                        return; // end client session
                    }
                }
                Err(e) => {
                    log::error!("{}", e);
                    return; // end client session
                }
            }
        }
    }

    async fn handle_message(
        &mut self,
        message: ClientMessage,
    ) -> Result<(), Box<dyn std::error::Error + '_>> {
        match message {
            ClientMessage::IssueJob { cmd, cwd } => {
                // Add new job
                self.ss.lock()?.add_job(cmd, cwd);

                // TODO: notify workers

                // Send response to client
                self.stream.send(&ServerMessage::AcceptJob).await?;
                Ok(())
            }
            ClientMessage::ListJobs => {
                // Get job list
                let job_list = self.ss.lock()?.get_job_list();

                // Send response to client
                self.stream.send(&ServerMessage::JobList(job_list)).await?;
                Ok(())
            }
            ClientMessage::Bye => {
                log::trace!("Bye client!");
                self.connection_closed = true;
                Ok(())
            }
        }
    }
}
