use crate::shared_state::SharedState;
use kueue::messages::{
    stream::{MessageError, MessageStream},
    ClientToServerMessage, ServerToClientMessage};
use std::sync::{Arc, Mutex};

pub struct ClientConnection {
    stream: MessageStream,
    ss: Arc<Mutex<SharedState>>,
    connection_closed: bool,
}

impl ClientConnection {
    pub fn new(stream: MessageStream, ss: Arc<Mutex<SharedState>>) -> Self {
        ClientConnection {
            stream,
            ss,
            connection_closed: false,
        }
    }

    pub async fn run(&mut self) {
        // Hello/Welcome messages are already exchanged at this point.

        while !self.connection_closed {
            match self.stream.receive::<ClientToServerMessage>().await {
                Ok(message) => {
                    log::debug!("Received message: {:?}", message);
                    if let Err(e) = self.handle_message(message).await {
                        log::error!("Failed to handle message: {}", e);
                        self.connection_closed = true; // end client session
                    }
                }
                Err(e) => {
                    log::error!("{}", e);
                    self.connection_closed = true; // end client session
                }
            }
        }
    }

    async fn handle_message(&mut self, message: ClientToServerMessage) -> Result<(), MessageError> {
        match message {
            ClientToServerMessage::IssueJob(job_info) => {
                // Add new job. We create a new JobInfo instance to make sure to
                // not adopt remote (non-unique) job ids or inconsistent states.
                let job = self.ss.lock().unwrap().add_new_job(job_info.cmd, job_info.cwd);
                let job_info = job.lock().unwrap().info.clone();

                // Send response to client
                self.stream
                    .send(&ServerToClientMessage::AcceptJob(job_info))
                    .await?;

                // Notify workers
                let new_jobs = self.ss.lock().unwrap().new_jobs();
                new_jobs.notify_waiters();

                Ok(())
            }
            ClientToServerMessage::ListJobs => {
                // Get job list
                let job_list = self.ss.lock().unwrap().get_all_job_infos();

                // Send response to client
                self.stream
                    .send(&ServerToClientMessage::JobList(job_list))
                    .await
            }
            ClientToServerMessage::ListWorkers => {
                // Get worker list
                let worker_list = self.ss.lock().unwrap().get_all_worker_infos();

                // Send response to client
                self.stream
                    .send(&ServerToClientMessage::WorkerList(worker_list))
                    .await
            }
            ClientToServerMessage::Bye => {
                log::trace!("Bye client!");
                self.connection_closed = true;

                Ok(())
            }
        }
    }
}
