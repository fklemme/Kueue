use crate::job_manager::Manager;
use anyhow::{anyhow, Result};
use kueue::{
    config::Config,
    messages::{stream::MessageStream, ClientToServerMessage, ServerToClientMessage},
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

pub struct ClientConnection {
    stream: MessageStream,
    manager: Arc<Mutex<Manager>>,
    config: Config,
    connection_closed: bool,
    authenticated: bool,
    salt: String,
}

impl ClientConnection {
    pub fn new(stream: MessageStream, manager: Arc<Mutex<Manager>>, config: Config) -> Self {
        // Salt is generated for each client connection.
        let salt: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        ClientConnection {
            stream,
            manager,
            config,
            connection_closed: false,
            authenticated: false,
            salt,
        }
    }

    pub async fn run(&mut self) {
        // Hello/Welcome messages are already exchanged at this point.

        while !self.connection_closed {
            match self.stream.receive::<ClientToServerMessage>().await {
                Ok(message) => {
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

    fn is_authenticated(&self) -> Result<()> {
        if self.authenticated {
            Ok(())
        } else {
            Err(anyhow!("Client is not authenticated!"))
        }
    }

    async fn handle_message(&mut self, message: ClientToServerMessage) -> Result<()> {
        match message {
            ClientToServerMessage::AuthRequest => {
                // Send salt to client.
                let message = ServerToClientMessage::AuthChallenge(self.salt.clone());
                self.stream.send(&message).await?;
                Ok(())
            }
            ClientToServerMessage::AuthResponse(response) => {
                // Calculate baseline result.
                let salted_secret = self.config.shared_secret.clone() + &self.salt;
                let salted_secret = salted_secret.into_bytes();
                let mut hasher = Sha256::new();
                hasher.update(salted_secret);
                let baseline = hasher.finalize().to_vec();
                let baseline = base64::encode(baseline);

                // Update status and send reply.
                if response == baseline {
                    self.authenticated = true;
                }
                let message = ServerToClientMessage::AuthAccepted(self.authenticated);
                self.stream.send(&message).await?;
                Ok(())
            }
            ClientToServerMessage::IssueJob(job_info) => {
                self.is_authenticated()?;

                // Add new job. We create a new JobInfo instance to make sure to
                // not adopt remote (non-unique) job ids or inconsistent states.
                let job = self
                    .manager
                    .lock()
                    .unwrap()
                    .add_new_job(job_info.cmd, job_info.cwd);
                let job_info = job.lock().unwrap().info.clone();

                // Send response to client
                self.stream
                    .send(&ServerToClientMessage::AcceptJob(job_info))
                    .await?;

                // Notify workers
                let new_jobs = self.manager.lock().unwrap().new_jobs();
                new_jobs.notify_waiters();
                Ok(())
            }
            ClientToServerMessage::ListJobs => {
                // Get job list
                let job_list = self.manager.lock().unwrap().get_all_job_infos();

                // Send response to client
                self.stream
                    .send(&ServerToClientMessage::JobList(job_list))
                    .await?;
                Ok(())
            }
            ClientToServerMessage::ListWorkers => {
                // Get worker list
                let worker_list = self.manager.lock().unwrap().get_all_worker_infos();

                // Send response to client
                self.stream
                    .send(&ServerToClientMessage::WorkerList(worker_list))
                    .await?;
                Ok(())
            }
            ClientToServerMessage::Bye => {
                log::trace!("Bye client!");
                self.connection_closed = true;
                Ok(())
            }
        }
    }
}
