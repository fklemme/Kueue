use kueue::message::stream::MessageStream;
use kueue::message::{HelloMessage, ServerMessage, WorkerMessage};
use std::sync::Arc;
use sysinfo::{System, SystemExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::{sleep, Duration};

pub struct Worker {
    name: String,
    stream: MessageStream,
    notify_update: Arc<Notify>,
    system: System,
}

impl Worker {
    pub async fn new<T: tokio::net::ToSocketAddrs>(
        name: String,
        addr: T,
    ) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Worker {
            name,
            stream: MessageStream::new(stream),
            notify_update: Arc::new(Notify::new()),
            system: System::new_all(),
        })
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Send hello from worker
        let hello = HelloMessage::HelloFromWorker {
            name: self.name.clone(),
        };
        self.stream.send(&hello).await?;

        // Await welcoming response from server
        match self.stream.receive::<ServerMessage>().await? {
            ServerMessage::WelcomeWorker => println!("Established connection to server..."), // continue
            other => return Err(format!("Expected WelcomeWorker, received: {:?}", other).into()),
        }

        // Send hardware information to server (once)
        self.update_hw_status().await?;

        // Notify worker regularly to send other updates
        let notify_update = Arc::clone(&self.notify_update);
        tokio::spawn(async move {
            loop {
                notify_update.notify_one();
                sleep(Duration::from_secs(60)).await;
            }
        });

        // Main loop
        loop {
            tokio::select! {
                // Read and handle incoming messages
                message = self.stream.receive::<ServerMessage>() => {
                    self.handle_message(message?).await;
                }
                // Or, get active when notified
                _ = self.notify_update.notified() => {
                    self.update_load_status().await?;
                    self.update_job_status().await?;
                }
            }
        }
    }

    async fn update_hw_status(&mut self) -> Result<(), kueue::message::error::MessageError> {
        // Read hardware information
        let kernel = self.system.kernel_version().unwrap_or("n/a".into());
        let cpu_cores = self.system.cpus().len();
        let total_memory = self.system.total_memory();

        // Put into message
        let hw_update = WorkerMessage::UpdateHwStatus {
            kernel,
            cpu_cores,
            total_memory,
        };

        // Send to server
        self.stream.send(&hw_update).await
    }

    async fn update_load_status(&mut self) -> Result<(), kueue::message::error::MessageError> {
        // Read load
        let load_avg = self.system.load_average();

        // Put into message
        let load_update = WorkerMessage::UpdateLoadStatus {
            one: load_avg.one,
            five: load_avg.five,
            fifteen: load_avg.fifteen,
        };

        // Send to server
        self.stream.send(&load_update).await
    }

    async fn update_job_status(&mut self) -> Result<(), kueue::message::error::MessageError> {
        // TODO!
        let job_update = WorkerMessage::UpdateJobStatus;
        self.stream.send(&job_update).await
    }

    async fn handle_message(&mut self, message: ServerMessage) {
        // TODO
        println!("Debug: Received message: {:?}", message);
    }
}
