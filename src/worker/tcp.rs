//! This module handles the communication with the server.

use crate::{config::Config, messages::stream::MessageStream, worker::common::Worker};
use anyhow::{bail, Result};
use tokio::{
    net::TcpStream,
    sync::mpsc::{channel, Receiver}, time::{sleep, Duration},
};
use tokio_util::sync::CancellationToken;

pub struct TcpWorker {
    /// Settings from the parsed config file.
    config: Config,
    /// Name of the worker. Used as an identifier.
    worker_name: String,
    /// Handles for graceful shutdown of the worker.
    shutdown: Option<(CancellationToken, Receiver<()>)>,
}

impl TcpWorker {
    /// Creates a new worker instance to process jobs.
    pub fn new(config: Config) -> Self {
        // Use hostname as worker name.
        let fqdn: String = gethostname::gethostname().to_string_lossy().into();
        let hostname = fqdn.split('.').next().unwrap().to_string();

        Self {
            config,
            worker_name: hostname,
            shutdown: None,
        }
    }

    /// Connect to the server and start processing jobs.
    pub async fn start(&mut self) -> Result<()> {
        if self.shutdown.is_some() {
            bail!("Worker is already started!");
        }

        // Connect to the server.
        let server_addr = self.config.get_server_address().await?;
        let stream = TcpStream::connect(server_addr).await?;
        let stream = MessageStream::new(stream);

        // Prepare handles for graceful shutdown later.
        let cancel_token = CancellationToken::new();
        let (keep_alive, shutdown) = channel::<()>(1);

        // All common logic is implemented in Worker struct.
        let mut worker = Worker::new(
            self.config.clone(),
            self.worker_name.clone(),
            stream,
            cancel_token.clone(),
            keep_alive.clone(),
        );

        // Perform hello/welcome handshake and challenge-response authentication.
        worker.connect_to_server().await?;
        worker.authenticate().await?;

        log::info!("Established connection to server!");

        // Send regular updates about system, load, and resources to the server.
        let cancel_system_update = cancel_token.clone();
        let keep_alive_system_update = keep_alive;
        let notify_system_update = worker.notify_system_update.clone();
        let update_interval = self.config.worker_settings.system_update_interval_seconds;
        tokio::spawn(async move {
            loop {
                // Send the first system update immediately.
                let mut wake_up = sleep(Duration::from_secs(0));
                tokio::select! {
                    _ = cancel_system_update.cancelled() => { break; }
                    _ = wake_up => {
                        notify_system_update.notify_one();
                        wake_up = sleep(Duration::from_secs(update_interval));
                    }
                }
            }
            drop(keep_alive_system_update);
        });

        // Handle messages and interrupts.
        tokio::spawn(async move {
            if let Err(e) = worker.run().await {
                log::error!("Worker stopped due to error: {e}");
            }
        });

        // Save handles for stop function.
        self.shutdown = Some((cancel_token, shutdown));
        Ok(())
    }

    // Shut down worker.
    pub async fn stop(&mut self) -> Result<()> {
        match &mut self.shutdown {
            Some((cancel_tasks, shutdown)) => {
                log::info!("Shutting down worker!");
                // Cancel all spawned tasks.
                cancel_tasks.cancel();
                // Wait for all senders in the spawned tasks to be dropped.
                shutdown.recv().await;
            }
            _ => bail!("Worker is not running!"),
        }

        self.shutdown = None;
        Ok(())
    }
}
