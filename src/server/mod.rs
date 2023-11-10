mod client_connection;
mod job_manager;
mod worker_connection;

use crate::{
    config::Config,
    messages::stream::MessageStream,
    messages::{HelloMessage, ServerToClientMessage, ServerToWorkerMessage},
};
use anyhow::{bail, Result};
use client_connection::ClientConnection;
use futures::future::{join_all, BoxFuture};
use job_manager::Manager;
use std::sync::{Arc, Mutex, RwLock};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{sleep, Duration},
};
use worker_connection::WorkerConnection;

pub struct Server {
    /// Shared config that can be accessed by all connected clients and workers.
    config: Arc<RwLock<Config>>,
    /// Job manager, representing the major shared state of the server.
    manager: Arc<Mutex<Manager>>,
    /// Handle to the maintenance task. Used to realize structured shutdown.
    maintenance: Option<JoinHandle<()>>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        // Initialize job manager.
        let manager = Arc::new(Mutex::new(Manager::new(config.clone())));

        // Maintain job manager, re-issuing jobs of dead workers, etc.
        let maintenance_interval_seconds = config.server_settings.maintenance_interval_seconds;
        let manager_handle = Arc::clone(&manager);
        let mut shutdown_rx = manager.lock().unwrap().shutdown_rx.clone();
        let maintenance = Some(tokio::spawn(async move {
            loop {
                let sleep = sleep(Duration::from_secs(maintenance_interval_seconds));
                tokio::select! {
                    _ = sleep => {
                        log::trace!("Performing job maintenance...");
                        manager_handle.lock().unwrap().run_maintenance();
                    }
                    _ = shutdown_rx.changed() => {
                        log::trace!("Shut down maintenance thread!");
                        break; // end this maintenance loop and task
                    }
                }
            }
        }));

        Server {
            config: Arc::new(RwLock::new(config)),
            manager,
            maintenance,
        }
    }

    pub fn async_shutdown(&self) -> BoxFuture<'static, ()> {
        let shutdown_tx = Arc::clone(&self.manager.lock().unwrap().shutdown_tx);
        Box::pin(async move { shutdown_tx.send("shutdown").unwrap() })
    }

    pub async fn run(&mut self) {
        let bind_addresses: Vec<String> = {
            let config = self.config.read().unwrap();
            let port = config.common_settings.server_port;
            config
                .server_settings
                .bind_addresses
                .split_whitespace()
                .map(|addr| format!("{}:{}", addr, port))
                .collect()
        };

        // Start accepting incoming connections.
        let listeners: Vec<_> = bind_addresses
            .into_iter()
            .map(|address| self.listen_on(address))
            .collect();

        // Listeners are polled in this thread, no tokio::spawn.
        join_all(listeners).await;

        // If all listeners joined without any shutdown signal being sent,
        // we probably failed to successfully bind any network address.
        log::trace!("All network listeners stopped!");

        // Trigger shutdown signal to stop the maintenance task and join.
        self.manager
            .lock()
            .unwrap()
            .shutdown_tx
            .send("listeners joined")
            .unwrap();

        if let Some(task) = self.maintenance.take() {
            task.await.unwrap();
        }
    }

    async fn listen_on(&self, address: String) -> Result<()> {
        let listener = match TcpListener::bind(address.clone()).await {
            Ok(listener) => {
                log::info!(
                    "Successfully started listening on {}...",
                    listener.local_addr().unwrap()
                );
                listener
            }
            Err(e) => {
                log::error!("Failed to start listening on {}: {}", address, e);
                bail!(e); // function stops here!
            }
        };

        let mut shutdown_rx = self.manager.lock().unwrap().shutdown_rx.clone();
        let mut connections = Vec::new();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    log::trace!("New connection from {}!", addr);

                    // Clean up finished connections.
                    connections.retain(|conn: &JoinHandle<()>| !conn.is_finished());

                    // New reference-counted pointer to job manager.
                    let config = Arc::clone(&self.config);
                    let manager = Arc::clone(&self.manager);

                    connections.push(tokio::spawn(async move {
                        // Process each connection concurrently.
                        handle_connection(stream, config, manager).await;
                        log::trace!("Closed connection to {}!", addr);
                    }));
                }
                _ = shutdown_rx.changed() => {
                    log::info!("Stop listening on {}!", address);
                    break; // end loop
                }
            }
        }

        // Wait for connections to shut down.
        join_all(connections).await;
        Ok(())
    }
}

async fn handle_connection(
    stream: TcpStream,
    config: Arc<RwLock<Config>>,
    manager: Arc<Mutex<Manager>>,
) {
    // Read hello message to distinguish between client and worker.
    let mut stream = MessageStream::new(stream);
    match stream.receive::<HelloMessage>().await {
        Ok(HelloMessage::HelloFromClient) => {
            // Handle client connection.
            match stream.send(&ServerToClientMessage::WelcomeClient).await {
                Ok(()) => {
                    log::trace!("Exchanged handshake with client!");
                    let mut client = ClientConnection::new(stream, config, manager);
                    client.run().await;
                }
                Err(e) => log::error!("Failed to send WelcomeClient: {}", e),
            }
        }
        Ok(HelloMessage::HelloFromWorker { worker_name }) => {
            // Handle worker connection.
            match stream.send(&ServerToWorkerMessage::WelcomeWorker).await {
                Ok(()) => {
                    log::trace!("Exchanged handshake with worker '{}'!", worker_name);
                    let mut worker =
                        WorkerConnection::new(worker_name.clone(), stream, config, manager);
                    worker.run().await;
                }
                Err(e) => log::error!("Failed to send WelcomeWorker: {}", e),
            }
            log::info!("Connection to worker {} closed!", worker_name);
        }
        // Connected client failed to identify correctly.
        Err(e) => log::error!("Failed to read HelloMessage: {}", e),
    }
}
