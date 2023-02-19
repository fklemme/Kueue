use crate::{
    client_connection::ClientConnection, job_manager::Manager, worker_connection::WorkerConnection,
};
use anyhow::{bail, Result};
use futures::future::join_all;
use kueue_lib::{
    config::Config,
    messages::{stream::MessageStream, HelloMessage, ServerToClientMessage, ServerToWorkerMessage},
};
use std::sync::{Arc, Mutex};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{sleep, Duration},
};

pub struct Server {
    config: Config,
    manager: Arc<Mutex<Manager>>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        // Initialize job manager.
        let manager = Arc::new(Mutex::new(Manager::new(config.clone())));

        // Maintain job manager, re-issuing jobs of dead workers, etc.
        let manager_handle = Arc::clone(&manager);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5 * 60)).await;
                log::trace!("Performing job maintenance...");
                manager_handle.lock().unwrap().run_maintenance();
                // TODO: Put something to allow shutdown.
            }
        });

        Server { config, manager }
    }

    pub async fn run(&self) -> Result<()> {
        let bind_addresses: Vec<&str> = self
            .config
            .server_settings
            .bind_addresses
            .split_whitespace()
            .collect();

        // Start accepting incoming connections.
        let listeners = bind_addresses
            .iter()
            .map(|addr| self.listen_on(addr.to_string()))
            .collect::<Vec<_>>();
        join_all(listeners).await;

        Ok(())
    }

    async fn listen_on(&self, address: String) -> Result<()> {
        let port = self.config.common_settings.server_port;
        let listener = TcpListener::bind(format!("{}:{}", address, port)).await;

        if let Err(e) = listener {
            log::error!("Failed to start listening on {}:{}: {}", address, port, e);
            bail!(e); // function stops here!
        }

        // Otherwise, we continue and accept connections on this socket.
        let listener = listener.unwrap();
        log::debug!("Start listening on {}...", listener.local_addr().unwrap());

        loop {
            let (stream, addr) = listener.accept().await?;
            log::trace!("New connection from {}!", addr);

            // New reference-counted pointer to job manager
            let manager = Arc::clone(&self.manager);
            let config = self.config.clone();

            tokio::spawn(async move {
                // Process each connection concurrently
                handle_connection(stream, manager, config).await;
                log::trace!("Closed connection to {}!", addr);
            });
        }
    }
}

async fn handle_connection(stream: TcpStream, manager: Arc<Mutex<Manager>>, config: Config) {
    // Read hello message to distinguish between client and worker
    let mut stream = MessageStream::new(stream);
    match stream.receive::<HelloMessage>().await {
        Ok(HelloMessage::HelloFromClient) => {
            // Handle client connection
            match stream.send(&ServerToClientMessage::WelcomeClient).await {
                Ok(()) => {
                    log::trace!("Established connection to client!");
                    let mut client = ClientConnection::new(stream, manager, config);
                    client.run().await;
                }
                Err(e) => log::error!("Failed to send WelcomeClient: {}", e),
            }
        }
        Ok(HelloMessage::HelloFromWorker { name }) => {
            // Handle worker connection
            match stream.send(&ServerToWorkerMessage::WelcomeWorker).await {
                Ok(()) => {
                    log::trace!("Established connection to worker '{}'!", name);
                    let mut worker = WorkerConnection::new(name.clone(), stream, manager, config);
                    worker.run().await;
                }
                Err(e) => log::error!("Failed to send WelcomeWorker: {}", e),
            }
            log::warn!("Connection to worker {} closed!", name);
        }
        // Warn, because this should not be common.
        Err(e) => log::error!("Failed to read HelloMessage: {}", e),
    }
}
