use crate::{
    config::Config,
    messages::{stream::MessageStream, HelloMessage, ServerToClientMessage, ServerToWorkerMessage},
    server::{
        client_connection::ClientConnection, shared_state::Manager,
        worker_connection::WorkerConnection,
    },
};
use anyhow::{bail, Result};
use std::sync::{Arc, RwLock};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{channel, Receiver, Sender},
    time::{sleep, Duration},
};
use tokio_util::sync::CancellationToken;

pub struct TcpServer {
    config: Arc<RwLock<Config>>,
    shared: Arc<RwLock<Manager>>,
    shutdown: Option<(CancellationToken, Receiver<()>)>,
}

impl TcpServer {
    /// Creates a new server to listen on the bind addresses in the config.
    pub fn new(config: Config) -> Self {
        Self {
            config: Arc::new(RwLock::new(config.clone())),
            shared: Arc::new(RwLock::new(Manager::new(config))),
            shutdown: None,
        }
    }

    /// Start accepting network connections.
    pub async fn start(&mut self) -> Result<()> {
        if self.shutdown.is_some() {
            bail!("Server is already started!");
        }

        // Get bind addresses to listen on.
        let bind_addresses = {
            let config = self.config.read().unwrap();
            let port = config.common_settings.server_port;
            config
                .server_settings
                .bind_addresses
                .split_whitespace()
                .map(|addr| format!("{}:{}", addr, port))
                .collect::<Vec<String>>()
        };

        if bind_addresses.is_empty() {
            bail!("No bind addresses configured!");
        }

        // Prepare handles for graceful shutdown later.
        let cancel_tasks = CancellationToken::new();
        let (keep_alive, shutdown) = channel::<()>(1);

        // Try to bind network addresses.
        let mut successful_binds = 0;
        for address in bind_addresses {
            match TcpListener::bind(address.clone()).await {
                Ok(listener) => {
                    log::info!("Started listening on {}...", address);
                    // Start accepting incoming connections.
                    tokio::spawn(listen_on(
                        listener,
                        self.config.clone(),
                        self.shared.clone(),
                        cancel_tasks.clone(),
                        keep_alive.clone(),
                    ));
                    successful_binds += 1;
                }
                Err(e) => {
                    log::error!("Failed to bind {}: {}", address, e);
                }
            }
        }

        if successful_binds == 0 {
            bail!("Failed to bind any address!");
        }

        // Start maintenance routine for the shared state.
        let cancel_maintenance = cancel_tasks.clone();
        let keep_alive_maintenance = keep_alive.clone();
        let maintenance_interval = self
            .config
            .read()
            .unwrap()
            .server_settings
            .maintenance_interval_seconds;
        let shared_state = self.shared.clone();
        tokio::spawn(async move {
            let _keep_alive = keep_alive_maintenance;
            loop {
                let sleep = sleep(Duration::from_secs(maintenance_interval));
                tokio::select! {
                    _ = cancel_maintenance.cancelled() => { break; }
                    _ = sleep => {
                        log::trace!("Performing job maintenance...");
                        shared_state.write().unwrap().run_maintenance();
                    }
                }
            }
        });

        // Save handles for stop function.
        self.shutdown = Some((cancel_tasks, shutdown));
        Ok(())
    }

    // Close all connections and stop listening.
    pub async fn stop(&mut self) -> Result<()> {
        match &mut self.shutdown {
            Some((cancel_tasks, shutdown)) => {
                log::info!("Shutting down server!");
                // Cancel all spawned tasks.
                cancel_tasks.cancel();
                // Wait for all senders in the spawned tasks to be dropped.
                shutdown.recv().await;
            }
            _ => bail!("Server is not running!"),
        }

        self.shutdown = None;
        Ok(())
    }
}

/// Runs asynchronously and accepts new TCP connections.
async fn listen_on(
    listener: TcpListener,
    config: Arc<RwLock<Config>>,
    shared: Arc<RwLock<Manager>>,
    cancel_tasks: CancellationToken,
    keep_alive: Sender<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = cancel_tasks.cancelled() => { return Ok(()); }
            result = listener.accept() => {
                let (stream, address) = result?;
                log::trace!("Established connection from {}!", address);

                tokio::spawn(handle_connection(
                    stream,
                    config.clone(),
                    shared.clone(),
                    cancel_tasks.clone(),
                    keep_alive.clone(),
                ));
            }
        }
    }
}

/// Initiate welcome handshake with new connection
/// to distinguish between client and worker.
async fn handle_connection(
    stream: TcpStream,
    config: Arc<RwLock<Config>>,
    shared: Arc<RwLock<Manager>>,
    cancel_tasks: CancellationToken,
    _keep_alive: Sender<()>,
) {
    // Read hello message to distinguish between client and worker.
    let mut stream = MessageStream::new(stream);
    match stream.receive::<HelloMessage>().await {
        Ok(HelloMessage::HelloFromClient) => {
            // Handle client connection.
            match stream.send(&ServerToClientMessage::WelcomeClient).await {
                Ok(()) => {
                    log::trace!("Exchanged welcome handshake with client!");
                    let mut client = ClientConnection::new(stream, config, shared, cancel_tasks);
                    client.run().await;
                }
                Err(e) => log::error!("Failed to send WelcomeClient: {}", e),
            }
        }
        Ok(HelloMessage::HelloFromWorker { worker_name }) => {
            // Handle worker connection.
            match stream.send(&ServerToWorkerMessage::WelcomeWorker).await {
                Ok(()) => {
                    log::trace!("Exchanged welcome handshake with worker '{}'!", worker_name);
                    let mut worker = WorkerConnection::new(
                        worker_name.clone(),
                        stream,
                        config,
                        shared,
                        cancel_tasks,
                    );
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
