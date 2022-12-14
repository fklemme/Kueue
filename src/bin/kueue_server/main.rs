mod client_connection;
mod job_manager;
mod worker_connection;

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use client_connection::ClientConnection;
use futures::future::join_all;
use job_manager::Manager;
use kueue::{
    config::Config,
    messages::stream::MessageStream,
    messages::{HelloMessage, ServerToClientMessage, ServerToWorkerMessage},
};
use simple_logger::SimpleLogger;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{sleep, Duration},
};
use worker_connection::WorkerConnection;

#[derive(Parser, Debug)]
#[command(version, author, about)]
pub struct Cli {
    /// Path to config file.
    #[arg(short, long)]
    pub config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Read command line arguments.
    let args = Cli::parse();

    // Read configuration from file or defaults.
    let config =
        Config::new(args.config.clone()).map_err(|e| anyhow!("Failed to load config: {}", e))?;
    // If there is no config file, create template.
    if let Err(e) = config.create_default_config(args.config) {
        bail!("Could not create default config: {}", e);
    }

    // Initialize logger.
    SimpleLogger::new()
        .with_level(config.get_log_level().to_level_filter())
        .init()
        .unwrap();

    // Initialize job manager.
    let manager = Arc::new(Mutex::new(Manager::new()));

    // Maintain job manager, re-issuing jobs of dead workers, etc.
    let manager_handle = Arc::clone(&manager);
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(5 * 60)).await;
            log::trace!("Performing job maintenance...");
            manager_handle.lock().unwrap().run_maintenance();
        }
    });

    // Start accepting incoming connections.
    let bind_addresses: Vec<&str> = config.server.address_bindings.split_whitespace().collect();
    let listeners = bind_addresses
        .iter()
        .map(|addr| {
            listen_on(
                addr.to_string(),
                config.common.server_port.clone(),
                config.clone(),
                manager.clone(),
            )
        })
        .collect::<Vec<_>>();
    join_all(listeners).await;

    Ok(())
}

async fn listen_on(
    address: String,
    port: u16,
    config: Config,
    manager: Arc<Mutex<Manager>>,
) -> Result<()> {
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
        let manager = Arc::clone(&manager);
        let config = config.clone();

        tokio::spawn(async move {
            // Process each connection concurrently
            handle_connection(stream, manager, config).await;
            log::trace!("Closed connection to {}!", addr);
        });
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
