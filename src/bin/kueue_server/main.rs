mod client_connection;
mod job_manager;
mod worker_connection;

use client_connection::ClientConnection;
use job_manager::Manager;
use kueue::{
    config::Config,
    messages::stream::MessageStream,
    messages::{HelloMessage, ServerToClientMessage, ServerToWorkerMessage},
};
use simple_logger::SimpleLogger;
use std::{
    net::Ipv4Addr,
    str::FromStr,
    sync::{Arc, Mutex}, error::Error,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{sleep, Duration},
};
use worker_connection::WorkerConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logger.
    SimpleLogger::new().init().unwrap();

    // Read configuration from file or defaults.
    let config = Config::new()?;
    // If there is no config file, create template.
    if let Err(e) = config.create_default_config() {
        log::error!("Could not create default config: {}", e);
    }

    // Start accepting incoming connections.
    let bind_addr = (
        Ipv4Addr::from_str(&config.server_bind_address)?,
        config.server_port,
    );
    let listener = TcpListener::bind(bind_addr).await?;

    // Initialize job manager.
    let manager = Arc::new(Mutex::new(Manager::new()));

    // Maintain job manager, re-issuing job of died workers, etc.
    let manager_handle = Arc::clone(&manager);
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(5 * 60)).await;
            log::trace!("Performing job maintenance...");
            manager_handle.lock().unwrap().run_maintenance();
        }
    });

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
