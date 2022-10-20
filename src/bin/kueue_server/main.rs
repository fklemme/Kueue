mod client_connection;
mod job_manager;
mod worker_connection;

use client_connection::ClientConnection;
use job_manager::Manager;
use kueue::{
    constants::{DEFAULT_BIND_ADDR, DEFAULT_PORT},
    messages::stream::MessageStream,
    messages::{HelloMessage, ServerToClientMessage, ServerToWorkerMessage},
};
use simple_logger::SimpleLogger;
use std::sync::{Arc, Mutex};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{sleep, Duration},
};
use worker_connection::WorkerConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    SimpleLogger::new().init().unwrap();

    // TODO: Handle cli arguments

    // Start accepting incoming connections
    let listener = TcpListener::bind((DEFAULT_BIND_ADDR, DEFAULT_PORT)).await?;

    // Initialize job manager
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
        let ss = Arc::clone(&manager);

        tokio::spawn(async move {
            // Process each connection concurrently
            handle_connection(stream, ss).await;
            log::trace!("Closed connection to {}!", addr);
        });
    }
}

async fn handle_connection(stream: TcpStream, manager: Arc<Mutex<Manager>>) {
    // Read hello message to distinguish between client and worker
    let mut stream = MessageStream::new(stream);
    match stream.receive::<HelloMessage>().await {
        Ok(HelloMessage::HelloFromClient) => {
            // Handle client connection
            match stream.send(&ServerToClientMessage::WelcomeClient).await {
                Ok(()) => {
                    log::trace!("Established connection to client!");
                    let mut client = ClientConnection::new(stream, manager);
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
                    let mut worker = WorkerConnection::new(name, stream, manager);
                    worker.run().await;
                }
                Err(e) => log::error!("Failed to send WelcomeWorker: {}", e),
            }
        }
        Err(e) => log::error!("Failed to read HelloMessage: {}", e),
    }
}
