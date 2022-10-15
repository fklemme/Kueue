mod client_connection;
mod shared_state;
mod worker_connection;

use client_connection::ClientConnection;
use kueue::{
    constants::{DEFAULT_BIND_ADDR, DEFAULT_PORT},
    messages::stream::MessageStream,
    messages::{HelloMessage, ServerToClientMessage, ServerToWorkerMessage},
};
use shared_state::SharedState;
use simple_logger::SimpleLogger;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use worker_connection::WorkerConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    SimpleLogger::new().init().unwrap();

    // TODO: Handle cli arguments

    // Initialize shared state
    let ss = Arc::new(Mutex::new(SharedState::new()));

    // Start accepting incoming connections
    let listener = TcpListener::bind((DEFAULT_BIND_ADDR, DEFAULT_PORT)).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        log::trace!("New connection from {}", addr);

        // New reference-counted pointer to shared state
        let ss = Arc::clone(&ss);

        tokio::spawn(async move {
            // Process each connection concurrently
            handle_connection(stream, ss).await;
            log::trace!("Closed connection to {}", addr);
        });
    }
}

async fn handle_connection(stream: TcpStream, ss: Arc<Mutex<SharedState>>) {
    // Read hello message to distinguish between client and worker
    let mut stream = MessageStream::new(stream);
    match stream.receive::<HelloMessage>().await {
        Ok(HelloMessage::HelloFromClient) => {
            // Handle client connection
            match stream.send(&ServerToClientMessage::WelcomeClient).await {
                Ok(()) => {
                    log::trace!("Established connection to client!");
                    let mut client = ClientConnection::new(stream, ss);
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
                    let mut worker = WorkerConnection::new(name, stream, ss);
                    worker.run().await;
                }
                Err(e) => log::error!("Failed to send WelcomeWorker: {}", e),
            }
        }
        Err(e) => log::error!("Failed to read HelloMessage: {}", e),
    }
}
