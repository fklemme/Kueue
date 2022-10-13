mod client;
mod shared_state;
mod worker;

use kueue::constants::*;
use kueue::message::error::MessageError;
use kueue::message::stream::MessageStream;
use kueue::message::{HelloMessage, ServerMessage};
use shared_state::SharedState;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use client::Client;
use worker::Worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Handle cli arguments

    // Initialize shared state
    let ss = Arc::new(Mutex::new(SharedState::new()));

    // Start accepting incoming connections
    let listener = TcpListener::bind((DEFAULT_BIND_ADDR, DEFAULT_PORT)).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("New connection from {}", addr);

        // New reference-counted pointer to shared state (local shadow)
        let ss = Arc::clone(&ss);

        tokio::spawn(async move {
            // Process each connection concurrently
            handle_connection(stream, ss).await;
        });
    }
}

async fn handle_connection(stream: TcpStream, ss: Arc<Mutex<SharedState>>) {
    // Read hello message to distinguish between client and worker
    let mut stream = MessageStream::new(stream);
    match stream.receive::<HelloMessage>().await {
        Ok(HelloMessage::HelloFromClient) => {
            // Handle client connection
            match stream.send(&ServerMessage::WelcomeClient).await {
                Ok(()) => {
                    println!("Established connection to client!");
                    let mut client = Client::new(stream);
                    client.run().await;
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        Ok(HelloMessage::HelloFromWorker { name }) => {
            // Handle worker connection
            match stream.send(&ServerMessage::WelcomeWorker).await {
                Ok(()) => {
                    println!("Established connection to worker '{}'!", name);
                    let mut worker = Worker::new(name, stream);
                    worker.run().await;
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        Err(e) => eprintln!("Error while reading HelloMessage: {:?}", e),
    }
}
