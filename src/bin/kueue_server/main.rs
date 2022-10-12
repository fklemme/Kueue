pub mod shared_state;

use kueue::constants::*;
use kueue::message::stream::MessageStream;
use kueue::message::WorkerMessage;
use shared_state::SharedState;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

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

        // New reference to shared state (local shadow)
        let ss = ss.clone();

        tokio::spawn(async move {
            // Process each connection concurrently
            process_connection(stream, ss).await;
        });
    }
}

async fn process_connection(stream: TcpStream, _ss: Arc<Mutex<SharedState>>) {
    // for testing: read messages and print out!
    let mut message_stream = MessageStream::new(stream);
    loop {
        match message_stream.read::<WorkerMessage>().await {
            Ok(message) => println!("Received message: {:?}", message),
            Err(e) => {
                eprintln!("Error: {}", e);
                return;
            }
        }
    }
}
