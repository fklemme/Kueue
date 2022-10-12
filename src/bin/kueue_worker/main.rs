use kueue::constants::DEFAULT_PORT;
use kueue::message::WorkerMessage;
use std::net::Ipv4Addr;
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Handle cli arguments

    // Connect to server
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, DEFAULT_PORT)).await?;

    let hello = WorkerMessage::Hello {
        worker_name: String::from("Workerman"),
    };

    for _ in 0..3 {
        stream
            .write_all(serde_json::to_string(&hello).unwrap().as_bytes())
            .await?;
    }

    Ok(())
}
