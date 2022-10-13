use kueue::constants::DEFAULT_PORT;
use kueue::message::stream::MessageStream;
use kueue::message::{ServerMessage, WorkerMessage};
use std::net::Ipv4Addr;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Handle cli arguments

    // Start worker: Connect to server and process work
    let worker_name = String::from("Workerman");
    let server_addr = (Ipv4Addr::LOCALHOST, DEFAULT_PORT);
    let mut worker = Worker::new(worker_name, server_addr).await?;
    worker.run().await?; // do worker things
    Ok(())
}

struct Worker {
    name: String,
    stream: MessageStream,
}

impl Worker {
    async fn new<T: tokio::net::ToSocketAddrs>(
        name: String,
        addr: T,
    ) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Worker {
            name,
            stream: MessageStream::new(stream),
        })
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Send hello to server
        let hello = WorkerMessage::HelloFromWorker { worker_name: self.name.clone() };
        self.stream.send(&hello).await?;

        // TODO: do things
        Ok(())
    }
}
