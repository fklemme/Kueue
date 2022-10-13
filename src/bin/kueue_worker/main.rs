mod worker;

use kueue::constants::DEFAULT_PORT;
use std::net::Ipv4Addr;
use worker::Worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Handle cli arguments

    // Start worker: Connect to server and process work
    let worker_name = String::from("Workerman");
    let server_addr = (Ipv4Addr::LOCALHOST, DEFAULT_PORT);
    let mut worker = Worker::new(worker_name, server_addr).await?;
    worker.run().await // do worker things
}
