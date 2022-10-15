pub mod job;
mod worker;

use kueue::constants::DEFAULT_PORT;
use simple_logger::SimpleLogger;
use std::net::Ipv4Addr;
use worker::Worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    SimpleLogger::new().init().unwrap();

    // TODO: Handle cli arguments

    // Generate unique name
    let hostname: String = gethostname::gethostname().to_string_lossy().into();
    let mut generator = names::Generator::default();
    let name_suffix = generator.next().unwrap_or("default".into());
    let worker_name = format!("{}-{}", hostname, name_suffix);
    log::debug!("Worker name: {}", worker_name);

    // Connect to server and process work
    let server_addr = (Ipv4Addr::LOCALHOST, DEFAULT_PORT);
    let mut worker = Worker::new(worker_name, server_addr).await?;
    worker.run().await // do worker things
}
