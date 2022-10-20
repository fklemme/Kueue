pub mod job;
mod worker;

use kueue::constants::{DEFAULT_PORT, DEFAULT_SERVER_ADDR};
use simple_logger::SimpleLogger;
use worker::Worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    SimpleLogger::new().init().unwrap();

    // TODO: Handle cli arguments

    // Generate unique name from hostname and random suffix
    let fqdn: String = gethostname::gethostname().to_string_lossy().into();
    let hostname = fqdn.split(|c| c == '.').next().unwrap().to_string();
    let mut generator = names::Generator::default();
    let name_suffix = generator.next().unwrap_or("default".into());
    let worker_name = format!("{}-{}", hostname, name_suffix);
    log::debug!("Worker name: {}", worker_name);

    // Connect to server and process work
    let server_addr = (DEFAULT_SERVER_ADDR, DEFAULT_PORT);
    let mut worker = Worker::new(worker_name, server_addr).await?;
    worker.run().await // do worker things
}
