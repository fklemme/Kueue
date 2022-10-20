pub mod job;
mod worker;

use kueue::config::Config;
use simple_logger::SimpleLogger;
use std::{net::Ipv4Addr, str::FromStr};
use worker::Worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger.
    SimpleLogger::new().init().unwrap();

    // Generate unique name from hostname and random suffix.
    let fqdn: String = gethostname::gethostname().to_string_lossy().into();
    let hostname = fqdn.split(|c| c == '.').next().unwrap().to_string();
    let mut generator = names::Generator::default();
    let name_suffix = generator.next().unwrap_or("default".into());
    let worker_name = format!("{}-{}", hostname, name_suffix);
    log::debug!("Worker name: {}", worker_name);

    // Read configuration from file or defaults.
    let config = Config::new()?;
    // If there is no config file, create template.
    if let Err(e) = config.create_default_config() {
        log::error!("Could not create default config: {}", e);
    }

    // Connect to server and process work.
    let server_addr = (
        Ipv4Addr::from_str(&config.server_address)?,
        config.server_port,
    );
    let mut worker = Worker::new(worker_name, config, server_addr).await?;
    worker.run().await // do worker things
}
