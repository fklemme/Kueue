mod job;
mod worker;

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use kueue::config::Config;
use simple_logger::SimpleLogger;
use std::path::PathBuf;
use worker::Worker;

#[derive(Parser, Debug)]
#[command(version, author, about)]
pub struct Cli {
    /// Path to config file.
    #[arg(short, long)]
    pub config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Read command line arguments.
    let args = Cli::parse();

    // Read configuration from file or defaults.
    let config =
        Config::new(args.config.clone()).map_err(|e| anyhow!("Failed to load config: {}", e))?;
    // If there is no config file, create template.
    if let Err(e) = config.create_default_config(args.config) {
        bail!("Could not create default config: {}", e);
    }

    // Initialize logger.
    SimpleLogger::new()
        .with_level(config.get_log_level().to_level_filter())
        .init()
        .unwrap();

    // Generate unique name from hostname and random suffix.
    let fqdn: String = gethostname::gethostname().to_string_lossy().into();
    let hostname = fqdn.split(|c| c == '.').next().unwrap().to_string();
    let mut generator = names::Generator::default();
    let name_suffix = generator.next().unwrap_or("default".into());
    let worker_name = format!("{}-{}", hostname, name_suffix);
    log::debug!("Worker name: {}", worker_name);

    // Connect to server and process work.
    let server_addr = config.get_server_address().await?;
    log::debug!("Server address: {}", server_addr);
    let mut worker = Worker::new(worker_name, config, server_addr).await?;
    worker.run().await // do worker things
}
