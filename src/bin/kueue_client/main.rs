mod cli;
mod client;
mod print;

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use cli::Cli;
use client::Client;
use kueue::config::Config;
use simple_logger::SimpleLogger;

#[tokio::main]
async fn main() -> Result<()> {
    // Read command line arguments.
    let args = Cli::parse();

    // Read configuration from file or defaults.
    let config =
        Config::new(args.config.clone()).map_err(|e| anyhow!("Failed to load config: {}", e))?;
    // If there is no config file, create template.
    if let Err(e) = config.create_default_config(args.config.clone()) {
        bail!("Could not create default config: {}", e);
    }

    // Initialize logger.
    SimpleLogger::new()
        .with_level(config.get_log_level().to_level_filter())
        .init()
        .unwrap();

    // Run client.
    let mut client = Client::new(args, config).await?;
    client.run().await
}
