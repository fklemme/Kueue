//#![warn(clippy::missing_docs_in_private_items)]

mod job;
mod worker;

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use kueue_lib::config::Config;
use simple_logger::SimpleLogger;
use std::path::PathBuf;
use worker::Worker;

/// Command line interface for the worker.
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
    if let Err(e) = config.save_as_template(args.config) {
        bail!("Could not create config file: {}", e);
    }

    // Initialize logger.
    SimpleLogger::new()
        .with_level(config.get_log_level().to_level_filter())
        .init()?;

    // Run worker.
    let mut worker = Worker::new(config).await?;
    worker.run().await
}
