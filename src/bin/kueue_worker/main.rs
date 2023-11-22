//! # Kueue_worker
//!
//! This binary create implements the Kueue worker.

#![warn(clippy::missing_docs_in_private_items)]

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use kueue_lib::{config::Config, worker::TcpWorker};
use simple_logger::SimpleLogger;
use std::path::PathBuf;
use tokio::signal::ctrl_c;

/// Command line interface for the worker.
#[derive(Parser, Debug)]
#[command(version, author, about)]
pub struct Cli {
    /// Path to config file.
    #[arg(short, long, id = "PATH")]
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
    if let Err(e) = config.create_template(args.config) {
        bail!("Could not create config file: {}", e);
    }

    // Initialize logger.
    SimpleLogger::new()
        .with_level(config.get_log_level()?.to_level_filter())
        .init()?;

    // Start worker and connect to server.
    let mut worker = TcpWorker::new(config);
    worker.start().await.map_err(|e| anyhow!("Failed to start worker: {}", e))?;

    // Shutdown when receiving interrupt signal.
    ctrl_c().await?;
    worker.stop().await?;

    Ok(())
}
