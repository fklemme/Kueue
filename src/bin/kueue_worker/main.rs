//! # Kueue_worker
//!
//! This binary create implements the Kueue worker.

#![warn(clippy::missing_docs_in_private_items)]

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use kueue_lib::{config::Config, worker::Worker};
use simple_logger::SimpleLogger;
use std::path::PathBuf;
use tokio::signal::unix::{signal, SignalKind};

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

    // Setup worker.
    let mut worker = Worker::new(config).await?;
    let shutdown = worker.async_shutdown();

    // Handle interrupt signal.
    let mut sigint = signal(SignalKind::interrupt())?;
    tokio::spawn(async move {
        if let Some(()) = sigint.recv().await {
            log::debug!("Shutdown signal received!");
            shutdown.await
        }
    });

    // Run worker until interrupted.
    worker.run().await
}
