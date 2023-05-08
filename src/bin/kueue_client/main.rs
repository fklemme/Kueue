//! # Kueue (client)
//!
//! This binary create implements the Kueue command line client.

//#![warn(clippy::missing_docs_in_private_items)]

mod cli;
mod client;
mod print;

use anyhow::{anyhow, bail, Result};
use clap::{CommandFactory, Parser};
use clap_complete::generate;
use cli::Cli;
use client::Client;
use kueue_lib::config::Config;
use simple_logger::SimpleLogger;
use std::io::stdout;

#[tokio::main]
async fn main() -> Result<()> {
    // Read command line arguments.
    let args = Cli::parse();

    // Generate shell completion scripts.
    if let cli::Command::Complete { shell } = args.command {
        let mut app = Cli::command();
        let name = app.get_name().to_string();
        generate(shell, &mut app, name, &mut stdout());
        return Ok(());
    }

    // Read configuration from file or defaults.
    let config =
        Config::new(args.config.clone()).map_err(|e| anyhow!("Failed to load config: {}", e))?;
    // If there is no config file, create template.
    if let Err(e) = config.create_template(args.config.clone()) {
        bail!("Could not create config file: {}", e);
    }

    // Initialize logger.
    SimpleLogger::new()
        .with_level(config.get_log_level()?.to_level_filter())
        .init()?;

    log::debug!("{:?}", config);

    // Run client.
    let mut client = Client::new(args, config).await?;
    client.run().await
}
