use crate::{
    config::Config,
    server::{handle_connection, shared_state::Manager},
};
use anyhow::{bail, Result};
use std::sync::{Arc, RwLock};
use tokio::{
    net::TcpListener,
    sync::mpsc::{channel, Receiver, Sender},
    time::{sleep, Duration},
};
use tokio_util::sync::CancellationToken;

pub struct TcpServer {
    config: Arc<RwLock<Config>>,
    shared: Arc<RwLock<Manager>>,
    /// Handles for graceful shutdown of the server.
    shutdown: Option<(CancellationToken, Receiver<()>)>,
}

impl TcpServer {
    /// Creates a new server to handle client and worker connections.
    pub fn new(config: Config) -> Self {
        Self {
            config: Arc::new(RwLock::new(config.clone())),
            shared: Arc::new(RwLock::new(Manager::new(config))),
            shutdown: None,
        }
    }

    /// Start accepting network connections.
    pub async fn start(&mut self) -> Result<()> {
        if self.shutdown.is_some() {
            bail!("Server is already started!");
        }

        // Get bind addresses to listen on.
        let bind_addresses = {
            let config = self.config.read().unwrap();
            let port = config.common_settings.server_port;
            config
                .server_settings
                .bind_addresses
                .split_whitespace()
                .map(|addr| format!("{}:{}", addr, port))
                .collect::<Vec<String>>()
        };

        if bind_addresses.is_empty() {
            bail!("No bind addresses configured!");
        }

        // Prepare handles for graceful shutdown later.
        let cancel_token = CancellationToken::new();
        let (keep_alive, shutdown) = channel::<()>(1);

        // Try to bind network addresses.
        let mut successful_binds = 0;
        for address in bind_addresses {
            match TcpListener::bind(address.clone()).await {
                Ok(listener) => {
                    log::info!("Started listening on {}...", address);
                    // Start accepting incoming connections.
                    tokio::spawn(listen_on(
                        listener,
                        self.config.clone(),
                        self.shared.clone(),
                        cancel_token.clone(),
                        keep_alive.clone(),
                    ));
                    successful_binds += 1;
                }
                Err(e) => {
                    log::error!("Failed to bind {}: {}", address, e);
                }
            }
        }

        if successful_binds == 0 {
            bail!("Failed to bind any address!");
        }

        // Start maintenance routine for the shared state.
        let cancel_maintenance = cancel_token.clone();
        let keep_alive_maintenance = keep_alive.clone();
        let maintenance_interval = self
            .config
            .read()
            .unwrap()
            .server_settings
            .maintenance_interval_seconds;
        let shared_state = self.shared.clone();
        tokio::spawn(async move {
            loop {
                let wake_up = sleep(Duration::from_secs(maintenance_interval));
                tokio::select! {
                    _ = cancel_maintenance.cancelled() => { break; }
                    _ = wake_up => {
                        log::trace!("Performing job maintenance...");
                        shared_state.write().unwrap().run_maintenance();
                    }
                }
            }
            drop(keep_alive_maintenance);
        });

        // Save handles for stop function.
        self.shutdown = Some((cancel_token, shutdown));
        Ok(())
    }

    // Close all connections and stop listening.
    pub async fn stop(&mut self) -> Result<()> {
        match self.shutdown.take() {
            Some((cancel_token, mut shutdown)) => {
                log::info!("Shutting down server!");
                // Cancel all spawned tasks.
                cancel_token.cancel();
                // Wait for all senders in the spawned tasks to be dropped.
                shutdown.recv().await;
                Ok(())
            }
            None => bail!("Server is not running!"),
        }
    }
}

/// Runs asynchronously and accepts new TCP connections.
pub async fn listen_on(
    listener: TcpListener,
    config: Arc<RwLock<Config>>,
    shared: Arc<RwLock<Manager>>,
    cancel_token: CancellationToken,
    keep_alive: Sender<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => { return Ok(()); }
            result = listener.accept() => {
                let (stream, address) = result?;
                log::trace!("Established connection from {}!", address);

                tokio::spawn(handle_connection(
                    stream,
                    config.clone(),
                    shared.clone(),
                    cancel_token.clone(),
                    keep_alive.clone(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{config::Config, server::TcpServer};

    #[tokio::test]
    async fn start_and_stop_tcp_server() {
        let config = Config::new(None).unwrap();
        let mut server = TcpServer::new(config);
        assert!(server.start().await.is_ok());

        // Network connections don't work in all test/build environments.
        // Therefore, we perform more detailed tests and interactions with
        // a `TestServer` implementation that operates on local streams.
        assert!(server.stop().await.is_ok());
    }
}
