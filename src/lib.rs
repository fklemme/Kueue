//! # Kueue
//!
//! A robust, user-level, work-stealing, distributed task scheduler.
//!
//! The Kueue package consists of multiple binary crates (client, server, worker)
//! to realize the distributed task scheduler. This library crate contains
//! shared code between the Kueue binaries. To obtain the Kueue task scheduler,
//! use "cargo install" instead:
//!
//! ```text
//! cargo install kueue
//! ```
//!
//! Find more information on [crates.io](https://crates.io/crates/kueue).
//! The [client](../kueue/index.html), [server](../kueue_server/index.html), and
//! [worker](../kueue_worker/index.html) crates are documented separately.

//#![warn(clippy::missing_docs_in_private_items)]

pub mod client;
pub mod config;
pub mod messages;
pub mod server;
pub mod structs;
pub mod worker;

#[cfg(test)]
mod tests {
    use crate::{
        config::Config,
        messages::{ClientToServerMessage, HelloMessage},
        server::Server,
        worker::Worker,
    };
    use simple_logger::SimpleLogger;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn general_test_setup() {
        // Run tests with `cargo test --lib -- --nocapture` to see output.

        // TODO: Customize config for tests?
        let config = Config::new(None).unwrap();

        SimpleLogger::new()
            .with_level(config.get_log_level().unwrap().to_level_filter())
            .init()
            .unwrap();

        // Start server and worker.
        let server_config = config.clone();
        let mut server = Server::new(server_config);
        let shutdown = server.async_shutdown();
        let server_handle = tokio::spawn(async move { server.run().await });
        sleep(Duration::from_millis(250)).await;
        let worker_handle = tokio::spawn(async move {
            let mut worker = Worker::new(config).await.unwrap();
            worker.run().await
        });

        // TODO: Do the things!
        sleep(Duration::from_millis(1000)).await;

        // Shutdown server and worker.
        shutdown.await;
        assert!(server_handle.await.is_ok());
        assert!(worker_handle.await.is_ok());
    }

    #[test]
    fn serde_message() {
        // Run tests with `cargo test --lib -- --nocapture` to see output.

        let message = HelloMessage::HelloFromClient;
        let buffer = serde_json::to_vec(&message).unwrap();
        println!("Hello: {}", String::from_utf8(buffer).unwrap());

        let message = ClientToServerMessage::ListWorkers;
        let buffer = serde_json::to_vec(&message).unwrap();
        println!("ListWorkers: {}", String::from_utf8(buffer).unwrap());

        let message = ClientToServerMessage::Bye;
        let buffer = serde_json::to_vec(&message).unwrap();
        println!("Bye: {}", String::from_utf8(buffer).unwrap());
    }
}
