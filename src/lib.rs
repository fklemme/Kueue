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

#![warn(clippy::missing_docs_in_private_items)]

pub mod config;
pub mod messages;
pub mod structs;

#[cfg(test)]
mod tests {
    use crate::{
        config::Config,
        messages::{ClientToServerMessage, HelloMessage},
    };

    #[test]
    fn general_test_setup() {
        // TODO: Is it possible to have some kind of integration test here?
        let _config = Config::new(None);
        // TODO...
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
