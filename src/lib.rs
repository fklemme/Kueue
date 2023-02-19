//! # Kueue
//!
//! A robust, user-level, work-stealing distributed task scheduler.
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
//! Find more information on [crates.io/crates/kueue](https://crates.io/crates/kueue).
//! The [client](../kueue/index.html), [server](../kueue_server/index.html), and
//! [worker](../kueue_worker/index.html) crates are documented separately.

//#![warn(missing_docs)]

pub mod config;
pub mod messages;
pub mod structs;

#[cfg(test)]
mod tests {
    use crate::config::Config;

    #[test]
    fn general_test_setup() {
        let _config = Config::new(None);
        // TODO...
    }
}
