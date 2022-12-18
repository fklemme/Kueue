//! # Kueue
//!
//! A robust, user-level, work-stealing distributed task scheduler.
//!
//! The Kueue package consists of multiple binary crates (client, selver, worker) to realize the distributed task scheduler.
//! This library crate contains shared code between the Kueue binaries. To obtain the Kueue task scheduler, use "cargo install" instead:
//!
//! ```text
//! cargo install kueue
//! ```
//!
//! Find more information on [crates.io/crates/kueue](https://crates.io/crates/kueue).

//#![warn(missing_docs)]

pub mod config;
pub mod messages;
pub mod structs;

pub mod constants {
    pub const OFFER_TIMEOUT_MINUTES: i64 = 5;
    pub const WORKER_TIMEOUT_MINUTES: i64 = 15;
    pub const CLEANUP_JOB_AFTER_HOURS: i64 = 48;
}


#[cfg(test)]
mod tests {
    use crate::config::Config;

    #[test]
    fn general_test_setup() {
        let config = Config::new(None);
        // TODO...
    }
}
