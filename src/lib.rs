//! # Kueue
//!
//! A robust, user-level, work-stealing distributed task scheduler.
//!
//! The Kueue package consists of multiple binary crates (client, selver, worker) to realize the distributed task scheduler.
//! This library crate contains shared code between the Kueue binaries. To obtain the Kueue task scheduler, use "cargo install" instead:
//!
//! ```
//! cargo install kueue
//! ```
//!
//! Find more information on [crates.io/crates/kueue](https://crates.io/crates/kueue).

pub mod config;
pub mod messages;
pub mod structs;
