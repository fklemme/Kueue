//! Shared config file "config.toml".
//!
//! All binary crates share a common config file, which is separated into
//! groups. The "common_settings" group contains settings related to multiple
//! crates while "server_settings", "worker_settings", "client_settings", and
//! "restart_workers" contain settings associated with their respective crates.

use anyhow::{anyhow, Result};
use directories::ProjectDirs;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{
    fs::{create_dir_all, File},
    io::Write,
    net::SocketAddr,
    path::PathBuf,
};
use tokio::net::lookup_host;

/// Common settings shared among all crates.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CommonSettings {
    /// Shared secret used to authenticate client and worker against the server.
    pub shared_secret: String,
    /// Host name (or IP address) of the server, used by client and worker.
    pub server_name: String,
    /// Network port used by the server.
    pub server_port: u16,
    /// Verbosity level of log messages.
    /// Options: "trace", "debug", "info", "warn", and "error".
    pub log_level: String,
}

/// Settings related to the server crate.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ServerSettings {
    /// Space-separated list of IP addresses to listen on.
    /// Defaults to: 0.0.0.0 (IPv4) and [::] (IPv6)
    pub bind_addresses: String,
    /// Time in seconds before a worker connection is considered timed-out.
    pub worker_timeout_seconds: i64,
    pub job_offer_timeout_seconds: i64,
    pub job_cleanup_after_minutes: i64,
}

/// Settings related to the worker crate.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerSettings {
    /// If set "true", the worker's maximum available resources will dynamically
    /// grow and shrink with free resources on the host system. This setting is
    /// useful for shared machines, which are not exclusively used with Kueue.
    pub dynamic_check_free_resources: bool,
}

/// Setting related to the optional "restart_workers" crate.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RestartWorkers {
    pub ssh_user: String,
    pub hostnames: String,
    pub sleep_minutes_before_recheck: Option<f64>,
}

/// The Config struct represents the read TOML config file
/// and holds the settings for all individual crates.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Config {
    /// Common settings shared among all crates.
    pub common_settings: CommonSettings,
    /// Settings related to the server crate.
    pub server_settings: ServerSettings,
    /// Settings related to the worker crate.
    pub worker_settings: WorkerSettings,
    /// Setting related to the optional "restart_workers" crate.
    pub restart_workers: Option<RestartWorkers>,
}

/// Returns the system-specific default path of the config file.
pub fn default_path() -> PathBuf {
    let config_file_name = if cfg!(debug_assertions) {
        "config-devel.toml"
    } else {
        "config.toml"
    };

    if let Some(project_dirs) = ProjectDirs::from("", "", "kueue") {
        project_dirs.config_dir().join(config_file_name)
    } else {
        config_file_name.into()
    }
}

impl Config {
    pub fn new(config_path: Option<PathBuf>) -> Result<Self, config::ConfigError> {
        let config_path = config_path.unwrap_or(default_path());

        // TODO: Raise default levels when more mature.
        let default_log_level = if cfg!(debug_assertions) {
            "debug"
        } else {
            "info"
        };

        // Generate initial random shared secret.
        let random_secret: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        let s = config::Config::builder();

        // Default common settings.
        let s = s
            .set_default("common_settings.shared_secret", random_secret)?
            .set_default("common_settings.server_name", "localhost")?
            .set_default("common_settings.server_port", 11236)?
            .set_default("common_settings.log_level", default_log_level)?;

        // Default server settings.
        let s = s.set_default("server_settings.bind_addresses", "0.0.0.0 [::]")?;
        let s = s.set_default("server_settings.worker_timeout_seconds", 5 * 60)?;
        let s = s.set_default("server_settings.job_offer_timeout_seconds", 60)?;
        let s = s.set_default("server_settings.job_cleanup_after_minutes", 48 * 60)?;

        // Default worker settings.
        let s = s.set_default("worker_settings.dynamic_check_free_resources", true)?;

        let s = s
            .add_source(
                config::File::with_name(config_path.to_string_lossy().as_ref()).required(false),
            )
            .build()?;

        // Deserialize into Config.
        s.try_deserialize()
    }

    pub fn save_as_template(&self, config_path: Option<PathBuf>) -> Result<()> {
        let config_path = config_path.unwrap_or(default_path());
        let toml = toml::to_string(&self)?;

        if let Some(config_dir) = config_path.parent() {
            if !config_dir.is_dir() {
                create_dir_all(config_dir)?;
            }
        }

        if !config_path.is_file() {
            let mut file = File::create(config_path)?;
            file.write_all(toml.as_bytes())?;
        }

        Ok(())
    }

    pub fn get_log_level(&self) -> log::Level {
        match self.common_settings.log_level.to_lowercase().as_str() {
            "trace" => log::Level::Trace,
            "debug" => log::Level::Debug,
            "info" => log::Level::Info,
            "warn" => log::Level::Warn,
            "error" => log::Level::Error,
            _ => log::Level::Info, // default
        }
    }

    pub async fn get_server_address(&self) -> Result<SocketAddr> {
        let host = format!(
            "{}:{}",
            self.common_settings.server_name, self.common_settings.server_port
        );
        let mut addr_iter = lookup_host(host).await?;
        match addr_iter.next() {
            Some(socket_address) => Ok(socket_address),
            None => Err(anyhow!(
                "Could not resolve server address: {}",
                self.common_settings.server_name
            )),
        }
    }
}
