// Shared code between all executables

pub mod config {
    use directories::ProjectDirs;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use serde::{Deserialize, Serialize};
    use std::{
        error::Error,
        fs::{create_dir_all, File},
        io::Write,
        path::PathBuf,
    };

    pub fn default_path() -> PathBuf {
        let config_file_name = if cfg!(debug_assertions) {
            "config-devel.toml"
        } else {
            "config.toml"
        };

        if let Some(project_dirs) = ProjectDirs::from("", "", "kueue") {
            return project_dirs.config_dir().join(config_file_name);
        }
        config_file_name.into()
    }

    #[derive(Clone, Serialize, Deserialize, Debug)]
    pub struct Config {
        pub server_bind_address: String,
        pub server_address: String,
        pub server_port: u16,
        pub shared_secret: String,
    }

    impl Config {
        pub fn new() -> Result<Self, config::ConfigError> {
            let config_path: String = default_path().to_string_lossy().into();

            let random_secret: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect();

            let s = config::Config::builder()
                .set_default("server_bind_address", "0.0.0.0")?
                .set_default("server_address", "127.0.0.1")?
                .set_default("server_port", 11236)?
                .set_default("shared_secret", random_secret)?
                .add_source(config::File::with_name(&config_path).required(false))
                .build()?;

            s.try_deserialize()
        }

        pub fn create_default_config(&self) -> Result<(), Box<dyn Error>> {
            let config_path = default_path();
            let toml = toml::to_vec(&self)?;

            if let Some(config_dir) = config_path.parent() {
                if !config_dir.is_dir() {
                    create_dir_all(config_dir)?;
                }
            }

            if !config_path.is_file() {
                let mut file = File::create(config_path)?;
                file.write_all(&toml)?;
            }

            Ok(())
        }
    }
}

pub mod messages;
pub mod structs;
