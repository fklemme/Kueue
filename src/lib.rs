// Shared code between all executables

pub mod config {
    use directories::ProjectDirs;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use serde::Deserialize;
    use std::path::PathBuf;

    pub fn default_path() -> PathBuf {
        let config_file_name = "config.toml";
        if let Some(project_dirs) = ProjectDirs::from("", "star", "kueue") {
            return project_dirs.config_dir().join(config_file_name);
        }
        config_file_name.into()
    }

    #[derive(Debug, Deserialize)]
    pub struct Config {
        pub server_bind_address: String,
        pub server_address: String,
        pub server_port: u16,
        pub shared_secret: String,
    }

    impl Config {
        pub fn new() -> Result<Self, config::ConfigError> {
            let config_path:String = default_path().to_string_lossy().into();

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

        pub fn create_default_config(&self) {
            // TODO!
        }
    }
}

pub mod messages;
pub mod structs;
