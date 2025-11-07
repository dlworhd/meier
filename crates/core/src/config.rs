use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind_arr: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_max_connections() -> usize {
    1000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_max_messages")]
    pub max_messages_per_partition: usize,
    #[serde(default = "default_max_size")]
    pub max_messages_size_byte: usize,
    #[serde(default = "default_max_topics")]
    pub max_messages_topics: usize,
}

fn default_max_messages() -> usize {
    10000
}

fn default_max_size() -> usize {
    1024 * 1024 // 1MB = 1_000_000B
}

fn default_max_topics() -> usize {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default)]
    pub level: LogLevel,
    pub file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    INFO,
    DEBUG,
    ERROR,
    WARN,
    FATAL,
}

impl Default for LogLevel {
    fn default() -> Self {
        LogLevel::INFO
    }
}

impl Config {
    pub fn default() -> Self {
        Self {
            server: ServerConfig {
                bind_arr: "127.0.0.1:2369".to_string(),
                max_connections: 1000,
            },
            storage: StorageConfig {
                max_messages_per_partition: 10000,
                max_messages_size_byte: 1024 * 1024,
                max_messages_topics: 100,
            },
            logging: LoggingConfig {
                level: LogLevel::INFO,
                file: None,
            },
        }
    }

    /*
     * 1. 파일 추출 후 환경변수에도 동시 반영
     * 2.
     */
    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::error::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path.as_ref().to_str().unwrap()))
            .add_source(config::Environment::with_prefix("MEIER"))
            .build()
            .map_err(|e| crate::error::MeierError::Config((e.to_string())))?;

        settings
            .try_deserialize()
            .map_err(|e| crate::error::MeierError::Config(e.to_string()))
    }

    // 환경변수 내 추출
    pub fn from_env() -> crate::error::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::Environment::with_prefix("MEIER"))
            .build()
            .map_err(|e| crate::error::MeierError::Config(e.to_string()))?;

        settings
            .try_deserialize()
            .map_err(|e| crate::error::MeierError::Config(e.to_string()))
    }
}
