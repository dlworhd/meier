use futures::TryFutureExt;
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

use crate::{Result, TesseractError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind_addr: String,
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
    pub max_message_size_bytes: usize,
    #[serde(default = "default_max_topics")]
    pub max_topics: usize,
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
                bind_addr: "127.0.0.1:2369".to_string(),
                max_connections: 1000,
            },
            storage: StorageConfig {
                max_messages_per_partition: 10000,
                max_message_size_bytes: 1024 * 1024,
                max_topics: 100,
            },
            logging: LoggingConfig {
                level: LogLevel::INFO,
                file: None,
            },
        }
    }

    pub fn user_config_file() -> Result<PathBuf> {
        let home = env::var("HOME")
            .or_else(|_| env::var("USERPROFILE"))
            .map_err(|_| TesseractError::Config("Failed to get home directory".to_string()))?;

        Ok(PathBuf::from(home)
            .join(".config")
            .join("tesseract")
            .join("config.toml"))
    }

    // 설정 파일이 포함된 디렉토리 반환
    pub fn user_config_dir() -> Result<PathBuf> {
        Self::user_config_file().map(|path| path.parent().unwrap().to_path_buf())
    }

    pub fn load_user_config() -> Result<Self> {
        let config_file = Self::user_config_file()?;
        if !config_file.exists() {
            return Err(TesseractError::Config(format!(
                "Config file not found: {}",
                config_file.display(),
            )));
        }

        Self::from_file(config_file)
    }

    pub fn save_user_config(&self) -> Result<()> {
        let config_file = Self::user_config_file()?;
        let config_dir = config_file.parent().unwrap();

        fs::create_dir_all(config_dir).map_err(|e| {
            TesseractError::Config(format!("Failed to create config directory: {}", e))
        })?;

        let toml_content = toml::to_string_pretty(self)
            .map_err(|e| TesseractError::Config(format!("Failed to serialize config: {}", e)))?;

        fs::write(&config_file, toml_content)
            .map_err(|e| TesseractError::Config(format!("Failed to write config file: {}", e)))?;
        Ok(())
    }

    pub fn user_config_exists() -> bool {
        Self::user_config_file()
            .map(|path| path.exists())
            .unwrap_or(false)
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::error::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path.as_ref().to_str().unwrap()))
            .add_source(config::Environment::with_prefix("MEIER"))
            .build()
            .map_err(|e| crate::error::TesseractError::Config(e.to_string()))?;

        settings
            .try_deserialize()
            .map_err(|e| crate::error::TesseractError::Config(e.to_string()))
    }

    // 환경변수 내 추출
    pub fn from_env() -> crate::error::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::Environment::with_prefix("MEIER"))
            .build()
            .map_err(|e| crate::error::TesseractError::Config(e.to_string()))?;

        settings
            .try_deserialize()
            .map_err(|e| crate::error::TesseractError::Config(e.to_string()))
    }
}
