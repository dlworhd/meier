use thiserror::Error;

#[derive(Error, Debug)]
pub enum TesseractError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Buffer overflow: {0}")]
    BufferOverflow(String),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Partition not found: {0}")]
    PartitionNotFound(String),
}

pub type Result<T> = std::result::Result<T, TesseractError>;
