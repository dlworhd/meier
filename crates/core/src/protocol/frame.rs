use serde::{Deserialize, Serialize};

use crate::Result;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]

pub enum Frame {
    Produce {
        topic: String,
        message: Vec<u8>,
    },
    Consume {
        topic: String,
        partition_id: usize,
        offset: usize,
    },
    Response {
        status: Status,
        data: Option<Vec<u8>>,
        message: Option<String>,
    },
    Ping,
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Status {
    Ok,
    Error(String),
}

impl Frame {
    /// Frame -> 바이트(직렬화)
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| crate::MeierError::Protocol(format!("Serialization error: {}", e)))
    }

    /// 바이트 -> Frame(역직렬화)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| crate::MeierError::Protocol(format!("Deserialization error: {}", e)))
    }

    pub fn size(&self) -> Result<usize> {
        Ok(self.to_bytes()?.len())
    }
}

impl Status {
    pub fn ok() -> Self {
        Self::Ok
    }

    pub fn error(msg: String) -> Self {
        Self::Error(msg)
    }
}
