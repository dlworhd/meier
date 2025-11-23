use crate::TesseractError;

#[derive(Debug, Clone)]
pub struct Message {
    pub data: Vec<u8>,
    pub timestamp: u64,
}

impl Message {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Return messages's bytes size
    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn to_string(&self) -> crate::Result<String> {
        String::from_utf8(self.data.clone())
            .map_err(|e| TesseractError::Protocol(format!("Invalid UTF-8: {}", e)))
    }
}
