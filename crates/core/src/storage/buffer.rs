use crate::{Result, TesseractError};

pub struct BufferManager {
    max_messages: usize,
    max_size_bytes: usize,
    current_size: usize,
    message_count: usize,
}

impl BufferManager {
    pub fn new(max_messages: usize, max_size_bytes: usize) -> Self {
        Self {
            max_messages,
            max_size_bytes,
            current_size: 0,
            message_count: 0,
        }
    }

    pub fn can_add(&self, message_size: usize) -> bool {
        self.message_count < self.max_messages
            && self.current_size + message_size <= self.max_size_bytes
    }

    pub fn add_message(&mut self, size: usize) -> Result<()> {
        if !self.can_add(size) {
            return Err(TesseractError::BufferOverflow(format!(
                "Cannot add message: size={}, current={}/{} messages, {}/{} bytes",
                size, self.message_count, self.max_messages, self.current_size, self.max_size_bytes
            )));
        }

        self.current_size += size;
        self.message_count += 1;

        Ok(())
    }

    pub fn remove_message(&mut self, size: usize) {
        if self.current_size >= size {
            self.current_size -= size;
        }

        if self.message_count > 0 {
            self.message_count -= 1;
        }
    }

    pub fn current_size(&self) -> usize {
        self.current_size
    }

    pub fn message_count(&self) -> usize {
        self.message_count
    }
}
