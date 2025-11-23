use std::{collections::VecDeque, sync::Arc};
use tokio::sync::RwLock;

use crate::{
    Result,
    storage::{BufferManager, Message},
};

pub struct Partition {
    id: String,
    messages: RwLock<VecDeque<Message>>,
    offset: RwLock<usize>,
    buffer_manager: Arc<RwLock<BufferManager>>,
}

impl Partition {
    pub fn new(id: String, buffer_manager: Arc<RwLock<BufferManager>>) -> Self {
        Self {
            id,
            messages: RwLock::new(VecDeque::new()),
            offset: RwLock::new(0),
            buffer_manager,
        }
    }

    pub fn id(&self) -> &str {
        &self.id.as_str()
    }

    /// 1. 버퍼에 메시지 길이 추가(Locked)
    /// 2. 파티션에 메시지 삽입
    /// 3. 버퍼에 메시지 추가(Locked)
    /// 4. 오프셋 증가
    pub async fn add_message(&self, msg: Message) -> Result<()> {
        let msg_size = msg.size();

        // 블럭 해제 시 락 해제
        {
            // 메시지 길이 만큼 버퍼에 삽입
            let mut buffer = self.buffer_manager.write().await;
            buffer.add_message(msg_size)?;
        }

        // 파티션 내 메시지 삽입
        let mut messages = self.messages.write().await;
        messages.push_back(msg);

        // 블럭 해제 시 락 해제
        {
            // 메시지를 버퍼에 삽입
            let mut buffer = self.buffer_manager.write().await;
            // 메시지가 꽉찬 경우
            while !buffer.can_add(0) && !messages.is_empty() {
                if let Some(old_msg) = messages.pop_front() {
                    // 메시지 제거
                    buffer.remove_message(old_msg.size());

                    // 오프셋 락
                    let mut offset = self.offset.write().await;

                    // 가변 참조 오프셋 증가
                    *offset += 1
                }
            }
        }
        Ok(())
    }

    pub async fn get_message(&self, offset: usize) -> Option<Message> {
        let messages = self.messages.read().await;
        let current_offset = *self.offset.read().await;

        if offset < current_offset {
            return None;
        }

        let index = offset - current_offset;
        messages.get(index).cloned()
    }

    pub async fn current_offset(&self) -> usize {
        *self.offset.read().await
    }

    pub async fn message_count(&self) -> usize {
        self.messages.read().await.len()
    }

    pub async fn consume_message(&self) -> Option<Message> {
        let mut messages = self.messages.write().await;
        let mut offset = self.offset.write().await;

        // 메시지 소비
        if let Some(msg) = messages.pop_front() {
            let msg_size = msg.size();
            *offset += 1;

            tokio::spawn({
                let buffer = self.buffer_manager.clone();
                async move {
                    let mut buffer = buffer.write().await;
                    buffer.remove_message(msg_size);
                }
            });

            Some(msg)
        } else {
            None
        }
    }
}
