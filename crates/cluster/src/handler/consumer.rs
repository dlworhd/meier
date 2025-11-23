use crate::{Frame, Result, TesseractError, protocol, storage::TopicManager};

pub async fn handle_consume(
    topic_manager: &TopicManager,
    topic: String,
    partition_id: usize,
    offset: usize,
) -> Result<Frame> {
    let topic = topic_manager
        .get_topic(&topic)
        .await
        .ok_or_else(|| TesseractError::TopicNotFound(topic.clone()))?;

    let partition = topic
        .get_partition(&partition_id.to_string())
        .ok_or_else(|| {
            TesseractError::PartitionNotFound(format!(
                "Partition {} not found in topic {}",
                partition_id,
                topic.name()
            ))
        })?;

    match partition.get_message(offset).await {
        Some(msg) => {
            let message_str = msg
                .to_string()
                .unwrap_or_else(|_| format!("[Binary data: {} bytes]", msg.size()));
            Ok(Frame::Response {
                status: protocol::Status::ok(),
                data: Some(msg.data.clone()),
                message: Some(message_str),
            })
        }
        None => Ok(Frame::Response {
            status: protocol::Status::error(format!("No message at offset {}", offset)),
            data: None,
            message: Some(format!(
                "Current offset: {} Requested offset: {}",
                partition.current_offset().await,
                offset
            )),
        }),
    }
}

pub async fn handle_consume_next(
    topic_manager: &TopicManager,
    topic: String,
    partition_id: usize,
) -> Result<Frame> {
    let topic = topic_manager
        .get_topic(&topic)
        .await
        .ok_or_else(|| TesseractError::TopicNotFound(topic.clone()))?;

    let partition = topic
        .get_partition(&partition_id.to_string())
        .ok_or_else(|| {
            TesseractError::PartitionNotFound(format!(
                "Partition {} not found in topic {}",
                partition_id,
                topic.name()
            ))
        })?;

    let current_offset = partition.current_offset().await;

    match partition.consume_message().await {
        Some(msg) => {
            let message_str = msg
                .to_string()
                .unwrap_or_else(|_| format!("[Binary data: {} bytes]", msg.size()));
            Ok(Frame::Response {
                status: protocol::Status::ok(),
                data: Some(msg.data.clone()),
                message: Some(format!("offset={}:{}", current_offset, message_str)),
            })
        }
        None => Ok(Frame::Response {
            status: protocol::Status::ok(),
            data: None,
            message: Some(format!(
                "No new messages. Current offset: {}",
                current_offset
            )),
        }),
    }
}
