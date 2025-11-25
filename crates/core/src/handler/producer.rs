use crate::{
    Frame, Result, protocol,
    storage::{Message, TopicManager},
};

pub async fn handle_produce(
    topic_manager: &TopicManager,
    topic: String,
    message: Vec<u8>,
) -> Result<Frame> {
    let msg = Message::new(message);
    let topic = topic_manager.get_or_create_topic(topic).await?;
    topic.add_message(msg).await?;

    Ok(Frame::Response {
        status: protocol::Status::ok(),
        data: None,
        message: Some("Message produced successfully".to_string()),
    })
}
