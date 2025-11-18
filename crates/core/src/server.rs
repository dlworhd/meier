use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{error, info};

use crate::{
    Config, Frame, MeierCodec, MeierError, Result,
    handler::{handle_consume, handle_produce},
    protocol,
    storage::TopicManager,
};

pub struct Server {
    config: Config,
    topic_manager: Arc<TopicManager>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        let topic_manager = Arc::new(TopicManager::new(
            config.storage.max_topics,
            config.storage.max_messages_per_partition,
            config.storage.max_message_size_bytes,
        ));

        Self {
            config,
            topic_manager,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let addr = &self.config.server.bind_addr;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| MeierError::Io(e))?;

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("New Connection from {}", peer_addr);

                    let topic_manager = self.topic_manager.clone();
                    let codec = MeierCodec::new();

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, topic_manager, codec).await
                        {
                            error!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
    async fn handle_connection(
        stream: TcpStream,
        topic_manager: Arc<TopicManager>,
        codec: MeierCodec,
    ) -> Result<()> {
        let (read_half, write_half) = stream.into_split();

        let mut reader = FramedRead::new(read_half, codec.clone());
        let mut writer = FramedWrite::new(write_half, codec);

        loop {
            tokio::select! {
                result = reader.next() => {
                    match result {
                        Some(Ok(frame)) => {
                            let response = Self::process_frame(frame, &topic_manager).await;

                            if let Err(e) = writer.send(response).await {
                                error!("Failed to send response: {}", e);
                            }
                        }

                        Some(Err(e)) => {
                            error!("Frame decode error: {}", e);
                        }

                        None => {
                            info!("Connection closed by client");
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_frame(frame: Frame, topic_manager: &TopicManager) -> Frame {
        match frame {
            Frame::Produce { topic, message } => {
                match handle_produce(topic_manager, topic, message).await {
                    Ok(response) => response,
                    Err(e) => Frame::Response {
                        status: protocol::Status::error(e.to_string()),
                        data: None,
                        message: Some(e.to_string()),
                    },
                }
            }
            Frame::Consume {
                topic,
                partition_id,
                offset,
            } => match handle_consume(topic_manager, topic, partition_id, offset).await {
                Ok(response) => response,
                Err(e) => Frame::Response {
                    status: protocol::Status::error(e.to_string()),
                    data: None,
                    message: Some(e.to_string()),
                },
            },
            Frame::Ping => Frame::Pong,
            Frame::Pong => Frame::Ping,
            Frame::Response { .. } => Frame::Response {
                status: protocol::Status::error(
                    "Server does not accept response frames".to_string(),
                ),
                data: None,
                message: Some("Invalid frame type".to_string()),
            },
        }
    }
}
