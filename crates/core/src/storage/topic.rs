use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::{
    MeierError, Result,
    storage::{BufferManager, Message, Partition},
};

const DEFAULT_PARTITIONS: usize = 3;

pub struct Topic {
    name: String,
    partitions: HashMap<String, Arc<Partition>>,
    rr_count: RwLock<usize>,
}

impl Topic {
    pub fn new(name: String, buffer_manager: Arc<RwLock<BufferManager>>) -> Self {
        let mut partitions = HashMap::new();

        for i in 0..DEFAULT_PARTITIONS {
            let partition_id = i.to_string();
            let partition = Arc::new(Partition::new(partition_id.clone(), buffer_manager.clone()));

            partitions.insert(partition_id, partition);
        }

        Self {
            name,
            partitions,
            rr_count: RwLock::new(0),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn add_message(&self, msg: Message) -> Result<()> {
        let partition = self.next_partition().await;
        partition.add_message(msg).await
    }

    async fn next_partition(&self) -> Arc<Partition> {
        let mut count = self.rr_count.write().await;
        let partition_id = (*count % self.partitions.len()).to_string();
        *count += 1;
        self.partitions.get(&partition_id).unwrap().clone()
    }

    pub fn get_partition(&self, partition_id: &str) -> Option<Arc<Partition>> {
        self.partitions.get(partition_id).cloned()
    }

    pub fn partition_ids(&self) -> Vec<String> {
        self.partitions.keys().cloned().collect()
    }
}

pub struct TopicManager {
    topics: RwLock<HashMap<String, Arc<Topic>>>,
    buffer_manager: Arc<RwLock<BufferManager>>,
    max_topics: usize,
}

impl TopicManager {
    pub fn new(
        max_topics: usize,
        max_messages_per_partition: usize,
        max_messages_size_bytes: usize,
    ) -> Self {
        let buffer_manager = Arc::new(RwLock::new(BufferManager::new(
            max_messages_per_partition,
            max_messages_size_bytes,
        )));

        Self {
            topics: RwLock::new(HashMap::new()),
            buffer_manager,
            max_topics,
        }
    }

    pub async fn create_topic(&self, name: String) -> Result<Arc<Topic>> {
        let mut topics = self.topics.write().await;

        if topics.len() >= self.max_topics {
            return Err(MeierError::Storage(format!(
                "Maximum topics limit reached: {}",
                self.max_topics
            )));
        }

        if topics.contains_key(&name) {
            return Err(MeierError::Storage(format!(
                "Topic already exists: {}",
                name
            )));
        }

        let topic = Arc::new(Topic::new(name.clone(), self.buffer_manager.clone()));
        topics.insert(name, topic.clone());

        Ok(topic)
    }

    pub async fn get_topic(&self, name: &str) -> Option<Arc<Topic>> {
        let topics = self.topics.read().await;
        topics.get(name).cloned()
    }

    pub async fn get_or_create_topic(&self, name: String) -> Result<Arc<Topic>> {
        if let Some(topic) = self.get_topic(&name).await {
            Ok(topic)
        } else {
            self.create_topic(name).await
        }
    }

    pub async fn list_topics(&self) -> Vec<String> {
        let topics = self.topics.read().await;
        topics.keys().cloned().collect()
    }

    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let mut topics = self.topics.write().await;
        topics
            .remove(name)
            .ok_or_else(|| MeierError::TopicNotFound(name.to_string()))?;
        Ok(())
    }
}
