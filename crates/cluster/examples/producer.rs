use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
struct AsyncConnectionPool {
    connections: Arc<Mutex<VecDeque<TcpStream>>>,
    host: String,
    port: u16,
    connection_semaphore: Arc<Semaphore>,
    max_pool_size: usize,
}

impl AsyncConnectionPool {
    fn new(host: String, port: u16, max_concurrent: usize, max_pool_size: usize) -> Self {
        Self {
            connections: Arc::new(Mutex::new(VecDeque::new())),
            host,
            port,
            connection_semaphore: Arc::new(Semaphore::new(max_concurrent)),
            max_pool_size,
        }
    }
    async fn get_connection(&self) -> Result<TcpStream, Box<dyn std::error::Error>> {
        {
            let mut pool = self.connections.lock().await;
            if let Some(conn) = pool.pop_front() {
                return Ok(conn);
            }
        }

        let _permit = self
            .connection_semaphore
            .acquire()
            .await
            .map_err(|e| format!("Failed to acquire connection permit: {}", e))?;

        let stream = TcpStream::connect("127.0.0.1:2369").await?;

        stream.set_nodelay(true)?;
        Ok(stream)
    }

    async fn return_connection(&self, conn: TcpStream) {
        let mut pool = self.connections.lock().await;
        if pool.len() < self.max_pool_size {
            pool.push_back(conn);
        }
    }
}

async fn produce_message_async(
    pool: &AsyncConnectionPool,
    topic: &str,
    message: &str,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut stream = pool.get_connection().await?;

    let message_bytes = message.as_bytes();
    let message_json = format!(
        "[{}]",
        message_bytes
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    let frame = format!(
        r#"{{"produce":{{"topic":"{}","message":{}}}}}"#,
        topic, message_json
    );

    let json_data = frame.as_bytes();
    let length = json_data.len() as u32;

    // 비동기 전송
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(json_data).await?;
    stream.flush().await?;

    // 비동기 수신
    let mut length_bytes = [0u8; 4];
    stream.read_exact(&mut length_bytes).await?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    let mut buffer = vec![0u8; length];
    stream.read_exact(&mut buffer).await?;

    pool.return_connection(stream).await;

    Ok(start.elapsed())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let num_messages = 1_000_000;
    let num_tasks = 200; // 비동기 태스크 수

    let start = Instant::now();
    let pool = Arc::new(AsyncConnectionPool::new(
        "127.0.0.1".to_string(),
        2369,
        50,
        100,
    ));

    let start = Instant::now();
    let mut handles = Vec::new();

    if num_messages < num_tasks {
        println!("에러;");
    }

    println!("전송 준비 완료");
    let messages_per_task = num_messages / num_tasks;
    println!("messages_per_task: {}", messages_per_task);

    for task_id in 0..num_tasks {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            for i in 0..messages_per_task {
                let msg_id = task_id * messages_per_task + i;
                produce_message_async(&pool, "topic-1", &format!("Message-{}", msg_id))
                    .await
                    .ok();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    let elapsed = start.elapsed();
    println!("✅ Completed in {:.2}s", elapsed.as_secs_f64());
    println!(
        "   Rate: {:.0} msg/s",
        num_messages as f64 / elapsed.as_secs_f64()
    );

    let end = start.elapsed();
    println!("{}", end.as_millis());

    Ok(())
}
