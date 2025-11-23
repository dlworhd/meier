use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

async fn read_frame(stream: &mut TcpStream) -> Result<String, Box<dyn std::error::Error>> {
    // 4바이트 길이 읽기
    let mut length_bytes = [0u8; 4];
    stream.read_exact(&mut length_bytes).await?;
    let length = u32::from_be_bytes(length_bytes) as usize;

    // JSON 데이터 읽기
    let mut buffer = vec![0u8; length];
    stream.read_exact(&mut buffer).await?;

    String::from_utf8(buffer).map_err(|e| format!("Invalid UTF-8: {}", e).into())
}

async fn send_consume_next(
    stream: &mut TcpStream,
    topic: &str,
    partition_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let frame = format!(
        r#"{{"ConsumeNext": {{"topic":"{}","partition_id":{}}}}}"#,
        topic, partition_id
    );
    let length = frame.len() as u32;

    let mut v = Vec::new();
    v.extend_from_slice(&length.to_be_bytes());
    v.extend_from_slice(frame.as_bytes());

    stream.write_all(&v).await?;
    stream.flush().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:2369").await?;
    stream.set_nodelay(true)?;

    let topic = "topic-1";
    let partition_id = 1;

    println!(
        "Consumer started. Polling for messages from topic: {}, partition: {}",
        topic, partition_id
    );
    println!("Press Ctrl+C to stop.\n");

    let mut message_count = 0;

    loop {
        // ConsumeNext 요청 보내기
        if let Err(e) = send_consume_next(&mut stream, topic, partition_id).await {
            eprintln!("Failed to send ConsumeNext request: {}", e);
            break;
        }

        // 응답 받기
        match read_frame(&mut stream).await {
            Ok(response) => {
                // JSON 파싱
                println!("{}", response);
                match serde_json::from_str::<Value>(&response) {
                    Ok(json) => {
                        if let Some(status) = json.get("status") {
                            if status == "Ok" {
                                if let Some(data) = json.get("data") {
                                    if !data.is_null() {
                                        message_count += 1;
                                        if let Some(msg) = json.get("message") {
                                            println!(
                                                "[{}] ✅ Message #{}",
                                                message_count,
                                                msg.as_str().unwrap_or("")
                                            );
                                        }
                                    } else {
                                        // 메시지가 없을 때는 조용히 대기
                                        // println!("⏳ No new messages");
                                    }
                                }
                            } else {
                                if let Some(msg) = json.get("message") {
                                    eprintln!("⚠️  Error: {}", msg.as_str().unwrap_or(""));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Raw response: {}", response);
                        eprintln!("Failed to parse JSON: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to read response: {}", e);
                break;
            }
        }

        // 짧은 딜레이 (CPU 사용량 줄이기)
        sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}
