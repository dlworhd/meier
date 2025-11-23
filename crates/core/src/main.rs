use clap::Parser;
use tesseract_core::{Config, Result, server::Server};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "tesseract")]
#[command(about = "A message broker server", long_about = None)]
struct Args {}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // config 파일이 있으면 가져오고 없으면 생성 후 그 내용을 가져온다.
    let mut config = if Config::user_config_exists() {
        Config::load_user_config()?
    } else {
        let default_config = Config::default();
        default_config.save_user_config()?;
        default_config
    };

    config.server.bind_addr = format!("{}:{}", "127.0.0.1", "2369");

    info!("Starting Tesseract server on {}", config.server.bind_addr);
    info!(
        "Storage config: max_topics={}, max_messages_per_partition={}, max_message_size_bytes={}",
        config.storage.max_topics,
        config.storage.max_messages_per_partition,
        config.storage.max_message_size_bytes
    );

    let server = Server::new(config);

    if let Err(e) = server.run().await {
        error!("Server error: {}", e);
        return Err(e);
    }

    Ok(())
}
