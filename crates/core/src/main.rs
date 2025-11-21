use clap::Parser;
use meier_core::{Config, Result, server::Server};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "meier")]
#[command(about = "A message broker server", long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<String>,
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let mut config = if let Some(config_path) = args.config {
        Config::from_file(config_path)?
    } else {
        Config::from_env().unwrap_or_else(|_| {
            info!("No environment config found, using defaults");
            Config::default()
        })
    };

    config.server.bind_addr = format!("{}:{}", "127.0.0.1", "2369");

    info!("Starting Meier server on {}", config.server.bind_addr);
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
