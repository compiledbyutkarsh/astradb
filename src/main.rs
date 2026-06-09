mod benchmark;
mod config;
mod engine;
mod network;
mod protocol;
mod replication;
mod server;
mod storage;
mod utils;
mod wal;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> =
        std::env::args().collect();

    if args.len() > 1 {
        match args[1].as_str() {
            "benchmark" => {
                benchmark::run().await?;
            }

            _ => {
                server::start().await?;
            }
        }
    } else {
        server::start().await?;
    }

    Ok(())
}