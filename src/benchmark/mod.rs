use anyhow::Result;

use std::time::Instant;

use tokio::io::{
    AsyncBufReadExt,
    AsyncWriteExt,
    BufReader,
};

use tokio::net::TcpStream;

pub async fn run() -> Result<()> {
    let connection_count = 1000;

    let start =
        Instant::now();

    for i in 0..connection_count {
        let mut stream =
            TcpStream::connect(
                "127.0.0.1:7878",
            )
            .await?;

        let command = format!(
            "SET key{} value{}\n",
            i,
            i,
        );

        stream
            .write_all(
                command.as_bytes(),
            )
            .await?;

        let mut reader =
            BufReader::new(stream);

        let mut response =
            String::new();

        reader
            .read_line(
                &mut response,
            )
            .await?;
    }

    let elapsed =
        start.elapsed();

    println!(
        "completed {} writes in {:?}",
        connection_count,
        elapsed
    );

    let throughput =
        connection_count as f64
            / elapsed.as_secs_f64();

    println!(
        "throughput: {:.2} ops/sec",
        throughput
    );

    Ok(())
}