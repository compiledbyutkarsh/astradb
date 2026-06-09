use anyhow::Result;

use tokio::io::{
    AsyncBufReadExt,
    AsyncWriteExt,
    BufReader,
};

use tokio::net::TcpStream;

use tracing::{
    error,
    info,
};

use crate::engine::Engine;

use crate::protocol::{
    parse,
    Command,
};

use crate::replication::{
    ReplicationBus,
    ReplicationEvent,
};

use crate::utils::Metrics;

use crate::wal::Wal;

const PIPELINE_BATCH_SIZE: usize = 32;

pub async fn handle_connection(
    socket: TcpStream,
    engine: Engine,
    wal: Wal,
    metrics: Metrics,
    replication: ReplicationBus,
) -> Result<()> {
    let address =
        socket.peer_addr()?;

    metrics.connection_opened();

    info!(
        "handling client {}",
        address
    );

    let (reader, mut writer) =
        socket.into_split();

    let mut reader =
        BufReader::new(reader);

    let mut response_buffer =
        String::new();

    loop {
        let mut commands =
            Vec::with_capacity(
                PIPELINE_BATCH_SIZE,
            );

        for _ in 0..PIPELINE_BATCH_SIZE {
            let mut line =
                String::new();

            let bytes_read =
                reader
                    .read_line(&mut line)
                    .await?;

            if bytes_read == 0 {
                metrics.connection_closed();

                info!(
                    "client disconnected {}",
                    address
                );

                return Ok(());
            }

            let trimmed =
                line.trim();

            if trimmed.is_empty() {
                break;
            }

            commands.push(
                trimmed.to_string(),
            );

            if bytes_read < 2 {
                break;
            }
        }

        response_buffer.clear();

        for raw_command in commands {
            metrics.command_executed();

            let command =
                parse(&raw_command);

            match command {
                Command::Ping => {
                    response_buffer
                        .push_str(
                            "PONG\n",
                        );
                }

                Command::Set {
                    key,
                    value,
                } => {
                    metrics
                        .write_operation();

                    wal.append(
                        "SET",
                        &key,
                        Some(&value),
                    )
                    .await?;

                    engine.set(
                        key.clone(),
                        value.clone(),
                    );

                    replication.publish(
                        ReplicationEvent {
                            operation:
                                "SET"
                                    .to_string(),

                            key,

                            value:
                                Some(value),
                        },
                    );

                    response_buffer
                        .push_str(
                            "OK\n",
                        );
                }

                Command::SetEx {
                    key,
                    value,
                    ttl,
                } => {
                    metrics
                        .write_operation();

                    wal.append(
                        "SETEX",
                        &key,
                        Some(&value),
                    )
                    .await?;

                    engine.set_with_ttl(
                        key.clone(),
                        value.clone(),
                        ttl,
                    );

                    replication.publish(
                        ReplicationEvent {
                            operation:
                                "SETEX"
                                    .to_string(),

                            key,

                            value:
                                Some(value),
                        },
                    );

                    response_buffer
                        .push_str(
                            "OK\n",
                        );
                }

                Command::Get { key } => {
                    metrics
                        .read_operation();

                    match engine.get(&key) {
                        Some(value) => {
                            response_buffer
                                .push_str(
                                    &format!(
                                        "{}\n",
                                        value
                                    ),
                                );
                        }

                        None => {
                            response_buffer
                                .push_str(
                                    "NULL\n",
                                );
                        }
                    }
                }

                Command::Delete {
                    key,
                } => {
                    metrics
                        .write_operation();

                    wal.append(
                        "DEL",
                        &key,
                        None,
                    )
                    .await?;

                    let deleted =
                        engine.delete(
                            &key,
                        );

                    replication.publish(
                        ReplicationEvent {
                            operation:
                                "DEL"
                                    .to_string(),

                            key,

                            value: None,
                        },
                    );

                    if deleted {
                        response_buffer
                            .push_str(
                                "OK\n",
                            );
                    } else {
                        response_buffer
                            .push_str(
                                "NULL\n",
                            );
                    }
                }

                Command::Metrics => {
                    let snapshot =
                        metrics.snapshot();

                    response_buffer
                        .push_str(
                            &format!(
                                "total_connections={}\nactive_connections={}\ntotal_commands={}\ntotal_reads={}\ntotal_writes={}\n",
                                snapshot.total_connections,
                                snapshot.active_connections,
                                snapshot.total_commands,
                                snapshot.total_reads,
                                snapshot.total_writes,
                            ),
                        );
                }

                Command::Unknown => {
                    error!(
                        "unknown command from {}",
                        address
                    );

                    response_buffer
                        .push_str(
                            "ERR unknown command\n",
                        );
                }
            }
        }

        writer
            .write_all(
                response_buffer
                    .as_bytes(),
            )
            .await?;
    }
}