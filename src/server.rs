use anyhow::Result;

use tokio::net::TcpListener;

use tokio::signal;

use tokio::sync::broadcast;

use tokio::time::{
    sleep,
    Duration,
};

use tracing::{
    error,
    info,
    warn,
};

use crate::config::Config;
use crate::engine::Engine;
use crate::network;

use crate::replication::ReplicationBus;

use crate::storage;
use crate::utils::Metrics;
use crate::wal::Wal;

pub async fn start() -> Result<()> {
    let config = Config::load();

    let listener =
        TcpListener::bind(
            config.address(),
        )
        .await?;

    let engine = Engine::new();

    let metrics =
        Metrics::new();

    let replication =
        ReplicationBus::new();

    let snapshot_loaded =
        storage::load_snapshot(
            &engine,
        )
        .await?;

    if snapshot_loaded {
        warn!(
            "snapshot restored successfully"
        );
    }

    let wal = Wal::new().await?;

    Wal::recover(&engine).await?;

    warn!(
        "recovered {} keys from wal",
        engine.len()
    );

    let (
        shutdown_tx,
        _,
    ) = broadcast::channel::<()>(1);

    let snapshot_engine =
        engine.clone();

    let cleanup_engine =
        engine.clone();

    let shutdown_snapshot =
        shutdown_tx.subscribe();

    let shutdown_cleanup =
        shutdown_tx.subscribe();

    let snapshot_interval =
        config.snapshot_interval_secs;

    tokio::spawn(async move {
        let mut shutdown_signal =
            shutdown_snapshot;

        loop {
            tokio::select! {
                _ = sleep(
                    Duration::from_secs(
                        snapshot_interval,
                    )
                ) => {
                    match storage::save_snapshot(
                        &snapshot_engine,
                    )
                    .await
                    {
                        Ok(_) => {
                            info!(
                                "snapshot persisted"
                            );

                            match storage::compact_wal()
                                .await
                            {
                                Ok(_) => {
                                    info!(
                                        "wal compacted"
                                    );
                                }

                                Err(err) => {
                                    error!(
                                        "wal compaction failure: {}",
                                        err
                                    );
                                }
                            }
                        }

                        Err(err) => {
                            error!(
                                "snapshot failure: {}",
                                err
                            );
                        }
                    }
                }

                _ = shutdown_signal.recv() => {
                    info!(
                        "snapshot worker shutting down"
                    );

                    break;
                }
            }
        }
    });

    tokio::spawn(async move {
        let mut shutdown_signal =
            shutdown_cleanup;

        loop {
            tokio::select! {
                _ = sleep(
                    Duration::from_secs(5)
                ) => {
                    cleanup_engine
                        .cleanup_expired();
                }

                _ = shutdown_signal.recv() => {
                    info!(
                        "cleanup worker shutting down"
                    );

                    break;
                }
            }
        }
    });

    let mut subscriber =
        replication.subscribe();

    tokio::spawn(async move {
        loop {
            match subscriber.recv().await {
                Ok(event) => {
                    info!(
                        "replication event op={} key={}",
                        event.operation,
                        event.key
                    );
                }

                Err(err) => {
                    error!(
                        "replication stream error: {}",
                        err
                    );
                }
            }
        }
    });

    info!(
        "astradb listening on {}",
        config.address()
    );

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, address)) => {
                        info!(
                            "accepted connection from {}",
                            address
                        );

                        let engine_clone =
                            engine.clone();

                        let wal_clone =
                            wal.clone();

                        let metrics_clone =
                            metrics.clone();

                        let replication_clone =
                            replication.clone();

                        tokio::spawn(async move {
                            if let Err(err) =
                                network::handle_connection(
                                    socket,
                                    engine_clone,
                                    wal_clone,
                                    metrics_clone,
                                    replication_clone,
                                )
                                .await
                            {
                                error!(
                                    "connection error from {}: {}",
                                    address,
                                    err
                                );
                            }
                        });
                    }

                    Err(err) => {
                        error!(
                            "failed to accept socket: {}",
                            err
                        );
                    }
                }
            }

            _ = signal::ctrl_c() => {
                warn!(
                    "shutdown signal received"
                );

                break;
            }
        }
    }

    warn!(
        "persisting final snapshot"
    );

    storage::save_snapshot(&engine)
        .await?;

    storage::compact_wal().await?;

    let _ =
        shutdown_tx.send(());

    warn!(
        "astradb shutdown complete"
    );

    Ok(())
}