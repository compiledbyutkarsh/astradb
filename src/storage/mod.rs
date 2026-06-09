use anyhow::Result;

use tokio::fs;

use crate::engine::{
    Engine,
    SnapshotData,
};

const SNAPSHOT_PATH: &str =
    "data/snapshot.json";

const WAL_PATH: &str =
    "data/operations.log";

pub async fn save_snapshot(
    engine: &Engine,
) -> Result<()> {
    let snapshot =
        engine.snapshot();

    let serialized =
        serde_json::to_string_pretty(
            &snapshot,
        )?;

    fs::write(
        SNAPSHOT_PATH,
        serialized,
    )
    .await?;

    Ok(())
}

pub async fn load_snapshot(
    engine: &Engine,
) -> Result<bool> {
    let exists =
        fs::try_exists(
            SNAPSHOT_PATH,
        )
        .await?;

    if !exists {
        return Ok(false);
    }

    let content =
        fs::read_to_string(
            SNAPSHOT_PATH,
        )
        .await?;

    let snapshot: SnapshotData =
        serde_json::from_str(
            &content,
        )?;

    engine.load_snapshot(snapshot);

    Ok(true)
}

pub async fn compact_wal() -> Result<()> {
    let exists =
        fs::try_exists(WAL_PATH).await?;

    if !exists {
        return Ok(());
    }

    fs::write(WAL_PATH, "").await?;

    Ok(())
}