use anyhow::Result;
use chrono::Utc;
use std::path::Path;
use std::sync::Arc;

use tokio::fs::{File, OpenOptions};
use tokio::io::{
    AsyncBufReadExt,
    AsyncWriteExt,
    BufReader,
};
use tokio::sync::Mutex;

use crate::engine::Engine;

#[derive(Clone)]
pub struct Wal {
    file: Arc<Mutex<File>>,
}

impl Wal {
    pub async fn new() -> Result<Self> {
        if !Path::new("data").exists() {
            tokio::fs::create_dir("data").await?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("data/operations.log")
            .await?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub async fn append(
        &self,
        operation: &str,
        key: &str,
        value: Option<&str>,
    ) -> Result<()> {
        let timestamp =
            Utc::now().timestamp_millis();

        let record = match value {
            Some(v) => {
                format!(
                    "{}|{}|{}|{}\n",
                    timestamp,
                    operation,
                    key,
                    v
                )
            }

            None => {
                format!(
                    "{}|{}|{}\n",
                    timestamp,
                    operation,
                    key
                )
            }
        };

        let mut file = self.file.lock().await;

        file.write_all(record.as_bytes())
            .await?;

        file.flush().await?;

        Ok(())
    }

    pub async fn recover(
        engine: &Engine,
    ) -> Result<()> {
        if !Path::new("data/operations.log")
            .exists()
        {
            return Ok(());
        }

        let file =
            File::open("data/operations.log")
                .await?;

        let reader = BufReader::new(file);

        let mut lines = reader.lines();

        while let Some(line) =
            lines.next_line().await?
        {
            let parts: Vec<&str> =
                line.split('|').collect();

            if parts.len() < 3 {
                continue;
            }

            let operation = parts[1];
            let key = parts[2];

            match operation {
                "SET" => {
                    if parts.len() != 4 {
                        continue;
                    }

                    let value = parts[3];

                    engine.recover_set(
                        key.to_string(),
                        value.to_string(),
                    );
                }

                "DEL" => {
                    engine.recover_delete(key);
                }

                _ => {}
            }
        }

        Ok(())
    }
}