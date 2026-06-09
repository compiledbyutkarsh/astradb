use std::env;

#[derive(Clone)]
pub struct Config {
    pub host: String,

    pub port: u16,

    pub snapshot_interval_secs: u64,
}

impl Config {
    pub fn load() -> Self {
        let host = env::var(
            "ASTRADB_HOST",
        )
        .unwrap_or_else(|_| {
            "127.0.0.1".to_string()
        });

        let port = env::var(
            "ASTRADB_PORT",
        )
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7878);

        let snapshot_interval_secs =
            env::var(
                "ASTRADB_SNAPSHOT_INTERVAL",
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);

        Self {
            host,
            port,
            snapshot_interval_secs,
        }
    }

    pub fn address(&self) -> String {
        format!(
            "{}:{}",
            self.host,
            self.port
        )
    }
}