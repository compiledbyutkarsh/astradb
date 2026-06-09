use dashmap::DashMap;

use serde::{
    Deserialize,
    Serialize,
};

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;

use std::hash::{
    Hash,
    Hasher,
};

use std::sync::Arc;

use std::time::{
    Duration,
    Instant,
};

const SHARD_COUNT: usize = 16;

#[derive(
    Clone,
    Serialize,
    Deserialize,
)]
pub struct SnapshotData {
    pub data:
        HashMap<String, String>,
}

#[derive(Clone)]
pub struct Engine {
    shards:
        Arc<Vec<DashMap<String, String>>>,

    expirations:
        Arc<DashMap<String, Instant>>,
}

impl Engine {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(
            SHARD_COUNT,
        );

        for _ in 0..SHARD_COUNT {
            shards.push(DashMap::new());
        }

        Self {
            shards: Arc::new(shards),

            expirations: Arc::new(
                DashMap::new(),
            ),
        }
    }

    fn shard_for(
        &self,
        key: &str,
    ) -> usize {
        let mut hasher =
            DefaultHasher::new();

        key.hash(&mut hasher);

        (hasher.finish() as usize)
            % SHARD_COUNT
    }

    pub fn set(
        &self,
        key: String,
        value: String,
    ) {
        let shard =
            self.shard_for(&key);

        self.shards[shard]
            .insert(key, value);
    }

    pub fn set_with_ttl(
        &self,
        key: String,
        value: String,
        ttl_seconds: u64,
    ) {
        self.set(
            key.clone(),
            value,
        );

        self.expirations.insert(
            key,
            Instant::now()
                + Duration::from_secs(
                    ttl_seconds,
                ),
        );
    }

    pub fn get(
        &self,
        key: &str,
    ) -> Option<String> {
        if self.is_expired(key) {
            self.delete(key);

            return None;
        }

        let shard =
            self.shard_for(key);

        self.shards[shard]
            .get(key)
            .map(|v| v.clone())
    }

    pub fn delete(
        &self,
        key: &str,
    ) -> bool {
        self.expirations.remove(key);

        let shard =
            self.shard_for(key);

        self.shards[shard]
            .remove(key)
            .is_some()
    }

    fn is_expired(
        &self,
        key: &str,
    ) -> bool {
        match self.expirations.get(key) {
            Some(expiry) => {
                Instant::now() > *expiry
            }

            None => false,
        }
    }

    pub fn cleanup_expired(
        &self,
    ) {
        let keys: Vec<String> = self
            .expirations
            .iter()
            .filter_map(|entry| {
                if Instant::now()
                    > *entry.value()
                {
                    Some(
                        entry.key().clone(),
                    )
                } else {
                    None
                }
            })
            .collect();

        for key in keys {
            self.delete(&key);
        }
    }

    pub fn recover_set(
        &self,
        key: String,
        value: String,
    ) {
        self.set(key, value);
    }

    pub fn recover_delete(
        &self,
        key: &str,
    ) {
        self.delete(key);
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.len())
            .sum()
    }

    pub fn snapshot(
        &self,
    ) -> SnapshotData {
        let mut data =
            HashMap::new();

        for shard in self.shards.iter() {
            for entry in shard.iter() {
                data.insert(
                    entry.key().clone(),
                    entry.value().clone(),
                );
            }
        }

        SnapshotData { data }
    }

    pub fn load_snapshot(
        &self,
        snapshot: SnapshotData,
    ) {
        for (key, value) in snapshot.data {
            self.set(key, value);
        }
    }
}