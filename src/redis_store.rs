use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use redis::aio::MultiplexedConnection;
use redis::Client;
use serde_json::Value;
use tokio::time::sleep;

use crate::state::JobState;

static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

pub type RedisEventCallback = Arc<dyn Fn(Value) -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Clone)]
pub struct RedisBackend {
    client: Client,
    redis_prefix: String,
    instance_id: String,
}

impl RedisBackend {
    pub fn new(redis_url: &str, redis_prefix: impl Into<String>) -> anyhow::Result<Self> {
        let client = Client::open(redis_url)?;
        Ok(Self {
            client,
            redis_prefix: redis_prefix.into(),
            instance_id: generate_instance_id(),
        })
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    fn state_key(&self, job_id: &str) -> String {
        format!("{}:jobs:{}:state", self.redis_prefix, job_id)
    }

    fn meta_key(&self, job_id: &str) -> String {
        format!("{}:jobs:{}:meta", self.redis_prefix, job_id)
    }

    fn active_jobs_key(&self) -> String {
        format!("{}:jobs:active", self.redis_prefix)
    }

    fn events_channel(&self) -> String {
        format!("{}:jobs:events", self.redis_prefix)
    }

    fn blurhash_key(&self, cache_key: &str) -> String {
        format!("{}:blurhash:{}", self.redis_prefix, cache_key)
    }

    async fn connect(&self) -> Option<MultiplexedConnection> {
        match self.client.get_multiplexed_async_connection().await {
            Ok(connection) => Some(connection),
            Err(err) => {
                tracing::debug!(error = %err, "redis connection failed");
                None
            }
        }
    }

    pub async fn read_job_state(&self, job_id: &str) -> Option<JobState> {
        let mut connection = self.connect().await?;
        let raw_state: redis::RedisResult<Option<String>> = redis::cmd("GET")
            .arg(self.state_key(job_id))
            .query_async(&mut connection)
            .await;
        let raw_state = match raw_state {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(job_id, error = %err, "redis state read failed");
                return None;
            }
        };
        let raw_state = raw_state?;
        match serde_json::from_str::<JobState>(&raw_state) {
            Ok(state) => Some(state),
            Err(err) => {
                tracing::warn!(job_id, error = %err, "failed to decode redis state");
                None
            }
        }
    }

    pub async fn write_job_state(&self, job_id: &str, state: &JobState) {
        let Some(mut connection) = self.connect().await else {
            return;
        };
        let payload = match serde_json::to_string(state) {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(job_id, error = %err, "failed to encode redis state");
                return;
            }
        };
        let result: redis::RedisResult<String> = redis::cmd("SET")
            .arg(self.state_key(job_id))
            .arg(payload)
            .query_async(&mut connection)
            .await;
        if let Err(err) = result {
            tracing::debug!(job_id, error = %err, "redis state write failed");
            return;
        }
        let result: redis::RedisResult<i64> = redis::cmd("SADD")
            .arg(self.active_jobs_key())
            .arg(job_id)
            .query_async(&mut connection)
            .await;
        if let Err(err) = result {
            tracing::debug!(job_id, error = %err, "redis active set write failed");
        }
    }

    pub async fn delete_job(&self, job_id: &str) {
        let Some(mut connection) = self.connect().await else {
            return;
        };
        let result: redis::RedisResult<i64> = redis::cmd("DEL")
            .arg(self.state_key(job_id))
            .arg(self.meta_key(job_id))
            .query_async(&mut connection)
            .await;
        if let Err(err) = result {
            tracing::debug!(job_id, error = %err, "redis job delete failed");
        }
        let result: redis::RedisResult<i64> = redis::cmd("SREM")
            .arg(self.active_jobs_key())
            .arg(job_id)
            .query_async(&mut connection)
            .await;
        if let Err(err) = result {
            tracing::debug!(job_id, error = %err, "redis active set removal failed");
        }
    }

    pub async fn read_meta(&self, job_id: &str) -> Option<Value> {
        let mut connection = self.connect().await?;
        let raw_meta: redis::RedisResult<Option<String>> = redis::cmd("GET")
            .arg(self.meta_key(job_id))
            .query_async(&mut connection)
            .await;
        let raw_meta = match raw_meta {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(job_id, error = %err, "redis meta read failed");
                return None;
            }
        };
        let raw_meta = raw_meta?;
        let loaded = match serde_json::from_str::<Value>(&raw_meta) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(job_id, error = %err, "failed to decode redis meta");
                return None;
            }
        };
        if loaded.is_object() {
            Some(loaded)
        } else {
            None
        }
    }

    pub async fn write_meta(&self, job_id: &str, data: &Value) {
        let Some(mut connection) = self.connect().await else {
            return;
        };
        let payload = match serde_json::to_string(data) {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(job_id, error = %err, "failed to encode redis meta");
                return;
            }
        };
        let result: redis::RedisResult<String> = redis::cmd("SET")
            .arg(self.meta_key(job_id))
            .arg(payload)
            .query_async(&mut connection)
            .await;
        if let Err(err) = result {
            tracing::debug!(job_id, error = %err, "redis meta write failed");
        }
    }

    pub async fn list_job_ids(&self) -> Option<Vec<String>> {
        let mut connection = self.connect().await?;
        let job_ids: redis::RedisResult<Vec<String>> = redis::cmd("SMEMBERS")
            .arg(self.active_jobs_key())
            .query_async(&mut connection)
            .await;
        let mut job_ids = match job_ids {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(error = %err, "redis active job list failed");
                return None;
            }
        };
        job_ids.sort();
        Some(job_ids)
    }

    pub async fn publish_event(&self, job_id: &str, state: &JobState, message: &Value) {
        let Some(mut connection) = self.connect().await else {
            return;
        };
        let payload = serde_json::json!({
            "kind": "event",
            "job_id": job_id,
            "origin_id": self.instance_id.clone(),
            "message": message,
            "state": state,
        });
        let result: redis::RedisResult<i64> = redis::cmd("PUBLISH")
            .arg(self.events_channel())
            .arg(payload.to_string())
            .query_async(&mut connection)
            .await;
        if let Err(err) = result {
            tracing::debug!(job_id, error = %err, "redis event publish failed");
        }
    }

    pub async fn publish_close_clients(&self, job_id: &str) {
        let Some(mut connection) = self.connect().await else {
            return;
        };
        let payload = serde_json::json!({
            "kind": "close_clients",
            "job_id": job_id,
            "origin_id": self.instance_id.clone(),
        });
        let result: redis::RedisResult<i64> = redis::cmd("PUBLISH")
            .arg(self.events_channel())
            .arg(payload.to_string())
            .query_async(&mut connection)
            .await;
        if let Err(err) = result {
            tracing::debug!(job_id, error = %err, "redis close publish failed");
        }
    }

    pub async fn publish_delete(&self, job_id: &str) {
        let Some(mut connection) = self.connect().await else {
            return;
        };
        let payload = serde_json::json!({
            "kind": "delete",
            "job_id": job_id,
            "origin_id": self.instance_id.clone(),
        });
        let result: redis::RedisResult<i64> = redis::cmd("PUBLISH")
            .arg(self.events_channel())
            .arg(payload.to_string())
            .query_async(&mut connection)
            .await;
        if let Err(err) = result {
            tracing::debug!(job_id, error = %err, "redis delete publish failed");
        }
    }

    pub async fn read_blurhash_cache(&self, cache_key: &str) -> Option<Value> {
        let mut connection = self.connect().await?;
        let raw_value: redis::RedisResult<Option<String>> = redis::cmd("GET")
            .arg(self.blurhash_key(cache_key))
            .query_async(&mut connection)
            .await;
        let raw_value = match raw_value {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(cache_key, error = %err, "redis blurhash read failed");
                return None;
            }
        };
        let raw_value = raw_value?;
        let loaded = match serde_json::from_str::<Value>(&raw_value) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(cache_key, error = %err, "failed to decode redis blurhash cache entry");
                return None;
            }
        };
        if loaded.is_object() {
            Some(loaded)
        } else {
            None
        }
    }

    pub async fn write_blurhash_cache(&self, cache_key: &str, data: &Value, ttl_seconds: u64) {
        let Some(mut connection) = self.connect().await else {
            return;
        };
        let payload = match serde_json::to_string(data) {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(cache_key, error = %err, "failed to encode redis blurhash cache entry");
                return;
            }
        };
        let key = self.blurhash_key(cache_key);
        let result: redis::RedisResult<String> = if ttl_seconds > 0 {
            redis::cmd("SET")
                .arg(key)
                .arg(payload)
                .arg("EX")
                .arg(ttl_seconds)
                .query_async(&mut connection)
                .await
        } else {
            redis::cmd("SET")
                .arg(key)
                .arg(payload)
                .query_async(&mut connection)
                .await
        };
        if let Err(err) = result {
            tracing::debug!(cache_key, error = %err, "redis blurhash write failed");
        }
    }

    pub async fn listen(&self, callback: RedisEventCallback) {
        loop {
            let pubsub = match self.client.get_async_pubsub().await {
                Ok(pubsub) => pubsub,
                Err(err) => {
                    tracing::warn!(error = %err, "redis pubsub connection failed");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            let mut pubsub = pubsub;
            if let Err(err) = pubsub.subscribe(self.events_channel()).await {
                tracing::warn!(error = %err, "redis pubsub subscribe failed");
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let mut stream = pubsub.into_on_message();
            while let Some(message) = stream.next().await {
                let data: String = match message.get_payload() {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(error = %err, "redis pubsub payload decode failed");
                        continue;
                    }
                };
                let payload: Value = match serde_json::from_str(&data) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to decode redis pubsub payload");
                        continue;
                    }
                };
                callback(payload).await;
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}

fn generate_instance_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id() as u128;
    let seq = NEXT_INSTANCE_ID.fetch_add(1, Ordering::Relaxed) as u128;
    format!("{now:x}{pid:x}{seq:x}")
}
