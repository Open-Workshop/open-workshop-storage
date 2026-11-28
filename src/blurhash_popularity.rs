use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::redis_store::{BlurhashPopularityItem, RedisBackend};

const QUEUE_CAPACITY: usize = 1024;
const MAX_PENDING_KEYS: usize = 1024;
const BATCH_SIZE: usize = 128;
const MAX_TOUCHES_PER_KEY: u32 = 16;

#[derive(Default)]
pub struct PopularityStats {
    pub queued: AtomicU64,
    pub dropped: AtomicU64,
    pub forwarded: AtomicU64,
    pub restored: AtomicU64,
    pub renewed: AtomicU64,
    pub failed: AtomicU64,
}

#[derive(Clone)]
pub struct PopularityReporter {
    sender: mpsc::Sender<(String, (String, u32, u32))>,
    pub stats: Arc<PopularityStats>,
}

impl PopularityReporter {
    pub fn start(redis: Arc<RedisBackend>, ttl: u64) -> Self {
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let stats = Arc::new(PopularityStats::default());
        tokio::spawn(run(redis, ttl, receiver, stats.clone()));
        Self { sender, stats }
    }

    pub fn record(&self, key: &str, value: &(String, u32, u32)) {
        if self.sender.try_send((key.to_owned(), value.clone())).is_ok() {
            self.stats.queued.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn run(
    redis: Arc<RedisBackend>,
    ttl: u64,
    mut receiver: mpsc::Receiver<(String, (String, u32, u32))>,
    stats: Arc<PopularityStats>,
) {
    let mut pending: HashMap<String, BlurhashPopularityItem> = HashMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            event = receiver.recv() => match event {
                Some((key, value)) => {
                    if let Some(item) = pending.get_mut(&key) {
                        if item.hits < MAX_TOUCHES_PER_KEY { item.hits += 1; }
                        else { stats.dropped.fetch_add(1, Ordering::Relaxed); }
                    } else if pending.len() < MAX_PENDING_KEYS {
                        pending.insert(key.clone(), BlurhashPopularityItem { key, value, hits: 1 });
                    } else {
                        stats.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => break,
            },
            _ = interval.tick() => {
                let items: Vec<_> = pending.drain().map(|(_, value)| value).collect();
                for chunk in items.chunks(BATCH_SIZE) {
                    match tokio::time::timeout(Duration::from_secs(3), redis.update_blurhash_popularity(chunk, ttl)).await {
                        Ok(Ok(result)) => {
                            stats.forwarded.fetch_add(result.forwarded, Ordering::Relaxed);
                            stats.restored.fetch_add(result.restored, Ordering::Relaxed);
                            stats.renewed.fetch_add(result.renewed, Ordering::Relaxed);
                        }
                        _ => { stats.failed.fetch_add(chunk.len() as u64, Ordering::Relaxed); }
                    }
                }
            }
        }
    }
}
