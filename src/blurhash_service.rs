use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use tokio::task;

use crate::blurhash_cache::{
    build_blurhash_cache_key, decode_blurhash_cache_value, encode_blurhash_cache_value,
};
use crate::runtime::AppState;
use crate::redis_store::BlurhashCacheLookup;

pub async fn get_or_compute_blurhash_for_key(
    state: Arc<AppState>,
    key: (PathBuf, u128, u64),
    components_x: u32,
    components_y: u32,
    max_dimension: u32,
) -> Option<(String, u32, u32)> {
    get_or_compute_with_stats(state, key, components_x, components_y, max_dimension)
        .await
        .value
}

#[derive(Default)]
pub struct BlurhashOutcome {
    pub value: Option<(String, u32, u32)>,
    pub source: &'static str,
    pub redis_status: &'static str,
    pub local_us: u64,
    pub redis_us: u64,
    pub read_us: u64,
    pub encode_us: u64,
    pub compute_us: u64,
    pub compute_queue_us: u64,
}

pub async fn get_or_compute_with_stats(
    state: Arc<AppState>,
    key: (PathBuf, u128, u64),
    components_x: u32,
    components_y: u32,
    max_dimension: u32,
) -> BlurhashOutcome {
    let (real_path, mtime_ns, size) = key;
    let cache_key = build_blurhash_cache_key(
        real_path.to_string_lossy().as_ref(),
        mtime_ns,
        size,
        components_x,
        components_y,
        max_dimension,
    );

    let mut outcome = BlurhashOutcome { source: "failed", redis_status: "not_checked", ..Default::default() };
    let started = Instant::now();
    let local = state.read_local_blurhash_cache(&cache_key).await;
    outcome.local_us = started.elapsed().as_micros() as u64;
    if let Some(cached) = local {
        outcome.source = "local";
        outcome.value = Some(cached);
        return outcome;
    }

    let started = Instant::now();
    let redis = state.read_blurhash_cache_checked(&cache_key).await;
    outcome.redis_us = started.elapsed().as_micros() as u64;
    match redis {
        BlurhashCacheLookup::Hit(cached) => {
            if let Some(result) = cached.as_object().and_then(decode_blurhash_cache_value) {
                state.write_local_blurhash_cache(&cache_key, result.clone()).await;
                outcome.redis_status = "hit";
                outcome.source = "redis";
                outcome.value = Some(result);
                return outcome;
            }
            outcome.redis_status = "error";
        }
        BlurhashCacheLookup::Miss => outcome.redis_status = "miss",
        BlurhashCacheLookup::Error => outcome.redis_status = "error",
        BlurhashCacheLookup::Disabled => outcome.redis_status = "disabled",
    }

    let path = real_path.clone();
    let queued = Instant::now();
    let Ok(permit) = state.blurhash_compute_limit.clone().acquire_owned().await else {
        return outcome;
    };
    outcome.compute_queue_us = queued.elapsed().as_micros() as u64;
    let started = Instant::now();
    let computed = task::spawn_blocking(move || {
        // Keep the permit until the blocking work actually ends, even when the
        // HTTP request is cancelled while the task is running.
        let _permit = permit;
        let read_started = Instant::now();
        let bytes = std::fs::read(path);
        let read_us = read_started.elapsed().as_micros() as u64;
        let encode_started = Instant::now();
        let result = bytes.ok().and_then(|data| {
            crate::images::image_bytes_to_blurhash(&data, components_x, components_y, max_dimension).ok()
        });
        (result, read_us, encode_started.elapsed().as_micros() as u64)
    }).await;
    outcome.compute_us = started.elapsed().as_micros() as u64;
    let Ok((result, read_us, encode_us)) = computed else { return outcome; };
    outcome.read_us = read_us;
    outcome.encode_us = encode_us;
    let Some(result) = result else { return outcome; };

    let (blurhash, width, height) = result.clone();
    let _ = state
        .write_local_blurhash_cache(&cache_key, (blurhash.clone(), width, height))
        .await;
    let _ = state
        .write_blurhash_cache(
            &cache_key,
            Value::Object(encode_blurhash_cache_value(&blurhash, width, height)),
        )
        .await;
    outcome.source = "computed";
    outcome.value = Some(result);
    outcome
}
