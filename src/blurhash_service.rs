use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use tokio::task;

use crate::blurhash_cache::{
    build_blurhash_cache_key, decode_blurhash_cache_value, encode_blurhash_cache_value,
};
use crate::runtime::AppState;

pub async fn get_or_compute_blurhash_for_key(
    state: Arc<AppState>,
    key: (PathBuf, u128, u64),
    components_x: u32,
    components_y: u32,
    max_dimension: u32,
) -> Option<(String, u32, u32)> {
    let (real_path, mtime_ns, size) = key;
    let cache_key = build_blurhash_cache_key(
        real_path.to_string_lossy().as_ref(),
        mtime_ns,
        size,
        components_x,
        components_y,
        max_dimension,
    );

    if let Some(cached) = state.read_blurhash_cache(&cache_key).await {
        if let Some(map) = cached.as_object() {
            if let Some(result) = decode_blurhash_cache_value(map) {
                return Some(result);
            }
        }
    }

    if let Some(cached) = state.read_local_blurhash_cache(&cache_key).await {
        return Some(cached);
    }

    let path = real_path.clone();
    let result = task::spawn_blocking(move || {
        crate::images::image_file_to_blurhash(path, components_x, components_y, max_dimension).ok()
    })
    .await
    .ok()
    .flatten()?;

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
    Some(result)
}
