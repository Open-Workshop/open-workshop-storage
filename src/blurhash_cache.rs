use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub fn build_blurhash_cache_key(
    real_path: &str,
    mtime_ns: u128,
    size: u64,
    components_x: u32,
    components_y: u32,
    max_dimension: u32,
) -> String {
    let payload = serde_json::json!({
        "path": real_path,
        "mtime_ns": mtime_ns,
        "size": size,
        "components_x": components_x,
        "components_y": components_y,
        "max_dimension": max_dimension,
    });
    let raw = serde_json::to_string(&payload).unwrap_or_default();
    let digest = Sha256::digest(raw.as_bytes());
    format!("{digest:x}")
}

pub fn encode_blurhash_cache_value(blurhash: &str, width: u32, height: u32) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert("blurhash".to_string(), Value::String(blurhash.to_string()));
    map.insert("width".to_string(), Value::from(width));
    map.insert("height".to_string(), Value::from(height));
    map
}

pub fn decode_blurhash_cache_value(data: &Map<String, Value>) -> Option<(String, u32, u32)> {
    let blurhash = data.get("blurhash")?.as_str()?.to_string();
    if blurhash.is_empty() {
        return None;
    }
    let width = data.get("width")?.as_u64()? as u32;
    let height = data.get("height")?.as_u64()? as u32;
    Some((blurhash, width, height))
}
