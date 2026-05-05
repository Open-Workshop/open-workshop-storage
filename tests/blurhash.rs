mod common;

use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use axum::body::Body;
use axum::http::{Method, StatusCode};
use open_workshop_storage::blurhash_cache::build_blurhash_cache_key;
use open_workshop_storage::blurhash_service::get_or_compute_blurhash_for_key;
use open_workshop_storage::web::build_distributor_router;
use serde_json::Value;

use common::{call_with_headers, response_text, temp_dir, test_state, write_png};

#[tokio::test]
async fn blurhash_endpoint_generates_hash_for_download_url() {
    let dir = temp_dir("blurhash-endpoint");
    let state = test_state(dir.path());
    let image_path = state.main_dir.join("resource/mods/123/logo.png");
    write_png(&image_path, 6, 4, [255, 64, 0]);
    let app = build_distributor_router(state);

    let source_url = "https://storage.openworkshop.miskler.ru/download/resource/mods/123/logo.png";
    let body = serde_json::json!({ "paths": [source_url] });
    let response = call_with_headers(
        &app,
        Method::POST,
        "/blurhashes",
        &[("content-type", "application/json")],
        Body::from(body.to_string()),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_str(&response_text(response).await).expect("parse response json");
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .expect("items array");
    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item.get("path").and_then(Value::as_str), Some(source_url));
    assert_eq!(item.get("width").and_then(Value::as_u64), Some(6));
    assert_eq!(item.get("height").and_then(Value::as_u64), Some(4));
    let blurhash = item
        .get("blurhash")
        .and_then(Value::as_str)
        .expect("blurhash string");
    assert_eq!(blurhash.len(), 28);
}

#[tokio::test]
async fn blurhash_cache_survives_file_deletion() {
    let dir = temp_dir("blurhash-cache");
    let state = test_state(dir.path());
    let image_path = state.main_dir.join("resource/mods/123/logo.png");
    write_png(&image_path, 6, 4, [255, 64, 0]);
    let metadata = std::fs::metadata(&image_path).expect("image metadata");
    let mtime_ns = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let key = (PathBuf::from(&image_path), mtime_ns, metadata.len());

    let first = get_or_compute_blurhash_for_key(state.clone(), key.clone(), 4, 3, 64)
        .await
        .expect("compute blurhash");
    std::fs::remove_file(&image_path).expect("remove image");

    let second = get_or_compute_blurhash_for_key(state.clone(), key, 4, 3, 64)
        .await
        .expect("load cached blurhash");

    assert_eq!(first, second);

    let cache_key = build_blurhash_cache_key(
        image_path.to_string_lossy().as_ref(),
        mtime_ns,
        metadata.len(),
        4,
        3,
        64,
    );
    let cached = state
        .read_local_blurhash_cache(&cache_key)
        .await
        .expect("local cache entry");
    assert_eq!(cached.1, 6);
    assert_eq!(cached.2, 4);
    assert!(state.read_blurhash_cache(&cache_key).await.is_none());
}
