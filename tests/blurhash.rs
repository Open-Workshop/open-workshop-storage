mod common;

use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use axum::body::Body;
use axum::http::{Method, StatusCode};
use open_workshop_storage::blurhash_cache::build_blurhash_cache_key;
use open_workshop_storage::blurhash_service::{get_or_compute_blurhash_for_key, get_or_compute_with_stats};
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
async fn blurhash_batch_preserves_order_duplicates_and_missing_paths() {
    let dir = temp_dir("blurhash-batch-order");
    let state = test_state(dir.path());
    write_png(&state.main_dir.join("resource/mods/123/logo.png"), 6, 4, [255, 64, 0]);
    let app = build_distributor_router(state);
    let image = "/download/resource/mods/123/logo.png";
    let missing = "/download/resource/mods/123/missing.png";
    let invalid = "/download/resource/../../outside.png";
    let paths = vec![image, missing, image, invalid];
    let response = call_with_headers(
        &app, Method::POST, "/blurhashes", &[("content-type", "application/json")],
        Body::from(serde_json::json!({"paths": paths}).to_string()),
    ).await;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = serde_json::from_str(&response_text(response).await).unwrap();
    let items = payload["items"].as_array().unwrap();
    assert_eq!(items.iter().map(|item| item["path"].as_str().unwrap()).collect::<Vec<_>>(), paths);
    assert!(items[0]["blurhash"].is_string());
    assert_eq!(items[0]["blurhash"], items[2]["blurhash"]);
    assert!(items[1]["blurhash"].is_null());
    assert!(items[3]["blurhash"].is_null());
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

#[tokio::test]
async fn blurhash_reports_computation_then_local_hit() {
    let dir = temp_dir("blurhash-stats");
    let state = test_state(dir.path());
    let image_path = state.main_dir.join("resource/mods/456/logo.png");
    write_png(&image_path, 6, 4, [30, 64, 120]);
    let metadata = std::fs::metadata(&image_path).unwrap();
    let mtime = metadata.modified().unwrap().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let key = (image_path, mtime, metadata.len());
    let first = get_or_compute_with_stats(state.clone(), key.clone(), 4, 3, 64).await;
    let second = get_or_compute_with_stats(state, key, 4, 3, 64).await;
    assert_eq!(first.source, "computed");
    assert_eq!(first.redis_status, "disabled");
    assert_eq!(second.source, "local");
    assert_eq!(second.redis_status, "not_checked");
    assert_eq!(first.value, second.value);
    assert_eq!(second.redis_us, 0);
}

#[tokio::test]
async fn uncached_computation_waits_for_shared_capacity() {
    let dir = temp_dir("blurhash-compute-limit");
    let state = test_state(dir.path());
    let image_path = state.main_dir.join("resource/mods/789/logo.png");
    write_png(&image_path, 6, 4, [80, 90, 100]);
    let metadata = std::fs::metadata(&image_path).unwrap();
    let mtime = metadata.modified().unwrap().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let key = (image_path, mtime, metadata.len());
    let permits = state.blurhash_compute_limit.clone().acquire_many_owned(4).await.unwrap();
    let worker_state = state.clone();
    let task = tokio::spawn(async move {
        get_or_compute_with_stats(worker_state, key, 4, 3, 64).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!task.is_finished(), "calculation must wait while capacity is exhausted");
    drop(permits);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), task).await.unwrap().unwrap();
    assert_eq!(outcome.source, "computed");
    assert!(outcome.compute_queue_us >= 10_000);
    assert_eq!(state.blurhash_compute_limit.available_permits(), 4);
}
