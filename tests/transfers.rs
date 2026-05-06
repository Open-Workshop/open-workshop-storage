mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Extension;
use axum::http::{Method, StatusCode};
use axum::response::Json;
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use futures_util::stream;
use open_workshop_storage::state::{new_job_state, WsClientHandle};
use open_workshop_storage::transfer_jobs::{run_download_job, run_repack_job};
use open_workshop_storage::web::{build_distributor_router, build_loader_router};
use serde_json::{Map, Value};
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::timeout;

use common::{
    call, call_with_headers, response_bytes, response_text, spawn_server, temp_dir, test_config,
    test_state, test_state_with_config, transfer_token, MANAGE_TOKEN,
};

#[derive(Default)]
struct AccessCapture {
    mod_id: Option<u64>,
    cookie: Option<String>,
}

async fn spawn_hanging_download_server() -> common::SpawnedServer {
    let notify = Arc::new(Notify::new());
    let app = Router::new()
        .route(
            "/archive.zip",
            get(|Extension(notify): Extension<Arc<Notify>>| async move {
                notify.notified().await;
                StatusCode::OK
            }),
        )
        .layer(Extension(notify));
    spawn_server(app).await
}

async fn spawn_access_service() -> (common::SpawnedServer, Arc<Mutex<AccessCapture>>) {
    let capture = Arc::new(Mutex::new(AccessCapture::default()));
    let app = Router::new()
        .route(
            "/mod/:mod_id",
            axum::routing::post(
                |axum::extract::Path(mod_id): axum::extract::Path<u64>,
                 headers: axum::http::HeaderMap,
                 Extension(capture): Extension<Arc<Mutex<AccessCapture>>>| async move {
                    let cookie = headers
                        .get(axum::http::header::COOKIE)
                        .and_then(|value| value.to_str().ok())
                        .map(|value| value.to_string());
                    let mut lock = capture.lock().await;
                    lock.mod_id = Some(mod_id);
                    lock.cookie = cookie;
                    Json(serde_json::json!({
                        "download": {
                            "value": true,
                            "reason": "allowed",
                            "reason_code": "public"
                        }
                    }))
                },
            ),
        )
        .layer(Extension(capture.clone()));
    (spawn_server(app).await, capture)
}

async fn spawn_callback_capture_server() -> (common::SpawnedServer, Arc<Mutex<Vec<Value>>>) {
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    async fn capture_callback(
        Extension(callbacks): Extension<Arc<Mutex<Vec<Value>>>>,
        axum::extract::Json(payload): axum::extract::Json<Value>,
    ) -> StatusCode {
        callbacks.lock().await.push(payload);
        StatusCode::OK
    }
    let app = Router::new()
        .route("/callback", axum::routing::post(capture_callback))
        .layer(Extension(callbacks.clone()));
    (spawn_server(app).await, callbacks)
}

fn transfer_start_payload(job_id: &str, download_url: String) -> Map<String, Value> {
    let mut payload = Map::new();
    payload.insert("job_id".to_string(), Value::String(job_id.to_string()));
    payload.insert("download_url".to_string(), Value::String(download_url));
    payload.insert(
        "filename".to_string(),
        Value::String("archive.zip".to_string()),
    );
    payload.insert("pack_format".to_string(), Value::String("zip".to_string()));
    payload.insert("pack_level".to_string(), Value::from(3));
    payload
}

fn transfer_upload_payload(job_id: &str) -> Map<String, Value> {
    let mut payload = Map::new();
    payload.insert("job_id".to_string(), Value::String(job_id.to_string()));
    payload.insert(
        "transfer_kind".to_string(),
        Value::String("archive".to_string()),
    );
    payload.insert("mode".to_string(), Value::String("create".to_string()));
    payload.insert("mod_id".to_string(), Value::from(123));
    payload.insert("pack_format".to_string(), Value::String("zip".to_string()));
    payload.insert("pack_level".to_string(), Value::from(3));
    payload
}

#[tokio::test]
async fn transfer_start_returns_started_and_persists_state() {
    let dir = temp_dir("transfer-start");
    let state = test_state(dir.path());
    let app = build_loader_router(state.clone());
    let server = spawn_hanging_download_server().await;
    let job_id = "a".repeat(32);
    let token = transfer_token(
        state.config.as_ref(),
        &transfer_start_payload(&job_id, format!("{}/archive.zip", server.base_url)),
        "storage",
        60,
    );

    let response = call(
        &app,
        Method::GET,
        &format!("/transfer/start?token={token}"),
        Body::empty(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&response_bytes(response).await).expect("parse json");
    assert_eq!(
        payload.get("job_id").and_then(Value::as_str),
        Some(job_id.as_str())
    );
    assert_eq!(
        payload.get("status").and_then(Value::as_str),
        Some("started")
    );
    let expected_ws_url = format!("/transfer/ws/{job_id}");
    assert_eq!(
        payload.get("ws_url").and_then(Value::as_str),
        Some(expected_ws_url.as_str())
    );

    let job_state = state.read_job_state(&job_id).await.expect("job state");
    assert!(job_state.started);
    assert_eq!(job_state.status, "pending");
    assert_eq!(job_state.stage, "pending");
}

#[tokio::test]
async fn download_queries_access_service_and_forwards_session_cookies() {
    let dir = temp_dir("download-access");
    let (server, capture) = spawn_access_service().await;
    let mut config = test_config(dir.path());
    config.access_service_url = server.base_url.clone();
    let state = test_state_with_config(config);
    let app = build_distributor_router(state.clone());

    let archive_path = state.main_dir.join("archive/mods/123/main.zip");
    if let Some(parent) = archive_path.parent() {
        std::fs::create_dir_all(parent).expect("create archive parent");
    }
    std::fs::write(&archive_path, b"archive-bytes").expect("write archive");

    let response = call_with_headers(
        &app,
        Method::GET,
        "/download/archive/mods/123/main.zip",
        &[(
            "cookie",
            "accessToken=access-token; refreshToken=refresh-token; userID=123",
        )],
        Body::empty(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_bytes(response).await,
        Bytes::from_static(b"archive-bytes")
    );

    let lock = capture.lock().await;
    assert_eq!(lock.mod_id, Some(123));
    assert_eq!(
        lock.cookie.as_deref(),
        Some("accessToken=access-token; refreshToken=refresh-token")
    );
}

#[tokio::test]
async fn download_returns_503_when_access_service_is_unreachable() {
    let dir = temp_dir("download-unreachable");
    let mut config = test_config(dir.path());
    config.access_service_url = "http://127.0.0.1:1".to_string();
    let state = test_state_with_config(config);
    let app = build_distributor_router(state.clone());

    let archive_path = state.main_dir.join("archive/mods/123/main.zip");
    if let Some(parent) = archive_path.parent() {
        std::fs::create_dir_all(parent).expect("create archive parent");
    }
    std::fs::write(&archive_path, b"archive-bytes").expect("write archive");

    let response = call(
        &app,
        Method::GET,
        "/download/archive/mods/123/main.zip",
        Body::empty(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response_text(response).await, "Access service unavailable");
}

#[tokio::test]
async fn transfer_move_releases_job_state_and_closes_clients() {
    let dir = temp_dir("transfer-move");
    let state = test_state(dir.path());
    let app = build_loader_router(state.clone());
    let job_id = "c".repeat(32);
    let packed_rel = format!("temp/{job_id}/packed.zip");
    let packed_abs = state.main_dir.join(&packed_rel);
    if let Some(parent) = packed_abs.parent() {
        std::fs::create_dir_all(parent).expect("create packed parent");
    }
    std::fs::write(&packed_abs, b"packed").expect("write packed file");
    state
        .write_meta(
            &job_id,
            Value::Object(
                [
                    ("job_id".to_string(), Value::String(job_id.clone())),
                    ("packed_path".to_string(), Value::String(packed_rel.clone())),
                ]
                .into_iter()
                .collect(),
            ),
        )
        .await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut job_state = new_job_state();
    job_state.started = true;
    job_state.status = "packed".to_string();
    job_state.stage = "packed".to_string();
    job_state.clients.push(WsClientHandle { id: 1, sender: tx });
    state.save_job_state(&job_id, Some(job_state)).await;

    let form_body = serde_urlencoded::to_string([
        ("job_id", job_id.as_str()),
        ("type", "archive"),
        ("path", "mods/123/main.zip"),
        ("token", MANAGE_TOKEN),
    ])
    .expect("encode form");
    let response = call_with_headers(
        &app,
        Method::POST,
        "/transfer/move",
        &[("content-type", "application/x-www-form-urlencoded")],
        Body::from(form_body),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&response_bytes(response).await).expect("parse json");
    assert_eq!(
        payload.get("job_id").and_then(Value::as_str),
        Some(job_id.as_str())
    );
    assert_eq!(
        payload.get("final_path").and_then(Value::as_str),
        Some("archive/mods/123/main.zip")
    );
    assert_eq!(payload.get("final_bytes").and_then(Value::as_u64), Some(6));

    let close_message = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("client close future")
        .expect("close message");
    assert!(matches!(
        close_message,
        axum::extract::ws::Message::Close(_)
    ));
    assert!(state.read_job_state(&job_id).await.is_none());
    assert!(!state.temp_dir.join(&job_id).exists());
}

#[tokio::test]
async fn transfer_upload_returns_busy_when_limiter_is_full() {
    let dir = temp_dir("transfer-upload-busy");
    let (callback_server, callbacks) = spawn_callback_capture_server().await;
    let mut config = test_config(dir.path());
    config.manager_transfer_callback_url = Some(format!("{}/callback", callback_server.base_url));
    let state = test_state_with_config(config);
    let app = build_loader_router(state.clone());
    let _permit = state.upload_limiter.try_acquire().expect("upload permit");
    let job_id = "f".repeat(32);
    let token = transfer_token(
        state.config.as_ref(),
        &transfer_upload_payload(&job_id),
        "storage",
        60,
    );
    let auth_header = format!("Bearer {token}");

    let response = call_with_headers(
        &app,
        Method::POST,
        &format!("/transfer/upload?token={token}&filename=archive.zip"),
        &[
            ("authorization", auth_header.as_str()),
            ("x-file-name", "archive.zip"),
        ],
        Body::from("archive"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response_text(response).await, "Storage busy");
    let callbacks = callbacks.lock().await;
    let callback = callbacks.last().expect("callback payload");
    assert_eq!(
        callback.get("job_id").and_then(Value::as_str),
        Some(job_id.as_str())
    );
    assert_eq!(
        callback.get("status").and_then(Value::as_str),
        Some("error")
    );
    assert_eq!(callback.get("reason").and_then(Value::as_str), Some("busy"));
    assert_eq!(callback.get("mode").and_then(Value::as_str), Some("create"));
    assert_eq!(
        callback.get("condition").and_then(Value::as_str),
        Some("draft")
    );
}

#[tokio::test]
async fn transfer_upload_times_out_and_cleans_up() {
    let dir = temp_dir("transfer-upload-timeout");
    let (callback_server, callbacks) = spawn_callback_capture_server().await;
    let mut config = test_config(dir.path());
    config.manager_transfer_callback_url = Some(format!("{}/callback", callback_server.base_url));
    config.transfer_upload_timeout_seconds = Some(0.01);
    config.transfer_callback_timeout_seconds = Some(30.0);
    let state = test_state_with_config(config);
    let app = build_loader_router(state.clone());
    let job_id = "9".repeat(32);
    let token = transfer_token(
        state.config.as_ref(),
        &transfer_upload_payload(&job_id),
        "storage",
        60,
    );
    let auth_header = format!("Bearer {token}");
    let body_stream = stream::pending::<Result<Bytes, std::io::Error>>();

    let response = call_with_headers(
        &app,
        Method::POST,
        &format!("/transfer/upload?token={token}&filename=archive.zip"),
        &[
            ("authorization", auth_header.as_str()),
            ("x-file-name", "archive.zip"),
        ],
        Body::from_stream(body_stream),
    )
    .await;

    let status = response.status();
    let text = response_text(response).await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_eq!(text, "Upload timed out");
    assert!(state.read_job_state(&job_id).await.is_none());
    assert!(!state.temp_dir.join(&job_id).exists());
    let callbacks = callbacks.lock().await;
    let callback = callbacks.last().expect("callback payload");
    assert_eq!(
        callback.get("job_id").and_then(Value::as_str),
        Some(job_id.as_str())
    );
    assert_eq!(
        callback.get("status").and_then(Value::as_str),
        Some("error")
    );
    assert_eq!(
        callback.get("reason").and_then(Value::as_str),
        Some("timeout")
    );
    assert_eq!(callback.get("mode").and_then(Value::as_str), Some("create"));
    assert_eq!(
        callback.get("condition").and_then(Value::as_str),
        Some("draft")
    );
}

#[tokio::test]
async fn run_download_job_returns_busy_when_limiter_is_full() {
    let dir = temp_dir("download-busy");
    let (callback_server, callbacks) = spawn_callback_capture_server().await;
    let mut config = test_config(dir.path());
    config.manager_transfer_callback_url = Some(format!("{}/callback", callback_server.base_url));
    let state = test_state_with_config(config);
    let job_id = "8".repeat(32);
    let download_abs = state.temp_dir.join(&job_id).join("source.zip");
    if let Some(parent) = download_abs.parent() {
        std::fs::create_dir_all(parent).expect("create download parent");
        std::fs::write(parent.join("placeholder.txt"), b"keep").expect("write placeholder");
    }
    let _permit = state
        .download_limiter
        .try_acquire()
        .expect("download permit");

    run_download_job(
        state.clone(),
        &job_id,
        "http://127.0.0.1:1/archive.zip",
        &download_abs,
        None,
        Map::new(),
    )
    .await;

    assert!(state.read_job_state(&job_id).await.is_none());
    assert!(!state.temp_dir.join(&job_id).exists());
    let callbacks = callbacks.lock().await;
    let callback = callbacks.last().expect("callback payload");
    assert_eq!(
        callback.get("job_id").and_then(Value::as_str),
        Some(job_id.as_str())
    );
    assert_eq!(
        callback.get("status").and_then(Value::as_str),
        Some("error")
    );
    assert_eq!(callback.get("reason").and_then(Value::as_str), Some("busy"));
}

#[tokio::test]
async fn run_repack_job_returns_busy_when_limiter_is_full() {
    let dir = temp_dir("repack-busy");
    let state = test_state(dir.path());
    let job_id = "7".repeat(32);
    let download_abs = state.temp_dir.join(&job_id).join("source.zip");
    if let Some(parent) = download_abs.parent() {
        std::fs::create_dir_all(parent).expect("create repack parent");
        std::fs::write(&download_abs, b"archive").expect("write source");
    }
    let _permit = state.repack_limiter.try_acquire().expect("repack permit");

    let result = run_repack_job(state.clone(), &job_id, &download_abs, "zip", 3).await;

    assert!(!result.0);
    assert_eq!(result.4.as_deref(), Some("busy"));
    let job_state = state.read_job_state(&job_id).await.expect("job state");
    assert_eq!(job_state.error.as_deref(), Some("busy"));
}
