#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::Request;
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt as _;
use image::codecs::webp::WebPEncoder;
use image::{ColorType, RgbaImage};
use open_workshop_storage::auth;
use open_workshop_storage::config::AppConfig;
use open_workshop_storage::runtime::AppState;
use serde_json::{Map, Value};
use tokio::net::TcpListener;
use tower::ServiceExt;

static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(1);

pub const DELETE_TOKEN: &str = "delete-token";
pub const UPLOAD_TOKEN: &str = "upload-token";
pub const MANAGE_TOKEN: &str = "manage-token";
pub const TRANSFER_SECRET: &str = "test-secret-with-safe-length-32+";

pub struct TestDir {
    path: PathBuf,
}

impl TestDir {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub struct SpawnedServer {
    pub base_url: String,
    handle: tokio::task::JoinHandle<()>,
}

pub struct EnvVarGuard {
    key: String,
    value: Option<std::ffi::OsString>,
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.value {
            Some(value) => std::env::set_var(&self.key, value),
            None => std::env::remove_var(&self.key),
        }
    }
}

pub fn temp_dir(prefix: &str) -> TestDir {
    let id = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("open-workshop-storage-{prefix}-{id}-{stamp}"));
    std::fs::create_dir_all(&path).expect("create temp dir");
    TestDir { path }
}

pub fn bcrypt_hash(token: &str) -> String {
    bcrypt::hash(token, 4).expect("hash token")
}

pub fn test_config(root: &Path) -> AppConfig {
    AppConfig {
        main_dir: root.join("storage"),
        manager_url: "http://127.0.0.1:7776".to_string(),
        access_service_url: "http://127.0.0.1:7777".to_string(),
        manager_transfer_callback_url: None,
        transfer_jwt_secret: Some(TRANSFER_SECRET.to_string()),
        redis_url: None,
        redis_prefix: "open-workshop-storage".to_string(),
        transfer_callback_ttl_seconds: 600,
        transfer_max_bytes: None,
        transfer_max_unpacked_bytes: None,
        transfer_upload_concurrency: 1,
        transfer_download_concurrency: 1,
        transfer_repack_concurrency: 1,
        transfer_upload_timeout_seconds: Some(3600.0),
        transfer_download_timeout_seconds: Some(3600.0),
        transfer_callback_timeout_seconds: Some(30.0),
        seven_zip_timeout_seconds: Some(3600.0),
        seven_zip_idle_timeout_seconds: Some(60.0),
        access_service_timeout_seconds: 30,
        blurhash_cache_size: 64,
        blurhash_cache_ttl_seconds: 604800,
        cleanup_interval_seconds: 60,
        job_ttl_seconds: 10800,
        delete_file: Some(bcrypt_hash(DELETE_TOKEN)),
        upload_file: Some(bcrypt_hash(UPLOAD_TOKEN)),
        storage_manage_token: Some(bcrypt_hash(MANAGE_TOKEN)),
    }
}

pub fn test_state(root: &Path) -> Arc<AppState> {
    AppState::new(test_config(root)).expect("build app state")
}

pub fn test_state_with_config(config: AppConfig) -> Arc<AppState> {
    AppState::new(config).expect("build app state")
}

pub fn prepend_path(dir: &Path) -> EnvVarGuard {
    let key = "PATH".to_string();
    let value = std::env::var_os(&key);
    let mut paths = vec![dir.to_path_buf()];
    if let Some(existing) = value.as_ref() {
        paths.extend(std::env::split_paths(existing));
    }
    let new_value = std::env::join_paths(paths).expect("join PATH");
    std::env::set_var(&key, &new_value);
    EnvVarGuard { key, value }
}

pub fn transfer_token(
    config: &AppConfig,
    payload: &Map<String, Value>,
    audience: &str,
    ttl_seconds: u64,
) -> String {
    auth::encode_transfer_jwt(config, payload, audience, ttl_seconds).expect("encode transfer jwt")
}

pub async fn response_bytes(response: axum::response::Response) -> Bytes {
    response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes()
}

pub async fn response_text(response: axum::response::Response) -> String {
    String::from_utf8(response_bytes(response).await.to_vec()).expect("utf8 body")
}

pub async fn call(
    router: &Router,
    method: axum::http::Method,
    uri: &str,
    body: Body,
) -> axum::response::Response {
    call_with_headers(router, method, uri, &[], body).await
}

pub async fn call_with_headers(
    router: &Router,
    method: axum::http::Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let request = builder.body(body).expect("build request");
    router.clone().oneshot(request).await.expect("call router")
}

pub async fn spawn_server(router: Router) -> SpawnedServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr: SocketAddr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    SpawnedServer {
        base_url: format!("http://{addr}"),
        handle,
    }
}

pub fn write_png(path: &Path, width: u32, height: u32, rgb: [u8; 3]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create png parent");
    }
    let mut image = RgbaImage::new(width, height);
    for pixel in image.pixels_mut() {
        *pixel = image::Rgba([rgb[0], rgb[1], rgb[2], 255]);
    }
    let mut bytes = Vec::new();
    let encoder = WebPEncoder::new_lossless(&mut bytes);
    encoder
        .encode(image.as_raw(), width, height, ColorType::Rgba8)
        .expect("encode webp");
    std::fs::write(path, bytes).expect("save image");
}
