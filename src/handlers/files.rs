use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{Extension, Multipart, Path as AxumPath, Query};
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::future::join_all;
use opentelemetry::trace::{FutureExt, TraceContextExt, Tracer};
use opentelemetry::{global, Context, KeyValue};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio_util::io::ReaderStream;

use crate::access_client;
use crate::archive::{archive_entries_unpacked_bytes, probe_archive};
use crate::auth;
use crate::blurhash_service::get_or_compute_with_stats;
use crate::fs_utils::{
    build_download_filename, is_allowed_type, is_allowed_upload_type, normalize_file_kind,
    safe_path,
};
use crate::images::image_bytes_to_webp;
use crate::runtime::AppState;

#[derive(Debug, Clone, Deserialize)]
pub struct BlurhashBatchRequest {
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlurhashItemRead {
    pub path: String,
    pub blurhash: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlurhashBatchResponse {
    pub items: Vec<BlurhashItemRead>,
}

#[derive(Debug, Deserialize)]
pub struct DownloadPath {
    storage_type: String,
    path: String,
}

#[derive(Debug, Deserialize)]
pub struct DownloadQuery {
    filename: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteRequest {
    #[serde(rename = "type")]
    storage_type: String,
    path: String,
    token: Option<String>,
}

fn error_response(status: StatusCode, content: impl Into<String>) -> Response {
    (status, content.into()).into_response()
}

fn normalize_blurhash_target(raw_path: &str) -> Option<(String, String)> {
    let mut value = raw_path.trim().to_string();
    if value.is_empty() {
        return None;
    }

    if let Ok(uri) = value.parse::<Uri>() {
        if uri.scheme_str().is_some() && uri.authority().is_some() {
            value = uri.path().to_string();
        }
    }

    let value = value.trim_start_matches('/').to_string();
    if !value.starts_with("download/") {
        return None;
    }
    let mut parts = value.splitn(3, '/');
    let _ = parts.next();
    let storage_type = parts.next()?.to_string();
    let storage_path = parts.next()?.to_string();
    if storage_type.is_empty() || storage_path.is_empty() {
        return None;
    }
    Some((storage_type, storage_path))
}

async fn prepare_blurhash_item(
    state: Arc<AppState>,
    raw_path: String,
) -> (BlurhashItemRead, Option<(PathBuf, u128, u64)>) {
    let item = BlurhashItemRead {
        path: raw_path.clone(),
        blurhash: None,
        width: None,
        height: None,
    };
    let fallback = item.clone();
    let Ok(permit) = state.blurhash_metadata_limit.clone().acquire_owned().await else {
        return (item, None);
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        prepare_blurhash_item_sync(&state, raw_path, item)
    }).await.unwrap_or((fallback, None))
}

fn prepare_blurhash_item_sync(
    state: &AppState,
    raw_path: String,
    item: BlurhashItemRead,
) -> (BlurhashItemRead, Option<(PathBuf, u128, u64)>) {
    let Some((storage_type, storage_path)) = normalize_blurhash_target(&raw_path) else {
        return (item, None);
    };
    if !is_allowed_type(&storage_type) {
        return (item, None);
    }

    let base_dir = state.main_dir.join(&storage_type);
    let Ok(real_path) = safe_path(&base_dir, &storage_path) else {
        return (item, None);
    };
    let Ok(stat_result) = std::fs::metadata(&real_path) else {
        return (item, None);
    };
    if !stat_result.is_file() {
        return (item, None);
    }
    let key = (
        real_path,
        stat_result
            .modified()
            .ok()
            .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|value| value.as_nanos())
            .unwrap_or(0),
        stat_result.len(),
    );
    (item, Some(key))
}

pub async fn blurhashes(
    Extension(state): Extension<Arc<AppState>>,
    Json(payload): Json<BlurhashBatchRequest>,
) -> impl IntoResponse {
    let root = Context::current();
    let requested = payload.paths.len();
    let tracer = global::tracer("open-workshop-storage.blurhash");
    let prepare_span = tracer.start_with_context("Blurhash: inspect files", &root);
    let prepare_context = root.with_span(prepare_span);
    let prepare_started = Instant::now();
    let prepared = join_all(
        payload
            .paths
            .into_iter()
            .map(|path| prepare_blurhash_item(state.clone(), path)),
    )
    .with_context(prepare_context.clone()).await;
    prepare_context.span().set_attribute(KeyValue::new("ow.blurhash.requested", requested as i64));
    prepare_context.span().end();
    let prepare_us = prepare_started.elapsed().as_micros() as i64;
    let mut seen = HashSet::new();
    let mut unique_keys = Vec::new();
    for (_, key) in &prepared {
        if let Some(key) = key.clone() {
            if seen.insert(key.clone()) {
                unique_keys.push(key);
            }
        }
    }

    let resolve_span = tracer.start_with_context("Blurhash: cache and generate", &root);
    let resolve_context = root.with_span(resolve_span);
    let computed = join_all(
        unique_keys
            .iter()
            .cloned()
            .map(|key| get_or_compute_with_stats(state.clone(), key, 4, 3, 64)),
    )
    .with_context(resolve_context.clone()).await;

    let local_hits = computed.iter().filter(|r| r.source == "local").count();
    let redis_hits = computed.iter().filter(|r| r.source == "redis").count();
    let generated = computed.iter().filter(|r| r.source == "computed").count();
    let failed = computed.iter().filter(|r| r.source == "failed").count();
    let span = resolve_context.span();
    span.set_attribute(KeyValue::new(
        "ow.blurhash.cache_policy",
        if state.config.blurhash_cache_size == 0 { "disabled" } else { "moka_tinylfu" },
    ));
    for (name, value) in [
        ("ow.blurhash.requested", requested as i64),
        ("ow.blurhash.unique", unique_keys.len() as i64),
        ("ow.blurhash.invalid", prepared.iter().filter(|(_, key)| key.is_none()).count() as i64),
        ("ow.blurhash.local_hits", local_hits as i64),
        ("ow.blurhash.redis_hits", redis_hits as i64),
        ("ow.blurhash.redis_misses", computed.iter().filter(|r| r.redis_status == "miss").count() as i64),
        ("ow.blurhash.redis_errors", computed.iter().filter(|r| r.redis_status == "error").count() as i64),
        ("ow.blurhash.redis_disabled", computed.iter().filter(|r| r.redis_status == "disabled").count() as i64),
        ("ow.blurhash.generated", generated as i64),
        ("ow.blurhash.failed", failed as i64),
        ("ow.blurhash.local_entries_estimate", state.local_blurhash_cache_entries() as i64),
        ("ow.blurhash.local_capacity", state.config.blurhash_cache_size as i64),
        ("ow.blurhash.prepare_us", prepare_us),
        ("ow.blurhash.local_lookup_us_sum", computed.iter().map(|r| r.local_us).sum::<u64>() as i64),
        ("ow.blurhash.redis_lookup_us_sum", computed.iter().map(|r| r.redis_us).sum::<u64>() as i64),
        ("ow.blurhash.read_us_sum", computed.iter().map(|r| r.read_us).sum::<u64>() as i64),
        ("ow.blurhash.read_us_max", computed.iter().map(|r| r.read_us).max().unwrap_or(0) as i64),
        ("ow.blurhash.encode_us_sum", computed.iter().map(|r| r.encode_us).sum::<u64>() as i64),
        ("ow.blurhash.encode_us_max", computed.iter().map(|r| r.encode_us).max().unwrap_or(0) as i64),
        ("ow.blurhash.compute_us_max", computed.iter().map(|r| r.compute_us).max().unwrap_or(0) as i64),
        ("ow.blurhash.compute_queue_us_max", computed.iter().map(|r| r.compute_queue_us).max().unwrap_or(0) as i64),
    ] {
        span.set_attribute(KeyValue::new(name, value));
    }
    span.end();

    let blurhash_results: HashMap<_, _> =
        unique_keys.into_iter().zip(computed.into_iter().map(|outcome| outcome.value)).collect();

    let mut items = Vec::new();
    for (mut item, key) in prepared {
        if let Some(key) = key {
            if let Some(Some((blurhash, width, height))) = blurhash_results.get(&key).cloned() {
                item.blurhash = Some(blurhash);
                item.width = Some(width);
                item.height = Some(height);
            }
        }
        items.push(item);
    }

    Json(BlurhashBatchResponse { items })
}

async fn open_file_response(
    real_path: PathBuf,
    download_name: String,
    head_only: bool,
    unpacked_bytes: Option<u64>,
) -> Result<Response, Response> {
    let file_size = fs::metadata(&real_path)
        .await
        .map_err(|_| error_response(StatusCode::NOT_FOUND, "File not found"))?
        .len();
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(
            CONTENT_DISPOSITION,
            format!(
                "attachment; filename=\"{}\"",
                download_name.replace('"', "")
            ),
        );
    builder = builder.header(CONTENT_LENGTH, file_size.to_string());
    if let Some(unpacked_bytes) = unpacked_bytes {
        builder = builder.header("X-Unpacked-Bytes", unpacked_bytes.to_string());
    }
    if head_only {
        return builder
            .body(Body::empty())
            .map_err(|err| error_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()));
    }
    let file = fs::File::open(&real_path)
        .await
        .map_err(|_| error_response(StatusCode::NOT_FOUND, "File not found"))?;
    let body = Body::from_stream(ReaderStream::new(file));
    builder
        .body(body)
        .map_err(|err| error_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

pub async fn download(
    Extension(state): Extension<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
    AxumPath(path): AxumPath<DownloadPath>,
    Query(query): Query<DownloadQuery>,
) -> impl IntoResponse {
    if !is_allowed_type(&path.storage_type) {
        return error_response(StatusCode::BAD_REQUEST, "Invalid type");
    }
    let base_dir = state.main_dir.join(&path.storage_type);
    let Ok(real_path) = safe_path(&base_dir, &path.path) else {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    };
    if !real_path.is_file() {
        return error_response(StatusCode::NOT_FOUND, "File not found");
    }

    let download_name = build_download_filename(query.filename.as_deref(), &real_path)
        .or_else(|| {
            real_path
                .file_name()
                .map(|value| value.to_string_lossy().to_string())
        })
        .unwrap_or_else(|| "download".to_string());

    if path.storage_type == "archive" && path.path.starts_with("mods/") {
        let Some(mod_id) = path
            .path
            .split('/')
            .nth(1)
            .and_then(|value| value.parse::<u64>().ok())
        else {
            return error_response(StatusCode::NOT_FOUND, "File not found");
        };
        match access_client::resolve_mod_download_access(
            &headers,
            mod_id,
            &state.config.access_service_url,
            state.config.access_service_timeout_seconds,
        )
        .await
        {
            Ok(result) if result.allowed => {}
            Ok(result) => return error_response(StatusCode::FORBIDDEN, result.reason),
            Err(err) => {
                let status = err.status_code.unwrap_or(503);
                return error_response(
                    StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
                    err.message,
                );
            }
        }
    }

    let mut unpacked_bytes = None;
    if method == Method::HEAD && path.storage_type == "archive" && path.path.starts_with("mods/") {
        let probe_timeout = state
            .config
            .seven_zip_timeout_seconds
            .map(std::time::Duration::from_secs_f64);
        if let Ok((_, is_encrypted, archive_entries)) =
            probe_archive(&real_path, probe_timeout).await
        {
            if !is_encrypted {
                unpacked_bytes = archive_entries_unpacked_bytes(archive_entries.as_deref());
            }
        }
    }

    match open_file_response(
        real_path,
        download_name,
        method == Method::HEAD,
        unpacked_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(response) => response,
    }
}

#[derive(Default)]
struct UploadParts {
    token: Option<String>,
    storage_type: Option<String>,
    storage_path: Option<String>,
    file_kind: Option<String>,
    file_bytes: Option<Vec<u8>>,
    file_name: Option<String>,
}

async fn read_multipart_parts(mut multipart: Multipart) -> Result<UploadParts, Response> {
    let mut parts = UploadParts::default();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| error_response(StatusCode::BAD_REQUEST, "Invalid multipart"))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "file" => {
                parts.file_name = field.file_name().map(|value| value.to_string());
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|_| error_response(StatusCode::BAD_REQUEST, "Invalid file"))?;
                parts.file_bytes = Some(bytes.to_vec());
            }
            "type" => {
                parts.storage_type = Some(
                    field
                        .text()
                        .await
                        .map_err(|_| error_response(StatusCode::BAD_REQUEST, "Invalid type"))?,
                );
            }
            "path" => {
                parts.storage_path = Some(
                    field
                        .text()
                        .await
                        .map_err(|_| error_response(StatusCode::BAD_REQUEST, "Invalid path"))?,
                );
            }
            "file_kind" => {
                parts.file_kind =
                    Some(field.text().await.map_err(|_| {
                        error_response(StatusCode::BAD_REQUEST, "Invalid file kind")
                    })?);
            }
            "token" => {
                parts.token = Some(
                    field
                        .text()
                        .await
                        .map_err(|_| error_response(StatusCode::BAD_REQUEST, "Invalid token"))?,
                );
            }
            _ => {
                let _ = field.bytes().await;
            }
        }
    }
    Ok(parts)
}

pub async fn upload(
    Extension(state): Extension<Arc<AppState>>,
    multipart: Multipart,
) -> impl IntoResponse {
    let parts = match read_multipart_parts(multipart).await {
        Ok(parts) => parts,
        Err(response) => return response,
    };

    let Some(token) = parts.token.as_deref() else {
        return error_response(StatusCode::UNAUTHORIZED, "Token not found");
    };
    if !auth::check_token(&state.config, "upload_file", token) {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    }

    let storage_type = parts.storage_type.unwrap_or_default();
    if !is_allowed_upload_type(&storage_type) {
        return error_response(StatusCode::BAD_REQUEST, "Invalid type");
    }
    let normalized_file_kind = normalize_file_kind(parts.file_kind.unwrap_or_default(), "");
    if normalized_file_kind.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "Invalid file kind");
    }
    if storage_type == "avatar" && normalized_file_kind != "img" {
        return error_response(StatusCode::BAD_REQUEST, "Avatar requires image file kind");
    }

    let Some(storage_path) = parts.storage_path else {
        return error_response(StatusCode::BAD_REQUEST, "Invalid path");
    };
    let base_dir = state.main_dir.join(&storage_type);
    let Ok(real_path) = safe_path(&base_dir, &storage_path) else {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    };
    if let Some(parent) = real_path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    let Some(file_bytes) = parts.file_bytes else {
        return error_response(StatusCode::BAD_REQUEST, "Missing file");
    };

    let write_bytes = if normalized_file_kind == "img" {
        if !storage_path.to_lowercase().ends_with(".webp") {
            return error_response(
                StatusCode::BAD_REQUEST,
                "Image storage path must end with .webp",
            );
        }
        match image_bytes_to_webp(&file_bytes, 80) {
            Ok(bytes) => bytes,
            Err(_) => return error_response(StatusCode::BAD_REQUEST, "Image expected"),
        }
    } else {
        file_bytes
    };

    if let Err(err) = fs::write(&real_path, write_bytes).await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }

    (StatusCode::CREATED, storage_path).into_response()
}

pub async fn delete(
    Extension(state): Extension<Arc<AppState>>,
    axum::extract::Form(request): axum::extract::Form<DeleteRequest>,
) -> impl IntoResponse {
    let Some(token) = request.token.as_deref() else {
        return error_response(StatusCode::UNAUTHORIZED, "Token not found");
    };
    if !auth::check_token(&state.config, "delete_file", token) {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    }
    if !is_allowed_type(&request.storage_type) {
        return error_response(StatusCode::BAD_REQUEST, "Invalid type");
    }

    let base_dir = state.main_dir.join(&request.storage_type);
    let Ok(real_path) = safe_path(&base_dir, &request.path) else {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    };
    if !real_path.is_file() {
        return error_response(StatusCode::NOT_FOUND, "File not found");
    }
    if let Err(err) = fs::remove_file(&real_path).await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }

    let mut folder_path = real_path.parent().map(|value| value.to_path_buf());
    let root_dir = base_dir.canonicalize().unwrap_or(base_dir);
    while let Some(current) = folder_path {
        if current == root_dir {
            break;
        }
        match fs::read_dir(&current).await {
            Ok(mut entries) => {
                if entries.next_entry().await.ok().flatten().is_none() {
                    let _ = fs::remove_dir(&current).await;
                    folder_path = current.parent().map(|value| value.to_path_buf());
                    continue;
                }
            }
            Err(_) => {}
        }
        break;
    }

    (StatusCode::OK, "File deleted").into_response()
}
