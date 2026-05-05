use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, Form, Path as AxumPath, Query, Request};
use axum::http::header::{AUTHORIZATION, CONTENT_LENGTH};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Json, Response};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt as _;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::auth;
use crate::fs_utils::{
    is_allowed_type, is_allowed_upload_type, normalize_file_kind, safe_path, sanitize_filename,
};
use crate::images;
use crate::job_meta::update_job_meta;
use crate::runtime::AppState;
use crate::state::{
    new_job_state, next_client_id, reset_job_state, state_event_payload, WsClientHandle,
};
use crate::transfer_jobs;

#[derive(Debug, Deserialize)]
pub struct TransferStartQuery {
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransferStartForm {
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransferRepackForm {
    job_id: String,
    #[serde(rename = "format")]
    pack_format: Option<String>,
    compression_level: Option<i64>,
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransferMoveForm {
    job_id: String,
    #[serde(rename = "type")]
    storage_type: String,
    #[serde(rename = "path")]
    target_path: String,
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransferWsPath {
    job_id: String,
}

#[derive(Debug, Serialize)]
struct TransferStartResponse {
    job_id: String,
    status: String,
    ws_url: String,
}

#[derive(Debug, Serialize)]
struct TransferUploadResponse {
    job_id: String,
    bytes: u64,
    total: Option<u64>,
}

#[derive(Debug, Serialize)]
struct TransferRepackResponse {
    job_id: String,
    packed_bytes: Option<u64>,
    packed_path: Option<String>,
    unpacked_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct TransferMoveResponse {
    job_id: String,
    final_path: String,
    final_bytes: u64,
}

#[derive(Debug, Clone)]
struct UploadSpec {
    job_id: String,
    transfer_kind: String,
    mod_id: Option<Value>,
    storage_type: String,
    file_kind: String,
    pack_format: String,
    pack_level: u32,
    callback_payload: Map<String, Value>,
    max_bytes: Option<u64>,
    safe_name: String,
    upload_rel: String,
    upload_abs: PathBuf,
}

#[derive(Debug, Clone)]
struct UploadStreamResult {
    downloaded: u64,
    final_total: u64,
}

fn error_response(status: StatusCode, content: impl Into<String>) -> Response {
    (status, content.into()).into_response()
}

fn busy_response() -> Response {
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header("Retry-After", "30")
        .body(Body::from("Storage busy"))
        .unwrap_or_else(|err| error_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn parse_query_map(uri: &Uri) -> HashMap<String, String> {
    uri.query()
        .and_then(|query| serde_urlencoded::from_str::<HashMap<String, String>>(query).ok())
        .unwrap_or_default()
}

fn query_value(query: &HashMap<String, String>, key: &str) -> Option<String> {
    query
        .get(key)
        .cloned()
        .filter(|value| !value.trim().is_empty())
}

fn extract_bearer_token(headers: &HeaderMap, query: &HashMap<String, String>) -> Option<String> {
    if let Some(token) = query_value(query, "token") {
        return Some(token);
    }
    let auth = headers.get(AUTHORIZATION)?.to_str().ok()?;
    auth.strip_prefix("Bearer ")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_non_negative_int(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|value| value.parse::<i64>().ok())
        .and_then(|value| if value >= 0 { Some(value as u64) } else { None })
}

fn parse_pack_level(raw_value: Option<i64>, default: i64) -> u32 {
    let level = raw_value.unwrap_or(default).clamp(0, 9);
    level as u32
}

fn read_timeout_seconds(raw_value: Option<f64>, default: f64) -> Option<Duration> {
    let value = raw_value.unwrap_or(default);
    if value <= 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(value))
    }
}

fn manage_token_ok(state: &Arc<AppState>, token: &str) -> bool {
    auth::check_token(&state.config, "storage_manage_token", token)
}

fn build_state_event_with_message(
    event: &str,
    state: &crate::state::JobState,
    message: Option<&str>,
) -> Value {
    let extra = message.map(|message| {
        let mut map = Map::new();
        map.insert("message".to_string(), Value::String(message.to_string()));
        map
    });
    state_event_payload(event, state, extra)
}

async fn build_upload_spec(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    payload: &Map<String, Value>,
) -> Result<UploadSpec, Response> {
    let job_id = payload
        .get("job_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    if !auth::is_safe_job_id(&job_id) {
        return Err(error_response(StatusCode::BAD_REQUEST, "Invalid job id"));
    }

    let transfer_kind = payload
        .get("transfer_kind")
        .and_then(|value| value.as_str())
        .unwrap_or("archive")
        .trim()
        .to_lowercase();
    if !matches!(transfer_kind.as_str(), "archive" | "img") {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "Unsupported transfer kind",
        ));
    }

    let callback_context = match payload.get("callback_context") {
        Some(Value::Object(map)) => Value::Object(map.clone()),
        _ => Value::Object(Map::new()),
    };
    let mut callback_payload = Map::new();
    callback_payload.insert(
        "transfer_kind".to_string(),
        Value::String(transfer_kind.clone()),
    );
    callback_payload.insert(
        "callback_action".to_string(),
        payload
            .get("callback_action")
            .cloned()
            .unwrap_or(Value::Null),
    );
    callback_payload.insert("callback_context".to_string(), callback_context);
    callback_payload.insert(
        "target_path".to_string(),
        payload.get("target_path").cloned().unwrap_or(Value::Null),
    );

    let mut mod_id = None;
    let mut pack_format = "zip".to_string();
    let mut pack_level = 3u32;
    let mut storage_type = String::new();
    let mut file_kind = String::new();

    if transfer_kind == "archive" {
        pack_format = payload
            .get("pack_format")
            .and_then(|value| value.as_str())
            .unwrap_or("zip")
            .to_string();
        if pack_format != "zip" {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "Unsupported format",
            ));
        }
        pack_level = parse_pack_level(
            payload.get("pack_level").and_then(|value| value.as_i64()),
            3,
        );
        mod_id = payload.get("mod_id").cloned();
        callback_payload.insert("mod_id".to_string(), mod_id.clone().unwrap_or(Value::Null));
        callback_payload.insert(
            "pack_format".to_string(),
            Value::String(pack_format.clone()),
        );
        callback_payload.insert("pack_level".to_string(), Value::from(pack_level));
        callback_payload.insert(
            "update_only".to_string(),
            Value::Bool(
                payload
                    .get("update_only")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
                    || payload
                        .get("keep_condition")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false),
            ),
        );
    } else {
        storage_type = payload
            .get("storage_type")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_lowercase();
        if !is_allowed_upload_type(&storage_type) {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "Invalid storage type",
            ));
        }
        file_kind = normalize_file_kind(
            payload
                .get("file_kind")
                .and_then(|value| value.as_str())
                .unwrap_or(""),
            "",
        );
        if file_kind != "img" {
            return Err(error_response(StatusCode::BAD_REQUEST, "Invalid file kind"));
        }
        callback_payload.insert(
            "storage_type".to_string(),
            Value::String(storage_type.clone()),
        );
        callback_payload.insert("file_kind".to_string(), Value::String(file_kind.clone()));
    }

    let default_name = if transfer_kind == "archive" {
        "upload.zip"
    } else {
        "upload.img"
    };
    let safe_name = sanitize_filename(
        query_value(query, "filename")
            .or_else(|| {
                headers
                    .get("X-File-Name")
                    .and_then(|value| value.to_str().ok())
                    .map(|value| value.to_string())
            })
            .as_deref(),
        default_name,
    );
    let upload_rel = format!("temp/{job_id}/{safe_name}");
    let upload_abs = state
        .job_dir(&job_id)
        .unwrap_or_else(|_| state.temp_dir.join(&job_id))
        .join(&safe_name);

    let max_bytes = payload
        .get("max_bytes")
        .and_then(|value| value.as_u64())
        .or(state.config.transfer_max_bytes);

    Ok(UploadSpec {
        job_id,
        transfer_kind,
        mod_id,
        storage_type,
        file_kind,
        pack_format,
        pack_level,
        callback_payload,
        max_bytes,
        safe_name,
        upload_rel,
        upload_abs,
    })
}

async fn initialize_upload_job(
    state: &Arc<AppState>,
    spec: &UploadSpec,
    total: Option<u64>,
) -> Result<(), Response> {
    let mut job_state = state
        .read_job_state(&spec.job_id)
        .await
        .unwrap_or_else(|| new_job_state());
    reset_job_state(&mut job_state, true, "uploading", "uploading", total);
    state.save_job_state(&spec.job_id, Some(job_state)).await;
    let mut meta = Map::new();
    meta.insert("job_id".to_string(), Value::String(spec.job_id.clone()));
    meta.insert(
        "mod_id".to_string(),
        spec.mod_id.clone().unwrap_or(Value::Null),
    );
    meta.insert(
        "transfer_kind".to_string(),
        Value::String(spec.transfer_kind.clone()),
    );
    meta.insert(
        "storage_type".to_string(),
        Value::String(spec.storage_type.clone()),
    );
    meta.insert(
        "file_kind".to_string(),
        Value::String(spec.file_kind.clone()),
    );
    meta.insert(
        "filename".to_string(),
        Value::String(spec.safe_name.clone()),
    );
    meta.insert(
        "download_path".to_string(),
        Value::String(spec.upload_rel.clone()),
    );
    meta.insert(
        "pack_format".to_string(),
        Value::String(spec.pack_format.clone()),
    );
    meta.insert("pack_level".to_string(), Value::from(spec.pack_level));
    meta.insert("status".to_string(), Value::String("uploading".to_string()));
    meta.insert(
        "created_at".to_string(),
        Value::from(now_unix_seconds() as i64),
    );
    state.write_meta(&spec.job_id, Value::Object(meta)).await;
    state.set_stage(&spec.job_id, "uploading").await;
    Ok(())
}

async fn notify_upload_error(state: &Arc<AppState>, spec: &UploadSpec, reason: &str) {
    let mut payload = spec.callback_payload.clone();
    payload.insert("job_id".to_string(), Value::String(spec.job_id.clone()));
    payload.insert("status".to_string(), Value::String("error".to_string()));
    payload.insert("reason".to_string(), Value::String(reason.to_string()));
    state.notify_manager(payload).await;
}

async fn stream_upload_body(
    state: &Arc<AppState>,
    mut body: Body,
    spec: &UploadSpec,
    total: Option<u64>,
) -> Result<UploadStreamResult, Response> {
    let mut downloaded = 0u64;
    let mut last_push = Instant::now() - state.progress_push_interval;
    if let Some(parent) = spec.upload_abs.parent() {
        if let Err(err) = fs::create_dir_all(parent).await {
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            ));
        }
    }
    let mut out_file = match fs::File::create(&spec.upload_abs).await {
        Ok(file) => file,
        Err(err) => {
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            ));
        }
    };

    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(err) => {
                let _ = fs::remove_file(&spec.upload_abs).await;
                let mut meta = Map::new();
                meta.insert("status".to_string(), Value::String("error".to_string()));
                meta.insert("error".to_string(), Value::String(err.to_string()));
                meta.insert(
                    "upload_completed_at".to_string(),
                    Value::from(now_unix_seconds() as i64),
                );
                update_job_meta(
                    state,
                    &spec.job_id,
                    meta,
                    "failed to update meta for job_id=%s",
                )
                .await;
                let mut updates = Map::new();
                updates.insert("status".to_string(), Value::String("error".to_string()));
                updates.insert("error".to_string(), Value::String(err.to_string()));
                state.set_state(&spec.job_id, updates).await;
                let event = build_state_event_with_message(
                    "error",
                    &state.read_job_state(&spec.job_id).await.unwrap_or_default(),
                    Some("upload failed"),
                );
                state.broadcast(&spec.job_id, event).await;
                notify_upload_error(state, spec, "exception").await;
                state.job_error_cleanup(&spec.job_id, "exception").await;
                return Err(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Upload failed",
                ));
            }
        };
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        if chunk.is_empty() {
            continue;
        }
        downloaded += chunk.len() as u64;
        if let Some(limit) = spec.max_bytes {
            if downloaded > limit {
                let mut updates = Map::new();
                updates.insert("status".to_string(), Value::String("error".to_string()));
                updates.insert("error".to_string(), Value::String("size_limit".to_string()));
                state.set_state(&spec.job_id, updates).await;
                let event = build_state_event_with_message(
                    "error",
                    &state.read_job_state(&spec.job_id).await.unwrap_or_default(),
                    Some("file too large"),
                );
                state.broadcast(&spec.job_id, event).await;
                let _ = fs::remove_file(&spec.upload_abs).await;
                notify_upload_error(state, spec, "size_limit").await;
                state.job_error_cleanup(&spec.job_id, "size_limit").await;
                return Err(error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "File too large",
                ));
            }
        }
        if let Err(err) = out_file.write_all(&chunk).await {
            let _ = fs::remove_file(&spec.upload_abs).await;
            let mut meta = Map::new();
            meta.insert("status".to_string(), Value::String("error".to_string()));
            meta.insert("error".to_string(), Value::String(err.to_string()));
            meta.insert(
                "upload_completed_at".to_string(),
                Value::from(now_unix_seconds() as i64),
            );
            update_job_meta(
                state,
                &spec.job_id,
                meta,
                "failed to update meta for job_id=%s",
            )
            .await;
            let mut updates = Map::new();
            updates.insert("status".to_string(), Value::String("error".to_string()));
            updates.insert("error".to_string(), Value::String(err.to_string()));
            state.set_state(&spec.job_id, updates).await;
            let event = build_state_event_with_message(
                "error",
                &state.read_job_state(&spec.job_id).await.unwrap_or_default(),
                Some("upload failed"),
            );
            state.broadcast(&spec.job_id, event).await;
            notify_upload_error(state, spec, "exception").await;
            state.job_error_cleanup(&spec.job_id, "exception").await;
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Upload failed",
            ));
        }

        if last_push.elapsed() >= state.progress_push_interval {
            last_push = Instant::now();
            let mut updates = Map::new();
            updates.insert("bytes".to_string(), Value::from(downloaded));
            state.set_state(&spec.job_id, updates).await;
            let event = state
                .build_state_event(&spec.job_id, "progress", None)
                .await;
            state.broadcast(&spec.job_id, event).await;
        }
    }

    let final_total = total.unwrap_or(downloaded);
    let mut updates = Map::new();
    updates.insert("bytes".to_string(), Value::from(downloaded));
    updates.insert("total".to_string(), Value::from(final_total));
    state.set_state(&spec.job_id, updates).await;
    let event = state
        .build_state_event(&spec.job_id, "progress", None)
        .await;
    state.broadcast(&spec.job_id, event).await;
    let mut updates = Map::new();
    updates.insert("status".to_string(), Value::String("done".to_string()));
    updates.insert("bytes".to_string(), Value::from(downloaded));
    updates.insert("total".to_string(), Value::from(final_total));
    state.set_state(&spec.job_id, updates).await;
    let mut meta = Map::new();
    meta.insert("status".to_string(), Value::String("uploaded".to_string()));
    meta.insert("downloaded_bytes".to_string(), Value::from(downloaded));
    meta.insert("total_bytes".to_string(), Value::from(final_total));
    meta.insert(
        "upload_completed_at".to_string(),
        Value::from(now_unix_seconds() as i64),
    );
    update_job_meta(
        state,
        &spec.job_id,
        meta,
        "failed to update meta for job_id=%s",
    )
    .await;
    Ok(UploadStreamResult {
        downloaded,
        final_total,
    })
}

async fn process_archive_upload(
    state: &Arc<AppState>,
    spec: &UploadSpec,
) -> Result<Option<u64>, Response> {
    state.set_stage(&spec.job_id, "uploaded").await;
    let (repack_ok, _, _, unpacked_bytes, repack_reason) = transfer_jobs::run_repack_job(
        state.clone(),
        &spec.job_id,
        &spec.upload_abs,
        &spec.pack_format,
        spec.pack_level,
    )
    .await;
    if repack_ok {
        return Ok(unpacked_bytes);
    }
    notify_upload_error(
        state,
        spec,
        repack_reason.as_deref().unwrap_or("repack_failed"),
    )
    .await;
    state
        .job_error_cleanup(
            &spec.job_id,
            repack_reason.as_deref().unwrap_or("repack_failed"),
        )
        .await;
    let response = match repack_reason.as_deref() {
        Some("encrypted_zip") => {
            error_response(StatusCode::BAD_REQUEST, "Encrypted zip not allowed")
        }
        Some("unpacked_size_limit") => {
            error_response(StatusCode::PAYLOAD_TOO_LARGE, "Archive too large")
        }
        Some("busy") => busy_response(),
        _ => error_response(StatusCode::INTERNAL_SERVER_ERROR, "Repack failed"),
    };
    Err(response)
}

async fn process_image_upload(state: &Arc<AppState>, spec: &UploadSpec) -> Result<(), Response> {
    state.set_stage(&spec.job_id, "processing").await;
    let packed_rel = PathBuf::from("temp").join(&spec.job_id).join("packed.webp");
    let packed_abs = state.temp_dir.join(&spec.job_id).join("packed.webp");

    let raw_bytes = match fs::read(&spec.upload_abs).await {
        Ok(bytes) => bytes,
        Err(err) => {
            notify_upload_error(state, spec, "image_prepare_failed").await;
            state
                .job_error_cleanup(&spec.job_id, "image_prepare_failed")
                .await;
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            ));
        }
    };

    let webp_bytes = match tokio::task::spawn_blocking(move || {
        images::image_bytes_to_webp(&raw_bytes, 80)
    })
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => {
            let _ = fs::remove_file(&spec.upload_abs).await;
            notify_upload_error(state, spec, "not_image").await;
            state.job_error_cleanup(&spec.job_id, "not_image").await;
            return Err(error_response(StatusCode::BAD_REQUEST, "Image expected"));
        }
        Err(_) => {
            let _ = fs::remove_file(&spec.upload_abs).await;
            let _ = fs::remove_file(&packed_abs).await;
            notify_upload_error(state, spec, "image_prepare_failed").await;
            state
                .job_error_cleanup(&spec.job_id, "image_prepare_failed")
                .await;
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Image preparation failed",
            ));
        }
    };

    if let Err(err) = fs::write(&packed_abs, webp_bytes).await {
        let _ = fs::remove_file(&spec.upload_abs).await;
        let _ = fs::remove_file(&packed_abs).await;
        notify_upload_error(state, spec, "image_prepare_failed").await;
        state
            .job_error_cleanup(&spec.job_id, "image_prepare_failed")
            .await;
        return Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        ));
    }

    let _ = fs::remove_file(&spec.upload_abs).await;
    let packed_bytes = fs::metadata(&packed_abs)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0);
    let mut meta = Map::new();
    meta.insert(
        "packed_path".to_string(),
        Value::String(packed_rel.to_string_lossy().to_string()),
    );
    meta.insert("packed_bytes".to_string(), Value::from(packed_bytes));
    meta.insert("status".to_string(), Value::String("packed".to_string()));
    meta.insert(
        "packed_format".to_string(),
        Value::String("webp".to_string()),
    );
    update_job_meta(
        state,
        &spec.job_id,
        meta,
        "failed to update image meta for job_id=%s",
    )
    .await;
    state.set_stage(&spec.job_id, "packed").await;
    Ok(())
}

async fn execute_transfer_upload(
    state: Arc<AppState>,
    spec: UploadSpec,
    total: Option<u64>,
    body: Body,
) -> Response {
    let response = match execute_transfer_upload_inner(state.clone(), &spec, total, body).await {
        Ok(response) => response,
        Err(response) => response,
    };
    state.close_clients(&spec.job_id).await;
    response
}

async fn execute_transfer_upload_inner(
    state: Arc<AppState>,
    spec: &UploadSpec,
    total: Option<u64>,
    body: Body,
) -> Result<Response, Response> {
    initialize_upload_job(&state, spec, total).await?;
    let start_ts = Instant::now();
    let stream_result = stream_upload_body(&state, body, spec, total).await?;
    let duration = start_ts.elapsed().as_secs_f64();
    eprintln!(
        "transfer upload done job_id={} bytes={} duration={:.2}s",
        spec.job_id, stream_result.downloaded, duration
    );

    let unpacked_bytes = if spec.transfer_kind == "archive" {
        process_archive_upload(&state, spec).await?
    } else {
        process_image_upload(&state, spec).await?;
        None
    };

    let event = state
        .build_state_event(&spec.job_id, "complete", None)
        .await;
    state.broadcast(&spec.job_id, event).await;
    let mut callback_success_payload = spec.callback_payload.clone();
    callback_success_payload.insert("job_id".to_string(), Value::String(spec.job_id.clone()));
    callback_success_payload.insert("status".to_string(), Value::String("success".to_string()));
    callback_success_payload.insert("bytes".to_string(), Value::from(stream_result.downloaded));
    callback_success_payload.insert("total".to_string(), Value::from(stream_result.final_total));
    callback_success_payload.insert(
        "packed_format".to_string(),
        Value::String(
            if spec.transfer_kind == "archive" {
                "zip"
            } else {
                "webp"
            }
            .to_string(),
        ),
    );
    if let Some(unpacked_bytes) = unpacked_bytes {
        callback_success_payload.insert("unpacked_bytes".to_string(), Value::from(unpacked_bytes));
    }
    state.notify_manager(callback_success_payload).await;
    Ok(Json(TransferUploadResponse {
        job_id: spec.job_id.clone(),
        bytes: stream_result.downloaded,
        total: Some(stream_result.final_total),
    })
    .into_response())
}

async fn start_transfer(state: Arc<AppState>, token: Option<String>) -> Response {
    let Some(token) = token else {
        return error_response(StatusCode::UNAUTHORIZED, "Token not found");
    };
    let Some(payload) = auth::decode_transfer_jwt(&state.config, &token, "storage") else {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    };

    let job_id = payload
        .get("job_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    if !auth::is_safe_job_id(&job_id) {
        return error_response(StatusCode::BAD_REQUEST, "Invalid job id");
    }
    let Some(download_url) = payload.get("download_url").and_then(|value| value.as_str()) else {
        return error_response(StatusCode::BAD_REQUEST, "Download URL missing");
    };
    let parsed: Uri = match download_url.parse() {
        Ok(uri) => uri,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "Invalid download URL"),
    };
    if !matches!(parsed.scheme_str(), Some("http") | Some("https")) {
        return error_response(StatusCode::BAD_REQUEST, "Invalid download URL");
    }

    let filename = payload
        .get("filename")
        .and_then(|value| value.as_str())
        .or_else(|| {
            Path::new(parsed.path())
                .file_name()
                .and_then(|value| value.to_str())
        })
        .unwrap_or("upload.bin");
    let safe_name = sanitize_filename(Some(filename), "upload.bin");
    let download_rel = PathBuf::from("temp").join(&job_id).join(&safe_name);
    let download_abs = state.temp_dir.join(&job_id).join(&safe_name);

    let pack_format = payload
        .get("pack_format")
        .and_then(|value| value.as_str())
        .unwrap_or("zip")
        .to_string();
    let pack_level = parse_pack_level(
        payload.get("pack_level").and_then(|value| value.as_i64()),
        3,
    );
    let mod_id = payload.get("mod_id").cloned();
    let max_bytes = payload
        .get("max_bytes")
        .and_then(|value| value.as_u64())
        .or(state.config.transfer_max_bytes);

    if let Some(existing) = state.read_job_state(&job_id).await {
        if existing.started {
            return Json(TransferStartResponse {
                job_id,
                status: existing.status,
                ws_url: format!(
                    "/transfer/ws/{}",
                    payload
                        .get("job_id")
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                ),
            })
            .into_response();
        }
    }

    let mut job_state = new_job_state();
    reset_job_state(&mut job_state, true, "pending", "pending", None);
    state.save_job_state(&job_id, Some(job_state)).await;
    let mut meta = Map::new();
    meta.insert("job_id".to_string(), Value::String(job_id.clone()));
    meta.insert("mod_id".to_string(), mod_id.clone().unwrap_or(Value::Null));
    meta.insert(
        "download_url".to_string(),
        Value::String(download_url.to_string()),
    );
    meta.insert("filename".to_string(), Value::String(safe_name.clone()));
    meta.insert(
        "download_path".to_string(),
        Value::String(download_rel.to_string_lossy().to_string()),
    );
    meta.insert(
        "pack_format".to_string(),
        Value::String(pack_format.clone()),
    );
    meta.insert("pack_level".to_string(), Value::from(pack_level));
    meta.insert("status".to_string(), Value::String("pending".to_string()));
    meta.insert(
        "created_at".to_string(),
        Value::from(now_unix_seconds() as i64),
    );
    state.write_meta(&job_id, Value::Object(meta)).await;

    let mut callback_payload = Map::new();
    callback_payload.insert("mod_id".to_string(), mod_id.clone().unwrap_or(Value::Null));
    callback_payload.insert(
        "pack_format".to_string(),
        Value::String(pack_format.clone()),
    );
    callback_payload.insert("pack_level".to_string(), Value::from(pack_level));
    callback_payload.insert(
        "update_only".to_string(),
        Value::Bool(
            payload
                .get("update_only")
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
                || payload
                    .get("keep_condition")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false),
        ),
    );

    let state_clone = state.clone();
    let download_url = download_url.to_string();
    let job_id_for_task = job_id.clone();
    tokio::spawn(async move {
        transfer_jobs::run_download_job(
            state_clone,
            &job_id_for_task,
            &download_url,
            download_abs,
            max_bytes,
            callback_payload,
        )
        .await;
    });

    Json(TransferStartResponse {
        job_id: job_id.clone(),
        status: "started".to_string(),
        ws_url: format!("/transfer/ws/{job_id}"),
    })
    .into_response()
}

pub async fn transfer_start_get(
    Extension(state): Extension<Arc<AppState>>,
    Query(query): Query<TransferStartQuery>,
) -> impl IntoResponse {
    start_transfer(state, query.token).await
}

pub async fn transfer_start_post(
    Extension(state): Extension<Arc<AppState>>,
    Form(form): Form<TransferStartForm>,
) -> impl IntoResponse {
    start_transfer(state, form.token).await
}

pub async fn transfer_upload(
    Extension(state): Extension<Arc<AppState>>,
    request: Request,
) -> impl IntoResponse {
    let headers = request.headers().clone();
    let uri = request.uri().clone();
    let query = parse_query_map(&uri);
    let token = extract_bearer_token(&headers, &query);
    let Some(token) = token else {
        return error_response(StatusCode::UNAUTHORIZED, "Token not found");
    };
    let Some(payload) = auth::decode_transfer_jwt(&state.config, &token, "storage") else {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    };

    let spec = match build_upload_spec(&state, &headers, &query, &payload).await {
        Ok(spec) => spec,
        Err(response) => return response,
    };
    let total = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| parse_non_negative_int(query_value(&query, "size").as_deref()))
        .or_else(|| {
            headers
                .get("X-File-Size")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| {
                    headers
                        .get("X-Upload-Size")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok())
                })
        });

    let Some(_permit) = state.upload_limiter.try_acquire() else {
        notify_upload_error(&state, &spec, "busy").await;
        return busy_response();
    };

    let upload_future =
        execute_transfer_upload(state.clone(), spec.clone(), total, request.into_body());
    let upload_timeout = read_timeout_seconds(state.config.transfer_upload_timeout_seconds, 3600.0);
    let response = match upload_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, upload_future).await {
            Ok(response) => response,
            Err(_) => {
                notify_upload_error(&state, &spec, "timeout").await;
                state.close_clients(&spec.job_id).await;
                state.job_error_cleanup(&spec.job_id, "timeout").await;
                return error_response(StatusCode::REQUEST_TIMEOUT, "Upload timed out");
            }
        },
        None => upload_future.await,
    };
    response
}

async fn authorize_manage_request(
    state: &Arc<AppState>,
    token: Option<String>,
    action: &str,
    job_id: &str,
) -> Result<(), Response> {
    let Some(token) = token else {
        return Err(error_response(StatusCode::UNAUTHORIZED, "Token not found"));
    };
    if !manage_token_ok(state, &token) {
        let _ = action;
        let _ = job_id;
        return Err(error_response(StatusCode::FORBIDDEN, "Access denied"));
    }
    Ok(())
}

pub async fn transfer_repack(
    Extension(state): Extension<Arc<AppState>>,
    Form(form): Form<TransferRepackForm>,
) -> impl IntoResponse {
    if let Err(response) =
        authorize_manage_request(&state, form.token.clone(), "repack", &form.job_id).await
    {
        return response;
    }
    if !auth::is_safe_job_id(&form.job_id) {
        return error_response(StatusCode::BAD_REQUEST, "Invalid job id");
    }

    let Some(meta) = state.read_meta(&form.job_id).await else {
        return error_response(StatusCode::NOT_FOUND, "Job not found");
    };
    let Some(download_rel) = meta.get("download_path").and_then(|value| value.as_str()) else {
        return error_response(StatusCode::NOT_FOUND, "Source file not found");
    };
    let Ok(download_abs) = safe_path(&state.main_dir, download_rel) else {
        return error_response(StatusCode::NOT_FOUND, "Source file not found");
    };
    let pack_format = form.pack_format.unwrap_or_else(|| "zip".to_string());
    if pack_format != "zip" {
        return error_response(StatusCode::BAD_REQUEST, "Unsupported format");
    }
    let pack_level = parse_pack_level(form.compression_level, 3);
    let (repack_ok, packed_rel, packed_bytes, unpacked_bytes, repack_reason) =
        transfer_jobs::run_repack_job(
            state.clone(),
            &form.job_id,
            download_abs,
            &pack_format,
            pack_level,
        )
        .await;
    if !repack_ok {
        return match repack_reason.as_deref() {
            Some("encrypted_zip") => {
                error_response(StatusCode::BAD_REQUEST, "Encrypted zip not allowed")
            }
            Some("unpacked_size_limit") => {
                error_response(StatusCode::PAYLOAD_TOO_LARGE, "Archive too large")
            }
            Some("busy") => busy_response(),
            _ => error_response(StatusCode::INTERNAL_SERVER_ERROR, "Repack failed"),
        };
    }

    Json(TransferRepackResponse {
        job_id: form.job_id,
        packed_bytes,
        packed_path: packed_rel,
        unpacked_bytes,
    })
    .into_response()
}

pub async fn transfer_move(
    Extension(state): Extension<Arc<AppState>>,
    Form(form): Form<TransferMoveForm>,
) -> impl IntoResponse {
    if let Err(response) =
        authorize_manage_request(&state, form.token.clone(), "move", &form.job_id).await
    {
        return response;
    }
    if !auth::is_safe_job_id(&form.job_id) {
        return error_response(StatusCode::BAD_REQUEST, "Invalid job id");
    }
    if !is_allowed_type(&form.storage_type) {
        return error_response(StatusCode::BAD_REQUEST, "Invalid type");
    }

    let Some(meta) = state.read_meta(&form.job_id).await else {
        return error_response(StatusCode::NOT_FOUND, "Job not found");
    };
    let Some(packed_rel) = meta.get("packed_path").and_then(|value| value.as_str()) else {
        return error_response(StatusCode::NOT_FOUND, "Packed file not found");
    };
    let Ok(packed_abs) = safe_path(&state.main_dir, packed_rel) else {
        return error_response(StatusCode::NOT_FOUND, "Packed file not found");
    };
    let base_dir = state.main_dir.join(&form.storage_type);
    let Ok(real_path) = safe_path(&base_dir, &form.target_path) else {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    };
    if let Some(parent) = real_path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }
    if let Err(err) = fs::rename(&packed_abs, &real_path).await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    }
    let final_rel = real_path
        .strip_prefix(&state.main_dir)
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|_| real_path.to_string_lossy().to_string());
    let final_bytes = fs::metadata(&real_path)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0);
    let mut meta = meta.clone();
    if let Some(map) = meta.as_object_mut() {
        map.insert("final_path".to_string(), Value::String(final_rel.clone()));
        map.insert("final_bytes".to_string(), Value::from(final_bytes));
        map.insert("status".to_string(), Value::String("moved".to_string()));
        map.insert(
            "moved_at".to_string(),
            Value::from(now_unix_seconds() as i64),
        );
        state.write_meta(&form.job_id, meta).await;
    }
    state.delete_job_and_dir(&form.job_id).await;

    Json(TransferMoveResponse {
        job_id: form.job_id,
        final_path: final_rel,
        final_bytes,
    })
    .into_response()
}

pub async fn transfer_ws(
    Extension(state): Extension<Arc<AppState>>,
    Query(query): Query<TransferStartQuery>,
    AxumPath(path): AxumPath<TransferWsPath>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let Some(token) = query.token else {
        return error_response(StatusCode::UNAUTHORIZED, "Token not found");
    };
    let Some(payload) = auth::decode_transfer_jwt(&state.config, &token, "storage") else {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    };
    if payload.get("job_id").and_then(|value| value.as_str()) != Some(path.job_id.as_str()) {
        return error_response(StatusCode::FORBIDDEN, "Access denied");
    }

    let job_id = path.job_id.clone();
    ws.on_upgrade(move |socket| async move {
        transfer_ws_connection(state, job_id, socket).await;
    })
}

async fn transfer_ws_connection(state: Arc<AppState>, job_id: String, websocket: WebSocket) {
    let (mut sender, mut receiver) = websocket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    let client_id = next_client_id();

    let snapshot = {
        let mut lock = state.job_state.lock().await;
        let state_entry = lock.entry(job_id.clone()).or_insert_with(new_job_state);
        let sender_clone = tx.clone();
        state_entry.clients.push(WsClientHandle {
            id: client_id,
            sender: sender_clone,
        });
        state_event_payload("progress", state_entry, None)
    };

    let send_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if sender.send(message.clone()).await.is_err() {
                break;
            }
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });

    let _ = tx.send(Message::Text(snapshot.to_string().into()));

    while let Some(message) = receiver.next().await {
        match message {
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }

    send_task.abort();

    let mut lock = state.job_state.lock().await;
    if let Some(job_state) = lock.get_mut(&job_id) {
        job_state.clients.retain(|client| client.id != client_id);
        if job_state.clients.is_empty() && !job_state.started {
            lock.remove(&job_id);
        }
    }
}
