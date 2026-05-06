use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::http::header::CONTENT_LENGTH;
use axum::http::{Request, Uri};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::StatusCode;
use serde_json::{Map, Value};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;

use crate::archive::{
    archive_entries_unpacked_bytes, probe_archive, safe_extract_archive, zip_dir_with_level,
    zip_uses_deflated_or_better,
};
use crate::http_client::build_hyper_client;
use crate::job_meta::update_job_meta;
use crate::runtime::AppState;
use crate::state::state_event_payload;

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn read_positive_int_setting(raw_value: Option<f64>, default: u64) -> Option<u64> {
    let value = raw_value.unwrap_or(default as f64);
    if value <= 0.0 {
        None
    } else {
        Some(value as u64)
    }
}

fn read_timeout_seconds(raw_value: Option<f64>, default: f64) -> Option<Duration> {
    let value = raw_value.unwrap_or(default);
    if value <= 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(value))
    }
}

async fn state_event_with_message(
    state: &Arc<AppState>,
    job_id: &str,
    event: &str,
    message: Option<&str>,
) -> Value {
    let extra = message.map(|message| {
        let mut map = Map::new();
        map.insert("message".to_string(), Value::String(message.to_string()));
        map
    });
    state.build_state_event(job_id, event, extra).await
}

async fn broadcast_progress(state: &Arc<AppState>, job_id: &str, stage: &str, percent: u32) {
    let percent = percent.min(100);
    let mut updates = Map::new();
    updates.insert("stage".to_string(), Value::String(stage.to_string()));
    updates.insert("percent".to_string(), Value::from(percent));
    state.set_state(job_id, updates).await;
    let snapshot = state_event_payload(
        "progress",
        &state.read_job_state(job_id).await.unwrap_or_default(),
        None,
    );
    state.broadcast(job_id, snapshot).await;
}

async fn broadcast_repack_progress(state: &Arc<AppState>, job_id: &str, percent: u32) {
    broadcast_progress(state, job_id, "repacking", percent).await;
}

async fn broadcast_extract_progress(state: &Arc<AppState>, job_id: &str, percent: u32) {
    broadcast_progress(state, job_id, "extracting", percent).await;
}

async fn update_state_error_and_broadcast(
    state: &Arc<AppState>,
    job_id: &str,
    error: &str,
    message: &str,
) {
    let mut updates = Map::new();
    updates.insert("status".to_string(), Value::String("error".to_string()));
    updates.insert("error".to_string(), Value::String(error.to_string()));
    state.set_state(job_id, updates).await;
    let event = state_event_with_message(state, job_id, "error", Some(message)).await;
    state.broadcast(job_id, event).await;
}

async fn http_get_bytes(
    url: &str,
    timeout: Option<Duration>,
) -> Result<(StatusCode, Option<u64>, Incoming), String> {
    let uri: Uri = url.parse::<Uri>().map_err(|err| err.to_string())?;
    let client = build_hyper_client();
    let request = Request::get(uri)
        .body(Full::new(Bytes::new()))
        .map_err(|err| err.to_string())?;
    let response = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, client.request(request))
            .await
            .map_err(|_| "timeout".to_string())?
            .map_err(|err| err.to_string())?,
        None => client
            .request(request)
            .await
            .map_err(|err| err.to_string())?,
    };
    let total = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    Ok((response.status(), total, response.into_body()))
}

pub async fn run_cleanup(state: &Arc<AppState>) {
    let now = now_unix_seconds() as f64;
    let cleanup_threshold = state.config.job_ttl_seconds;

    let active_job_ids = state.list_job_ids().await;
    let local_job_ids = state.local_job_ids().await;
    let job_ids = active_job_ids
        .into_iter()
        .chain(local_job_ids.into_iter())
        .collect::<std::collections::BTreeSet<_>>();
    let mut jobs_to_remove = Vec::new();
    for job_id in &job_ids {
        let Some(job_state) = state.read_job_state(job_id).await else {
            continue;
        };
        if now - job_state.last_activity >= cleanup_threshold as f64 {
            jobs_to_remove.push(job_id.clone());
        }
    }

    for job_id in jobs_to_remove {
        state.delete_job_and_dir(&job_id).await;
        eprintln!("cleanup removed inactive job job_id={job_id}");
    }

    let temp_dir = state.temp_dir.clone();
    let read_dir = match std::fs::read_dir(&temp_dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in read_dir.flatten() {
        let job_path = entry.path();
        if !job_path.is_dir() {
            continue;
        }
        let Some(job_folder) = entry.file_name().to_str().map(|value| value.to_string()) else {
            continue;
        };
        if job_ids.contains(&job_folder) {
            continue;
        }
        match entry.metadata().and_then(|meta| meta.modified()) {
            Ok(modified) => {
                let modified_secs = modified
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                if now - modified_secs >= cleanup_threshold as f64 {
                    let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(job_path))
                        .await;
                    eprintln!("cleanup removed old dir job_id={job_folder}");
                }
            }
            Err(_) => {
                eprintln!("failed to check/cleanup old dir job_id={job_folder}");
            }
        }
    }
}

pub async fn cleanup_loop(state: Arc<AppState>) {
    let cleanup_interval = Duration::from_secs(state.config.cleanup_interval_seconds.max(1));
    loop {
        sleep(cleanup_interval).await;
        run_cleanup(&state).await;
    }
}

pub async fn run_repack_job(
    state: Arc<AppState>,
    job_id: &str,
    download_abs: impl AsRef<Path>,
    pack_format: &str,
    pack_level: u32,
) -> (
    bool,
    Option<String>,
    Option<u64>,
    Option<u64>,
    Option<String>,
) {
    tracing::info!(job_id, pack_format, pack_level, "transfer repack requested");
    let Some(_permit) = state.repack_limiter.try_acquire() else {
        tracing::warn!(
            job_id,
            pack_format,
            pack_level,
            "transfer repack rejected: busy"
        );
        update_state_error_and_broadcast(&state, job_id, "busy", "storage busy").await;
        let mut updates = Map::new();
        updates.insert("status".to_string(), Value::String("error".to_string()));
        updates.insert(
            "error_reason".to_string(),
            Value::String("busy".to_string()),
        );
        update_job_meta(
            &state,
            job_id,
            updates,
            "failed to update meta for job_id=%s",
        )
        .await;
        return (false, None, None, None, Some("busy".to_string()));
    };

    _run_repack_job_limited(
        state,
        job_id,
        download_abs.as_ref(),
        pack_format,
        pack_level,
    )
    .await
}

async fn _run_repack_job_limited(
    state: Arc<AppState>,
    job_id: &str,
    download_abs: &Path,
    pack_format: &str,
    pack_level: u32,
) -> (
    bool,
    Option<String>,
    Option<u64>,
    Option<u64>,
    Option<String>,
) {
    tracing::info!(job_id, pack_format, pack_level, "transfer repack started");
    if pack_format != "zip" {
        tracing::warn!(
            job_id,
            pack_format,
            "transfer repack rejected: unsupported format"
        );
        update_state_error_and_broadcast(
            &state,
            job_id,
            "unsupported_format",
            "unsupported format",
        )
        .await;
        return (
            false,
            None,
            None,
            None,
            Some("unsupported_format".to_string()),
        );
    }

    let packed_rel = PathBuf::from("temp").join(job_id).join("packed.zip");
    let packed_abs = state.temp_dir.join(job_id).join("packed.zip");

    let probe_timeout = read_timeout_seconds(
        state.config.seven_zip_timeout_seconds,
        crate::archive::DEFAULT_SEVEN_ZIP_TIMEOUT_SECONDS as f64,
    );
    let probe = probe_archive(download_abs, probe_timeout).await;
    let (archive_type, is_encrypted, archive_entries) = match probe {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(job_id, error = %err, "transfer archive probe failed");
            let mut updates = Map::new();
            updates.insert("status".to_string(), Value::String("error".to_string()));
            updates.insert("error".to_string(), Value::String(err.to_string()));
            state.set_state(job_id, updates).await;
            let event =
                state_event_with_message(&state, job_id, "error", Some("archive probe failed"))
                    .await;
            state.broadcast(job_id, event).await;
            return (false, None, None, None, Some("repack_failed".to_string()));
        }
    };
    let unpacked_bytes = archive_entries_unpacked_bytes(archive_entries.as_deref());

    let max_unpacked_bytes = read_positive_int_setting(
        state
            .config
            .transfer_max_unpacked_bytes
            .map(|value| value as f64),
        0,
    );
    if is_encrypted {
        tracing::warn!(job_id, "transfer repack denied: encrypted zip");
        update_state_error_and_broadcast(&state, job_id, "encrypted_zip", "zip encrypted").await;
        let mut updates = Map::new();
        updates.insert("status".to_string(), Value::String("error".to_string()));
        updates.insert(
            "error_reason".to_string(),
            Value::String("encrypted_zip".to_string()),
        );
        update_job_meta(
            &state,
            job_id,
            updates,
            "failed to update meta for job_id=%s",
        )
        .await;
        eprintln!("transfer repack denied (encrypted zip) job_id={job_id}");
        return (
            false,
            None,
            None,
            unpacked_bytes,
            Some("encrypted_zip".to_string()),
        );
    }

    if let (Some(limit), Some(unpacked_bytes)) = (max_unpacked_bytes, unpacked_bytes) {
        if unpacked_bytes > limit {
            tracing::warn!(
                job_id,
                unpacked_bytes,
                limit,
                "transfer repack denied: unpacked size limit"
            );
            update_state_error_and_broadcast(
                &state,
                job_id,
                "unpacked_size_limit",
                "archive too large",
            )
            .await;
            let mut updates = Map::new();
            updates.insert("status".to_string(), Value::String("error".to_string()));
            updates.insert(
                "error_reason".to_string(),
                Value::String("unpacked_size_limit".to_string()),
            );
            updates.insert("unpacked_bytes".to_string(), Value::from(unpacked_bytes));
            update_job_meta(
                &state,
                job_id,
                updates,
                "failed to update meta for job_id=%s",
            )
            .await;
            eprintln!(
                "transfer repack denied (unpacked size limit) job_id={job_id} unpacked_bytes={unpacked_bytes} limit={limit}"
            );
            return (
                false,
                None,
                None,
                Some(unpacked_bytes),
                Some("unpacked_size_limit".to_string()),
            );
        }
    }

    if archive_type.as_deref() == Some("zip") {
        if zip_uses_deflated_or_better(archive_entries.as_deref()) {
            match fs::metadata(download_abs).await {
                Ok(metadata) => {
                    let packed_bytes = metadata.len();
                    let packed_rel = download_abs
                        .strip_prefix(&state.main_dir)
                        .map(|path| path.to_path_buf())
                        .unwrap_or_else(|_| download_abs.to_path_buf());
                    let mut updates = Map::new();
                    updates.insert(
                        "packed_path".to_string(),
                        Value::String(packed_rel.to_string_lossy().to_string()),
                    );
                    updates.insert("packed_bytes".to_string(), Value::from(packed_bytes));
                    updates.insert(
                        "pack_format".to_string(),
                        Value::String(pack_format.to_string()),
                    );
                    updates.insert("pack_level".to_string(), Value::from(pack_level));
                    updates.insert("status".to_string(), Value::String("packed".to_string()));
                    update_job_meta(
                        &state,
                        job_id,
                        updates,
                        "failed to update meta for job_id=%s",
                    )
                    .await;
                    let _ = state.set_stage(job_id, "packed").await;
                    tracing::info!(
                        job_id,
                        packed_bytes,
                        unpacked_bytes = ?unpacked_bytes,
                        "transfer repack skipped: zip already acceptable"
                    );
                    return (
                        true,
                        Some(packed_rel.to_string_lossy().to_string()),
                        Some(packed_bytes),
                        unpacked_bytes,
                        None,
                    );
                }
                Err(err) => {
                    tracing::warn!(job_id, error = %err, "failed to stat packed file");
                }
            }
        }
    }

    if fs::metadata(&packed_abs).await.is_ok() {
        let packed_bytes = fs::metadata(&packed_abs).await.map(|meta| meta.len()).ok();
        if let Some(packed_bytes) = packed_bytes {
            let mut updates = Map::new();
            updates.insert(
                "packed_path".to_string(),
                Value::String(packed_rel.to_string_lossy().to_string()),
            );
            updates.insert("packed_bytes".to_string(), Value::from(packed_bytes));
            updates.insert(
                "pack_format".to_string(),
                Value::String(pack_format.to_string()),
            );
            updates.insert("pack_level".to_string(), Value::from(pack_level));
            updates.insert("status".to_string(), Value::String("packed".to_string()));
            update_job_meta(
                &state,
                job_id,
                updates,
                "failed to update meta for job_id=%s",
            )
            .await;
            let _ = state.set_stage(job_id, "packed").await;
            return (
                true,
                Some(packed_rel.to_string_lossy().to_string()),
                Some(packed_bytes),
                unpacked_bytes,
                None,
            );
        }
    }

    let _repack_rel = PathBuf::from("temp").join(job_id).join("repack");
    let repack_abs = state.temp_dir.join(job_id).join("repack");
    if repack_abs.exists() {
        let _ = tokio::fs::remove_dir_all(&repack_abs).await;
    }
    if let Err(err) = fs::create_dir_all(&repack_abs).await {
        tracing::warn!(job_id, error = %err, "failed to create repack dir");
        return (
            false,
            None,
            None,
            unpacked_bytes,
            Some("repack_failed".to_string()),
        );
    }

    let start_ts = Instant::now();
    let _extract_progress = (Instant::now(), -1i32);
    let _repack_progress = (Instant::now(), -1i32);

    let _ = state.set_stage(job_id, "extracting").await;
    broadcast_extract_progress(&state, job_id, 0).await;

    if archive_type.is_some() {
        let state_for_progress = state.clone();
        let job_id_for_progress = job_id.to_string();
        let on_extract_progress = move |percent: u32| {
            let state = state_for_progress.clone();
            let job_id = job_id_for_progress.clone();
            tokio::spawn(async move {
                broadcast_extract_progress(&state, &job_id, percent).await;
            });
        };
        let result = safe_extract_archive(
            download_abs,
            &repack_abs,
            archive_entries.as_deref(),
            Some(&on_extract_progress),
            probe_timeout,
            probe_timeout,
        )
        .await;
        if let Err(err) = result {
            tracing::warn!(job_id, error = %err, "transfer repack failed");
            update_state_error_and_broadcast(&state, job_id, "repack_failed", "repack failed")
                .await;
            return (
                false,
                None,
                None,
                unpacked_bytes,
                Some("repack_failed".to_string()),
            );
        }
    } else {
        let dest_name = download_abs
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("input.bin");
        let dest_path = repack_abs.join(dest_name);
        if let Err(err) = fs::rename(download_abs, &dest_path).await {
            eprintln!("transfer repack move failed job_id={job_id} error={err}");
            update_state_error_and_broadcast(&state, job_id, "repack_failed", "repack failed")
                .await;
            return (
                false,
                None,
                None,
                unpacked_bytes,
                Some("repack_failed".to_string()),
            );
        }
        let mut updates = Map::new();
        updates.insert(
            "download_path".to_string(),
            Value::String(
                PathBuf::from("temp")
                    .join(job_id)
                    .join(dest_name)
                    .to_string_lossy()
                    .to_string(),
            ),
        );
        update_job_meta(
            &state,
            job_id,
            updates,
            "failed to update meta download_path for job_id=%s",
        )
        .await;
    }

    let _ = state.set_stage(job_id, "repacking").await;
    broadcast_repack_progress(&state, job_id, 0).await;

    let state_for_progress = state.clone();
    let job_id_for_progress = job_id.to_string();
    let on_repack_progress = move |percent: u32| {
        let state = state_for_progress.clone();
        let job_id = job_id_for_progress.clone();
        tokio::spawn(async move {
            broadcast_repack_progress(&state, &job_id, percent).await;
        });
    };

    if let Err(err) = zip_dir_with_level(
        &repack_abs,
        &packed_abs,
        pack_level,
        Some(&on_repack_progress),
        probe_timeout,
        probe_timeout,
    )
    .await
    {
        eprintln!("transfer repack failed job_id={job_id} error={err}");
        update_state_error_and_broadcast(&state, job_id, "repack_failed", "repack failed").await;
        return (
            false,
            None,
            None,
            unpacked_bytes,
            Some("repack_failed".to_string()),
        );
    }

    let _ = broadcast_repack_progress(&state, job_id, 100).await;
    let packed_bytes = fs::metadata(&packed_abs).await.map(|meta| meta.len()).ok();
    let Some(packed_bytes) = packed_bytes else {
        update_state_error_and_broadcast(&state, job_id, "repack_failed", "repack failed").await;
        return (
            false,
            None,
            None,
            unpacked_bytes,
            Some("repack_failed".to_string()),
        );
    };

    let mut updates = Map::new();
    updates.insert(
        "packed_path".to_string(),
        Value::String(packed_rel.to_string_lossy().to_string()),
    );
    updates.insert("packed_bytes".to_string(), Value::from(packed_bytes));
    updates.insert(
        "pack_format".to_string(),
        Value::String(pack_format.to_string()),
    );
    updates.insert("pack_level".to_string(), Value::from(pack_level));
    updates.insert("status".to_string(), Value::String("packed".to_string()));
    update_job_meta(
        &state,
        job_id,
        updates,
        "failed to update meta for job_id=%s",
    )
    .await;
    let _ = state.set_stage(job_id, "packed").await;
    tracing::info!(
        job_id,
        packed_bytes,
        unpacked_bytes = ?unpacked_bytes,
        duration = start_ts.elapsed().as_secs_f64(),
        "transfer repack completed"
    );

    (
        true,
        Some(packed_rel.to_string_lossy().to_string()),
        Some(packed_bytes),
        unpacked_bytes,
        None,
    )
}

pub async fn run_download_job(
    state: Arc<AppState>,
    job_id: &str,
    download_url: &str,
    download_abs: impl AsRef<Path>,
    max_bytes: Option<u64>,
    callback_payload: Map<String, Value>,
) {
    let Some(_permit) = state.download_limiter.try_acquire() else {
        tracing::warn!(job_id, "transfer download rejected: busy");
        update_state_error_and_broadcast(&state, job_id, "busy", "storage busy").await;
        let mut updates = Map::new();
        updates.insert("status".to_string(), Value::String("error".to_string()));
        updates.insert(
            "error_reason".to_string(),
            Value::String("busy".to_string()),
        );
        update_job_meta(
            &state,
            job_id,
            updates,
            "failed to update meta for job_id=%s",
        )
        .await;
        let mut callback = callback_payload.clone();
        callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
        callback.insert("status".to_string(), Value::String("error".to_string()));
        callback.insert("reason".to_string(), Value::String("busy".to_string()));
        state.notify_manager(callback).await;
        state.job_error_cleanup(job_id, "busy").await;
        return;
    };

    _run_download_job_limited(
        state,
        job_id,
        download_url,
        download_abs.as_ref(),
        max_bytes,
        callback_payload,
    )
    .await;
}

async fn _run_download_job_limited(
    state: Arc<AppState>,
    job_id: &str,
    download_url: &str,
    download_abs: &Path,
    max_bytes: Option<u64>,
    callback_payload: Map<String, Value>,
) {
    tracing::info!(
        job_id,
        download_url = %download_url.split('?').next().unwrap_or(download_url),
        max_bytes = ?max_bytes,
        "transfer download started"
    );
    let mut updates = Map::new();
    updates.insert(
        "status".to_string(),
        Value::String("downloading".to_string()),
    );
    updates.insert("error".to_string(), Value::Null);
    state.set_state(job_id, updates).await;
    let _ = state.set_stage(job_id, "downloading").await;
    let mut downloaded: u64 = 0;
    let mut total = None;
    let mut last_push = Instant::now() - state.progress_push_interval;
    let mut meta_updates = Map::new();
    meta_updates.insert(
        "status".to_string(),
        Value::String("downloading".to_string()),
    );
    meta_updates.insert(
        "download_started_at".to_string(),
        Value::from(now_unix_seconds() as i64),
    );
    update_job_meta(
        &state,
        job_id,
        meta_updates,
        "failed to update meta (start) for job_id=%s",
    )
    .await;

    let download_timeout =
        read_timeout_seconds(state.config.transfer_download_timeout_seconds, 3600.0);
    let result = http_get_bytes(download_url, download_timeout).await;
    let (status, content_length, mut body) = match result {
        Ok(value) => value,
        Err(err) if err == "timeout" => {
            tracing::warn!(job_id, "transfer download timed out");
            update_state_error_and_broadcast(&state, job_id, "timeout", "download timed out").await;
            let mut callback = callback_payload.clone();
            callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
            callback.insert("status".to_string(), Value::String("error".to_string()));
            callback.insert("reason".to_string(), Value::String("timeout".to_string()));
            state.notify_manager(callback).await;
            state.job_error_cleanup(job_id, "timeout").await;
            return;
        }
        Err(err) => {
            tracing::warn!(job_id, error = %err, "transfer download failed");
            let mut meta_updates = Map::new();
            meta_updates.insert("status".to_string(), Value::String("error".to_string()));
            meta_updates.insert("error".to_string(), Value::String(err.to_string()));
            meta_updates.insert(
                "download_completed_at".to_string(),
                Value::from(now_unix_seconds() as i64),
            );
            update_job_meta(
                &state,
                job_id,
                meta_updates,
                "failed to update meta for job_id=%s",
            )
            .await;
            update_state_error_and_broadcast(&state, job_id, &err, "download failed").await;
            let mut callback = callback_payload.clone();
            callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
            callback.insert("status".to_string(), Value::String("error".to_string()));
            callback.insert("reason".to_string(), Value::String("exception".to_string()));
            state.notify_manager(callback).await;
            state.job_error_cleanup(job_id, "download_exception").await;
            return;
        }
    };

    if status != StatusCode::OK {
        update_state_error_and_broadcast(
            &state,
            job_id,
            &format!("status:{status}"),
            &format!("download failed with status {status}"),
        )
        .await;
        let mut callback = callback_payload.clone();
        callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
        callback.insert("status".to_string(), Value::String("error".to_string()));
        callback.insert(
            "reason".to_string(),
            Value::String(format!("status:{status}")),
        );
        state.notify_manager(callback).await;
        return;
    }

    if let Some(total_bytes) = content_length {
        total = Some(total_bytes);
        let mut updates = Map::new();
        updates.insert("total".to_string(), Value::from(total_bytes));
        state.set_state(job_id, updates).await;
        if let Some(limit) = max_bytes {
            if total_bytes > limit {
                update_state_error_and_broadcast(&state, job_id, "size_limit", "file too large")
                    .await;
                let _ = fs::remove_file(download_abs).await;
                let mut callback = callback_payload.clone();
                callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
                callback.insert("status".to_string(), Value::String("error".to_string()));
                callback.insert(
                    "reason".to_string(),
                    Value::String("size_limit".to_string()),
                );
                state.notify_manager(callback).await;
                return;
            }
        }
    }

    if let Some(parent) = download_abs.parent() {
        let _ = fs::create_dir_all(parent).await;
    }
    let mut out_file = match fs::File::create(download_abs).await {
        Ok(file) => file,
        Err(err) => {
            tracing::warn!(job_id, error = %err, "failed to create download file");
            update_state_error_and_broadcast(&state, job_id, &err.to_string(), "download failed")
                .await;
            let mut callback = callback_payload.clone();
            callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
            callback.insert("status".to_string(), Value::String("error".to_string()));
            callback.insert("reason".to_string(), Value::String("exception".to_string()));
            state.notify_manager(callback).await;
            state.job_error_cleanup(job_id, "download_exception").await;
            return;
        }
    };

    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(err) => {
                tracing::warn!(job_id, error = %err, "transfer download failed");
                let _ = fs::remove_file(download_abs).await;
                let mut meta_updates = Map::new();
                meta_updates.insert("status".to_string(), Value::String("error".to_string()));
                meta_updates.insert("error".to_string(), Value::String(err.to_string()));
                meta_updates.insert(
                    "download_completed_at".to_string(),
                    Value::from(now_unix_seconds() as i64),
                );
                update_job_meta(
                    &state,
                    job_id,
                    meta_updates,
                    "failed to update meta for job_id=%s",
                )
                .await;
                update_state_error_and_broadcast(
                    &state,
                    job_id,
                    &err.to_string(),
                    "download failed",
                )
                .await;
                let mut callback = callback_payload.clone();
                callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
                callback.insert("status".to_string(), Value::String("error".to_string()));
                callback.insert("reason".to_string(), Value::String("exception".to_string()));
                state.notify_manager(callback).await;
                state.job_error_cleanup(job_id, "download_exception").await;
                return;
            }
        };
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        downloaded += chunk.len() as u64;
        if let Some(limit) = max_bytes {
            if downloaded > limit {
                update_state_error_and_broadcast(&state, job_id, "size_limit", "file too large")
                    .await;
                let _ = fs::remove_file(download_abs).await;
                let mut callback = callback_payload.clone();
                callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
                callback.insert("status".to_string(), Value::String("error".to_string()));
                callback.insert(
                    "reason".to_string(),
                    Value::String("size_limit".to_string()),
                );
                state.notify_manager(callback).await;
                return;
            }
        }
        if let Err(err) = out_file.write_all(&chunk).await {
            tracing::warn!(job_id, error = %err, "transfer download write failed");
            let _ = fs::remove_file(download_abs).await;
            update_state_error_and_broadcast(&state, job_id, &err.to_string(), "download failed")
                .await;
            let mut callback = callback_payload.clone();
            callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
            callback.insert("status".to_string(), Value::String("error".to_string()));
            callback.insert("reason".to_string(), Value::String("exception".to_string()));
            state.notify_manager(callback).await;
            state.job_error_cleanup(job_id, "download_exception").await;
            return;
        }
        if last_push.elapsed() >= state.progress_push_interval {
            last_push = Instant::now();
            let mut updates = Map::new();
            updates.insert("bytes".to_string(), Value::from(downloaded));
            state.set_state(job_id, updates).await;
            let event = state_event_with_message(&state, job_id, "progress", None).await;
            state.broadcast(job_id, event).await;
        }
    }

    let mut updates = Map::new();
    updates.insert("status".to_string(), Value::String("done".to_string()));
    updates.insert("bytes".to_string(), Value::from(downloaded));
    updates.insert(
        "total".to_string(),
        total.map(Value::from).unwrap_or(Value::Null),
    );
    state.set_state(job_id, updates).await;
    let mut meta_updates = Map::new();
    meta_updates.insert(
        "status".to_string(),
        Value::String("downloaded".to_string()),
    );
    meta_updates.insert("downloaded_bytes".to_string(), Value::from(downloaded));
    meta_updates.insert(
        "total_bytes".to_string(),
        total.map(Value::from).unwrap_or(Value::Null),
    );
    meta_updates.insert(
        "download_completed_at".to_string(),
        Value::from(now_unix_seconds() as i64),
    );
    update_job_meta(
        &state,
        job_id,
        meta_updates,
        "failed to update meta for job_id=%s",
    )
    .await;
    let _ = state.set_stage(job_id, "downloaded").await;

    let pack_format = callback_payload
        .get("pack_format")
        .and_then(|value| value.as_str())
        .unwrap_or("zip");
    let pack_level = callback_payload
        .get("pack_level")
        .and_then(|value| value.as_u64())
        .unwrap_or(3) as u32;

    let (repack_ok, _packed_rel, _packed_bytes, unpacked_bytes, repack_reason) =
        run_repack_job(state.clone(), job_id, download_abs, pack_format, pack_level).await;
    if !repack_ok {
        if repack_reason.as_deref() == Some("encrypted_zip") {
            let _ = fs::remove_file(download_abs).await;
        }
        let mut callback = callback_payload.clone();
        callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
        callback.insert("status".to_string(), Value::String("error".to_string()));
        callback.insert(
            "reason".to_string(),
            Value::String(
                repack_reason
                    .clone()
                    .unwrap_or_else(|| "repack_failed".to_string()),
            ),
        );
        state.notify_manager(callback).await;
        state
            .job_error_cleanup(job_id, repack_reason.as_deref().unwrap_or("repack_failed"))
            .await;
        return;
    }

    let event = state_event_with_message(&state, job_id, "complete", None).await;
    state.broadcast(job_id, event).await;
    let mut callback = callback_payload.clone();
    callback.insert("job_id".to_string(), Value::String(job_id.to_string()));
    callback.insert("status".to_string(), Value::String("success".to_string()));
    callback.insert("bytes".to_string(), Value::from(downloaded));
    callback.insert(
        "total".to_string(),
        total.map(Value::from).unwrap_or(Value::Null),
    );
    if let Some(unpacked_bytes) = unpacked_bytes {
        callback.insert("unpacked_bytes".to_string(), Value::from(unpacked_bytes));
    }
    tracing::info!(
        job_id,
        bytes = downloaded,
        total = ?total,
        unpacked_bytes = ?unpacked_bytes,
        "transfer download completed"
    );
    state.notify_manager(callback).await;
    state.close_clients(job_id).await;
}
