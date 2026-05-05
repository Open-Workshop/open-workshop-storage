mod common;

use std::path::Path;
use std::time::Duration;

use axum::extract::ws::Message;
use once_cell::sync::Lazy;
use open_workshop_storage::archive::{probe_archive, zip_dir_with_level, SevenZipError};
use open_workshop_storage::state::{new_job_state, WsClientHandle};
use open_workshop_storage::transfer_jobs::run_repack_job;
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};

use common::{prepend_path, temp_dir, test_config, test_state, test_state_with_config};

static FAKE_7Z_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[cfg(unix)]
fn write_fake_7z_script(dir: &Path, script: &str) {
    use std::os::unix::fs::PermissionsExt;

    let path = dir.join("7z");
    std::fs::write(&path, script).expect("write fake 7z");
    let mut permissions = std::fs::metadata(&path)
        .expect("fake 7z metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).expect("chmod fake 7z");
}

#[cfg(unix)]
fn progress_fake_7z_script(list_size: u64) -> String {
    let script = r#"#!/usr/bin/env python3
import os
import sys
import time

args = sys.argv[1:]
if not args or args[0] == "--help":
    print("7-Zip [fake]")
    sys.exit(0)

cmd = args[0]
if cmd == "l":
    print("Type = zip")
    print()
    print("Path = content.bin")
    print("Folder = -")
    print("Size = $LIST_SIZE$")
    print("Packed Size = $LIST_SIZE$")
    print("Method = Store")
    sys.exit(0)

if cmd == "x":
    dest = next((arg[2:] for arg in args if arg.startswith("-o")), None)
    if dest is None:
        sys.exit(2)
    os.makedirs(dest, exist_ok=True)
    print("20%", flush=True)
    print("80%", flush=True)
    with open(os.path.join(dest, "content.bin"), "wb") as handle:
        handle.write(b"extracted")
    sys.exit(0)

if cmd == "a":
    dest = next((arg for arg in args[1:] if arg and not arg.startswith("-") and arg != "."), None)
    if dest is None:
        sys.exit(2)
    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
    print("10%", flush=True)
    print("100%", flush=True)
    with open(dest, "wb") as handle:
        handle.write(b"packed")
    sys.exit(0)

time.sleep(10)
"#;
    script.replace("$LIST_SIZE$", &list_size.to_string())
}

#[cfg(unix)]
fn probe_timeout_fake_7z_script() -> String {
    r#"#!/usr/bin/env python3
import sys
import time

args = sys.argv[1:]
if not args or args[0] == "--help":
    print("7-Zip [fake]")
    sys.exit(0)

if args[0] == "l":
    time.sleep(10)
    sys.exit(0)

print("ok")
"#
    .to_string()
}

#[cfg(unix)]
fn idle_timeout_fake_7z_script() -> String {
    r#"#!/usr/bin/env python3
import os
import sys
import time

args = sys.argv[1:]
if not args or args[0] == "--help":
    print("7-Zip [fake]")
    sys.exit(0)

if args[0] == "a":
    dest = next((arg for arg in args[1:] if arg and not arg.startswith("-") and arg != "."), None)
    if dest is None:
        sys.exit(2)
    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
    print("1%", flush=True)
    time.sleep(10)

print("ok")
"#
    .to_string()
}

#[cfg(unix)]
async fn install_fake_7z(script: String, prefix: &str) -> common::TestDir {
    let dir = temp_dir(prefix);
    write_fake_7z_script(dir.path(), &script);
    dir
}

#[tokio::test]
async fn probe_archive_times_out_after_custom_timeout() {
    #[cfg(not(unix))]
    return;

    #[cfg(unix)]
    {
        let _lock = FAKE_7Z_LOCK.lock().await;
        let fake_7z =
            install_fake_7z(probe_timeout_fake_7z_script(), "fake-7z-probe-timeout").await;
        let _path_guard = prepend_path(fake_7z.path());
        let archive_dir = temp_dir("probe-timeout-archive");
        let archive_path = archive_dir.path().join("source.zip");
        std::fs::write(&archive_path, b"archive").expect("write archive");

        let err = probe_archive(&archive_path, Some(Duration::from_millis(50)))
            .await
            .expect_err("expected timeout");
        let text = err.to_string();
        assert!(matches!(err, SevenZipError::Timeout { .. }));
        assert!(text.contains("7z timed out"));
        assert!(text.contains("0.05s"));
    }
}

#[tokio::test]
async fn zip_dir_with_level_reports_idle_timeout() {
    #[cfg(not(unix))]
    return;

    #[cfg(unix)]
    {
        let _lock = FAKE_7Z_LOCK.lock().await;
        let fake_7z = install_fake_7z(idle_timeout_fake_7z_script(), "fake-7z-idle-timeout").await;
        let _path_guard = prepend_path(fake_7z.path());
        let src_dir = temp_dir("idle-timeout-src");
        let src_path = src_dir.path().join("src");
        std::fs::create_dir_all(&src_path).expect("create src dir");
        std::fs::write(src_path.join("content.bin"), b"content").expect("write source");
        let dest_zip = src_dir.path().join("packed.zip");

        let err = zip_dir_with_level(
            &src_path,
            &dest_zip,
            3,
            None,
            Some(Duration::from_secs(5)),
            Some(Duration::from_millis(50)),
        )
        .await
        .expect_err("expected idle timeout");
        let text = err.to_string();
        assert!(matches!(err, SevenZipError::IdleTimeout { .. }));
        assert!(text.contains("7z idle timed out"));
        assert!(text.contains("0.05s"));
    }
}

#[tokio::test]
async fn repack_rejects_archive_over_unpacked_size_limit() {
    #[cfg(not(unix))]
    return;

    #[cfg(unix)]
    {
        let _lock = FAKE_7Z_LOCK.lock().await;
        let fake_7z = install_fake_7z(progress_fake_7z_script(101), "fake-7z-size-limit").await;
        let _path_guard = prepend_path(fake_7z.path());
        let dir = temp_dir("repack-size-limit");
        let mut config = test_config(dir.path());
        config.transfer_max_unpacked_bytes = Some(100);
        let state = test_state_with_config(config);
        let job_id = "a".repeat(32);
        let download_abs = state.temp_dir.join(&job_id).join("source.zip");
        if let Some(parent) = download_abs.parent() {
            std::fs::create_dir_all(parent).expect("create source dir");
        }
        std::fs::write(&download_abs, b"archive").expect("write source");

        let result = run_repack_job(state.clone(), &job_id, &download_abs, "zip", 3).await;
        assert!(!result.0);
        assert_eq!(result.4.as_deref(), Some("unpacked_size_limit"));
        assert_eq!(result.3, Some(101));

        let job_state = state.read_job_state(&job_id).await.expect("job state");
        assert_eq!(job_state.status, "error");
        assert_eq!(job_state.error.as_deref(), Some("unpacked_size_limit"));
    }
}

#[tokio::test]
async fn repack_uses_separate_extracting_stage() {
    #[cfg(not(unix))]
    return;

    #[cfg(unix)]
    {
        let _lock = FAKE_7Z_LOCK.lock().await;
        let fake_7z = install_fake_7z(progress_fake_7z_script(10), "fake-7z-progress").await;
        let _path_guard = prepend_path(fake_7z.path());
        let dir = temp_dir("repack-progress");
        let state = test_state(dir.path());
        let job_id = "c".repeat(32);
        let download_abs = state.temp_dir.join(&job_id).join("source.zip");
        if let Some(parent) = download_abs.parent() {
            std::fs::create_dir_all(parent).expect("create source dir");
        }
        std::fs::write(&download_abs, b"archive").expect("write source");

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut job_state = new_job_state();
        job_state.started = true;
        job_state.clients.push(WsClientHandle { id: 1, sender: tx });
        state.save_job_state(&job_id, Some(job_state)).await;

        let result = run_repack_job(state.clone(), &job_id, &download_abs, "zip", 3).await;
        assert!(result.0);
        assert_eq!(result.4, None);

        let mut messages = Vec::new();
        while let Ok(message) = rx.try_recv() {
            if let Message::Text(text) = message {
                if let Ok(payload) = serde_json::from_str::<Value>(&text) {
                    messages.push(payload);
                }
            }
        }

        let extracting_index = messages.iter().position(|payload| {
            payload.get("event").and_then(Value::as_str) == Some("progress")
                && payload.get("stage").and_then(Value::as_str) == Some("extracting")
                && payload.get("percent").and_then(Value::as_u64).unwrap_or(0) > 0
        });
        let repacking_index = messages.iter().position(|payload| {
            payload.get("event").and_then(Value::as_str) == Some("progress")
                && payload.get("stage").and_then(Value::as_str) == Some("repacking")
                && payload.get("percent").and_then(Value::as_u64).unwrap_or(0) > 0
        });

        let extracting_index = extracting_index.expect("extracting progress");
        let repacking_index = repacking_index.expect("repacking progress");
        assert!(extracting_index < repacking_index);
    }
}
