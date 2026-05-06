use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use regex::Regex;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::{sleep_until, Instant};

use crate::fs_utils::safe_path;

pub const SEVEN_ZIP_BIN: &str = "7z";
pub const ZIP_MIN_COMPRESSION_SAVINGS_RATIO: f64 = 0.01;
pub const DEFAULT_SEVEN_ZIP_TIMEOUT_SECONDS: u64 = 3600;
pub const DEFAULT_SEVEN_ZIP_IDLE_TIMEOUT_SECONDS: u64 = 60;
pub type ArchiveEntry = HashMap<String, String>;

#[derive(Debug, thiserror::Error)]
pub enum SevenZipError {
    #[error("7z binary is required but not found in PATH")]
    MissingBinary,
    #[error("7z timed out{suffix}")]
    Timeout { suffix: String },
    #[error("7z idle timed out{suffix}")]
    IdleTimeout { suffix: String },
    #[error("{0}")]
    CommandFailed(String),
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct SevenZipOutput {
    pub status: std::process::ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

pub fn ensure_7z_available() -> Result<(), SevenZipError> {
    let status = std::process::Command::new(SEVEN_ZIP_BIN)
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match status {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(SevenZipError::MissingBinary),
        Err(err) => Err(SevenZipError::Io(err)),
    }
}

async fn run_7z_capture(
    args: &[String],
    cwd: Option<&Path>,
    timeout: Option<Duration>,
) -> Result<SevenZipOutput, SevenZipError> {
    ensure_7z_available()?;
    let mut command = Command::new(SEVEN_ZIP_BIN);
    command.args(args);
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, command.output())
            .await
            .map_err(|_| SevenZipError::Timeout {
                suffix: timeout_suffix(timeout),
            })??,
        None => command.output().await?,
    };
    Ok(SevenZipOutput {
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn drain_progress_stream(
    chunk: &str,
    pending: &mut String,
    last_percent: &mut i32,
    on_progress: Option<&(dyn Fn(u32) + Send + Sync)>,
) {
    pending.push_str(chunk);
    if pending.len() > 256 {
        let start = pending.len() - 256;
        *pending = pending[start..].to_string();
    }
    static PROGRESS_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
        Regex::new(r"(?:^|[^\d])(100|[1-9]?\d)%").expect("valid regex")
    });
    for caps in PROGRESS_RE.captures_iter(pending) {
        if let Ok(percent) = caps
            .get(1)
            .map(|m| m.as_str())
            .unwrap_or_default()
            .parse::<i32>()
        {
            if percent > *last_percent {
                *last_percent = percent;
                if let Some(cb) = on_progress {
                    cb(percent as u32);
                }
            }
        }
    }
}

async fn run_7z_with_progress(
    args: &[String],
    cwd: Option<&Path>,
    on_progress: Option<&(dyn Fn(u32) + Send + Sync)>,
    timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
) -> Result<SevenZipOutput, SevenZipError> {
    ensure_7z_available()?;
    let mut command = Command::new(SEVEN_ZIP_BIN);
    command.args(args);
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }

    let mut child = command.spawn()?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "missing stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "missing stderr"))?;

    let stderr_handle = tokio::spawn(async move {
        let mut stderr = stderr;
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf).await;
        buf
    });

    let mut stdout_bytes = Vec::new();
    let mut pending = String::new();
    let mut last_percent = -1;
    let total_deadline = timeout.map(|dur| Instant::now() + dur);
    let total_timeout_suffix = timeout.map(timeout_suffix);
    let idle_timeout_suffix = idle_timeout.map(timeout_suffix);
    let mut idle_deadline = idle_timeout.map(|dur| Instant::now() + dur);
    let mut buf = [0u8; 4096];

    loop {
        let deadline = match (total_deadline, idle_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        if let Some(deadline) = deadline {
            tokio::select! {
                read = stdout.read(&mut buf) => {
                    let n = read?;
                    if n == 0 {
                        break;
                    }
                    let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                    stdout_bytes.extend_from_slice(&buf[..n]);
                    drain_progress_stream(&chunk, &mut pending, &mut last_percent, on_progress);
                    if let Some(idle_timeout) = idle_timeout {
                        idle_deadline = Some(Instant::now() + idle_timeout);
                    }
                }
                _ = sleep_until(deadline) => {
                    let _ = child.kill().await;
                    return Err(match (total_deadline, idle_deadline) {
                        (Some(total_deadline), Some(idle_deadline)) if total_deadline <= idle_deadline => {
                            SevenZipError::Timeout {
                                suffix: total_timeout_suffix.clone().unwrap_or_default(),
                            }
                        }
                        (Some(_), Some(_)) => SevenZipError::IdleTimeout {
                            suffix: idle_timeout_suffix.clone().unwrap_or_default(),
                        },
                        (Some(_), None) => SevenZipError::Timeout {
                            suffix: total_timeout_suffix.clone().unwrap_or_default(),
                        },
                        (None, Some(_)) => SevenZipError::IdleTimeout {
                            suffix: idle_timeout_suffix.clone().unwrap_or_default(),
                        },
                        (None, None) => unreachable!("deadline fired without timeout"),
                    });
                }
            }
        } else {
            let n = stdout.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
            stdout_bytes.extend_from_slice(&buf[..n]);
            drain_progress_stream(&chunk, &mut pending, &mut last_percent, on_progress);
            if let Some(idle_timeout) = idle_timeout {
                idle_deadline = Some(Instant::now() + idle_timeout);
            }
        }
    }

    let status = child.wait().await?;
    let stderr_bytes = stderr_handle.await.unwrap_or_default();
    Ok(SevenZipOutput {
        status,
        stdout: String::from_utf8_lossy(&stdout_bytes).to_string(),
        stderr: String::from_utf8_lossy(&stderr_bytes).to_string(),
    })
}

fn parse_list_output(stdout: &str) -> Vec<ArchiveEntry> {
    let mut entries = Vec::new();
    let mut current = ArchiveEntry::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            if !current.is_empty() {
                entries.push(current);
                current = ArchiveEntry::new();
            }
            continue;
        }
        if let Some((key, value)) = line.split_once(" = ") {
            current.insert(key.to_string(), value.to_string());
        }
    }
    if !current.is_empty() {
        entries.push(current);
    }
    entries
}

pub async fn run_7z_list(
    path: impl AsRef<Path>,
    archive_type: Option<&str>,
    timeout: Option<Duration>,
) -> Result<(Vec<ArchiveEntry>, String, i32), SevenZipError> {
    let mut args = vec!["l".to_string(), "-slt".to_string()];
    if let Some(archive_type) = archive_type {
        args.push(format!("-t{archive_type}"));
    }
    args.push(path.as_ref().to_string_lossy().to_string());
    let output = run_7z_capture(&args, None, timeout).await?;
    let code = output.status.code().unwrap_or(-1);
    if code != 0 {
        let error = format!("{}\n{}", output.stderr, output.stdout)
            .trim()
            .to_string();
        return Ok((Vec::new(), error, code));
    }
    Ok((parse_list_output(&output.stdout), String::new(), code))
}

pub async fn probe_archive(
    path: impl AsRef<Path>,
    timeout: Option<Duration>,
) -> Result<(Option<String>, bool, Option<Vec<ArchiveEntry>>), SevenZipError> {
    let (entries, error, code) = run_7z_list(path, None, timeout).await?;
    if code != 0 || entries.is_empty() {
        let lowered = error.to_lowercase();
        if lowered.contains("password") || lowered.contains("encrypted") {
            return Ok((None, true, None));
        }
        return Ok((None, false, None));
    }
    let archive_type = entries[0].get("Type").map(|value| value.to_lowercase());
    let encrypted = entries.iter().any(|entry| {
        entry
            .get("Encrypted")
            .map(|value| value == "+")
            .unwrap_or(false)
    });
    Ok((archive_type, encrypted, Some(entries)))
}

pub fn archive_entries_unpacked_bytes(entries: Option<&[ArchiveEntry]>) -> Option<u64> {
    let entries = entries?;
    let mut total = 0u64;
    for entry in entries {
        if entry.contains_key("Type") {
            continue;
        }
        if !entry.contains_key("Path") {
            continue;
        }
        if entry
            .get("Folder")
            .map(|value| value == "+")
            .unwrap_or(false)
        {
            continue;
        }
        let size = entry.get("Size")?.parse::<i64>().ok()?;
        if size < 0 {
            return None;
        }
        total += size as u64;
    }
    Some(total)
}

pub fn archive_entries_packed_bytes(entries: Option<&[ArchiveEntry]>) -> Option<u64> {
    let entries = entries?;
    let mut total = 0u64;
    for entry in entries {
        if entry.contains_key("Type") {
            continue;
        }
        if !entry.contains_key("Path") {
            continue;
        }
        if entry
            .get("Folder")
            .map(|value| value == "+")
            .unwrap_or(false)
        {
            continue;
        }
        let size = entry.get("Packed Size")?.parse::<i64>().ok()?;
        if size < 0 {
            return None;
        }
        total += size as u64;
    }
    Some(total)
}

pub fn zip_uses_deflated_or_better(entries: Option<&[ArchiveEntry]>) -> bool {
    let Some(entries) = entries else {
        return false;
    };
    for entry in entries {
        if entry.contains_key("Type") {
            continue;
        }
        if !entry.contains_key("Path") {
            continue;
        }
        if entry
            .get("Folder")
            .map(|value| value == "+")
            .unwrap_or(false)
        {
            continue;
        }
        if entry
            .get("Encrypted")
            .map(|value| value == "+")
            .unwrap_or(false)
        {
            return false;
        }
        let method = entry
            .get("Method")
            .map(|value| value.to_lowercase())
            .unwrap_or_default();
        if method.is_empty() {
            return false;
        }
        if method.contains("deflate")
            || method.contains("lzma")
            || method.contains("bzip2")
            || method.contains("ppmd")
            || method.contains("store")
        {
            continue;
        }
        return false;
    }

    let unpacked_bytes = archive_entries_unpacked_bytes(Some(entries));
    let packed_bytes = archive_entries_packed_bytes(Some(entries));
    let (Some(unpacked_bytes), Some(packed_bytes)) = (unpacked_bytes, packed_bytes) else {
        return false;
    };
    if unpacked_bytes == 0 {
        return true;
    }
    if packed_bytes >= unpacked_bytes {
        return false;
    }
    let savings_ratio = (unpacked_bytes - packed_bytes) as f64 / unpacked_bytes as f64;
    savings_ratio >= ZIP_MIN_COMPRESSION_SAVINGS_RATIO
}

fn find_single_tar(dest_dir: impl AsRef<Path>) -> Option<PathBuf> {
    let mut entries = std::fs::read_dir(dest_dir)
        .ok()?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>();
    if entries.len() != 1 {
        return None;
    }
    let entry = entries.pop()?;
    let path = entry.path();
    if path.is_file()
        && path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("tar"))
            .unwrap_or(false)
    {
        Some(path)
    } else {
        None
    }
}

pub async fn zip_dir_with_level(
    src_dir: impl AsRef<Path>,
    dest_zip_path: impl AsRef<Path>,
    compresslevel: u32,
    on_progress: Option<&(dyn Fn(u32) + Send + Sync)>,
    timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
) -> Result<(), SevenZipError> {
    let src_dir = src_dir.as_ref().to_path_buf();
    let dest_zip_path = dest_zip_path.as_ref().to_path_buf();
    if let Some(parent) = dest_zip_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if dest_zip_path.exists() {
        std::fs::remove_file(&dest_zip_path)?;
    }
    let args = vec![
        "a".to_string(),
        "-tzip".to_string(),
        "-mm=Deflate".to_string(),
        format!("-mx={compresslevel}"),
        "-mmt=on".to_string(),
        "-bb0".to_string(),
        "-bso0".to_string(),
        "-bsp1".to_string(),
        dest_zip_path.to_string_lossy().to_string(),
        ".".to_string(),
    ];
    let output =
        run_7z_with_progress(&args, Some(&src_dir), on_progress, timeout, idle_timeout).await?;
    if !output.status.success() {
        let error = if output.stderr.trim().is_empty() {
            output.stdout.trim()
        } else {
            output.stderr.trim()
        };
        return Err(SevenZipError::CommandFailed(error.to_string()));
    }
    Ok(())
}

pub async fn safe_extract_archive(
    archive_path: impl AsRef<Path>,
    dest_dir: impl AsRef<Path>,
    entries: Option<&[ArchiveEntry]>,
    on_progress: Option<&(dyn Fn(u32) + Send + Sync)>,
    timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
) -> Result<(), SevenZipError> {
    let mut archive_path = archive_path.as_ref().to_path_buf();
    let dest_dir = dest_dir.as_ref().to_path_buf();
    let mut entries = entries.map(|entries| entries.to_vec());
    let mut remove_after_extract: Option<PathBuf> = None;

    loop {
        let current_entries = if let Some(entries) = entries.as_ref() {
            entries.as_slice()
        } else {
            let (listed_entries, error, code) = run_7z_list(&archive_path, None, timeout).await?;
            if code != 0 || listed_entries.is_empty() {
                let lowered = error.to_lowercase();
                if lowered.contains("password") || lowered.contains("encrypted") {
                    return Err(SevenZipError::CommandFailed(
                        "Encrypted archive entries are not supported".to_string(),
                    ));
                }
                return Err(SevenZipError::CommandFailed(if error.trim().is_empty() {
                    "Invalid archive".to_string()
                } else {
                    error
                }));
            }
            entries = Some(listed_entries);
            entries.as_ref().unwrap().as_slice()
        };

        for entry in current_entries {
            if entry
                .get("Encrypted")
                .map(|value| value == "+")
                .unwrap_or(false)
            {
                return Err(SevenZipError::CommandFailed(
                    "Encrypted archive entries are not supported".to_string(),
                ));
            }
            if entry.contains_key("Type") {
                continue;
            }
            let name = entry
                .get("Path")
                .cloned()
                .unwrap_or_default()
                .replace('\\', "/");
            if name.is_empty() {
                continue;
            }
            let _ = safe_path(&dest_dir, &name).map_err(SevenZipError::Io)?;
        }

        std::fs::create_dir_all(&dest_dir)?;
        let args = vec![
            "x".to_string(),
            format!("-o{}", dest_dir.to_string_lossy()),
            "-y".to_string(),
            "-bb0".to_string(),
            "-bso0".to_string(),
            "-bsp1".to_string(),
            archive_path.to_string_lossy().to_string(),
        ];
        let output = run_7z_with_progress(&args, None, on_progress, timeout, idle_timeout).await?;
        if !output.status.success() {
            let error = if output.stderr.trim().is_empty() {
                output.stdout.trim()
            } else {
                output.stderr.trim()
            };
            return Err(SevenZipError::CommandFailed(error.to_string()));
        }

        if let Some(path) = remove_after_extract.take() {
            std::fs::remove_file(path)?;
        }

        let archive_type = current_entries[0]
            .get("Type")
            .map(|value| value.to_lowercase())
            .unwrap_or_default();
        if ["gzip", "bzip2", "xz"].contains(&archive_type.as_str()) {
            if let Some(tar_path) = find_single_tar(&dest_dir) {
                remove_after_extract = Some(tar_path.clone());
                archive_path = tar_path;
                entries = None;
                continue;
            }
        }
        break;
    }
    Ok(())
}

fn timeout_suffix(timeout: Duration) -> String {
    format!(" after {}s", timeout.as_secs_f64())
}
