use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

pub const ALLOWED_FILENAME_CHARS: &str =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
pub const ALLOWED_FILENAME_CHARS_WITH_DOT: &str =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.";
pub const ALLOWED_TYPES: &[&str] = &["archive", "resource", "avatar"];
pub const ALLOWED_UPLOAD_TYPES: &[&str] = &["resource", "avatar"];
pub const ALLOWED_FILE_KINDS: &[&str] = &["img", "bin"];

pub fn safe_path(base_dir: impl AsRef<Path>, rel_path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let base_dir = base_dir.as_ref().canonicalize()?;
    let mut relative = PathBuf::new();
    for component in rel_path.as_ref().components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => relative.push(part),
            Component::ParentDir => {
                if !relative.pop() {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"));
            }
        }
    }
    let target = base_dir.join(relative);
    if !target.starts_with(&base_dir) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"));
    }
    Ok(target)
}

pub fn copy_fileobj_to_path<R: Read>(
    mut reader: R,
    dest_path: impl AsRef<Path>,
) -> io::Result<u64> {
    let mut file = fs::File::create(dest_path)?;
    io::copy(&mut reader, &mut file)
}

pub fn normalize_file_kind(file_kind: impl AsRef<str>, default: &str) -> String {
    let value = file_kind.as_ref().trim().to_lowercase();
    let value = if value.is_empty() {
        default.trim().to_lowercase()
    } else {
        value
    };
    if ALLOWED_FILE_KINDS.contains(&value.as_str()) {
        value
    } else {
        String::new()
    }
}

pub fn is_allowed_type(type_name: impl AsRef<str>) -> bool {
    ALLOWED_TYPES.contains(&type_name.as_ref())
}

pub fn is_allowed_upload_type(type_name: impl AsRef<str>) -> bool {
    ALLOWED_UPLOAD_TYPES.contains(&type_name.as_ref())
}

pub fn build_download_filename(
    requested_name: Option<&str>,
    real_path: impl AsRef<Path>,
) -> Option<String> {
    let requested_name = requested_name?;
    if requested_name.is_empty() {
        return None;
    }
    if !requested_name
        .chars()
        .all(|ch| ALLOWED_FILENAME_CHARS.contains(ch))
    {
        return None;
    }
    let ext = real_path.as_ref().extension()?.to_string_lossy();
    Some(format!("{requested_name}.{ext}"))
}

pub fn sanitize_filename(filename: Option<&str>, default: &str) -> String {
    let Some(filename) = filename else {
        return default.to_string();
    };
    let basename = Path::new(filename)
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_default();
    let mut cleaned = String::new();
    for ch in basename.chars() {
        if ALLOWED_FILENAME_CHARS_WITH_DOT.contains(ch) {
            cleaned.push(ch);
        } else if ch.is_whitespace() {
            cleaned.push('_');
        }
    }
    let cleaned = cleaned
        .trim_matches(|ch| ch == '.' || ch == '_')
        .to_string();
    if cleaned.is_empty() {
        default.to_string()
    } else {
        cleaned.chars().take(128).collect()
    }
}
