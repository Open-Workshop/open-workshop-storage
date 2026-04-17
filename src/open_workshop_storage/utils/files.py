from __future__ import annotations

import os
import shutil
from typing import Any, Optional

ALLOWED_FILENAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
ALLOWED_TYPES = {"archive", "resource", "avatar"}
ALLOWED_UPLOAD_TYPES = {"resource", "avatar"}
ALLOWED_FILE_KINDS = {"img", "bin"}
ALLOWED_FILENAME_CHARS_WITH_DOT = ALLOWED_FILENAME_CHARS | {"."}


def safe_path(base_dir: str, path: str) -> str:
    base_dir = os.path.abspath(base_dir)
    target = os.path.abspath(os.path.join(base_dir, path))
    if os.path.commonpath([target, base_dir]) != base_dir:
        raise ValueError("Invalid path")
    return target


def copy_fileobj_to_path(fileobj: Any, dest_path: str) -> None:
    with open(dest_path, "wb") as buffer:
        shutil.copyfileobj(fileobj, buffer)


def normalize_file_kind(file_kind: Any, default: str = "bin") -> str:
    value = str(file_kind or default).strip().lower()
    return value if value in ALLOWED_FILE_KINDS else ""


def is_allowed_type(type_name: str) -> bool:
    return type_name in ALLOWED_TYPES


def is_allowed_upload_type(type_name: str) -> bool:
    return type_name in ALLOWED_UPLOAD_TYPES


def build_download_filename(requested_name: Optional[str], real_path: str) -> Optional[str]:
    if not requested_name:
        return None
    for ch in requested_name:
        if ch not in ALLOWED_FILENAME_CHARS:
            return None
    ext = os.path.splitext(real_path)[1]
    return requested_name + ext


def sanitize_filename(filename: Optional[str], default: str = "file.bin") -> str:
    if not filename:
        return default
    filename = os.path.basename(filename)
    cleaned = []
    for ch in filename:
        if ch in ALLOWED_FILENAME_CHARS_WITH_DOT:
            cleaned.append(ch)
        elif ch.isspace():
            cleaned.append("_")
    cleaned_name = "".join(cleaned).strip("._")
    if not cleaned_name:
        return default
    return cleaned_name[:128]
