import os
import re
import shutil
import subprocess
import tempfile
from typing import Optional, Any
from datetime import datetime, timedelta, timezone
import ow_config as config
import bcrypt
import jwt

ALLOWED_FILENAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
ALLOWED_TYPES = {"archive", "resource", "avatar"}
ALLOWED_UPLOAD_TYPES = {"resource", "avatar"}
ALLOWED_FILENAME_CHARS_WITH_DOT = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.")
TRANSFER_JWT_ALG = "HS256"
SEVEN_ZIP_BIN = "7z"


def safe_path(base_dir: str, path: str) -> str:
    base_dir = os.path.abspath(base_dir)
    target = os.path.abspath(os.path.join(base_dir, path))
    if os.path.commonpath([target, base_dir]) != base_dir:
        raise ValueError("Invalid path")
    return target


def copy_fileobj_to_path(fileobj, dest_path: str) -> None:
    with open(dest_path, "wb") as buffer:
        shutil.copyfileobj(fileobj, buffer)


def ensure_7z_available() -> None:
    if shutil.which(SEVEN_ZIP_BIN) is None:
        raise RuntimeError("7z binary is required but not found in PATH")


def _run_7z(args: list[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
    ensure_7z_available()
    return subprocess.run(
        [SEVEN_ZIP_BIN, *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )


def _list_7z_entries(zip_path: str) -> Optional[list[dict[str, str]]]:
    result = _run_7z(["l", "-slt", "-tzip", zip_path])
    if result.returncode != 0:
        return None
    entries: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            if current:
                entries.append(current)
                current = {}
            continue
        if " = " in line:
            key, value = line.split(" = ", 1)
            current[key] = value
    if current:
        entries.append(current)
    if entries and entries[0].get("Type") == "zip":
        entries = entries[1:]
    return entries


def zip_single_file_with_level(
    src_path: str,
    dest_zip_path: str,
    arcname: str,
    compresslevel: int = 3,
) -> None:
    os.makedirs(os.path.dirname(dest_zip_path), exist_ok=True)
    if os.path.exists(dest_zip_path):
        os.remove(dest_zip_path)
    src_dir = os.path.dirname(src_path) or "."
    src_name = os.path.basename(src_path)
    if arcname == src_name:
        result = _run_7z(
            [
                "a",
                "-tzip",
                "-mm=Deflate",
                f"-mx={compresslevel}",
                "-mmt=on",
                dest_zip_path,
                src_name,
            ],
            cwd=src_dir,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "7z failed to create zip")
        return
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = os.path.join(temp_dir, arcname)
        os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
        shutil.copy2(src_path, tmp_path)
        zip_dir_with_level(temp_dir, dest_zip_path, compresslevel)


def zip_dir_with_level(src_dir: str, dest_zip_path: str, compresslevel: int = 3) -> None:
    src_dir = os.path.abspath(src_dir)
    os.makedirs(os.path.dirname(dest_zip_path), exist_ok=True)
    if os.path.exists(dest_zip_path):
        os.remove(dest_zip_path)
    result = _run_7z(
        [
            "a",
            "-tzip",
            "-mm=Deflate",
            f"-mx={compresslevel}",
            "-mmt=on",
            dest_zip_path,
            ".",
        ],
        cwd=src_dir,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "7z failed to create zip")


def is_zip_file(path: str) -> bool:
    if not os.path.isfile(path):
        return False
    try:
        with open(path, "rb") as handle:
            signature = handle.read(4)
        return signature in (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
    except OSError:
        return False


def zip_uses_deflated_or_better(path: str) -> bool:
    entries = _list_7z_entries(path)
    if not entries:
        return False

    for entry in entries:
        if entry.get("Folder") == "+":
            continue
        if entry.get("Encrypted") == "+":
            return False
        method = (entry.get("Method") or "").lower()
        if not method:
            return False
        if "deflate" in method:
            continue
        if "lzma" in method or "bzip2" in method or "ppmd" in method:
            continue
        if "store" in method:
            try:
                size = int(entry.get("Size", "0"))
            except ValueError:
                size = 0
            if size == 0:
                continue
        return False
    return True


def zip_has_encrypted(path: str) -> bool:
    entries = _list_7z_entries(path)
    if entries is None:
        return True
    for entry in entries:
        if entry.get("Encrypted") == "+":
            return True
    return False


def safe_extract_zip(zip_path: str, dest_dir: str) -> None:
    dest_dir = os.path.abspath(dest_dir)
    entries = _list_7z_entries(zip_path)
    if entries is None:
        raise ValueError("Invalid zip archive")
    for entry in entries:
        if entry.get("Encrypted") == "+":
            raise ValueError("Encrypted zip entries are not supported")
        name = (entry.get("Path") or "").replace("\\", "/")
        if not name:
            continue
        target_path = os.path.abspath(os.path.join(dest_dir, name))
        if os.path.commonpath([target_path, dest_dir]) != dest_dir:
            raise ValueError("Unsafe path in zip")
    os.makedirs(dest_dir, exist_ok=True)
    result = _run_7z(["x", "-tzip", f"-o{dest_dir}", "-y", zip_path])
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "7z failed to extract zip")


def is_allowed_type(type_name: str) -> bool:
    return type_name in ALLOWED_TYPES


def is_allowed_upload_type(type_name: str) -> bool:
    return type_name in ALLOWED_UPLOAD_TYPES


def build_download_filename(requested_name: Optional[str], real_path: str) -> Optional[str]:
    if not requested_name:
        return None
    # Разрешены только латиница/цифры/_/-, без точек и пробелов
    for ch in requested_name:
        if ch not in ALLOWED_FILENAME_CHARS:
            return None
    ext = os.path.splitext(real_path)[1]
    return requested_name + ext


def check_token(token_name: str, token: str) -> bool:
    # Получаем значение хеша токена из config по имени token_name
    stored_token_hash = getattr(config, token_name, None)
    
    if stored_token_hash is None:
        print(f"Токен `{token_name}` не найден в config!")
        return False
    
    # Хеш из config должен быть строкой, конвертируем в байты
    stored_token_hash = stored_token_hash.encode()
    
    # Хешируем переданный токен с использованием bcrypt и проверяем соответствие
    return bcrypt.checkpw(token.encode(), stored_token_hash)


def is_safe_job_id(job_id: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{8,128}", job_id or ""))


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


def decode_transfer_jwt(token: str, audience: str) -> Optional[dict[str, Any]]:
    secret = getattr(config, "TRANSFER_JWT_SECRET", None)
    if not secret:
        return None
    try:
        return jwt.decode(
            token,
            secret,
            algorithms=[TRANSFER_JWT_ALG],
            audience=audience,
        )
    except jwt.PyJWTError:
        return None


def encode_transfer_jwt(payload: dict[str, Any], audience: str, ttl_seconds: int) -> Optional[str]:
    secret = getattr(config, "TRANSFER_JWT_SECRET", None)
    if not secret:
        return None
    now = datetime.now(timezone.utc)
    payload = dict(payload)
    payload.update(
        {
            "aud": audience,
            "iss": "storage",
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(seconds=ttl_seconds)).timestamp()),
        }
    )
    return jwt.encode(payload, secret, algorithm=TRANSFER_JWT_ALG)
