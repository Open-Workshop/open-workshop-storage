import os
import re
import shutil
from typing import Optional, Any
from datetime import datetime, timedelta, timezone
from zipfile import ZipFile, ZIP_LZMA
import ow_config as config
import bcrypt
import jwt

ALLOWED_FILENAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
ALLOWED_TYPES = {"archive", "resource", "avatar"}
ALLOWED_UPLOAD_TYPES = {"resource", "avatar"}
ALLOWED_FILENAME_CHARS_WITH_DOT = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.")
TRANSFER_JWT_ALG = "HS256"


def safe_path(base_dir: str, path: str) -> str:
    base_dir = os.path.abspath(base_dir)
    target = os.path.abspath(os.path.join(base_dir, path))
    if os.path.commonpath([target, base_dir]) != base_dir:
        raise ValueError("Invalid path")
    return target


def copy_fileobj_to_path(fileobj, dest_path: str) -> None:
    with open(dest_path, "wb") as buffer:
        shutil.copyfileobj(fileobj, buffer)


def zip_single_file_with_level(
    src_path: str,
    dest_zip_path: str,
    arcname: str,
    compresslevel: int = 9,
) -> None:
    with ZipFile(dest_zip_path, "w", compression=ZIP_LZMA, compresslevel=compresslevel) as zipped:
        zipped.write(src_path, arcname)


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
