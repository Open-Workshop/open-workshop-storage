import os
import re
import shutil
from typing import Optional, Any
from datetime import datetime, timedelta, timezone
from zipfile import ZipFile, ZIP_DEFLATED, ZIP_LZMA, ZIP_BZIP2, ZIP_STORED, BadZipFile, is_zipfile
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
    compresslevel: int = 3,
) -> None:
    with ZipFile(dest_zip_path, "w", compression=ZIP_DEFLATED, compresslevel=compresslevel) as zipped:
        zipped.write(src_path, arcname)


def zip_dir_with_level(src_dir: str, dest_zip_path: str, compresslevel: int = 3) -> None:
    src_dir = os.path.abspath(src_dir)
    with ZipFile(dest_zip_path, "w", compression=ZIP_DEFLATED, compresslevel=compresslevel) as zipped:
        for root, dirs, files in os.walk(src_dir):
            rel_root = os.path.relpath(root, src_dir)
            rel_root = "" if rel_root == "." else rel_root

            if not files and not dirs:
                arc_dir = rel_root + "/" if rel_root else ""
                if arc_dir:
                    zipped.writestr(arc_dir, "")
                continue

            for filename in files:
                abs_path = os.path.join(root, filename)
                arcname = os.path.join(rel_root, filename) if rel_root else filename
                zipped.write(abs_path, arcname)


def is_zip_file(path: str) -> bool:
    return os.path.isfile(path) and is_zipfile(path)


def zip_uses_deflated_or_better(path: str) -> bool:
    try:
        with ZipFile(path, "r") as zipped:
            for info in zipped.infolist():
                if info.is_dir():
                    continue
                if info.flag_bits & 0x1:
                    return False
                if info.compress_type in (ZIP_DEFLATED, ZIP_LZMA, ZIP_BZIP2):
                    continue
                if info.compress_type == ZIP_STORED and info.file_size == 0:
                    continue
                return False
        return True
    except BadZipFile:
        return False


def zip_has_encrypted(path: str) -> bool:
    try:
        with ZipFile(path, "r") as zipped:
            for info in zipped.infolist():
                if info.flag_bits & 0x1:
                    return True
        return False
    except BadZipFile:
        return False


def safe_extract_zip(zip_path: str, dest_dir: str) -> None:
    dest_dir = os.path.abspath(dest_dir)
    with ZipFile(zip_path, "r") as zipped:
        for info in zipped.infolist():
            if info.flag_bits & 0x1:
                raise ValueError("Encrypted zip entries are not supported")
            name = info.filename.replace("\\", "/")
            target_path = os.path.abspath(os.path.join(dest_dir, name))
            if os.path.commonpath([target_path, dest_dir]) != dest_dir:
                raise ValueError("Unsafe path in zip")
            if info.is_dir():
                os.makedirs(target_path, exist_ok=True)
                continue
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with zipped.open(info, "r") as src, open(target_path, "wb") as dst:
                shutil.copyfileobj(src, dst)


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
