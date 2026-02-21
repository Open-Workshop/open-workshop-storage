import os
import re
import shutil
import subprocess
from io import BytesIO
from typing import Optional, Any
from datetime import datetime, timedelta, timezone
import ow_config as config
import bcrypt
import jwt
from PIL import Image, UnidentifiedImageError

ALLOWED_FILENAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
ALLOWED_TYPES = {"archive", "resource", "avatar"}
ALLOWED_UPLOAD_TYPES = {"resource", "avatar"}
ALLOWED_FILE_KINDS = {"img", "bin"}
ALLOWED_FILENAME_CHARS_WITH_DOT = ALLOWED_FILENAME_CHARS | {"."}
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


def _run_7z_list(
    path: str, archive_type: Optional[str] = None
) -> tuple[Optional[list[dict[str, str]]], str, int]:
    args = ["l", "-slt"]
    if archive_type:
        args.append(f"-t{archive_type}")
    args.append(path)
    result = _run_7z(args)
    if result.returncode != 0:
        output = (result.stderr or "") + "\n" + (result.stdout or "")
        return None, output.strip(), result.returncode
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
    return entries, "", 0


def _list_7z_entries(path: str, archive_type: Optional[str] = None) -> Optional[list[dict[str, str]]]:
    entries, _, code = _run_7z_list(path, archive_type=archive_type)
    if code != 0:
        return None
    return entries


def probe_archive(path: str) -> tuple[Optional[str], bool, Optional[list[dict[str, str]]]]:
    entries, error, code = _run_7z_list(path)
    if code != 0 or not entries:
        lowered = error.lower()
        if "password" in lowered or "encrypted" in lowered:
            return None, True, None
        return None, False, None
    archive_type = entries[0].get("Type")
    archive_type = archive_type.lower() if archive_type else None
    encrypted = False
    for entry in entries:
        if entry.get("Encrypted") == "+":
            encrypted = True
            break
    return archive_type, encrypted, entries

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


def zip_uses_deflated_or_better(
    path: str, entries: Optional[list[dict[str, str]]] = None
) -> bool:
    entries = entries or _list_7z_entries(path, archive_type="zip")
    if not entries:
        return False

    for entry in entries:
        if entry.get("Type"):
            continue
        if not entry.get("Path"):
            continue
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


def _find_single_tar(dest_dir: str) -> Optional[str]:
    entries = os.listdir(dest_dir)
    if len(entries) != 1:
        return None
    path = os.path.join(dest_dir, entries[0])
    if os.path.isfile(path) and entries[0].lower().endswith(".tar"):
        return path
    return None


def safe_extract_archive(
    archive_path: str,
    dest_dir: str,
    entries: Optional[list[dict[str, str]]] = None,
) -> None:
    dest_dir = os.path.abspath(dest_dir)
    entries = entries or _list_7z_entries(archive_path)
    if entries is None:
        raise ValueError("Invalid archive")
    for entry in entries:
        if entry.get("Encrypted") == "+":
            raise ValueError("Encrypted archive entries are not supported")
        if entry.get("Type"):
            continue
        name = (entry.get("Path") or "").replace("\\", "/")
        if not name:
            continue
        target_path = os.path.abspath(os.path.join(dest_dir, name))
        if os.path.commonpath([target_path, dest_dir]) != dest_dir:
            raise ValueError("Unsafe path in archive")
    os.makedirs(dest_dir, exist_ok=True)
    result = _run_7z(["x", f"-o{dest_dir}", "-y", archive_path])
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "7z failed to extract archive")

    archive_type = (entries[0].get("Type") or "").lower()
    if archive_type in {"gzip", "bzip2", "xz"}:
        tar_path = _find_single_tar(dest_dir)
        if tar_path:
            safe_extract_archive(tar_path, dest_dir)
            os.remove(tar_path)


def normalize_file_kind(file_kind: Any, default: str = "bin") -> str:
    value = str(file_kind or default).strip().lower()
    return value if value in ALLOWED_FILE_KINDS else ""


def image_bytes_to_webp(data: bytes, quality: int = 80) -> bytes:
    try:
        with Image.open(BytesIO(data)) as img:
            img.load()
            if img.mode in ("RGBA", "LA") or (
                img.mode == "P" and "transparency" in img.info
            ):
                img = img.convert("RGBA")
            else:
                img = img.convert("RGB")

            out = BytesIO()
            img.save(out, format="WEBP", quality=quality, method=6)
            return out.getvalue()
    except (UnidentifiedImageError, OSError) as exc:
        raise ValueError("not an image") from exc


def image_file_to_webp(src_path: str, dst_path: str, quality: int = 80) -> None:
    with open(src_path, "rb") as src_file:
        data = src_file.read()
    converted = image_bytes_to_webp(data, quality=quality)
    with open(dst_path, "wb") as dst_file:
        dst_file.write(converted)




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
