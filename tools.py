"""Compatibility facade for storage utility helpers."""

from storage_service.archive_tools import (
    archive_entries_packed_bytes,
    archive_entries_unpacked_bytes,
    ensure_7z_available,
    probe_archive,
    safe_extract_archive,
    zip_dir_with_level,
    zip_uses_deflated_or_better,
)
from storage_service.auth_tools import (
    check_token,
    decode_transfer_jwt,
    encode_transfer_jwt,
    is_safe_job_id,
)
from storage_service.file_tools import (
    build_download_filename,
    copy_fileobj_to_path,
    is_allowed_type,
    is_allowed_upload_type,
    normalize_file_kind,
    safe_path,
    sanitize_filename,
)
from storage_service.image_tools import image_bytes_to_webp, image_file_to_webp


__all__ = [
    "archive_entries_packed_bytes",
    "archive_entries_unpacked_bytes",
    "build_download_filename",
    "check_token",
    "copy_fileobj_to_path",
    "decode_transfer_jwt",
    "encode_transfer_jwt",
    "ensure_7z_available",
    "image_bytes_to_webp",
    "image_file_to_webp",
    "is_allowed_type",
    "is_allowed_upload_type",
    "is_safe_job_id",
    "normalize_file_kind",
    "probe_archive",
    "safe_extract_archive",
    "safe_path",
    "sanitize_filename",
    "zip_dir_with_level",
    "zip_uses_deflated_or_better",
]
