from __future__ import annotations

import hashlib
import json
from typing import Any, Optional


def build_blurhash_cache_key(
    real_path: str,
    mtime_ns: int,
    size: int,
    components_x: int = 4,
    components_y: int = 3,
    max_dimension: int = 64,
) -> str:
    payload = {
        "path": real_path,
        "mtime_ns": int(mtime_ns),
        "size": int(size),
        "components_x": int(components_x),
        "components_y": int(components_y),
        "max_dimension": int(max_dimension),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def encode_blurhash_cache_value(blurhash: str, width: int, height: int) -> dict[str, Any]:
    return {
        "blurhash": blurhash,
        "width": int(width),
        "height": int(height),
    }


def decode_blurhash_cache_value(data: dict[str, Any]) -> Optional[tuple[str, int, int]]:
    try:
        blurhash = str(data["blurhash"])
        width = int(data["width"])
        height = int(data["height"])
    except (KeyError, TypeError, ValueError):
        return None
    if not blurhash:
        return None
    return blurhash, width, height
