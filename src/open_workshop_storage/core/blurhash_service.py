from __future__ import annotations

import anyio
from typing import Callable, Optional

from .blurhash_cache import build_blurhash_cache_key, decode_blurhash_cache_value, encode_blurhash_cache_value
from .context import ServiceContext


async def get_or_compute_blurhash_for_key(
    ctx: ServiceContext,
    key: tuple[str, int, int],
    compute_fn: Callable[[str, int, int], tuple[str, int, int]],
) -> Optional[tuple[str, int, int]]:
    real_path, mtime_ns, size = key
    cache_key = build_blurhash_cache_key(real_path, mtime_ns, size)
    try:
        cache_data = await ctx.read_blurhash_cache(cache_key)
    except Exception:
        ctx.logger.debug("blurhash redis cache read failed cache_key=%s", cache_key, exc_info=True)
        cache_data = None

    if cache_data is not None:
        cached = decode_blurhash_cache_value(cache_data)
        if cached is not None:
            return cached

    try:
        result = await anyio.to_thread.run_sync(compute_fn, real_path, mtime_ns, size)
    except (OSError, ValueError):
        return None

    try:
        await ctx.write_blurhash_cache(cache_key, encode_blurhash_cache_value(*result))
    except Exception:
        ctx.logger.debug("blurhash redis cache write failed cache_key=%s", cache_key, exc_info=True)
    return result
