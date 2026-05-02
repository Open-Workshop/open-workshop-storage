from __future__ import annotations

import json
import logging
from typing import Any

from .job_storage import JobStorage, JsonDict


class BlurhashCacheStore:
    def __init__(
        self,
        *,
        storage: JobStorage,
        logger: logging.Logger,
        redis_prefix: str = "open-workshop-storage",
        ttl_seconds: int = 604800,
    ) -> None:
        self.storage = storage
        self.logger = logger
        self.redis_prefix = redis_prefix or "open-workshop-storage"
        try:
            self.ttl_seconds = max(0, int(ttl_seconds))
        except (TypeError, ValueError):
            self.ttl_seconds = 604800

    def _redis(self) -> Any | None:
        return self.storage.redis

    def _key(self, cache_key: str) -> str:
        return f"{self.redis_prefix}:blurhash:{cache_key}"

    async def read(self, cache_key: str) -> JsonDict | None:
        redis = self._redis()
        if redis is None:
            return None

        raw_value = await redis.get(self._key(cache_key))
        if not raw_value:
            return None

        try:
            loaded = json.loads(raw_value)
        except Exception:
            self.logger.warning("failed to decode redis blurhash cache entry cache_key=%s", cache_key)
            return None
        return loaded if isinstance(loaded, dict) else None

    async def write(self, cache_key: str, data: JsonDict) -> None:
        redis = self._redis()
        if redis is None:
            return

        payload = json.dumps(dict(data), ensure_ascii=True)
        key = self._key(cache_key)
        if self.ttl_seconds > 0:
            await redis.set(key, payload, ex=self.ttl_seconds)
        else:
            await redis.set(key, payload)


def build_blurhash_cache_store(
    *,
    storage: JobStorage,
    logger: logging.Logger,
    redis_prefix: str = "open-workshop-storage",
    ttl_seconds: int = 604800,
) -> BlurhashCacheStore:
    return BlurhashCacheStore(
        storage=storage,
        logger=logger,
        redis_prefix=redis_prefix,
        ttl_seconds=ttl_seconds,
    )
