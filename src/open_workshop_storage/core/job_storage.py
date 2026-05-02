from __future__ import annotations

import asyncio
import json
import logging
import uuid
from contextlib import suppress
from typing import Any, Awaitable, Callable

from .job_state import JobSnapshot, new_job_state

JsonDict = dict[str, Any]


def _snapshot_state(state: JobSnapshot) -> JobSnapshot:
    snapshot = dict(state)
    snapshot.pop("clients", None)
    return snapshot


def _normalize_state(state: JobSnapshot | None) -> JobSnapshot | None:
    if state is None:
        return None

    snapshot = new_job_state()
    snapshot.update({key: value for key, value in state.items() if key != "clients"})
    clients = state.get("clients", [])
    snapshot["clients"] = list(clients) if isinstance(clients, list) else list(clients or [])
    return snapshot


class JobStorage:
    def __init__(
        self,
        *,
        state_cache: dict[str, JobSnapshot],
        meta_cache: dict[str, JsonDict],
        logger: logging.Logger,
        redis_url: str | None = None,
        redis_prefix: str = "open-workshop-storage",
    ) -> None:
        self.state_cache = state_cache
        self.meta_cache = meta_cache
        self.logger = logger
        self.redis_prefix = redis_prefix or "open-workshop-storage"
        self.instance_id = uuid.uuid4().hex
        self._lock = asyncio.Lock()
        self._redis = None
        self.supports_pubsub = False
        if redis_url:
            self._redis = self._create_redis_client(redis_url)
            self.supports_pubsub = True

    @staticmethod
    def _create_redis_client(redis_url: str) -> Any:
        try:
            from redis.asyncio import Redis  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover - dependency resolution
            raise RuntimeError("redis package is required when REDIS_URL is configured") from exc

        return Redis.from_url(redis_url, decode_responses=True)

    def _state_key(self, job_id: str) -> str:
        return f"{self.redis_prefix}:jobs:{job_id}:state"

    def _meta_key(self, job_id: str) -> str:
        return f"{self.redis_prefix}:jobs:{job_id}:meta"

    def _active_jobs_key(self) -> str:
        return f"{self.redis_prefix}:jobs:active"

    def _events_channel(self) -> str:
        return f"{self.redis_prefix}:jobs:events"

    @property
    def redis(self) -> Any | None:
        return self._redis

    async def close(self) -> None:
        if self._redis is None:
            return
        await self._redis.aclose()

    async def _read_remote_state(self, job_id: str) -> JobSnapshot | None:
        if self._redis is None:
            return None
        raw_state = await self._redis.get(self._state_key(job_id))
        if not raw_state:
            return None
        try:
            loaded = json.loads(raw_state)
        except Exception:
            self.logger.warning("failed to decode redis state for job_id=%s", job_id)
            return None
        return _normalize_state(loaded)

    async def _write_remote_state(self, job_id: str, state: JobSnapshot) -> None:
        if self._redis is None:
            return
        await self._redis.set(self._state_key(job_id), json.dumps(_snapshot_state(state), ensure_ascii=True))
        await self._redis.sadd(self._active_jobs_key(), job_id)

    async def _delete_remote_state(self, job_id: str) -> None:
        if self._redis is None:
            return
        await self._redis.delete(self._state_key(job_id), self._meta_key(job_id))
        await self._redis.srem(self._active_jobs_key(), job_id)

    async def _read_remote_meta(self, job_id: str) -> JsonDict | None:
        if self._redis is None:
            return None
        raw_meta = await self._redis.get(self._meta_key(job_id))
        if not raw_meta:
            return None
        try:
            loaded = json.loads(raw_meta)
        except Exception:
            self.logger.warning("failed to decode redis meta for job_id=%s", job_id)
            return None
        return loaded if isinstance(loaded, dict) else None

    async def _write_remote_meta(self, job_id: str, data: JsonDict) -> None:
        if self._redis is None:
            return
        await self._redis.set(self._meta_key(job_id), json.dumps(data, ensure_ascii=True))

    async def read_state(self, job_id: str) -> JobSnapshot | None:
        remote_state = await self._read_remote_state(job_id)
        if remote_state is not None:
            async with self._lock:
                local_clients = self.state_cache.get(job_id, {}).get("clients", [])
                remote_state["clients"] = list(local_clients) if isinstance(local_clients, list) else list(local_clients or [])
                self.state_cache[job_id] = remote_state
                return remote_state

        async with self._lock:
            local_state = self.state_cache.get(job_id)
            if local_state is None:
                return None
            normalized = _normalize_state(local_state)
            if normalized is not None:
                self.state_cache[job_id] = normalized
            return normalized

    async def save_state(self, job_id: str, state: JobSnapshot | None = None) -> JobSnapshot:
        async with self._lock:
            current = state if state is not None else self.state_cache.get(job_id)
            if current is None:
                current = new_job_state()
            normalized = _normalize_state(current)
            if normalized is None:
                normalized = new_job_state()
            self.state_cache[job_id] = normalized
        await self._write_remote_state(job_id, normalized)
        return normalized

    async def update_state(self, job_id: str, updates: dict[str, Any]) -> JobSnapshot:
        async with self._lock:
            current = self.state_cache.get(job_id)
            if current is None:
                current = new_job_state()
            current.update(updates)
            normalized = _normalize_state(current)
            assert normalized is not None
            self.state_cache[job_id] = normalized
        await self._write_remote_state(job_id, normalized)
        return normalized

    async def read_meta(self, job_id: str) -> JsonDict | None:
        remote_meta = await self._read_remote_meta(job_id)
        if remote_meta is not None:
            async with self._lock:
                self.meta_cache[job_id] = dict(remote_meta)
                return dict(remote_meta)

        async with self._lock:
            local_meta = self.meta_cache.get(job_id)
            return dict(local_meta) if isinstance(local_meta, dict) else None

    async def write_meta(self, job_id: str, data: JsonDict) -> None:
        snapshot = dict(data)
        async with self._lock:
            self.meta_cache[job_id] = snapshot
        await self._write_remote_meta(job_id, snapshot)

    async def delete_job(self, job_id: str) -> None:
        async with self._lock:
            self.state_cache.pop(job_id, None)
            self.meta_cache.pop(job_id, None)
        await self._delete_remote_state(job_id)

    async def list_job_ids(self) -> list[str]:
        if self._redis is None:
            async with self._lock:
                return list(self.state_cache.keys())
        job_ids = await self._redis.smembers(self._active_jobs_key())
        return sorted(str(job_id) for job_id in job_ids)

    async def publish_event(self, job_id: str, message: JsonDict) -> None:
        if self._redis is None:
            return
        payload = {
            "kind": "event",
            "job_id": job_id,
            "origin_id": self.instance_id,
            "message": message,
            "state": _snapshot_state(self.state_cache.get(job_id, {})),
        }
        await self._redis.publish(self._events_channel(), json.dumps(payload, ensure_ascii=True))

    async def publish_close_clients(self, job_id: str) -> None:
        if self._redis is None:
            return
        payload = {
            "kind": "close_clients",
            "job_id": job_id,
            "origin_id": self.instance_id,
        }
        await self._redis.publish(self._events_channel(), json.dumps(payload, ensure_ascii=True))

    async def publish_delete(self, job_id: str) -> None:
        if self._redis is None:
            return
        payload = {
            "kind": "delete",
            "job_id": job_id,
            "origin_id": self.instance_id,
        }
        await self._redis.publish(self._events_channel(), json.dumps(payload, ensure_ascii=True))

    async def listen(self, callback: Callable[[JsonDict], Awaitable[None]]) -> None:
        if self._redis is None:
            return

        while True:
            pubsub = self._redis.pubsub()
            try:
                await pubsub.subscribe(self._events_channel())
                while True:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message is None:
                        await asyncio.sleep(0.1)
                        continue
                    if message.get("type") != "message":
                        continue
                    data = message.get("data")
                    if not data:
                        continue
                    try:
                        payload = json.loads(data)
                    except Exception:
                        self.logger.warning("failed to decode redis event payload")
                        continue
                    await callback(payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("redis event listener failed")
                await asyncio.sleep(1)
            finally:
                with suppress(Exception):
                    await pubsub.close()


def build_job_storage(
    *,
    state_cache: dict[str, JobSnapshot],
    meta_cache: dict[str, JsonDict],
    logger: logging.Logger,
    redis_url: str | None = None,
    redis_prefix: str = "open-workshop-storage",
) -> JobStorage:
    return JobStorage(
        state_cache=state_cache,
        meta_cache=meta_cache,
        logger=logger,
        redis_url=redis_url,
        redis_prefix=redis_prefix,
    )
