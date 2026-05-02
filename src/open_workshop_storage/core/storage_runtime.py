from __future__ import annotations

import asyncio
import os
import shutil
import time
from contextlib import suppress
from typing import Any, Awaitable, Callable, Coroutine

import anyio
from fastapi import FastAPI

from .blurhash_store import BlurhashCacheStore, build_blurhash_cache_store
from .context import JsonDict, ServiceContext
from .job_state import new_job_state, state_event_payload
from .job_storage import JobStorage, build_job_storage


class StorageRuntime:
    def __init__(
        self,
        *,
        config: Any,
        logger: Any,
        tools: Any,
        main_dir: str,
        manager_url: str,
        temp_dir: str,
        redis_url: str | None,
        redis_prefix: str,
        blurhash_cache_ttl_seconds: int,
        upload_limiter: Any,
        download_limiter: Any,
        repack_limiter: Any,
        progress_push_interval: float,
    ) -> None:
        self.config = config
        self.logger = logger
        self.tools = tools
        self.main_dir = main_dir
        self.manager_url = manager_url
        self.temp_dir = temp_dir
        self.upload_limiter = upload_limiter
        self.download_limiter = download_limiter
        self.repack_limiter = repack_limiter
        self.progress_push_interval = progress_push_interval
        self.job_state: dict[str, dict[str, Any]] = {}
        self.job_meta: dict[str, dict[str, Any]] = {}
        self.job_lock = asyncio.Lock()
        self.job_storage: JobStorage = build_job_storage(
            state_cache=self.job_state,
            meta_cache=self.job_meta,
            logger=logger,
            redis_url=redis_url,
            redis_prefix=redis_prefix,
        )
        self.blurhash_cache: BlurhashCacheStore = build_blurhash_cache_store(
            storage=self.job_storage,
            logger=logger,
            redis_prefix=redis_prefix,
            ttl_seconds=blurhash_cache_ttl_seconds,
        )
        self._job_event_listener_task: asyncio.Task[None] | None = None
        self._job_event_listener_users = 0
        self._job_event_listener_lock = asyncio.Lock()

    def _new_job_state(self) -> dict[str, Any]:
        return new_job_state()

    def job_dir(self, job_id: str) -> str:
        return self.tools.safe_path(self.temp_dir, job_id)

    async def read_job_state(self, job_id: str) -> dict[str, Any] | None:
        return await self.job_storage.read_state(job_id)

    async def save_job_state(self, job_id: str, state: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self.job_storage.save_state(job_id, state)

    async def list_job_ids(self) -> list[str]:
        return await self.job_storage.list_job_ids()

    async def read_meta(self, job_id: str) -> dict[str, Any] | None:
        return await self.job_storage.read_meta(job_id)

    async def write_meta(self, job_id: str, data: dict[str, Any]) -> None:
        await self.job_storage.write_meta(job_id, data)

    async def read_blurhash_cache(self, cache_key: str) -> dict[str, Any] | None:
        return await self.blurhash_cache.read(cache_key)

    async def write_blurhash_cache(self, cache_key: str, data: dict[str, Any]) -> None:
        await self.blurhash_cache.write(cache_key, data)

    def read_meta_sync(self, job_id: str) -> dict[str, Any] | None:
        return anyio.run(self.read_meta, job_id)

    def write_meta_sync(self, job_id: str, data: dict[str, Any]) -> None:
        anyio.run(self.write_meta, job_id, data)

    def _state_event_payload(
        self,
        event: str,
        state: dict[str, Any],
        **extra: Any,
    ) -> dict[str, Any]:
        return state_event_payload(event, state, **extra)

    async def build_state_event(self, job_id: str, event: str, **extra: Any) -> dict[str, Any]:
        async with self.job_lock:
            state = self.job_state.setdefault(job_id, self._new_job_state())
            return self._state_event_payload(event, state, **extra)

    async def _deliver_local_message(self, job_id: str, message: dict[str, Any]) -> None:
        async with self.job_lock:
            state = self.job_state.get(job_id)
            if not state:
                return
            clients = list(state.get("clients", []))
        dead_clients: list[Any] = []
        for ws in clients:
            try:
                await ws.send_json(message)
            except Exception:
                dead_clients.append(ws)
                with suppress(Exception):
                    await ws.close()

        if dead_clients:
            async with self.job_lock:
                state = self.job_state.get(job_id)
                if state:
                    state["clients"] = [ws for ws in state.get("clients", []) if ws not in dead_clients]

    async def broadcast(self, job_id: str, message: dict[str, Any]) -> None:
        await self._deliver_local_message(job_id, message)
        await self.job_storage.publish_event(job_id, message)

    async def _close_local_clients(self, job_id: str) -> None:
        async with self.job_lock:
            state = self.job_state.get(job_id)
            if not state:
                return
            clients = list(state.get("clients", []))
            state["clients"] = []
        for ws in clients:
            try:
                await ws.close()
            except Exception:
                pass

    async def close_clients(self, job_id: str) -> None:
        await self._close_local_clients(job_id)
        await self.job_storage.publish_close_clients(job_id)

    async def set_state(self, job_id: str, **updates: Any) -> None:
        state = await self.job_storage.read_state(job_id)
        async with self.job_lock:
            if state is None:
                state = self.job_state.setdefault(job_id, self._new_job_state())
            else:
                state = self.job_state.setdefault(job_id, state)
            updates["last_activity"] = time.time()
            state.update(updates)
        await self.job_storage.save_state(job_id, state)

    async def set_stage(self, job_id: str, stage: str) -> None:
        state = await self.job_storage.read_state(job_id)
        async with self.job_lock:
            if state is None:
                state = self.job_state.setdefault(job_id, self._new_job_state())
            else:
                state = self.job_state.setdefault(job_id, state)
            state["stage"] = stage
            state["percent"] = None
            state["last_activity"] = time.time()
            payload = self._state_event_payload("stage", state)
        await self.job_storage.save_state(job_id, state)
        await self.broadcast(job_id, payload)

    async def delete_job_and_dir(self, job_id: str) -> None:
        await self._close_local_clients(job_id)
        await self.job_storage.delete_job(job_id)
        await self.job_storage.publish_delete(job_id)

        async with self.job_lock:
            state = self.job_state.get(job_id)
            if state and not state.get("clients"):
                self.job_state.pop(job_id, None)

        try:
            job_dir = self.job_dir(job_id)
            if os.path.exists(job_dir):
                await anyio.to_thread.run_sync(shutil.rmtree, job_dir)
        except Exception:
            self.logger.warning("failed to cleanup job dir %s", job_id)

    async def job_error_cleanup(self, job_id: str, reason: str) -> None:
        self.logger.info("job error cleanup job_id=%s reason=%s", job_id, reason)
        await self.delete_job_and_dir(job_id)

    async def _merge_remote_state(self, job_id: str, remote_state: dict[str, Any] | None) -> None:
        if remote_state is None:
            return
        async with self.job_lock:
            local_state = self.job_state.get(job_id)
            if local_state is None:
                local_state = self._new_job_state()
                self.job_state[job_id] = local_state
            clients = list(local_state.get("clients", []))
            local_state.clear()
            local_state.update(self._new_job_state())
            local_state.update({key: value for key, value in remote_state.items() if key != "clients"})
            local_state["clients"] = clients

    async def _remove_local_job(self, job_id: str) -> None:
        async with self.job_lock:
            state = self.job_state.pop(job_id, None)
            self.job_meta.pop(job_id, None)
            clients = list(state.get("clients", [])) if state else []
        for ws in clients:
            try:
                await ws.close()
            except Exception:
                pass

    async def _handle_remote_event(self, payload: dict[str, Any]) -> None:
        if payload.get("origin_id") == self.job_storage.instance_id:
            return

        job_id = str(payload.get("job_id", ""))
        if not job_id:
            return

        kind = payload.get("kind")
        if kind == "event":
            await self._merge_remote_state(job_id, payload.get("state") if isinstance(payload.get("state"), dict) else None)
            message = payload.get("message")
            if isinstance(message, dict):
                await self._deliver_local_message(job_id, message)
            return

        if kind == "close_clients":
            await self._close_local_clients(job_id)
            return

        if kind == "delete":
            await self._remove_local_job(job_id)

    async def start_job_event_listener(self) -> None:
        async with self._job_event_listener_lock:
            self._job_event_listener_users += 1
            if not self.job_storage.supports_pubsub:
                return
            if self._job_event_listener_task is not None and not self._job_event_listener_task.done():
                return
            self._job_event_listener_task = asyncio.create_task(self.job_storage.listen(self._handle_remote_event))

    async def stop_job_event_listener(self) -> None:
        task: asyncio.Task[None] | None = None
        async with self._job_event_listener_lock:
            if self._job_event_listener_users > 0:
                self._job_event_listener_users -= 1
            if self._job_event_listener_users == 0 and self._job_event_listener_task is not None:
                task = self._job_event_listener_task
                self._job_event_listener_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    def build_service_context(
        self,
        app: FastAPI,
        *,
        run_repack_job: Callable[[str, str, str, int], Awaitable[tuple[bool, str | None, int | None, int | None, str | None]]],
        run_download_job: Callable[[str, str, str, int | None, dict[str, Any]], Coroutine[Any, Any, None]],
        notify_manager: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> ServiceContext:
        return ServiceContext(
            app=app,
            config=self.config,
            logger=self.logger,
            main_dir=self.main_dir,
            manager_url=self.manager_url,
            temp_dir=self.temp_dir,
            tools=self.tools,
            job_state=self.job_state,
            job_meta=self.job_meta,
            job_lock=self.job_lock,
            upload_limiter=self.upload_limiter,
            download_limiter=self.download_limiter,
            repack_limiter=self.repack_limiter,
            progress_push_interval=self.progress_push_interval,
            new_job_state=self._new_job_state,
            job_dir=self.job_dir,
            read_job_state=self.read_job_state,
            save_job_state=self.save_job_state,
            list_job_ids=self.list_job_ids,
            read_meta=self.read_meta,
            write_meta=self.write_meta,
            read_blurhash_cache=self.read_blurhash_cache,
            write_blurhash_cache=self.write_blurhash_cache,
            read_meta_sync=self.read_meta_sync,
            write_meta_sync=self.write_meta_sync,
            build_state_event=self.build_state_event,
            broadcast=self.broadcast,
            close_clients=self.close_clients,
            set_state=self.set_state,
            set_stage=self.set_stage,
            delete_job_and_dir=self.delete_job_and_dir,
            job_error_cleanup=self.job_error_cleanup,
            run_repack_job=run_repack_job,
            run_download_job=run_download_job,
            notify_manager=notify_manager,
        )
