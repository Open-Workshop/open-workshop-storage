from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Coroutine, Optional

from fastapi import FastAPI

from .limits import ConcurrencyLimiter

JsonDict = dict[str, Any]
JobState = dict[str, Any]


@dataclass(frozen=True)
class ServiceContext:
    app: FastAPI
    config: Any
    logger: logging.Logger
    main_dir: str
    manager_url: str
    temp_dir: str
    tools: Any
    job_state: dict[str, JobState]
    job_meta: dict[str, JsonDict]
    job_lock: asyncio.Lock
    upload_limiter: ConcurrencyLimiter
    download_limiter: ConcurrencyLimiter
    repack_limiter: ConcurrencyLimiter
    progress_push_interval: float
    new_job_state: Callable[[], JobState]
    job_dir: Callable[[str], str]
    read_job_state: Callable[[str], Awaitable[JobState | None]]
    save_job_state: Callable[[str, JobState | None], Awaitable[JobState]]
    list_job_ids: Callable[[], Awaitable[list[str]]]
    read_meta: Callable[[str], Awaitable[JsonDict | None]]
    write_meta: Callable[[str, JsonDict], Awaitable[None]]
    read_blurhash_cache: Callable[[str], Awaitable[JsonDict | None]]
    write_blurhash_cache: Callable[[str, JsonDict], Awaitable[None]]
    read_meta_sync: Callable[[str], JsonDict | None]
    write_meta_sync: Callable[[str, JsonDict], None]
    build_state_event: Callable[..., Awaitable[JsonDict]]
    broadcast: Callable[[str, JsonDict], Awaitable[None]]
    close_clients: Callable[[str], Awaitable[None]]
    set_state: Callable[..., Awaitable[None]]
    set_stage: Callable[[str, str], Awaitable[None]]
    delete_job_and_dir: Callable[[str], Awaitable[None]]
    job_error_cleanup: Callable[[str, str], Awaitable[None]]
    run_repack_job: Callable[
        [str, str, str, int],
        Awaitable[tuple[bool, Optional[str], Optional[int], Optional[int], Optional[str]]],
    ]
    run_download_job: Callable[
        [str, str, str, Optional[int], JsonDict],
        Coroutine[Any, Any, None],
    ]
    notify_manager: Callable[[JsonDict], Awaitable[None]]
