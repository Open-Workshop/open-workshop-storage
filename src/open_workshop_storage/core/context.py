from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Coroutine, Optional

from fastapi import FastAPI

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
    job_lock: asyncio.Lock
    progress_push_interval: float
    new_job_state: Callable[[], JobState]
    job_dir: Callable[[str], str]
    read_meta_sync: Callable[[str], JsonDict]
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
