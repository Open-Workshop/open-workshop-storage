import asyncio
from contextlib import asynccontextmanager, suppress
import json
import logging
import os
import shutil
import time
from typing import Any

import anyio
import ow_config as config
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

from . import utils as tools
from .api.routes.files import (
    configure_context_provider as configure_file_routes,
    delete,
    download,
    router as file_router,
    upload,
)
from .api.routes.transfers import (
    configure_context_provider as configure_transfer_routes,
    transfer_move,
    transfer_repack,
    transfer_start,
    transfer_upload,
    transfer_ws,
    router as transfer_router,
)
from .core.context import ServiceContext
from .core.job_state import new_job_state, state_event_payload
from .observability.uptrace import setup_uptrace_telemetry
from .services.transfer_jobs import cleanup_loop, notify_manager, run_cleanup, run_download_job, run_repack_job


MAIN_DIR = config.MAIN_DIR
MANAGER_URL = config.MANAGER_URL
logger = logging.getLogger("open_workshop.storage")

TEMP_DIR = os.path.join(MAIN_DIR, "temp")
JOB_STATE: dict[str, dict[str, Any]] = {}
JOB_LOCK = asyncio.Lock()
PROGRESS_PUSH_INTERVAL = 0.25


def _new_job_state() -> dict[str, Any]:
    return new_job_state()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        tools.ensure_7z_available()
    except Exception as exc:
        logger.error("7z dependency missing: %s", exc)
        raise

    cleanup_task = asyncio.create_task(_cleanup_loop())
    try:
        yield
    finally:
        cleanup_task.cancel()
        with suppress(asyncio.CancelledError):
            await cleanup_task


app = FastAPI(
    title="Open Workshop",
    contact={
        "name": "GitHub",
        "url": "https://github.com/Open-Workshop",
    },
    license_info={
        "name": "MPL-2.0 license",
        "identifier": "MPL-2.0",
    },
    docs_url="/",
    lifespan=lifespan,
)
setup_uptrace_telemetry(app)


def _job_dir(job_id: str) -> str:
    return tools.safe_path(TEMP_DIR, job_id)


def _job_meta_path(job_id: str) -> str:
    return os.path.join(_job_dir(job_id), "meta.json")


def _read_meta_sync(job_id: str) -> dict[str, Any]:
    with open(_job_meta_path(job_id), "r", encoding="utf-8") as meta_file:
        return json.load(meta_file)


def _write_meta_sync(job_id: str, data: dict[str, Any]) -> None:
    os.makedirs(_job_dir(job_id), exist_ok=True)
    with open(_job_meta_path(job_id), "w", encoding="utf-8") as meta_file:
        json.dump(data, meta_file, ensure_ascii=True)


def _state_event_payload(
    event: str,
    state: dict[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return state_event_payload(event, state, **extra)


async def _build_state_event(job_id: str, event: str, **extra: Any) -> dict[str, Any]:
    async with JOB_LOCK:
        state = JOB_STATE.setdefault(job_id, _new_job_state())
        return _state_event_payload(event, state, **extra)


async def _broadcast(job_id: str, message: dict[str, Any]) -> None:
    async with JOB_LOCK:
        state = JOB_STATE.get(job_id)
        if not state:
            return
        clients = list(state.get("clients", []))
    for ws in clients:
        try:
            await ws.send_json(message)
        except Exception:
            pass


async def _close_clients(job_id: str) -> None:
    async with JOB_LOCK:
        state = JOB_STATE.get(job_id)
        if not state:
            return
        clients = list(state.get("clients", []))
        state["clients"] = []
    for ws in clients:
        try:
            await ws.close()
        except Exception:
            pass


async def _set_state(job_id: str, **updates: Any) -> None:
    async with JOB_LOCK:
        state = JOB_STATE.setdefault(job_id, _new_job_state())
        updates["last_activity"] = time.time()
        state.update(updates)


async def _set_stage(job_id: str, stage: str) -> None:
    async with JOB_LOCK:
        state = JOB_STATE.setdefault(job_id, _new_job_state())
        state["stage"] = stage
        state["percent"] = None
        state["last_activity"] = time.time()
        payload = _state_event_payload("stage", state)
    await _broadcast(job_id, payload)


async def _delete_job_and_dir(job_id: str) -> None:
    async with JOB_LOCK:
        state = JOB_STATE.get(job_id)
        clients_to_close = []
        if state:
            clients_to_close = list(state.get("clients", []))
            del JOB_STATE[job_id]

    for ws in clients_to_close:
        try:
            await ws.close()
        except Exception:
            pass

    try:
        job_dir = _job_dir(job_id)
        if os.path.exists(job_dir):
            await anyio.to_thread.run_sync(shutil.rmtree, job_dir)
    except Exception:
        logger.warning("failed to cleanup job dir %s", job_id)


async def _job_error_cleanup(job_id: str, reason: str) -> None:
    logger.info("job error cleanup job_id=%s reason=%s", job_id, reason)
    await _delete_job_and_dir(job_id)


def _build_service_context() -> ServiceContext:
    return ServiceContext(
        app=app,
        config=config,
        logger=logger,
        main_dir=MAIN_DIR,
        manager_url=MANAGER_URL,
        temp_dir=TEMP_DIR,
        tools=tools,
        job_state=JOB_STATE,
        job_lock=JOB_LOCK,
        progress_push_interval=PROGRESS_PUSH_INTERVAL,
        new_job_state=_new_job_state,
        job_dir=_job_dir,
        read_meta_sync=_read_meta_sync,
        write_meta_sync=_write_meta_sync,
        build_state_event=_build_state_event,
        broadcast=_broadcast,
        close_clients=_close_clients,
        set_state=_set_state,
        set_stage=_set_stage,
        delete_job_and_dir=_delete_job_and_dir,
        job_error_cleanup=_job_error_cleanup,
        run_repack_job=_run_repack_job,
        run_download_job=_run_download_job,
        notify_manager=_notify_manager,
    )


async def _run_cleanup() -> None:
    await run_cleanup(_build_service_context())


async def _cleanup_loop() -> None:
    await cleanup_loop(_build_service_context())


async def _run_repack_job(
    job_id: str,
    download_abs: str,
    pack_format: str,
    pack_level: int,
) -> tuple[bool, str | None, int | None, int | None, str | None]:
    return await run_repack_job(
        _build_service_context(),
        job_id,
        download_abs,
        pack_format,
        pack_level,
    )


async def _notify_manager(payload: dict[str, Any]) -> None:
    await notify_manager(_build_service_context(), payload)


async def _run_download_job(
    job_id: str,
    download_url: str,
    download_abs: str,
    max_bytes: int | None,
    callback_payload: dict[str, Any],
) -> None:
    await run_download_job(
        _build_service_context(),
        job_id,
        download_url,
        download_abs,
        max_bytes,
        callback_payload,
    )


@app.middleware("http")
async def modify_header(request: Request, call_next):
    if request.method == "OPTIONS":
        response = PlainTextResponse(status_code=200, content="OK")
    else:
        response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization,X-File-Name"
    response.headers["Access-Control-Expose-Headers"] = "Content-Type,Content-Disposition"
    return response


configure_transfer_routes(_build_service_context)
configure_file_routes(_build_service_context)
app.include_router(transfer_router)
app.include_router(file_router)
