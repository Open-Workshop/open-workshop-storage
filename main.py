import os
import logging
import asyncio
import json
import time
import shutil
from typing import Optional, Any
from urllib.parse import urlparse
import anyio
import aiohttp
import tools
import ow_config as config
from fastapi import FastAPI, Request, UploadFile, Form, File, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse


MAIN_DIR = config.MAIN_DIR
MANAGER_URL = config.MANAGER_URL
logger = logging.getLogger("open_workshop.storage")

TEMP_DIR = os.path.join(MAIN_DIR, "temp")
JOB_STATE: dict[str, dict[str, Any]] = {}
JOB_LOCK = asyncio.Lock()
PROGRESS_PUSH_INTERVAL = 0.25


# Создание приложения
app = FastAPI(
    title="Open Workshop",
    contact={
        "name": "GitHub",
        "url": "https://github.com/Open-Workshop"
    },
    license_info={
        "name": "MPL-2.0 license",
        "identifier": "MPL-2.0"
    },
    docs_url="/"
)

@app.middleware("http")
async def modify_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Expose-Headers"] = "Content-Type,Content-Disposition"
    return response


async def _extract_token(request: Request) -> Optional[str]:
    token = request.query_params.get("token")
    if token:
        return token
    if request.method in ("POST", "PUT", "DELETE"):
        form = await request.form()
        return form.get("token")
    return None


@app.get("/transfer/start")
@app.post("/transfer/start")
async def transfer_start(request: Request):
    token = await _extract_token(request)
    if not token:
        return PlainTextResponse(status_code=401, content="Token not found")

    payload = tools.decode_transfer_jwt(token, audience="storage")
    if not payload:
        return PlainTextResponse(status_code=403, content="Access denied")

    job_id = str(payload.get("job_id", ""))
    if not tools.is_safe_job_id(job_id):
        return PlainTextResponse(status_code=400, content="Invalid job id")

    download_url = payload.get("download_url")
    if not download_url:
        return PlainTextResponse(status_code=400, content="Download URL missing")

    parsed = urlparse(download_url)
    if parsed.scheme not in {"http", "https"}:
        return PlainTextResponse(status_code=400, content="Invalid download URL")

    filename = payload.get("filename") or os.path.basename(parsed.path)
    safe_name = tools.sanitize_filename(filename)

    download_rel = os.path.join("temp", job_id, safe_name)
    download_abs = tools.safe_path(MAIN_DIR, download_rel)

    pack_format = payload.get("pack_format", "zip")
    try:
        pack_level = int(payload.get("pack_level", 9))
    except (TypeError, ValueError):
        pack_level = 9
    mod_id = payload.get("mod_id")

    max_bytes_raw = payload.get("max_bytes", None)
    max_bytes = max_bytes_raw if max_bytes_raw is not None else getattr(config, "TRANSFER_MAX_BYTES", None)
    try:
        max_bytes = int(max_bytes) if max_bytes is not None else None
    except (TypeError, ValueError):
        max_bytes = None
    if max_bytes is not None and max_bytes <= 0:
        max_bytes = None

    async with JOB_LOCK:
        if job_id in JOB_STATE:
            state = JOB_STATE[job_id]
            return {
                "job_id": job_id,
                "status": state.get("status"),
                "ws_url": f"/transfer/ws/{job_id}",
            }
        JOB_STATE[job_id] = {
            "status": "pending",
            "bytes": 0,
            "total": None,
            "error": None,
            "clients": set(),
        }

    meta = {
        "job_id": job_id,
        "mod_id": mod_id,
        "download_url": download_url,
        "filename": safe_name,
        "download_path": download_rel,
        "pack_format": pack_format,
        "pack_level": pack_level,
        "status": "pending",
        "created_at": int(time.time()),
    }
    await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)

    callback_payload = {
        "mod_id": mod_id,
        "pack_format": pack_format,
        "pack_level": pack_level,
    }
    asyncio.create_task(
        _run_download_job(job_id, download_url, download_abs, max_bytes, callback_payload)
    )

    return {
        "job_id": job_id,
        "status": "started",
        "ws_url": f"/transfer/ws/{job_id}",
    }


@app.post("/transfer/upload")
async def transfer_upload(
    request: Request,
    file: UploadFile = File(...),
    token: Optional[str] = Form(None),
):
    client = request.client.host if request.client else "unknown"
    token = token or request.query_params.get("token")
    if not token:
        logger.warning("transfer upload denied (token missing) client=%s", client)
        return PlainTextResponse(status_code=401, content="Token not found")

    payload = tools.decode_transfer_jwt(token, audience="storage")
    if not payload:
        logger.warning("transfer upload denied (token) client=%s", client)
        return PlainTextResponse(status_code=403, content="Access denied")

    job_id = str(payload.get("job_id", ""))
    if not tools.is_safe_job_id(job_id):
        return PlainTextResponse(status_code=400, content="Invalid job id")

    pack_format = payload.get("pack_format", "zip")
    if pack_format != "zip":
        return PlainTextResponse(status_code=400, content="Unsupported format")

    try:
        pack_level = int(payload.get("pack_level", 9))
    except (TypeError, ValueError):
        pack_level = 9
    pack_level = max(0, min(pack_level, 9))
    mod_id = payload.get("mod_id")

    max_bytes_raw = payload.get("max_bytes", None)
    max_bytes = max_bytes_raw if max_bytes_raw is not None else getattr(config, "TRANSFER_MAX_BYTES", None)
    try:
        max_bytes = int(max_bytes) if max_bytes is not None else None
    except (TypeError, ValueError):
        max_bytes = None
    if max_bytes is not None and max_bytes <= 0:
        max_bytes = None

    safe_name = tools.sanitize_filename(file.filename)
    upload_rel = os.path.join("temp", job_id, safe_name)
    upload_abs = tools.safe_path(MAIN_DIR, upload_rel)

    total = None
    content_len = request.headers.get("content-length")
    if content_len:
        try:
            total = int(content_len)
        except (TypeError, ValueError):
            total = None
    logger.info(
        "transfer upload start job_id=%s mod_id=%s filename=%s size_hint=%s client=%s",
        job_id,
        mod_id,
        safe_name,
        total,
        client,
    )

    async with JOB_LOCK:
        state = JOB_STATE.get(job_id)
        if not state:
            JOB_STATE[job_id] = {
                "status": "uploading",
                "bytes": 0,
                "total": total,
                "error": None,
                "clients": set(),
            }
        else:
            state.update({"status": "uploading", "bytes": 0, "total": total, "error": None})

    meta = {
        "job_id": job_id,
        "mod_id": mod_id,
        "filename": safe_name,
        "download_path": upload_rel,
        "pack_format": pack_format,
        "pack_level": pack_level,
        "status": "uploading",
        "created_at": int(time.time()),
    }
    await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)

    await _broadcast(job_id, {"event": "status", "status": "uploading"})

    downloaded = 0
    last_push = 0.0
    start_ts = time.monotonic()
    last_log_bytes = 0
    next_percent = 10
    callback_payload = {
        "mod_id": mod_id,
        "pack_format": pack_format,
        "pack_level": pack_level,
    }

    try:
        os.makedirs(os.path.dirname(upload_abs), exist_ok=True)
        with open(upload_abs, "wb") as out_file:
            while True:
                chunk = await file.read(1024 * 256)
                if not chunk:
                    break
                downloaded += len(chunk)
                if max_bytes and downloaded > max_bytes:
                    await _set_state(job_id, status="error", error="size_limit")
                    await _broadcast(job_id, {"event": "error", "message": "file too large"})
                    try:
                        if os.path.exists(upload_abs):
                            os.remove(upload_abs)
                    except Exception:
                        logger.warning("failed to cleanup partial file job_id=%s", job_id)
                    await _notify_manager(
                        {
                            **callback_payload,
                            "job_id": job_id,
                            "status": "error",
                            "reason": "size_limit",
                        }
                    )
                    return PlainTextResponse(status_code=413, content="File too large")

                await anyio.to_thread.run_sync(out_file.write, chunk)
                now = time.monotonic()
                if now - last_push >= PROGRESS_PUSH_INTERVAL:
                    last_push = now
                    await _set_state(job_id, bytes=downloaded)
                    await _broadcast(
                        job_id,
                        {
                            "event": "progress",
                            "bytes": downloaded,
                            "total": total,
                        },
                    )
                if total:
                    percent = int((downloaded / total) * 100)
                    if percent >= next_percent:
                        logger.info(
                            "transfer upload progress job_id=%s percent=%s bytes=%s",
                            job_id,
                            percent,
                            downloaded,
                        )
                        next_percent += 10
                elif downloaded - last_log_bytes >= 50 * 1024 * 1024:
                    last_log_bytes = downloaded
                    logger.info(
                        "transfer upload progress job_id=%s bytes=%s",
                        job_id,
                        downloaded,
                    )

        await _set_state(job_id, status="done", bytes=downloaded, total=total)
        await _broadcast(
            job_id,
            {
                "event": "complete",
                "bytes": downloaded,
                "total": total,
            },
        )
        try:
            meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
            meta.update(
                {
                    "status": "uploaded",
                    "downloaded_bytes": downloaded,
                    "total_bytes": total,
                    "upload_completed_at": int(time.time()),
                }
            )
            await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
        except Exception:
            logger.warning("failed to update meta for job_id=%s", job_id)

        duration = time.monotonic() - start_ts
        logger.info(
            "transfer upload done job_id=%s bytes=%s duration=%.2fs",
            job_id,
            downloaded,
            duration,
        )
        await _notify_manager(
            {
                **callback_payload,
                "job_id": job_id,
                "status": "success",
                "bytes": downloaded,
                "total": total,
            }
        )
        return {
            "job_id": job_id,
            "bytes": downloaded,
            "total": total,
        }
    except Exception as exc:
        logger.exception("transfer upload failed job_id=%s", job_id)
        try:
            if os.path.exists(upload_abs):
                os.remove(upload_abs)
        except Exception:
            logger.warning("failed to cleanup partial file job_id=%s", job_id)
        try:
            meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
            meta.update(
                {
                    "status": "error",
                    "error": str(exc),
                    "upload_completed_at": int(time.time()),
                }
            )
            await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
        except Exception:
            logger.warning("failed to update meta for job_id=%s", job_id)
        await _set_state(job_id, status="error", error=str(exc))
        await _broadcast(job_id, {"event": "error", "message": "upload failed"})
        await _notify_manager(
            {**callback_payload, "job_id": job_id, "status": "error", "reason": "exception"}
        )
        return PlainTextResponse(status_code=500, content="Upload failed")
    finally:
        await _close_clients(job_id)


@app.websocket("/transfer/ws/{job_id}")
async def transfer_ws(websocket: WebSocket, job_id: str):
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=1008)
        return
    payload = tools.decode_transfer_jwt(token, audience="storage")
    if not payload or str(payload.get("job_id", "")) != job_id:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    logger.info("transfer ws connect job_id=%s", job_id)
    async with JOB_LOCK:
        state = JOB_STATE.get(job_id)
        if not state:
            state = {
                "status": "pending",
                "bytes": 0,
                "total": None,
                "error": None,
                "clients": set(),
            }
            JOB_STATE[job_id] = state
        state.setdefault("clients", set()).add(websocket)
        await websocket.send_json(
            {
                "event": "progress",
                "bytes": state.get("bytes", 0),
                "total": state.get("total"),
                "status": state.get("status"),
            }
        )
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with JOB_LOCK:
            state = JOB_STATE.get(job_id)
            if state and websocket in state.get("clients", set()):
                state["clients"].remove(websocket)
        logger.info("transfer ws disconnect job_id=%s", job_id)


@app.post("/transfer/repack")
async def transfer_repack(
    request: Request,
    job_id: str = Form(),
    format: str = Form("zip"),
    compression_level: int = Form(9),
    token: str = Form(),
):
    client = request.client.host if request.client else "unknown"
    if not token:
        logger.warning("transfer repack denied (token missing) job_id=%s client=%s", job_id, client)
        return PlainTextResponse(status_code=401, content="Token not found")
    if not await anyio.to_thread.run_sync(tools.check_token, "storage_manage_token", token):
        logger.warning("transfer repack denied (token) job_id=%s client=%s", job_id, client)
        return PlainTextResponse(status_code=403, content="Access denied")
    if not tools.is_safe_job_id(job_id):
        return PlainTextResponse(status_code=400, content="Invalid job id")

    try:
        meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
    except Exception:
        return PlainTextResponse(status_code=404, content="Job not found")

    if format != "zip":
        return PlainTextResponse(status_code=400, content="Unsupported format")

    download_rel = meta.get("download_path")
    if not download_rel:
        return PlainTextResponse(status_code=404, content="Source file not found")

    download_abs = tools.safe_path(MAIN_DIR, download_rel)
    packed_rel = os.path.join("temp", job_id, "packed.zip")
    packed_abs = tools.safe_path(MAIN_DIR, packed_rel)

    arcname = meta.get("filename") or os.path.basename(download_abs)
    try:
        compression_level = int(compression_level)
    except (TypeError, ValueError):
        compression_level = 9
    compression_level = max(0, min(compression_level, 9))

    logger.info(
        "transfer repack start job_id=%s format=%s level=%s client=%s",
        job_id,
        format,
        compression_level,
        client,
    )
    if os.path.exists(packed_abs):
        packed_bytes = os.path.getsize(packed_abs)
        meta.update(
            {
                "packed_path": packed_rel,
                "packed_bytes": packed_bytes,
                "pack_format": format,
                "pack_level": compression_level,
                "status": "packed",
            }
        )
        await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
        logger.info(
            "transfer repack reuse job_id=%s packed_bytes=%s",
            job_id,
            packed_bytes,
        )
        return {
            "job_id": job_id,
            "packed_bytes": packed_bytes,
            "packed_path": packed_rel,
        }

    start_ts = time.monotonic()
    await anyio.to_thread.run_sync(
        tools.zip_single_file_with_level, download_abs, packed_abs, arcname, compression_level
    )
    duration = time.monotonic() - start_ts

    packed_bytes = os.path.getsize(packed_abs)
    meta.update(
        {
            "packed_path": packed_rel,
            "packed_bytes": packed_bytes,
            "pack_format": format,
            "pack_level": compression_level,
            "status": "packed",
        }
    )
    await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
    logger.info(
        "transfer repack done job_id=%s packed_bytes=%s duration=%.2fs",
        job_id,
        packed_bytes,
        duration,
    )

    return {
        "job_id": job_id,
        "packed_bytes": packed_bytes,
        "packed_path": packed_rel,
    }


@app.post("/transfer/move")
async def transfer_move(
    request: Request,
    job_id: str = Form(),
    type: str = Form(),
    path: str = Form(),
    token: str = Form(),
):
    client = request.client.host if request.client else "unknown"
    if not token:
        logger.warning("transfer move denied (token missing) job_id=%s client=%s", job_id, client)
        return PlainTextResponse(status_code=401, content="Token not found")
    if not await anyio.to_thread.run_sync(tools.check_token, "storage_manage_token", token):
        logger.warning("transfer move denied (token) job_id=%s client=%s", job_id, client)
        return PlainTextResponse(status_code=403, content="Access denied")
    if not tools.is_safe_job_id(job_id):
        return PlainTextResponse(status_code=400, content="Invalid job id")
    if not tools.is_allowed_type(type):
        return PlainTextResponse(status_code=400, content="Invalid type")

    try:
        meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
    except Exception:
        return PlainTextResponse(status_code=404, content="Job not found")

    packed_rel = meta.get("packed_path")
    if not packed_rel:
        return PlainTextResponse(status_code=404, content="Packed file not found")

    packed_abs = tools.safe_path(MAIN_DIR, packed_rel)
    base_dir = os.path.join(MAIN_DIR, type)
    try:
        real_path = tools.safe_path(base_dir, path)
    except ValueError:
        return PlainTextResponse(status_code=423, content="Access denied")

    logger.info(
        "transfer move start job_id=%s type=%s path=%s client=%s",
        job_id,
        type,
        path,
        client,
    )
    start_ts = time.monotonic()
    os.makedirs(os.path.dirname(real_path), exist_ok=True)
    await anyio.to_thread.run_sync(shutil.move, packed_abs, real_path)

    final_rel = os.path.relpath(real_path, MAIN_DIR)
    final_bytes = os.path.getsize(real_path)
    duration = time.monotonic() - start_ts
    meta.update(
        {
            "final_path": final_rel,
            "final_bytes": final_bytes,
            "status": "moved",
            "moved_at": int(time.time()),
        }
    )
    await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)

    # Cleanup temp dir
    try:
        await anyio.to_thread.run_sync(shutil.rmtree, _job_dir(job_id))
    except Exception:
        logger.warning("failed to cleanup temp dir job_id=%s", job_id)

    logger.info(
        "transfer move done job_id=%s final_bytes=%s duration=%.2fs",
        job_id,
        final_bytes,
        duration,
    )
    return {
        "job_id": job_id,
        "final_path": final_rel,
        "final_bytes": final_bytes,
    }


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
            # Client will be cleaned up on disconnect
            pass


async def _close_clients(job_id: str) -> None:
    async with JOB_LOCK:
        state = JOB_STATE.get(job_id)
        if not state:
            return
        clients = list(state.get("clients", []))
        state["clients"] = set()
    for ws in clients:
        try:
            await ws.close()
        except Exception:
            pass


async def _set_state(job_id: str, **updates: Any) -> None:
    async with JOB_LOCK:
        state = JOB_STATE.setdefault(
            job_id,
            {"status": "pending", "bytes": 0, "total": None, "error": None, "clients": set()},
        )
        state.update(updates)


async def _notify_manager(payload: dict[str, Any]) -> None:
    callback_url = getattr(config, "MANAGER_TRANSFER_CALLBACK_URL", None) or (
        f"{MANAGER_URL}/storage/transfer/complete"
    )
    ttl_raw = getattr(config, "TRANSFER_CALLBACK_TTL_SECONDS", 600)
    try:
        ttl_seconds = int(ttl_raw)
    except (TypeError, ValueError):
        ttl_seconds = 600
    token = tools.encode_transfer_jwt(payload, audience="manager", ttl_seconds=ttl_seconds)
    if not token:
        logger.warning("transfer callback skipped (missing JWT secret)")
        return
    headers = {"Authorization": f"Bearer {token}"}
    async with aiohttp.ClientSession() as session:
        try:
            logger.info(
                "transfer callback send url=%s job_id=%s status=%s",
                callback_url,
                payload.get("job_id"),
                payload.get("status"),
            )
            async with session.post(callback_url, headers=headers) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.warning(
                        "transfer callback failed status=%s body=%s", resp.status, body
                    )
                else:
                    logger.info(
                        "transfer callback ok status=%s job_id=%s",
                        resp.status,
                        payload.get("job_id"),
                    )
        except Exception:
            logger.exception("transfer callback error")


async def _run_download_job(
    job_id: str,
    download_url: str,
    download_abs: str,
    max_bytes: Optional[int],
    callback_payload: dict[str, Any],
) -> None:
    await _set_state(job_id, status="downloading", error=None)
    await _broadcast(job_id, {"event": "status", "status": "downloading"})
    downloaded = 0
    total = None
    last_push = 0.0
    try:
        meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
        meta.update({"status": "downloading", "download_started_at": int(time.time())})
        await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
    except Exception:
        logger.warning("failed to update meta (start) for job_id=%s", job_id)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(download_url) as resp:
                if resp.status != 200:
                    await _set_state(job_id, status="error", error=f"status:{resp.status}")
                    await _broadcast(
                        job_id,
                        {
                            "event": "error",
                            "message": f"download failed with status {resp.status}",
                        },
                    )
                    await _notify_manager(
                        {
                            **callback_payload,
                            "job_id": job_id,
                            "status": "error",
                            "reason": f"status:{resp.status}",
                        }
                    )
                    return

                total = resp.content_length
                await _set_state(job_id, total=total)
                if max_bytes and total and total > max_bytes:
                    await _set_state(job_id, status="error", error="size_limit")
                    await _broadcast(
                        job_id,
                        {"event": "error", "message": "file too large"},
                    )
                    try:
                        if os.path.exists(download_abs):
                            os.remove(download_abs)
                    except Exception:
                        logger.warning("failed to cleanup partial file job_id=%s", job_id)
                    await _notify_manager(
                        {
                            **callback_payload,
                            "job_id": job_id,
                            "status": "error",
                            "reason": "size_limit",
                        }
                    )
                    return

                os.makedirs(os.path.dirname(download_abs), exist_ok=True)
                with open(download_abs, "wb") as out_file:
                    async for chunk in resp.content.iter_chunked(1024 * 256):
                        if not chunk:
                            continue
                        downloaded += len(chunk)
                        if max_bytes and downloaded > max_bytes:
                            await _set_state(job_id, status="error", error="size_limit")
                            await _broadcast(
                                job_id,
                                {"event": "error", "message": "file too large"},
                            )
                            try:
                                if os.path.exists(download_abs):
                                    os.remove(download_abs)
                            except Exception:
                                logger.warning("failed to cleanup partial file job_id=%s", job_id)
                            await _notify_manager(
                                {
                                    **callback_payload,
                                    "job_id": job_id,
                                    "status": "error",
                                    "reason": "size_limit",
                                }
                            )
                            return
                        await anyio.to_thread.run_sync(out_file.write, chunk)
                        now = time.monotonic()
                        if now - last_push >= PROGRESS_PUSH_INTERVAL:
                            last_push = now
                            await _set_state(job_id, bytes=downloaded)
                            await _broadcast(
                                job_id,
                                {
                                    "event": "progress",
                                    "bytes": downloaded,
                                    "total": total,
                                },
                            )

        await _set_state(job_id, status="done", bytes=downloaded, total=total)
        await _broadcast(
            job_id,
            {
                "event": "complete",
                "bytes": downloaded,
                "total": total,
            },
        )
        try:
            meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
            meta.update(
                {
                    "status": "downloaded",
                    "downloaded_bytes": downloaded,
                    "total_bytes": total,
                    "download_completed_at": int(time.time()),
                }
            )
            await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
        except Exception:
            logger.warning("failed to update meta for job_id=%s", job_id)
        await _notify_manager(
            {
                **callback_payload,
                "job_id": job_id,
                "status": "success",
                "bytes": downloaded,
                "total": total,
            }
        )
    except Exception as exc:
        logger.exception("transfer download failed job_id=%s", job_id)
        try:
            if os.path.exists(download_abs):
                os.remove(download_abs)
        except Exception:
            logger.warning("failed to cleanup partial file job_id=%s", job_id)
        try:
            meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
            meta.update(
                {
                    "status": "error",
                    "error": str(exc),
                    "download_completed_at": int(time.time()),
                }
            )
            await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
        except Exception:
            logger.warning("failed to update meta for job_id=%s", job_id)
        await _set_state(job_id, status="error", error=str(exc))
        await _broadcast(job_id, {"event": "error", "message": "download failed"})
        await _notify_manager(
            {**callback_payload, "job_id": job_id, "status": "error", "reason": "exception"}
        )
    finally:
        await _close_clients(job_id)


@app.get(
    "/download/{type}/{path:path}",
    status_code=200,
    response_class=FileResponse,
    responses={
        200: {
            "description": "File send successfully",
            "content": {"application/octet-stream": {}},
        },
        400: {
            "description": "Invalid type",
            "content": {"text/plain": {'example': 'Invalid type'}}
        },
        403: {
            "description": "Access denied",
            "content": {"text/plain": {'example': 'Access denied'}}
        },
        423: {
            "description": "Access denied",
            "content": {"text/plain": {'example': 'Access denied'}}
        },
        404: {
            "description": "File not found on server",
            "content": {"text/plain": {'example': 'File not found'}}
        },
        503: {
            "description": "Manager unavailable",
            "content": {"text/plain": {'example': 'Manager unavailable'}}
        }
    },
)
async def download(request: Request, type: str, path: str, filename: Optional[str] = None):
    """
    Возвращает запрашиваемый файл, если он существует.
    """
    client = request.client.host if request.client else "unknown"
    logger.info("download request type=%s path=%s client=%s", type, path, client)
    if not tools.is_allowed_type(type):
        logger.warning("download invalid type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=400, content="Invalid type")
    base_dir = os.path.join(MAIN_DIR, type)
    try:
        real_path = tools.safe_path(base_dir, path)
    except ValueError:
        logger.warning("download path traversal type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=423, content="Access denied")
    
    if os.path.isfile(real_path):
        download_name = tools.build_download_filename(filename, real_path)
        if type == 'archive' and path.startswith('mod/'):
            parts = path.split('/', 2)
            if len(parts) < 2:
                logger.info("download not found (bad mod path) type=%s path=%s client=%s", type, path, client)
                return PlainTextResponse(status_code=404, content="File not found")
            try:
                mod_id = int(parts[1])
            except ValueError:
                logger.info("download not found (bad mod id) type=%s path=%s client=%s", type, path, client)
                return PlainTextResponse(status_code=404, content="File not found")
            # Асинхронно спрашиваем у Manager правомерность доступа к файлу
            async with aiohttp.ClientSession() as session:
                user = request.cookies.get('userID', 0)
                headers = {
                    "x-token": f"{config.check_access}",
                }
                async with session.get(f"{MANAGER_URL}/list/mods/access/[{mod_id}]?user={user}", headers=headers) as resp:
                    if resp.status == 200:
                        # Возвращает такой же список, проверяем, есть ли в нем интересующий нас ID
                        data = await resp.json()
                        if mod_id in data:
                            logger.info("download allowed mod_id=%s type=%s path=%s client=%s", mod_id, type, path, client)
                            # Если есть, то возвращаем сам файл
                            return FileResponse(real_path, filename=download_name)
                        else:
                            logger.warning("download denied mod_id=%s type=%s path=%s client=%s", mod_id, type, path, client)
                            return PlainTextResponse(status_code=403, content="Access denied")
                    else:
                        logger.warning("manager unavailable status=%s mod_id=%s client=%s", resp.status, mod_id, client)
                        return PlainTextResponse(status_code=503, content="Manager unavailable")
        else:
            logger.info("download ok type=%s path=%s client=%s", type, path, client)
            return FileResponse(real_path, filename=download_name)
    else:
        logger.info("download not found type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=404, content="File not found")

@app.post(
    "/upload", 
    status_code=201, 
    response_class=PlainTextResponse, 
    response_model=str, 
    responses={
        201: {
            "description": "File uploaded successfully", 
            "content": {"text/plain": {'example': 'file/is/saved/as.tmp'}}, 
            "model": str
        },
        401: {
            "description": "Token not found",
            "content": {"text/plain": {'example': 'Token not found'}},
            "model": str
        },
        400: {
            "description": "Invalid type",
            "content": {"text/plain": {'example': 'Invalid type'}},
            "model": str
        },
    }
)
async def upload(request: Request, file: UploadFile, type: str = Form(), path: str = Form(), token: str = Form()):
    """
    Загружает файл в Storage (микросервис хранения, функция управляется другим микросервисов).

    type: Тип файла. Поддерживает следующие значения: resource, avatar.

    path: Путь и имя файла. В формате "директории/поддиректории/имя.файла". Если под папок нет существует, то они создаются.
    """
    client = request.client.host if request.client else "unknown"
    logger.info("upload request type=%s path=%s filename=%s client=%s", type, path, file.filename, client)
    if not token:
        logger.warning("upload denied (token missing) type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=401, content="Token not found")
    if not await anyio.to_thread.run_sync(tools.check_token, 'upload_file', token):
        logger.warning("upload denied (token) type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=403, content="Access denied")
    if not tools.is_allowed_upload_type(type):
        logger.warning("upload invalid type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=400, content="Invalid type")

    base_dir = os.path.join(MAIN_DIR, type)
    try:
        real_path = tools.safe_path(base_dir, path)
    except ValueError:
        logger.warning("upload path traversal type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=423, content="Access denied")
    # Проверяем существует ли директория, если нет, то создаем
    if not os.path.exists(os.path.dirname(real_path)):
        os.makedirs(os.path.dirname(real_path))

    # Сохраняем файл
    await anyio.to_thread.run_sync(tools.copy_fileobj_to_path, file.file, real_path)
    logger.info("upload saved type=%s path=%s client=%s", type, path, client)
    return path

@app.delete(
    "/delete",
    status_code=200,
    response_class=PlainTextResponse,
    response_model=str,
    responses={
        200: {
            "description": "File deleted successfully", 
            "content": {"text/plain": {'example': 'File deleted'}}, 
            "model": str
        },
        401: {
            "description": "Token not found",
            "content": {"text/plain": {'example': 'Token not found'}},
            "model": str
        },
        400: {
            "description": "Invalid type",
            "content": {"text/plain": {'example': 'Invalid type'}},
            "model": str
        },
        404: {
            "description": "File not found on server",
            "content": {"text/plain": {'example': 'File not found'}},
            "model": str
        }
    },
)
async def delete(request: Request, type: str = Form(), path: str = Form(), token: str = Form()):
    """
    Удаляет файл из Steam Workshop (микросервис хранения, функция управляется другим микросервисов).

    К удалению разрешены только файлы.

    Если после удаления файла, папка пуста, то она тоже удаляется (так же происходит со всеми родительскими папками)
    """
    client = request.client.host if request.client else "unknown"
    logger.info("delete request type=%s path=%s client=%s", type, path, client)
    if not token:
        logger.warning("delete denied (token missing) type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=401, content="Token not found")
    if not await anyio.to_thread.run_sync(tools.check_token, 'delete_file', token):
        logger.warning("delete denied (token) type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=403, content="Access denied")
    if not tools.is_allowed_type(type):
        logger.warning("delete invalid type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=400, content="Invalid type")


    base_dir = os.path.join(MAIN_DIR, type)
    try:
        real_path = tools.safe_path(base_dir, path)
    except ValueError:
        logger.warning("delete path traversal type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=403, content="Access denied")


    # Функция которая удаляет файл и после этого рекурсивно удаляет все родительские папки, если они пусты
    def delete_file_and_parent_folders(file_path: str, root_dir: str):
        """
        Удаляет файл и после этого рекурсивно удаляет все родительские папки, если они пусты. Рекурсия прерывается при доходе до неприкосновенной части (no_delete).

        Parameters:
            file_path (str): The path of the file to be deleted.

        Returns:
            PlainTextResponse
        """
        # Если файл не существует, то ничего не делаем
        if not os.path.isfile(file_path):
            logger.info("delete not found type=%s path=%s client=%s", type, path, client)
            return PlainTextResponse(status_code=404, content="File not found")
        # Удаляем файл
        os.remove(file_path)
        # Получаем путь к родительской папке файла
        folder_path = os.path.dirname(file_path)
        root_dir = os.path.abspath(root_dir)
        # Удаляем папку, если она пуста
        while (
            folder_path
            and os.path.commonpath([folder_path, root_dir]) == root_dir
            and folder_path != root_dir
        ):
            if not os.listdir(folder_path):
                # удаляем ее
                os.rmdir(folder_path)
                # получаем путь к родительской папке
                folder_path = os.path.dirname(folder_path)
            # Если в папке есть файлы,
            else:
                # то ничего не делаем
                break
        
        logger.info("delete ok type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=200, content="File deleted")

    return await anyio.to_thread.run_sync(delete_file_and_parent_folders, real_path, base_dir)
