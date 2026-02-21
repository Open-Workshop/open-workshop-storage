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
from telemetry import setup_uptrace_telemetry


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
setup_uptrace_telemetry(app)


@app.on_event("startup")
async def _check_7z_dependency() -> None:
    try:
        tools.ensure_7z_available()
    except Exception as exc:
        logger.error("7z dependency missing: %s", exc)
        raise

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


async def _extract_token(request: Request) -> Optional[str]:
    token = request.query_params.get("token")
    if token:
        return token
    if request.method in ("POST", "PUT", "DELETE"):
        form = await request.form()
        return form.get("token")
    return None


@app.get(
    "/transfer/start",
    tags=["Transfer"],
    summary="Start transfer from URL",
    description=(
        "Starts a background download from the URL embedded in the transfer JWT. "
        "JWT must contain: job_id, mod_id (optional), download_url, pack_format, pack_level. "
        "Token can be passed as query param `token` or form field `token` for POST. "
        "Returns job_id and WebSocket URL for progress updates."
    ),
    responses={
        200: {
            "description": "Transfer started",
            "content": {
                "application/json": {
                    "example": {
                        "job_id": "3f2c1b7a0c9f4c7c8e5f1a2b3c4d5e6f",
                        "status": "started",
                        "ws_url": "/transfer/ws/3f2c1b7a0c9f4c7c8e5f1a2b3c4d5e6f",
                    }
                }
            },
        },
        400: {"description": "Invalid request", "content": {"text/plain": {"example": "Invalid job id"}}},
        401: {"description": "Token not found", "content": {"text/plain": {"example": "Token not found"}}},
        403: {"description": "Access denied", "content": {"text/plain": {"example": "Access denied"}}},
    },
    openapi_extra={
        "parameters": [
            {
                "name": "token",
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
                "description": "Transfer JWT (can also be sent in form body for POST).",
            }
        ]
    },
)
@app.post(
    "/transfer/start",
    tags=["Transfer"],
    summary="Start transfer from URL",
    description=(
        "Starts a background download from the URL embedded in the transfer JWT. "
        "JWT must contain: job_id, mod_id (optional), download_url, pack_format, pack_level. "
        "Token can be passed as query param `token` or form field `token` for POST. "
        "Returns job_id and WebSocket URL for progress updates."
    ),
    responses={
        200: {
            "description": "Transfer started",
            "content": {
                "application/json": {
                    "example": {
                        "job_id": "3f2c1b7a0c9f4c7c8e5f1a2b3c4d5e6f",
                        "status": "started",
                        "ws_url": "/transfer/ws/3f2c1b7a0c9f4c7c8e5f1a2b3c4d5e6f",
                    }
                }
            },
        },
        400: {"description": "Invalid request", "content": {"text/plain": {"example": "Invalid job id"}}},
        401: {"description": "Token not found", "content": {"text/plain": {"example": "Token not found"}}},
        403: {"description": "Access denied", "content": {"text/plain": {"example": "Access denied"}}},
    },
    openapi_extra={
        "parameters": [
            {
                "name": "token",
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
                "description": "Transfer JWT (can also be sent in form body for POST).",
            }
        ]
    },
)
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
        pack_level = int(payload.get("pack_level", 3))
    except (TypeError, ValueError):
        pack_level = 3
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

    update_only = bool(payload.get("update_only") or payload.get("keep_condition"))
    callback_payload = {
        "mod_id": mod_id,
        "pack_format": pack_format,
        "pack_level": pack_level,
        "update_only": update_only,
    }
    asyncio.create_task(
        _run_download_job(job_id, download_url, download_abs, max_bytes, callback_payload)
    )

    return {
        "job_id": job_id,
        "status": "started",
        "ws_url": f"/transfer/ws/{job_id}",
    }


@app.post(
    "/transfer/upload",
    tags=["Transfer"],
    summary="Upload file to Storage (raw body)",
    description=(
        "Uploads a file stream directly to Storage. "
        "Request body must be raw binary (application/octet-stream). "
        "Token can be passed via query `token` or `Authorization: Bearer <token>`. "
        "Optional filename can be passed via query `filename` or header `X-File-Name`. "
        "Returns job_id and byte counters. Progress is available via WebSocket."
    ),
    responses={
        200: {
            "description": "Upload accepted",
            "content": {
                "application/json": {
                    "example": {
                        "job_id": "3f2c1b7a0c9f4c7c8e5f1a2b3c4d5e6f",
                        "bytes": 123456,
                        "total": 654321,
                    }
                }
            },
        },
        400: {"description": "Invalid request", "content": {"text/plain": {"example": "Invalid job id"}}},
        401: {"description": "Token not found", "content": {"text/plain": {"example": "Token not found"}}},
        403: {"description": "Access denied", "content": {"text/plain": {"example": "Access denied"}}},
        413: {"description": "File too large", "content": {"text/plain": {"example": "File too large"}}},
        500: {"description": "Server error", "content": {"text/plain": {"example": "Upload failed"}}},
    },
    openapi_extra={
        "parameters": [
            {
                "name": "token",
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
                "description": "Transfer JWT (or use Authorization: Bearer <token>).",
            },
            {
                "name": "filename",
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
                "description": "Original filename (optional).",
            },
            {
                "name": "Authorization",
                "in": "header",
                "required": False,
                "schema": {"type": "string"},
                "description": "Bearer <transfer_jwt> (alternative to query token).",
            },
            {
                "name": "X-File-Name",
                "in": "header",
                "required": False,
                "schema": {"type": "string"},
                "description": "Original filename (alternative to query filename).",
            },
        ],
        "requestBody": {
            "required": True,
            "content": {"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}},
        }
    },
)
async def transfer_upload(request: Request):
    client = request.client.host if request.client else "unknown"
    token = request.query_params.get("token")
    if not token:
        auth = request.headers.get("Authorization") or ""
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1].strip()
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

    transfer_kind = str(payload.get("transfer_kind") or "archive").strip().lower()
    if transfer_kind not in {"archive", "img"}:
        return PlainTextResponse(status_code=400, content="Unsupported transfer kind")

    callback_context = payload.get("callback_context")
    if not isinstance(callback_context, dict):
        callback_context = {}
    callback_payload: dict[str, Any] = {
        "transfer_kind": transfer_kind,
        "callback_action": payload.get("callback_action"),
        "callback_context": callback_context,
        "target_path": payload.get("target_path"),
    }

    mod_id = None
    pack_format = "zip"
    pack_level = 3
    storage_type = ""
    file_kind = ""

    if transfer_kind == "archive":
        pack_format = payload.get("pack_format", "zip")
        if pack_format != "zip":
            return PlainTextResponse(status_code=400, content="Unsupported format")

        try:
            pack_level = int(payload.get("pack_level", 3))
        except (TypeError, ValueError):
            pack_level = 3
        pack_level = max(0, min(pack_level, 9))
        mod_id = payload.get("mod_id")
        update_only = bool(payload.get("update_only") or payload.get("keep_condition"))
        callback_payload.update(
            {
                "mod_id": mod_id,
                "pack_format": pack_format,
                "pack_level": pack_level,
                "update_only": update_only,
            }
        )
    else:
        storage_type = str(payload.get("storage_type") or "").strip().lower()
        if not tools.is_allowed_upload_type(storage_type):
            return PlainTextResponse(status_code=400, content="Invalid storage type")
        file_kind = tools.normalize_file_kind(payload.get("file_kind"), default="")
        if file_kind != "img":
            return PlainTextResponse(status_code=400, content="Invalid file kind")
        callback_payload.update({"storage_type": storage_type, "file_kind": file_kind})

    max_bytes_raw = payload.get("max_bytes", None)
    max_bytes = max_bytes_raw if max_bytes_raw is not None else getattr(config, "TRANSFER_MAX_BYTES", None)
    try:
        max_bytes = int(max_bytes) if max_bytes is not None else None
    except (TypeError, ValueError):
        max_bytes = None
    if max_bytes is not None and max_bytes <= 0:
        max_bytes = None

    filename = request.query_params.get("filename") or request.headers.get("X-File-Name")
    safe_name = tools.sanitize_filename(
        filename,
        default="upload.zip" if transfer_kind == "archive" else "upload.img",
    )
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
        "transfer upload start job_id=%s kind=%s mod_id=%s storage_type=%s filename=%s size_hint=%s client=%s",
        job_id,
        transfer_kind,
        mod_id,
        storage_type,
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
        "transfer_kind": transfer_kind,
        "storage_type": storage_type,
        "file_kind": file_kind,
        "filename": safe_name,
        "download_path": upload_rel,
        "pack_format": pack_format,
        "pack_level": pack_level,
        "status": "uploading",
        "created_at": int(time.time()),
    }
    await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)

    await _set_stage(job_id, "uploading")

    downloaded = 0
    last_push = 0.0
    start_ts = time.monotonic()
    last_log_bytes = 0
    next_percent = 10

    try:
        os.makedirs(os.path.dirname(upload_abs), exist_ok=True)
        with open(upload_abs, "wb") as out_file:
            async for chunk in request.stream():
                if not chunk:
                    continue
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
                            "stage": "uploading",
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

        if transfer_kind == "archive":
            archive_type, is_encrypted, archive_entries = await anyio.to_thread.run_sync(
                tools.probe_archive, upload_abs
            )
            if is_encrypted:
                await _set_state(job_id, status="error", error="encrypted_zip")
                await _broadcast(job_id, {"event": "error", "message": "zip encrypted"})
                try:
                    meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
                    meta.update({"status": "error", "error_reason": "encrypted_zip"})
                    await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
                except Exception:
                    logger.warning("failed to update meta for job_id=%s", job_id)
                try:
                    if os.path.exists(upload_abs):
                        os.remove(upload_abs)
                except Exception:
                    logger.warning("failed to cleanup encrypted zip job_id=%s", job_id)
                await _notify_manager(
                    {
                        **callback_payload,
                        "job_id": job_id,
                        "status": "error",
                        "reason": "encrypted_zip",
                    }
                )
                return PlainTextResponse(status_code=400, content="Encrypted zip not allowed")

            await _set_stage(job_id, "uploaded")

            repack_ok, _, _, repack_reason = await _run_repack_job(
                job_id=job_id,
                download_abs=upload_abs,
                pack_format=pack_format,
                pack_level=pack_level,
            )
            if not repack_ok:
                await _notify_manager(
                    {
                        **callback_payload,
                        "job_id": job_id,
                        "status": "error",
                        "reason": repack_reason or "repack_failed",
                    }
                )
                if repack_reason == "encrypted_zip":
                    return PlainTextResponse(status_code=400, content="Encrypted zip not allowed")
                return PlainTextResponse(status_code=500, content="Repack failed")
        else:
            await _set_stage(job_id, "processing")
            packed_rel = os.path.join("temp", job_id, "packed.webp")
            packed_abs = tools.safe_path(MAIN_DIR, packed_rel)
            try:
                await anyio.to_thread.run_sync(tools.image_file_to_webp, upload_abs, packed_abs)
            except ValueError:
                await _set_state(job_id, status="error", error="not_image")
                await _broadcast(job_id, {"event": "error", "message": "image expected"})
                try:
                    meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
                    meta.update({"status": "error", "error_reason": "not_image"})
                    await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
                except Exception:
                    logger.warning("failed to update meta for job_id=%s", job_id)
                try:
                    if os.path.exists(upload_abs):
                        os.remove(upload_abs)
                except Exception:
                    logger.warning("failed to cleanup invalid image job_id=%s", job_id)
                await _notify_manager(
                    {
                        **callback_payload,
                        "job_id": job_id,
                        "status": "error",
                        "reason": "not_image",
                    }
                )
                return PlainTextResponse(status_code=400, content="Image expected")
            except Exception:
                logger.exception("transfer image preparation failed job_id=%s", job_id)
                try:
                    if os.path.exists(upload_abs):
                        os.remove(upload_abs)
                    if os.path.exists(packed_abs):
                        os.remove(packed_abs)
                except Exception:
                    logger.warning("failed to cleanup image prep files job_id=%s", job_id)
                await _notify_manager(
                    {
                        **callback_payload,
                        "job_id": job_id,
                        "status": "error",
                        "reason": "image_prepare_failed",
                    }
                )
                return PlainTextResponse(status_code=500, content="Image preparation failed")

            try:
                if os.path.exists(upload_abs):
                    os.remove(upload_abs)
            except Exception:
                logger.warning("failed to cleanup source upload file job_id=%s", job_id)

            packed_bytes = os.path.getsize(packed_abs)
            try:
                meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
                meta.update(
                    {
                        "packed_path": packed_rel,
                        "packed_bytes": packed_bytes,
                        "status": "packed",
                        "packed_format": "webp",
                    }
                )
                await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
            except Exception:
                logger.warning("failed to update image meta for job_id=%s", job_id)
            await _set_stage(job_id, "packed")

        await _broadcast(
            job_id,
            {
                "event": "complete",
                "bytes": downloaded,
                "total": total,
                "stage": "packed",
            },
        )
        await _notify_manager(
            {
                **callback_payload,
                "job_id": job_id,
                "status": "success",
                "bytes": downloaded,
                "total": total,
                "packed_format": "zip" if transfer_kind == "archive" else "webp",
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
                "stage": "pending",
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
                "stage": state.get("stage"),
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


@app.post(
    "/transfer/repack",
    tags=["Transfer"],
    summary="Repack uploaded file",
    description=(
        "Repackages the uploaded file into a ZIP archive. "
        "Intended for manager-side maintenance. Requires `storage_manage_token`."
    ),
    responses={
        200: {
            "description": "Repack complete",
            "content": {
                "application/json": {
                    "example": {
                        "job_id": "3f2c1b7a0c9f4c7c8e5f1a2b3c4d5e6f",
                        "packed_bytes": 123456,
                        "packed_path": "temp/3f2c1b7a0c9f4c7c8e5f1a2b3c4d5e6f/packed.zip",
                    }
                }
            },
        },
        400: {"description": "Invalid request", "content": {"text/plain": {"example": "Invalid job id"}}},
        401: {"description": "Token not found", "content": {"text/plain": {"example": "Token not found"}}},
        403: {"description": "Access denied", "content": {"text/plain": {"example": "Access denied"}}},
        404: {"description": "Job not found", "content": {"text/plain": {"example": "Job not found"}}},
        500: {"description": "Repack failed", "content": {"text/plain": {"example": "Repack failed"}}},
    },
)
async def transfer_repack(
    request: Request,
    job_id: str = Form(),
    format: str = Form("zip"),
    compression_level: int = Form(3),
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
    try:
        compression_level = int(compression_level)
    except (TypeError, ValueError):
        compression_level = 3
    compression_level = max(0, min(compression_level, 9))

    logger.info(
        "transfer repack start job_id=%s format=%s level=%s client=%s",
        job_id,
        format,
        compression_level,
        client,
    )
    repack_ok, packed_rel, packed_bytes, repack_reason = await _run_repack_job(
        job_id=job_id,
        download_abs=download_abs,
        pack_format=format,
        pack_level=compression_level,
    )
    if not repack_ok:
        if repack_reason == "encrypted_zip":
            return PlainTextResponse(status_code=400, content="Encrypted zip not allowed")
        return PlainTextResponse(status_code=500, content="Repack failed")

    return {
        "job_id": job_id,
        "packed_bytes": packed_bytes,
        "packed_path": packed_rel,
    }


@app.post(
    "/transfer/move",
    tags=["Transfer"],
    summary="Move packed archive to permanent storage",
    description=(
        "Moves repacked file to permanent storage path. "
        "Requires `storage_manage_token` and a valid job_id."
    ),
    responses={
        200: {
            "description": "Move complete",
            "content": {
                "application/json": {
                    "example": {
                        "job_id": "3f2c1b7a0c9f4c7c8e5f1a2b3c4d5e6f",
                        "final_path": "archive/mods/1234/main.zip",
                        "final_bytes": 123456,
                    }
                }
            },
        },
        400: {"description": "Invalid request", "content": {"text/plain": {"example": "Invalid type"}}},
        401: {"description": "Token not found", "content": {"text/plain": {"example": "Token not found"}}},
        403: {"description": "Access denied", "content": {"text/plain": {"example": "Access denied"}}},
        404: {"description": "Job not found", "content": {"text/plain": {"example": "Job not found"}}},
        423: {"description": "Access denied", "content": {"text/plain": {"example": "Access denied"}}},
    },
)
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
            {
                "status": "pending",
                "stage": "pending",
                "bytes": 0,
                "total": None,
                "error": None,
                "clients": set(),
            },
        )
        state.update(updates)


async def _set_stage(job_id: str, stage: str) -> None:
    await _set_state(job_id, stage=stage)
    await _broadcast(job_id, {"event": "stage", "stage": stage})


async def _run_repack_job(
    job_id: str,
    download_abs: str,
    pack_format: str,
    pack_level: int,
) -> tuple[bool, Optional[str], Optional[int], Optional[str]]:
    if pack_format != "zip":
        await _set_state(job_id, status="error", error="unsupported_format")
        await _broadcast(job_id, {"event": "error", "message": "unsupported format"})
        return False, None, None, "unsupported_format"

    packed_rel = os.path.join("temp", job_id, "packed.zip")
    packed_abs = tools.safe_path(MAIN_DIR, packed_rel)

    await _set_stage(job_id, "repacking")
    archive_type, is_encrypted, archive_entries = await anyio.to_thread.run_sync(
        tools.probe_archive, download_abs
    )
    if is_encrypted:
        await _set_state(job_id, status="error", error="encrypted_zip")
        await _broadcast(job_id, {"event": "error", "message": "zip encrypted"})
        try:
            meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
            meta.update({"status": "error", "error_reason": "encrypted_zip"})
            await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
        except Exception:
            logger.warning("failed to update meta for job_id=%s", job_id)
        logger.warning("transfer repack denied (encrypted zip) job_id=%s", job_id)
        return False, None, None, "encrypted_zip"
    if archive_type == "zip":
        zip_ok = await anyio.to_thread.run_sync(
            tools.zip_uses_deflated_or_better,
            download_abs,
            archive_entries,
        )
        if zip_ok:
            try:
                packed_rel = os.path.relpath(download_abs, MAIN_DIR)
                packed_abs = download_abs
                packed_bytes = os.path.getsize(packed_abs)
                meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
                meta.update(
                    {
                        "packed_path": packed_rel,
                        "packed_bytes": packed_bytes,
                        "pack_format": pack_format,
                        "pack_level": pack_level,
                        "status": "packed",
                    }
                )
                await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
                await _set_stage(job_id, "packed")
                logger.info(
                    "transfer repack skipped (zip ok) job_id=%s packed_bytes=%s",
                    job_id,
                    packed_bytes,
                )
                return True, packed_rel, packed_bytes, None
            except Exception:
                logger.warning("failed to update meta for job_id=%s", job_id)

    if os.path.exists(packed_abs):
        packed_bytes = os.path.getsize(packed_abs)
        try:
            meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
            meta.update(
                {
                    "packed_path": packed_rel,
                    "packed_bytes": packed_bytes,
                    "pack_format": pack_format,
                    "pack_level": pack_level,
                    "status": "packed",
                }
            )
            await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
        except Exception:
            logger.warning("failed to update meta for job_id=%s", job_id)
        await _set_stage(job_id, "packed")
        return True, packed_rel, packed_bytes, None

    try:
        repack_rel = os.path.join("temp", job_id, "repack")
        repack_abs = tools.safe_path(MAIN_DIR, repack_rel)
        if os.path.exists(repack_abs):
            await anyio.to_thread.run_sync(shutil.rmtree, repack_abs)
        os.makedirs(repack_abs, exist_ok=True)

        if archive_type:
            await anyio.to_thread.run_sync(
                tools.safe_extract_archive,
                download_abs,
                repack_abs,
                archive_entries,
            )
        else:
            dest_name = os.path.basename(download_abs)
            dest_path = os.path.join(repack_abs, dest_name)
            await anyio.to_thread.run_sync(shutil.move, download_abs, dest_path)
            try:
                meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
                meta["download_path"] = os.path.relpath(dest_path, MAIN_DIR)
                await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
            except Exception:
                logger.warning("failed to update meta download_path for job_id=%s", job_id)

        start_ts = time.monotonic()
        await anyio.to_thread.run_sync(
            tools.zip_dir_with_level,
            repack_abs,
            packed_abs,
            pack_level,
        )
        duration = time.monotonic() - start_ts
        packed_bytes = os.path.getsize(packed_abs)
        try:
            meta = await anyio.to_thread.run_sync(_read_meta_sync, job_id)
            meta.update(
                {
                    "packed_path": packed_rel,
                    "packed_bytes": packed_bytes,
                    "pack_format": pack_format,
                    "pack_level": pack_level,
                    "status": "packed",
                }
            )
            await anyio.to_thread.run_sync(_write_meta_sync, job_id, meta)
        except Exception:
            logger.warning("failed to update meta for job_id=%s", job_id)
        await _set_stage(job_id, "packed")
        logger.info(
            "transfer repack done job_id=%s packed_bytes=%s duration=%.2fs",
            job_id,
            packed_bytes,
            duration,
        )
        return True, packed_rel, packed_bytes, None
    except Exception as exc:
        logger.exception("transfer repack failed job_id=%s", job_id)
        await _set_state(job_id, status="error", error=str(exc))
        await _broadcast(job_id, {"event": "error", "message": "repack failed"})
        return False, None, None, "repack_failed"


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
    await _set_stage(job_id, "downloading")
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
                                    "stage": "downloading",
                                },
                            )

        await _set_state(job_id, status="done", bytes=downloaded, total=total)
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
        await _set_stage(job_id, "downloaded")

        repack_ok, _, _, repack_reason = await _run_repack_job(
            job_id=job_id,
            download_abs=download_abs,
            pack_format=callback_payload.get("pack_format", "zip"),
            pack_level=int(callback_payload.get("pack_level", 3)),
        )
        if not repack_ok:
            if repack_reason == "encrypted_zip":
                try:
                    if os.path.exists(download_abs):
                        os.remove(download_abs)
                except Exception:
                    logger.warning("failed to cleanup encrypted zip job_id=%s", job_id)
            await _notify_manager(
                {
                    **callback_payload,
                    "job_id": job_id,
                    "status": "error",
                    "reason": repack_reason or "repack_failed",
                }
            )
            return

        await _broadcast(
            job_id,
            {
                "event": "complete",
                "bytes": downloaded,
                "total": total,
                "stage": "packed",
            },
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
    tags=["Files"],
    summary="Download stored file",
    description=(
        "Downloads a stored file. For archive/mod downloads access is validated via Manager. "
        "Optional query param `filename` can be used to override download name (safe chars only)."
    ),
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
                async with session.get(f"{MANAGER_URL}/mods/access/[{mod_id}]?user={user}", headers=headers) as resp:
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
    tags=["Files"],
    summary="Upload file to Storage (internal)",
    description=(
        "Internal upload endpoint for Manager. "
        "Accepts multipart form-data with file, type, path and file_kind. "
        "Requires upload token."
    ),
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
async def upload(
    request: Request,
    file: UploadFile,
    type: str = Form(),
    path: str = Form(),
    file_kind: str = Form("bin"),
    token: str = Form(),
):
    """
    Загружает файл в Storage (микросервис хранения, функция управляется другим микросервисов).

    type: Тип файла. Поддерживает следующие значения: resource, avatar.

    path: Путь и имя файла. В формате "директории/поддиректории/имя.файла". Если под папок нет существует, то они создаются.
    """
    client = request.client.host if request.client else "unknown"
    logger.info(
        "upload request type=%s path=%s file_kind=%s filename=%s client=%s",
        type,
        path,
        file_kind,
        file.filename,
        client,
    )
    if not token:
        logger.warning("upload denied (token missing) type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=401, content="Token not found")
    if not await anyio.to_thread.run_sync(tools.check_token, 'upload_file', token):
        logger.warning("upload denied (token) type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=403, content="Access denied")
    if not tools.is_allowed_upload_type(type):
        logger.warning("upload invalid type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=400, content="Invalid type")
    normalized_file_kind = tools.normalize_file_kind(file_kind, default="")
    if not normalized_file_kind:
        logger.warning(
            "upload invalid file_kind=%s type=%s path=%s client=%s",
            file_kind,
            type,
            path,
            client,
        )
        return PlainTextResponse(status_code=400, content="Invalid file kind")
    if type == "avatar" and normalized_file_kind != "img":
        return PlainTextResponse(status_code=400, content="Avatar requires image file kind")

    base_dir = os.path.join(MAIN_DIR, type)
    try:
        real_path = tools.safe_path(base_dir, path)
    except ValueError:
        logger.warning("upload path traversal type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=423, content="Access denied")
    # Проверяем существует ли директория, если нет, то создаем
    if not os.path.exists(os.path.dirname(real_path)):
        os.makedirs(os.path.dirname(real_path))

    if normalized_file_kind == "img":
        if not path.lower().endswith(".webp"):
            return PlainTextResponse(
                status_code=400, content="Image storage path must end with .webp"
            )
        raw_bytes = await file.read()
        try:
            webp_bytes = await anyio.to_thread.run_sync(tools.image_bytes_to_webp, raw_bytes)
        except ValueError:
            return PlainTextResponse(status_code=400, content="Image expected")

        def _write_bytes_sync() -> None:
            with open(real_path, "wb") as out_file:
                out_file.write(webp_bytes)

        await anyio.to_thread.run_sync(_write_bytes_sync)
    else:
        # Сохраняем файл без изменений
        await anyio.to_thread.run_sync(tools.copy_fileobj_to_path, file.file, real_path)
    logger.info(
        "upload saved type=%s path=%s file_kind=%s client=%s",
        type,
        path,
        normalized_file_kind,
        client,
    )
    return path

@app.delete(
    "/delete",
    tags=["Files"],
    summary="Delete file from Storage (internal)",
    description=(
        "Internal delete endpoint for Manager. "
        "Deletes file and empty parent folders. Requires delete token."
    ),
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
