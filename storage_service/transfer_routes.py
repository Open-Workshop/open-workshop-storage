from __future__ import annotations

import asyncio
import os
import shutil
import time
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import anyio
from fastapi import APIRouter, Form, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse

from .context import ServiceContext


router = APIRouter()
_context_provider: Optional[Callable[[], ServiceContext]] = None


def configure_context_provider(provider: Callable[[], ServiceContext]) -> None:
    global _context_provider
    _context_provider = provider


def _ctx() -> ServiceContext:
    if _context_provider is None:
        raise RuntimeError("transfer context provider is not configured")
    return _context_provider()


async def _extract_token(request: Request) -> Optional[str]:
    token = request.query_params.get("token")
    if token:
        return token
    if request.method in ("POST", "PUT", "DELETE"):
        form = await request.form()
        return form.get("token")
    return None


def _coerce_max_bytes(raw_value: Any, fallback: Any) -> Optional[int]:
    max_bytes = raw_value if raw_value is not None else fallback
    try:
        max_bytes = int(max_bytes) if max_bytes is not None else None
    except (TypeError, ValueError):
        max_bytes = None
    if max_bytes is not None and max_bytes <= 0:
        return None
    return max_bytes


def _parse_non_negative_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


async def _update_meta_fields(
    ctx: ServiceContext,
    job_id: str,
    updates: dict[str, Any],
    *,
    warning_message: str = "failed to update meta for job_id=%s",
) -> None:
    try:
        meta = await anyio.to_thread.run_sync(ctx.read_meta_sync, job_id)
        meta.update(updates)
        await anyio.to_thread.run_sync(ctx.write_meta_sync, job_id, meta)
    except Exception:
        ctx.logger.warning(warning_message, job_id)


def _remove_if_exists(ctx: ServiceContext, path: str, warning_template: str, job_id: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        ctx.logger.warning(warning_template, job_id)


@router.get(
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
@router.post(
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
    ctx = _ctx()
    token = await _extract_token(request)
    if not token:
        return PlainTextResponse(status_code=401, content="Token not found")

    payload = ctx.tools.decode_transfer_jwt(token, audience="storage")
    if not payload:
        return PlainTextResponse(status_code=403, content="Access denied")

    job_id = str(payload.get("job_id", ""))
    if not ctx.tools.is_safe_job_id(job_id):
        return PlainTextResponse(status_code=400, content="Invalid job id")

    download_url = payload.get("download_url")
    if not download_url:
        return PlainTextResponse(status_code=400, content="Download URL missing")

    parsed = urlparse(download_url)
    if parsed.scheme not in {"http", "https"}:
        return PlainTextResponse(status_code=400, content="Invalid download URL")

    filename = payload.get("filename") or os.path.basename(parsed.path)
    safe_name = ctx.tools.sanitize_filename(filename)

    download_rel = os.path.join("temp", job_id, safe_name)
    download_abs = ctx.tools.safe_path(ctx.main_dir, download_rel)

    pack_format = payload.get("pack_format", "zip")
    try:
        pack_level = int(payload.get("pack_level", 3))
    except (TypeError, ValueError):
        pack_level = 3
    mod_id = payload.get("mod_id")

    max_bytes = _coerce_max_bytes(
        payload.get("max_bytes", None),
        getattr(ctx.config, "TRANSFER_MAX_BYTES", None),
    )

    async with ctx.job_lock:
        state = ctx.job_state.get(job_id)
        if state and state.get("started"):
            return {
                "job_id": job_id,
                "status": state.get("status"),
                "ws_url": f"/transfer/ws/{job_id}",
            }
        if not state:
            state = ctx.new_job_state()
            ctx.job_state[job_id] = state
        state.update(
            {
                "started": True,
                "status": "pending",
                "stage": "pending",
                "bytes": 0,
                "total": None,
                "percent": None,
                "error": None,
                "last_activity": time.time(),
            }
        )

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
    await anyio.to_thread.run_sync(ctx.write_meta_sync, job_id, meta)

    update_only = bool(payload.get("update_only") or payload.get("keep_condition"))
    callback_payload = {
        "mod_id": mod_id,
        "pack_format": pack_format,
        "pack_level": pack_level,
        "update_only": update_only,
    }
    asyncio.create_task(
        ctx.run_download_job(job_id, download_url, download_abs, max_bytes, callback_payload)
    )

    return {
        "job_id": job_id,
        "status": "started",
        "ws_url": f"/transfer/ws/{job_id}",
    }


@router.post(
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
        },
    },
)
async def transfer_upload(request: Request):
    ctx = _ctx()
    client = request.client.host if request.client else "unknown"
    token = request.query_params.get("token")
    if not token:
        auth = request.headers.get("Authorization") or ""
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1].strip()
    if not token:
        ctx.logger.warning("transfer upload denied (token missing) client=%s", client)
        return PlainTextResponse(status_code=401, content="Token not found")

    payload = ctx.tools.decode_transfer_jwt(token, audience="storage")
    if not payload:
        ctx.logger.warning("transfer upload denied (token) client=%s", client)
        return PlainTextResponse(status_code=403, content="Access denied")

    job_id = str(payload.get("job_id", ""))
    if not ctx.tools.is_safe_job_id(job_id):
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
        if not ctx.tools.is_allowed_upload_type(storage_type):
            return PlainTextResponse(status_code=400, content="Invalid storage type")
        file_kind = ctx.tools.normalize_file_kind(payload.get("file_kind"), default="")
        if file_kind != "img":
            return PlainTextResponse(status_code=400, content="Invalid file kind")
        callback_payload.update({"storage_type": storage_type, "file_kind": file_kind})

    max_bytes = _coerce_max_bytes(
        payload.get("max_bytes", None),
        getattr(ctx.config, "TRANSFER_MAX_BYTES", None),
    )

    filename = request.query_params.get("filename") or request.headers.get("X-File-Name")
    safe_name = ctx.tools.sanitize_filename(
        filename,
        default="upload.zip" if transfer_kind == "archive" else "upload.img",
    )
    upload_rel = os.path.join("temp", job_id, safe_name)
    upload_abs = ctx.tools.safe_path(ctx.main_dir, upload_rel)

    total = _parse_non_negative_int(request.headers.get("content-length"))
    if total is None:
        total = _parse_non_negative_int(request.query_params.get("size"))
    if total is None:
        total = _parse_non_negative_int(
            request.headers.get("X-File-Size") or request.headers.get("X-Upload-Size")
        )
    ctx.logger.info(
        "transfer upload start job_id=%s kind=%s mod_id=%s storage_type=%s filename=%s size_hint=%s client=%s",
        job_id,
        transfer_kind,
        mod_id,
        storage_type,
        safe_name,
        total,
        client,
    )

    async with ctx.job_lock:
        state = ctx.job_state.get(job_id)
        if not state:
            state = ctx.new_job_state()
            ctx.job_state[job_id] = state
        state.update(
            {
                "started": True,
                "status": "uploading",
                "stage": "uploading",
                "bytes": 0,
                "total": total,
                "percent": None,
                "error": None,
                "last_activity": time.time(),
            }
        )

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
    await anyio.to_thread.run_sync(ctx.write_meta_sync, job_id, meta)

    await ctx.set_stage(job_id, "uploading")

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
                    await ctx.set_state(job_id, status="error", error="size_limit")
                    await ctx.broadcast(
                        job_id,
                        await ctx.build_state_event(job_id, "error", message="file too large"),
                    )
                    _remove_if_exists(
                        ctx,
                        upload_abs,
                        "failed to cleanup partial file job_id=%s",
                        job_id,
                    )
                    await ctx.notify_manager(
                        {
                            **callback_payload,
                            "job_id": job_id,
                            "status": "error",
                            "reason": "size_limit",
                        }
                    )
                    await ctx.job_error_cleanup(job_id, "size_limit")
                    return PlainTextResponse(status_code=413, content="File too large")

                await anyio.to_thread.run_sync(out_file.write, chunk)
                now = time.monotonic()
                if now - last_push >= ctx.progress_push_interval:
                    last_push = now
                    await ctx.set_state(job_id, bytes=downloaded)
                    await ctx.broadcast(job_id, await ctx.build_state_event(job_id, "progress"))
                if total:
                    percent = int((downloaded / total) * 100)
                    if percent >= next_percent:
                        ctx.logger.info(
                            "transfer upload progress job_id=%s percent=%s bytes=%s",
                            job_id,
                            percent,
                            downloaded,
                        )
                        next_percent += 10
                elif downloaded - last_log_bytes >= 50 * 1024 * 1024:
                    last_log_bytes = downloaded
                    ctx.logger.info(
                        "transfer upload progress job_id=%s bytes=%s",
                        job_id,
                        downloaded,
                    )

        final_total = total if total is not None else downloaded
        await ctx.set_state(job_id, bytes=downloaded, total=final_total)
        await ctx.broadcast(job_id, await ctx.build_state_event(job_id, "progress"))
        await ctx.set_state(job_id, status="done", bytes=downloaded, total=final_total)
        await _update_meta_fields(
            ctx,
            job_id,
            {
                "status": "uploaded",
                "downloaded_bytes": downloaded,
                "total_bytes": final_total,
                "upload_completed_at": int(time.time()),
            },
        )

        duration = time.monotonic() - start_ts
        ctx.logger.info(
            "transfer upload done job_id=%s bytes=%s duration=%.2fs",
            job_id,
            downloaded,
            duration,
        )

        unpacked_bytes = None
        if transfer_kind == "archive":
            _, is_encrypted, _ = await anyio.to_thread.run_sync(ctx.tools.probe_archive, upload_abs)
            if is_encrypted:
                await ctx.set_state(job_id, status="error", error="encrypted_zip")
                await ctx.broadcast(
                    job_id,
                    await ctx.build_state_event(job_id, "error", message="zip encrypted"),
                )
                await _update_meta_fields(
                    ctx,
                    job_id,
                    {"status": "error", "error_reason": "encrypted_zip"},
                )
                _remove_if_exists(
                    ctx,
                    upload_abs,
                    "failed to cleanup encrypted zip job_id=%s",
                    job_id,
                )
                await ctx.notify_manager(
                    {
                        **callback_payload,
                        "job_id": job_id,
                        "status": "error",
                        "reason": "encrypted_zip",
                    }
                )
                await ctx.job_error_cleanup(job_id, "encrypted_zip")
                return PlainTextResponse(status_code=400, content="Encrypted zip not allowed")

            await ctx.set_stage(job_id, "uploaded")

            repack_ok, _, _, unpacked_bytes, repack_reason = await ctx.run_repack_job(
                job_id,
                upload_abs,
                pack_format,
                pack_level,
            )
            if not repack_ok:
                await ctx.notify_manager(
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
            await ctx.set_stage(job_id, "processing")
            packed_rel = os.path.join("temp", job_id, "packed.webp")
            packed_abs = ctx.tools.safe_path(ctx.main_dir, packed_rel)
            try:
                await anyio.to_thread.run_sync(ctx.tools.image_file_to_webp, upload_abs, packed_abs)
            except ValueError:
                await ctx.set_state(job_id, status="error", error="not_image")
                await ctx.broadcast(
                    job_id,
                    await ctx.build_state_event(job_id, "error", message="image expected"),
                )
                await _update_meta_fields(
                    ctx,
                    job_id,
                    {"status": "error", "error_reason": "not_image"},
                )
                _remove_if_exists(
                    ctx,
                    upload_abs,
                    "failed to cleanup invalid image job_id=%s",
                    job_id,
                )
                await ctx.notify_manager(
                    {
                        **callback_payload,
                        "job_id": job_id,
                        "status": "error",
                        "reason": "not_image",
                    }
                )
                await ctx.job_error_cleanup(job_id, "not_image")
                return PlainTextResponse(status_code=400, content="Image expected")
            except Exception:
                ctx.logger.exception("transfer image preparation failed job_id=%s", job_id)
                _remove_if_exists(
                    ctx,
                    upload_abs,
                    "failed to cleanup image prep files job_id=%s",
                    job_id,
                )
                _remove_if_exists(
                    ctx,
                    packed_abs,
                    "failed to cleanup image prep files job_id=%s",
                    job_id,
                )
                await ctx.notify_manager(
                    {
                        **callback_payload,
                        "job_id": job_id,
                        "status": "error",
                        "reason": "image_prepare_failed",
                    }
                )
                await ctx.job_error_cleanup(job_id, "image_prepare_failed")
                return PlainTextResponse(status_code=500, content="Image preparation failed")

            _remove_if_exists(
                ctx,
                upload_abs,
                "failed to cleanup source upload file job_id=%s",
                job_id,
            )

            packed_bytes = os.path.getsize(packed_abs)
            await _update_meta_fields(
                ctx,
                job_id,
                {
                    "packed_path": packed_rel,
                    "packed_bytes": packed_bytes,
                    "status": "packed",
                    "packed_format": "webp",
                },
                warning_message="failed to update image meta for job_id=%s",
            )
            await ctx.set_stage(job_id, "packed")

        await ctx.broadcast(job_id, await ctx.build_state_event(job_id, "complete"))
        callback_success_payload = {
            **callback_payload,
            "job_id": job_id,
            "status": "success",
            "bytes": downloaded,
            "total": final_total,
            "packed_format": "zip" if transfer_kind == "archive" else "webp",
        }
        if transfer_kind == "archive" and unpacked_bytes is not None:
            callback_success_payload["unpacked_bytes"] = unpacked_bytes
        await ctx.notify_manager(callback_success_payload)
        return {
            "job_id": job_id,
            "bytes": downloaded,
            "total": final_total,
        }
    except Exception as exc:
        ctx.logger.exception("transfer upload failed job_id=%s", job_id)
        _remove_if_exists(ctx, upload_abs, "failed to cleanup partial file job_id=%s", job_id)
        await _update_meta_fields(
            ctx,
            job_id,
            {
                "status": "error",
                "error": str(exc),
                "upload_completed_at": int(time.time()),
            },
        )
        await ctx.set_state(job_id, status="error", error=str(exc))
        await ctx.broadcast(job_id, await ctx.build_state_event(job_id, "error", message="upload failed"))
        await ctx.notify_manager(
            {**callback_payload, "job_id": job_id, "status": "error", "reason": "exception"}
        )
        return PlainTextResponse(status_code=500, content="Upload failed")
    finally:
        await ctx.close_clients(job_id)


@router.websocket("/transfer/ws/{job_id}")
async def transfer_ws(websocket: WebSocket, job_id: str):
    ctx = _ctx()
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=1008)
        return
    payload = ctx.tools.decode_transfer_jwt(token, audience="storage")
    if not payload or str(payload.get("job_id", "")) != job_id:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    ctx.logger.info("transfer ws connect job_id=%s", job_id)
    snapshot = None
    async with ctx.job_lock:
        state = ctx.job_state.get(job_id)
        if not state:
            state = ctx.new_job_state()
            ctx.job_state[job_id] = state
        clients = state.get("clients")
        if not isinstance(clients, list):
            clients = list(clients) if clients else []
            state["clients"] = clients
        if websocket not in clients:
            clients.append(websocket)
        snapshot = {
            "event": "progress",
            "bytes": state.get("bytes", 0),
            "total": state.get("total"),
            "status": state.get("status"),
            "stage": state.get("stage"),
            "percent": state.get("percent"),
        }
    if snapshot is not None:
        try:
            await websocket.send_json(snapshot)
        except Exception:
            ctx.logger.exception("transfer ws initial send failed job_id=%s", job_id)
            await websocket.close()
            return
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                break
    except WebSocketDisconnect:
        pass
    except Exception:
        ctx.logger.exception("transfer ws failed job_id=%s", job_id)
    finally:
        async with ctx.job_lock:
            state = ctx.job_state.get(job_id)
            if state and websocket in state.get("clients", []):
                state["clients"].remove(websocket)
        ctx.logger.info("transfer ws disconnect job_id=%s", job_id)


@router.post(
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
    ctx = _ctx()
    client = request.client.host if request.client else "unknown"
    if not token:
        ctx.logger.warning("transfer repack denied (token missing) job_id=%s client=%s", job_id, client)
        return PlainTextResponse(status_code=401, content="Token not found")
    if not await anyio.to_thread.run_sync(ctx.tools.check_token, "storage_manage_token", token):
        ctx.logger.warning("transfer repack denied (token) job_id=%s client=%s", job_id, client)
        return PlainTextResponse(status_code=403, content="Access denied")
    if not ctx.tools.is_safe_job_id(job_id):
        return PlainTextResponse(status_code=400, content="Invalid job id")

    try:
        meta = await anyio.to_thread.run_sync(ctx.read_meta_sync, job_id)
    except Exception:
        return PlainTextResponse(status_code=404, content="Job not found")

    if format != "zip":
        return PlainTextResponse(status_code=400, content="Unsupported format")

    download_rel = meta.get("download_path")
    if not download_rel:
        return PlainTextResponse(status_code=404, content="Source file not found")

    download_abs = ctx.tools.safe_path(ctx.main_dir, download_rel)
    try:
        compression_level = int(compression_level)
    except (TypeError, ValueError):
        compression_level = 3
    compression_level = max(0, min(compression_level, 9))

    ctx.logger.info(
        "transfer repack start job_id=%s format=%s level=%s client=%s",
        job_id,
        format,
        compression_level,
        client,
    )
    repack_ok, packed_rel, packed_bytes, unpacked_bytes, repack_reason = await ctx.run_repack_job(
        job_id,
        download_abs,
        format,
        compression_level,
    )
    if not repack_ok:
        if repack_reason == "encrypted_zip":
            return PlainTextResponse(status_code=400, content="Encrypted zip not allowed")
        return PlainTextResponse(status_code=500, content="Repack failed")

    return {
        "job_id": job_id,
        "packed_bytes": packed_bytes,
        "packed_path": packed_rel,
        "unpacked_bytes": unpacked_bytes,
    }


@router.post(
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
    ctx = _ctx()
    client = request.client.host if request.client else "unknown"
    if not token:
        ctx.logger.warning("transfer move denied (token missing) job_id=%s client=%s", job_id, client)
        return PlainTextResponse(status_code=401, content="Token not found")
    if not await anyio.to_thread.run_sync(ctx.tools.check_token, "storage_manage_token", token):
        ctx.logger.warning("transfer move denied (token) job_id=%s client=%s", job_id, client)
        return PlainTextResponse(status_code=403, content="Access denied")
    if not ctx.tools.is_safe_job_id(job_id):
        return PlainTextResponse(status_code=400, content="Invalid job id")
    if not ctx.tools.is_allowed_type(type):
        return PlainTextResponse(status_code=400, content="Invalid type")

    try:
        meta = await anyio.to_thread.run_sync(ctx.read_meta_sync, job_id)
    except Exception:
        return PlainTextResponse(status_code=404, content="Job not found")

    packed_rel = meta.get("packed_path")
    if not packed_rel:
        return PlainTextResponse(status_code=404, content="Packed file not found")

    packed_abs = ctx.tools.safe_path(ctx.main_dir, packed_rel)
    base_dir = os.path.join(ctx.main_dir, type)
    try:
        real_path = ctx.tools.safe_path(base_dir, path)
    except ValueError:
        return PlainTextResponse(status_code=423, content="Access denied")

    ctx.logger.info(
        "transfer move start job_id=%s type=%s path=%s client=%s",
        job_id,
        type,
        path,
        client,
    )
    start_ts = time.monotonic()
    os.makedirs(os.path.dirname(real_path), exist_ok=True)
    await anyio.to_thread.run_sync(shutil.move, packed_abs, real_path)

    final_rel = os.path.relpath(real_path, ctx.main_dir)
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
    await anyio.to_thread.run_sync(ctx.write_meta_sync, job_id, meta)

    try:
        await anyio.to_thread.run_sync(shutil.rmtree, ctx.job_dir(job_id))
    except Exception:
        ctx.logger.warning("failed to cleanup temp dir job_id=%s", job_id)

    ctx.logger.info(
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
