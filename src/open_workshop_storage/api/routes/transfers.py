from __future__ import annotations

import asyncio
import os
import shutil
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import anyio
from fastapi import APIRouter, Form, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse

from ...core.context import ServiceContext
from ...core.job_meta import update_job_meta
from ...core.job_state import reset_job_state, state_event_payload

router = APIRouter()
_context_provider: Optional[Callable[[], ServiceContext]] = None

TRANSFER_START_DESCRIPTION = (
    "Starts a background download from the URL embedded in the transfer JWT. "
    "JWT must contain: job_id, mod_id (optional), download_url, pack_format, pack_level. "
    "Token can be passed as query param `token` or form field `token` for POST. "
    "Returns job_id and WebSocket URL for progress updates."
)
TRANSFER_START_RESPONSES: dict[int | str, dict[str, Any]] = {
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
}
TRANSFER_START_OPENAPI_EXTRA: dict[str, Any] = {
    "parameters": [
        {
            "name": "token",
            "in": "query",
            "required": False,
            "schema": {"type": "string"},
            "description": "Transfer JWT (can also be sent in form body for POST).",
        }
    ]
}


@dataclass(frozen=True)
class UploadSpec:
    job_id: str
    transfer_kind: str
    mod_id: Any
    storage_type: str
    file_kind: str
    pack_format: str
    pack_level: int
    callback_payload: dict[str, Any]
    max_bytes: Optional[int]
    safe_name: str
    upload_rel: str
    upload_abs: str


@dataclass(frozen=True)
class UploadStreamResult:
    downloaded: int
    final_total: int


def configure_context_provider(provider: Callable[[], ServiceContext]) -> None:
    global _context_provider
    _context_provider = provider


def _ctx() -> ServiceContext:
    if _context_provider is None:
        raise RuntimeError("transfer context provider is not configured")
    return _context_provider()


def _client_host(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _error_response(status_code: int, content: str) -> PlainTextResponse:
    return PlainTextResponse(status_code=status_code, content=content)


async def _extract_token(request: Request) -> Optional[str]:
    token = request.query_params.get("token")
    if token:
        return token
    if request.method in ("POST", "PUT", "DELETE"):
        form = await request.form()
        form_token = form.get("token")
        return form_token if isinstance(form_token, str) else None
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


def _extract_bearer_token(request: Request) -> Optional[str]:
    token = request.query_params.get("token")
    if token:
        return token
    auth = request.headers.get("Authorization") or ""
    if auth.startswith("Bearer "):
        return auth.split(" ", 1)[1].strip()
    return None


def _parse_pack_level(raw_value: Any, default: int = 3) -> int:
    try:
        level = int(raw_value)
    except (TypeError, ValueError):
        level = default
    return max(0, min(level, 9))


def _read_upload_size_hint(request: Request) -> Optional[int]:
    total = _parse_non_negative_int(request.headers.get("content-length"))
    if total is None:
        total = _parse_non_negative_int(request.query_params.get("size"))
    if total is None:
        total = _parse_non_negative_int(request.headers.get("X-File-Size") or request.headers.get("X-Upload-Size"))
    return total


def _remove_if_exists(ctx: ServiceContext, path: str, warning_template: str, job_id: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        ctx.logger.warning(warning_template, job_id)


def _build_upload_spec(
    ctx: ServiceContext,
    request: Request,
    payload: dict[str, Any],
) -> UploadSpec | PlainTextResponse:
    job_id = str(payload.get("job_id", ""))
    if not ctx.tools.is_safe_job_id(job_id):
        return _error_response(400, "Invalid job id")

    transfer_kind = str(payload.get("transfer_kind") or "archive").strip().lower()
    if transfer_kind not in {"archive", "img"}:
        return _error_response(400, "Unsupported transfer kind")

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
            return _error_response(400, "Unsupported format")
        pack_level = _parse_pack_level(payload.get("pack_level", 3))
        mod_id = payload.get("mod_id")
        callback_payload.update(
            {
                "mod_id": mod_id,
                "pack_format": pack_format,
                "pack_level": pack_level,
                "update_only": bool(payload.get("update_only") or payload.get("keep_condition")),
            }
        )
    else:
        storage_type = str(payload.get("storage_type") or "").strip().lower()
        if not ctx.tools.is_allowed_upload_type(storage_type):
            return _error_response(400, "Invalid storage type")
        file_kind = ctx.tools.normalize_file_kind(payload.get("file_kind"), default="")
        if file_kind != "img":
            return _error_response(400, "Invalid file kind")
        callback_payload.update({"storage_type": storage_type, "file_kind": file_kind})

    safe_name = ctx.tools.sanitize_filename(
        request.query_params.get("filename") or request.headers.get("X-File-Name"),
        default="upload.zip" if transfer_kind == "archive" else "upload.img",
    )
    upload_rel = os.path.join("temp", job_id, safe_name)

    return UploadSpec(
        job_id=job_id,
        transfer_kind=transfer_kind,
        mod_id=mod_id,
        storage_type=storage_type,
        file_kind=file_kind,
        pack_format=pack_format,
        pack_level=pack_level,
        callback_payload=callback_payload,
        max_bytes=_coerce_max_bytes(
            payload.get("max_bytes", None),
            getattr(ctx.config, "TRANSFER_MAX_BYTES", None),
        ),
        safe_name=safe_name,
        upload_rel=upload_rel,
        upload_abs=ctx.tools.safe_path(ctx.main_dir, upload_rel),
    )


async def _notify_upload_error(ctx: ServiceContext, spec: UploadSpec, reason: str) -> None:
    await ctx.notify_manager(
        {
            **spec.callback_payload,
            "job_id": spec.job_id,
            "status": "error",
            "reason": reason,
        }
    )


async def _authorize_manage_request(
    ctx: ServiceContext,
    *,
    token: str,
    client: str,
    action: str,
    job_id: str,
) -> Optional[PlainTextResponse]:
    if not token:
        ctx.logger.warning("transfer %s denied (token missing) job_id=%s client=%s", action, job_id, client)
        return _error_response(401, "Token not found")
    if not await anyio.to_thread.run_sync(ctx.tools.check_token, "storage_manage_token", token):
        ctx.logger.warning("transfer %s denied (token) job_id=%s client=%s", action, job_id, client)
        return _error_response(403, "Access denied")
    return None


async def _read_job_meta(ctx: ServiceContext, job_id: str) -> Optional[dict[str, Any]]:
    try:
        return await anyio.to_thread.run_sync(ctx.read_meta_sync, job_id)
    except Exception:
        return None


async def _initialize_upload_job(
    ctx: ServiceContext,
    spec: UploadSpec,
    total: Optional[int],
) -> None:
    async with ctx.job_lock:
        state = ctx.job_state.get(spec.job_id)
        if not state:
            state = ctx.new_job_state()
            ctx.job_state[spec.job_id] = state
        reset_job_state(
            state,
            started=True,
            status="uploading",
            stage="uploading",
            total=total,
        )

    meta = {
        "job_id": spec.job_id,
        "mod_id": spec.mod_id,
        "transfer_kind": spec.transfer_kind,
        "storage_type": spec.storage_type,
        "file_kind": spec.file_kind,
        "filename": spec.safe_name,
        "download_path": spec.upload_rel,
        "pack_format": spec.pack_format,
        "pack_level": spec.pack_level,
        "status": "uploading",
        "created_at": int(time.time()),
    }
    await anyio.to_thread.run_sync(ctx.write_meta_sync, spec.job_id, meta)
    await ctx.set_stage(spec.job_id, "uploading")


def _log_upload_progress(
    ctx: ServiceContext,
    job_id: str,
    downloaded: int,
    total: Optional[int],
    last_log_bytes: int,
    next_percent: int,
) -> tuple[int, int]:
    if total:
        percent = int((downloaded / total) * 100)
        while percent >= next_percent:
            ctx.logger.info(
                "transfer upload progress job_id=%s percent=%s bytes=%s",
                job_id,
                next_percent,
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
    return last_log_bytes, next_percent


async def _stream_upload_body(
    ctx: ServiceContext,
    request: Request,
    spec: UploadSpec,
    total: Optional[int],
) -> UploadStreamResult | PlainTextResponse:
    downloaded = 0
    last_push = 0.0
    last_log_bytes = 0
    next_percent = 10

    os.makedirs(os.path.dirname(spec.upload_abs), exist_ok=True)
    with open(spec.upload_abs, "wb") as out_file:
        async for chunk in request.stream():
            if not chunk:
                continue
            downloaded += len(chunk)
            if spec.max_bytes and downloaded > spec.max_bytes:
                await ctx.set_state(spec.job_id, status="error", error="size_limit")
                await ctx.broadcast(
                    spec.job_id,
                    await ctx.build_state_event(spec.job_id, "error", message="file too large"),
                )
                _remove_if_exists(
                    ctx,
                    spec.upload_abs,
                    "failed to cleanup partial file job_id=%s",
                    spec.job_id,
                )
                await _notify_upload_error(ctx, spec, "size_limit")
                await ctx.job_error_cleanup(spec.job_id, "size_limit")
                return _error_response(413, "File too large")

            await anyio.to_thread.run_sync(out_file.write, chunk)
            now = time.monotonic()
            if now - last_push >= ctx.progress_push_interval:
                last_push = now
                await ctx.set_state(spec.job_id, bytes=downloaded)
                await ctx.broadcast(spec.job_id, await ctx.build_state_event(spec.job_id, "progress"))
            last_log_bytes, next_percent = _log_upload_progress(
                ctx,
                spec.job_id,
                downloaded,
                total,
                last_log_bytes,
                next_percent,
            )

    final_total = total if total is not None else downloaded
    await ctx.set_state(spec.job_id, bytes=downloaded, total=final_total)
    await ctx.broadcast(spec.job_id, await ctx.build_state_event(spec.job_id, "progress"))
    await ctx.set_state(spec.job_id, status="done", bytes=downloaded, total=final_total)
    await update_job_meta(
        ctx,
        spec.job_id,
        {
            "status": "uploaded",
            "downloaded_bytes": downloaded,
            "total_bytes": final_total,
            "upload_completed_at": int(time.time()),
        },
    )
    return UploadStreamResult(downloaded=downloaded, final_total=final_total)


async def _process_archive_upload(
    ctx: ServiceContext,
    spec: UploadSpec,
) -> tuple[Optional[int], Optional[PlainTextResponse]]:
    _, is_encrypted, _ = await anyio.to_thread.run_sync(ctx.tools.probe_archive, spec.upload_abs)
    if is_encrypted:
        await ctx.set_state(spec.job_id, status="error", error="encrypted_zip")
        await ctx.broadcast(
            spec.job_id,
            await ctx.build_state_event(spec.job_id, "error", message="zip encrypted"),
        )
        await update_job_meta(
            ctx,
            spec.job_id,
            {"status": "error", "error_reason": "encrypted_zip"},
        )
        _remove_if_exists(
            ctx,
            spec.upload_abs,
            "failed to cleanup encrypted zip job_id=%s",
            spec.job_id,
        )
        await _notify_upload_error(ctx, spec, "encrypted_zip")
        await ctx.job_error_cleanup(spec.job_id, "encrypted_zip")
        return None, _error_response(400, "Encrypted zip not allowed")

    await ctx.set_stage(spec.job_id, "uploaded")
    repack_ok, _, _, unpacked_bytes, repack_reason = await ctx.run_repack_job(
        spec.job_id,
        spec.upload_abs,
        spec.pack_format,
        spec.pack_level,
    )
    if repack_ok:
        return unpacked_bytes, None

    await _notify_upload_error(ctx, spec, repack_reason or "repack_failed")
    if repack_reason == "encrypted_zip":
        return None, _error_response(400, "Encrypted zip not allowed")
    return None, _error_response(500, "Repack failed")


async def _process_image_upload(
    ctx: ServiceContext,
    spec: UploadSpec,
) -> Optional[PlainTextResponse]:
    await ctx.set_stage(spec.job_id, "processing")
    packed_rel = os.path.join("temp", spec.job_id, "packed.webp")
    packed_abs = ctx.tools.safe_path(ctx.main_dir, packed_rel)

    try:
        await anyio.to_thread.run_sync(ctx.tools.image_file_to_webp, spec.upload_abs, packed_abs)
    except ValueError:
        await ctx.set_state(spec.job_id, status="error", error="not_image")
        await ctx.broadcast(
            spec.job_id,
            await ctx.build_state_event(spec.job_id, "error", message="image expected"),
        )
        await update_job_meta(
            ctx,
            spec.job_id,
            {"status": "error", "error_reason": "not_image"},
        )
        _remove_if_exists(
            ctx,
            spec.upload_abs,
            "failed to cleanup invalid image job_id=%s",
            spec.job_id,
        )
        await _notify_upload_error(ctx, spec, "not_image")
        await ctx.job_error_cleanup(spec.job_id, "not_image")
        return _error_response(400, "Image expected")
    except Exception:
        ctx.logger.exception("transfer image preparation failed job_id=%s", spec.job_id)
        _remove_if_exists(
            ctx,
            spec.upload_abs,
            "failed to cleanup image prep files job_id=%s",
            spec.job_id,
        )
        _remove_if_exists(
            ctx,
            packed_abs,
            "failed to cleanup image prep files job_id=%s",
            spec.job_id,
        )
        await _notify_upload_error(ctx, spec, "image_prepare_failed")
        await ctx.job_error_cleanup(spec.job_id, "image_prepare_failed")
        return _error_response(500, "Image preparation failed")

    _remove_if_exists(
        ctx,
        spec.upload_abs,
        "failed to cleanup source upload file job_id=%s",
        spec.job_id,
    )

    packed_bytes = os.path.getsize(packed_abs)
    await update_job_meta(
        ctx,
        spec.job_id,
        {
            "packed_path": packed_rel,
            "packed_bytes": packed_bytes,
            "status": "packed",
            "packed_format": "webp",
        },
        warning_message="failed to update image meta for job_id=%s",
    )
    await ctx.set_stage(spec.job_id, "packed")
    return None


@router.api_route(
    "/transfer/start",
    methods=["GET", "POST"],
    tags=["Transfer"],
    summary="Start transfer from URL",
    description=TRANSFER_START_DESCRIPTION,
    responses=TRANSFER_START_RESPONSES,
    openapi_extra=TRANSFER_START_OPENAPI_EXTRA,
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
        reset_job_state(
            state,
            started=True,
            status="pending",
            stage="pending",
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
    asyncio.create_task(ctx.run_download_job(job_id, download_url, download_abs, max_bytes, callback_payload))

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
    client = _client_host(request)
    token = _extract_bearer_token(request)
    if not token:
        ctx.logger.warning("transfer upload denied (token missing) client=%s", client)
        return _error_response(401, "Token not found")

    payload = ctx.tools.decode_transfer_jwt(token, audience="storage")
    if not payload:
        ctx.logger.warning("transfer upload denied (token) client=%s", client)
        return _error_response(403, "Access denied")

    spec = _build_upload_spec(ctx, request, payload)
    if isinstance(spec, PlainTextResponse):
        return spec

    total = _read_upload_size_hint(request)
    ctx.logger.info(
        "transfer upload start job_id=%s kind=%s mod_id=%s storage_type=%s filename=%s size_hint=%s client=%s",
        spec.job_id,
        spec.transfer_kind,
        spec.mod_id,
        spec.storage_type,
        spec.safe_name,
        total,
        client,
    )

    await _initialize_upload_job(ctx, spec, total)
    start_ts = time.monotonic()

    try:
        stream_result = await _stream_upload_body(ctx, request, spec, total)
        if isinstance(stream_result, PlainTextResponse):
            return stream_result
        duration = time.monotonic() - start_ts
        ctx.logger.info(
            "transfer upload done job_id=%s bytes=%s duration=%.2fs",
            spec.job_id,
            stream_result.downloaded,
            duration,
        )

        unpacked_bytes = None
        if spec.transfer_kind == "archive":
            unpacked_bytes, archive_error = await _process_archive_upload(ctx, spec)
            if archive_error is not None:
                return archive_error
        else:
            image_error = await _process_image_upload(ctx, spec)
            if image_error is not None:
                return image_error

        await ctx.broadcast(spec.job_id, await ctx.build_state_event(spec.job_id, "complete"))
        callback_success_payload = {
            **spec.callback_payload,
            "job_id": spec.job_id,
            "status": "success",
            "bytes": stream_result.downloaded,
            "total": stream_result.final_total,
            "packed_format": "zip" if spec.transfer_kind == "archive" else "webp",
        }
        if spec.transfer_kind == "archive" and unpacked_bytes is not None:
            callback_success_payload["unpacked_bytes"] = unpacked_bytes
        await ctx.notify_manager(callback_success_payload)
        return {
            "job_id": spec.job_id,
            "bytes": stream_result.downloaded,
            "total": stream_result.final_total,
        }
    except Exception as exc:
        ctx.logger.exception("transfer upload failed job_id=%s", spec.job_id)
        _remove_if_exists(
            ctx,
            spec.upload_abs,
            "failed to cleanup partial file job_id=%s",
            spec.job_id,
        )
        await update_job_meta(
            ctx,
            spec.job_id,
            {
                "status": "error",
                "error": str(exc),
                "upload_completed_at": int(time.time()),
            },
        )
        await ctx.set_state(spec.job_id, status="error", error=str(exc))
        await ctx.broadcast(
            spec.job_id,
            await ctx.build_state_event(spec.job_id, "error", message="upload failed"),
        )
        await _notify_upload_error(ctx, spec, "exception")
        return _error_response(500, "Upload failed")
    finally:
        await ctx.close_clients(spec.job_id)


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
        snapshot = state_event_payload("progress", state)
    try:
        if snapshot is not None:
            await websocket.send_json(snapshot)
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                break
    except WebSocketDisconnect:
        pass
    except Exception:
        ctx.logger.exception("transfer ws failed job_id=%s", job_id)
        with suppress(Exception):
            await websocket.close()
    finally:
        async with ctx.job_lock:
            state = ctx.job_state.get(job_id)
            if state and websocket in state.get("clients", []):
                state["clients"].remove(websocket)
            if state and not state.get("clients") and not state.get("started"):
                ctx.job_state.pop(job_id, None)
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
    pack_format: str = Form("zip", alias="format"),
    compression_level: int = Form(3),
    token: str = Form(),
):
    ctx = _ctx()
    client = _client_host(request)
    auth_error = await _authorize_manage_request(
        ctx,
        token=token,
        client=client,
        action="repack",
        job_id=job_id,
    )
    if auth_error is not None:
        return auth_error
    if not ctx.tools.is_safe_job_id(job_id):
        return _error_response(400, "Invalid job id")

    meta = await _read_job_meta(ctx, job_id)
    if meta is None:
        return _error_response(404, "Job not found")

    if pack_format != "zip":
        return _error_response(400, "Unsupported format")

    download_rel = meta.get("download_path")
    if not download_rel:
        return _error_response(404, "Source file not found")

    download_abs = ctx.tools.safe_path(ctx.main_dir, download_rel)
    compression_level = _parse_pack_level(compression_level)

    ctx.logger.info(
        "transfer repack start job_id=%s format=%s level=%s client=%s",
        job_id,
        pack_format,
        compression_level,
        client,
    )
    repack_ok, packed_rel, packed_bytes, unpacked_bytes, repack_reason = await ctx.run_repack_job(
        job_id,
        download_abs,
        pack_format,
        compression_level,
    )
    if not repack_ok:
        if repack_reason == "encrypted_zip":
            return _error_response(400, "Encrypted zip not allowed")
        return _error_response(500, "Repack failed")

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
        "Moves repacked file to permanent storage path. " "Requires `storage_manage_token` and a valid job_id."
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
    storage_type: str = Form(alias="type"),
    target_path: str = Form(alias="path"),
    token: str = Form(),
):
    ctx = _ctx()
    client = _client_host(request)
    auth_error = await _authorize_manage_request(
        ctx,
        token=token,
        client=client,
        action="move",
        job_id=job_id,
    )
    if auth_error is not None:
        return auth_error
    if not ctx.tools.is_safe_job_id(job_id):
        return _error_response(400, "Invalid job id")
    if not ctx.tools.is_allowed_type(storage_type):
        return _error_response(400, "Invalid type")

    meta = await _read_job_meta(ctx, job_id)
    if meta is None:
        return _error_response(404, "Job not found")

    packed_rel = meta.get("packed_path")
    if not packed_rel:
        return _error_response(404, "Packed file not found")

    packed_abs = ctx.tools.safe_path(ctx.main_dir, packed_rel)
    base_dir = os.path.join(ctx.main_dir, storage_type)
    try:
        real_path = ctx.tools.safe_path(base_dir, target_path)
    except ValueError:
        return _error_response(423, "Access denied")

    ctx.logger.info(
        "transfer move start job_id=%s type=%s path=%s client=%s",
        job_id,
        storage_type,
        target_path,
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
    await ctx.delete_job_and_dir(job_id)

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
