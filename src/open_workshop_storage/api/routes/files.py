from __future__ import annotations

import asyncio
import os
from functools import lru_cache
from typing import Callable, Optional
from urllib.parse import urlparse

import anyio
import ow_config as config
from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel, Field

from ...core.blurhash_service import get_or_compute_blurhash_for_key
from ...core.context import ServiceContext
from ...services import access_client
from ...service_factory import get_current_service_context
from ...utils import image_file_to_blurhash

router = APIRouter()
_context_provider: Optional[Callable[[], ServiceContext]] = None


def configure_context_provider(provider: Callable[[], ServiceContext]) -> None:
    global _context_provider
    _context_provider = provider


def _ctx() -> ServiceContext:
    try:
        return get_current_service_context()
    except RuntimeError:
        if _context_provider is None:
            raise RuntimeError("file context provider is not configured")
        return _context_provider()


def _client_host(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _error_response(status_code: int, content: str) -> PlainTextResponse:
    return PlainTextResponse(status_code=status_code, content=content)


def _read_int_setting(name: str, default: int) -> int:
    raw_value = getattr(config, name, default)
    try:
        return max(0, int(raw_value))
    except (TypeError, ValueError):
        return max(0, int(default))


# Keep the in-process cache modest: it is replicated in every worker process,
# so a very large default can inflate RSS quickly on busy distributors.
BLURHASH_CACHE_SIZE = _read_int_setting("BLURHASH_CACHE_SIZE", 4096)


class BlurhashBatchRequest(BaseModel):
    paths: list[str] = Field(default_factory=list, description="Storage download URLs or storage-relative paths.")


class BlurhashItemRead(BaseModel):
    path: str
    blurhash: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None


class BlurhashBatchResponse(BaseModel):
    items: list[BlurhashItemRead] = Field(default_factory=list)


def _normalize_blurhash_target(raw_path: str) -> Optional[tuple[str, str]]:
    value = str(raw_path or "").strip()
    if not value:
        return None

    parsed = urlparse(value)
    if parsed.scheme and parsed.netloc:
        value = parsed.path

    value = value.lstrip("/")
    if not value.startswith("download/"):
        return None

    parts = value.split("/", 2)
    if len(parts) < 3:
        return None

    storage_type = parts[1]
    storage_path = parts[2]
    if not storage_type or not storage_path:
        return None
    return storage_type, storage_path


@lru_cache(maxsize=BLURHASH_CACHE_SIZE)
def _blurhash_for_file(
    real_path: str,
    mtime_ns: int,
    size: int,
    components_x: int = 4,
    components_y: int = 3,
    max_dimension: int = 64,
) -> tuple[str, int, int]:
    del mtime_ns, size
    return image_file_to_blurhash(
        real_path,
        components_x=components_x,
        components_y=components_y,
        max_dimension=max_dimension,
    )


def _prepare_blurhash_item(
    ctx: ServiceContext,
    raw_path: str,
) -> tuple[BlurhashItemRead, Optional[tuple[str, int, int]]]:
    item = BlurhashItemRead(path=raw_path)
    normalized = _normalize_blurhash_target(raw_path)
    if normalized is None:
        return item, None

    storage_type, storage_path = normalized
    if not ctx.tools.is_allowed_type(storage_type):
        return item, None

    base_dir = os.path.join(ctx.main_dir, storage_type)
    try:
        real_path = ctx.tools.safe_path(base_dir, storage_path)
    except ValueError:
        return item, None

    if not os.path.isfile(real_path):
        return item, None

    try:
        stat_result = os.stat(real_path)
    except (OSError, ValueError):
        return item, None

    return item, (real_path, stat_result.st_mtime_ns, stat_result.st_size)


async def _compute_blurhash_for_key(
    ctx: ServiceContext,
    key: tuple[str, int, int],
) -> Optional[tuple[str, int, int]]:
    return await get_or_compute_blurhash_for_key(ctx, key, _blurhash_for_file)


@router.post(
    "/blurhashes",
    tags=["Files"],
    summary="Generate blurhashes for stored images",
    description=(
        "Generates BlurHash strings for a batch of stored image URLs or storage-relative download paths.\n\n"
        "Use full storage download URLs from the website or relative paths like `download/resource/...`."
    ),
    status_code=200,
    response_model=BlurhashBatchResponse,
    response_description="Batch of BlurHash results.",
    responses={
        200: {
            "description": "BlurHash batch generated successfully",
        },
    },
)
async def blurhashes(request: Request, payload: BlurhashBatchRequest) -> BlurhashBatchResponse:
    ctx = _ctx()
    client = _client_host(request)
    ctx.logger.info("blurhash batch request items=%s client=%s", len(payload.paths), client)
    prepared_items = [_prepare_blurhash_item(ctx, raw_path) for raw_path in payload.paths]
    unique_keys = list(dict.fromkeys(key for _, key in prepared_items if key is not None))
    blurhash_results: dict[tuple[str, int, int], Optional[tuple[str, int, int]]] = {}

    if unique_keys:
        computed = await asyncio.gather(*(_compute_blurhash_for_key(ctx, key) for key in unique_keys))
        blurhash_results = {key: result for key, result in zip(unique_keys, computed)}

    items: list[BlurhashItemRead] = []
    for item, key in prepared_items:
        if key is not None:
            blurhash_data = blurhash_results.get(key)
            if blurhash_data is not None:
                blurhash, width, height = blurhash_data
                item.blurhash = blurhash
                item.width = width
                item.height = height
        items.append(item)

    return BlurhashBatchResponse(items=items)


@router.api_route(
    "/download/{type}/{path:path}",
    methods=["GET", "HEAD"],
    tags=["Files"],
    summary="Download stored file",
    description=(
        "Downloads a stored file. For archive/mod downloads access is validated via the access service. "
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
            "content": {"text/plain": {"example": "Invalid type"}},
        },
        403: {
            "description": "Access denied",
            "content": {"text/plain": {"example": "Access denied"}},
        },
        423: {
            "description": "Access denied",
            "content": {"text/plain": {"example": "Access denied"}},
        },
        404: {
            "description": "File not found on server",
            "content": {"text/plain": {"example": "File not found"}},
        },
        503: {
            "description": "Access service unavailable",
            "content": {"text/plain": {"example": "Access service unavailable"}},
        },
    },
)
async def download(request: Request, type: str, path: str, filename: Optional[str] = None):
    ctx = _ctx()
    client = request.client.host if request.client else "unknown"
    ctx.logger.info("download request type=%s path=%s client=%s", type, path, client)
    if not ctx.tools.is_allowed_type(type):
        ctx.logger.warning("download invalid type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=400, content="Invalid type")
    base_dir = os.path.join(ctx.main_dir, type)
    try:
        real_path = ctx.tools.safe_path(base_dir, path)
    except ValueError:
        ctx.logger.warning("download path traversal type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=423, content="Access denied")

    if not os.path.isfile(real_path):
        ctx.logger.info("download not found type=%s path=%s client=%s", type, path, client)
        return PlainTextResponse(status_code=404, content="File not found")

    download_name = ctx.tools.build_download_filename(filename, real_path)

    async def file_response_with_meta() -> FileResponse:
        response = FileResponse(real_path, filename=download_name)
        if request.method == "HEAD" and type == "archive" and path.startswith("mods/"):
            try:
                _, is_encrypted, archive_entries = await anyio.to_thread.run_sync(
                    ctx.tools.probe_archive,
                    real_path,
                )
                if not is_encrypted:
                    unpacked_bytes = await anyio.to_thread.run_sync(
                        ctx.tools.archive_entries_unpacked_bytes,
                        archive_entries,
                    )
                    if unpacked_bytes is not None:
                        response.headers["X-Unpacked-Bytes"] = str(unpacked_bytes)
            except Exception:
                ctx.logger.warning(
                    "download unpacked header failed type=%s path=%s client=%s",
                    type,
                    path,
                    client,
                )
        return response

    if type == "archive" and path.startswith("mods/"):
        parts = path.split("/", 2)
        if len(parts) < 2:
            ctx.logger.info("download not found (bad mod id) type=%s path=%s client=%s", type, path, client)
            return _error_response(404, "File not found")
        try:
            mod_id = int(parts[1])
        except ValueError:
            ctx.logger.info("download not found (bad mod id) type=%s path=%s client=%s", type, path, client)
            return _error_response(404, "File not found")
        try:
            access_result = await access_client.resolve_mod_download_access(
                request=request,
                mod_id=mod_id,
                access_service_url=ctx.config.ACCESS_SERVICE_URL,
                timeout_seconds=int(ctx.config.ACCESS_SERVICE_TIMEOUT_SECONDS),
            )
            if access_result.allowed:
                ctx.logger.info(
                    "download allowed mod_id=%s type=%s path=%s client=%s",
                    mod_id,
                    type,
                    path,
                    client,
                )
                return await file_response_with_meta()
            ctx.logger.warning(
                "download denied mod_id=%s type=%s path=%s client=%s reason_code=%s",
                mod_id,
                type,
                path,
                client,
                access_result.reason_code,
            )
            return _error_response(403, access_result.reason or "Access denied")
        except access_client.AccessServiceError as exc:
            status_code = exc.status_code if exc.status_code is not None else 503
            ctx.logger.warning(
                "access service error status=%s mod_id=%s type=%s path=%s client=%s message=%s",
                status_code,
                mod_id,
                type,
                path,
                client,
                str(exc),
            )
            return _error_response(status_code, str(exc))
        except (asyncio.TimeoutError, TypeError, ValueError):
            ctx.logger.warning(
                "access service unavailable network error mod_id=%s type=%s path=%s client=%s",
                mod_id,
                type,
                path,
                client,
            )
            return _error_response(503, "Access service unavailable")

    ctx.logger.info("download ok type=%s path=%s client=%s", type, path, client)
    return await file_response_with_meta()


@router.post(
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
            "content": {"text/plain": {"example": "file/is/saved/as.tmp"}},
            "model": str,
        },
        401: {
            "description": "Token not found",
            "content": {"text/plain": {"example": "Token not found"}},
            "model": str,
        },
        400: {
            "description": "Invalid type",
            "content": {"text/plain": {"example": "Invalid type"}},
            "model": str,
        },
    },
)
async def upload(
    request: Request,
    file: UploadFile = File(),
    storage_type: str = Form(alias="type"),
    storage_path: str = Form(alias="path"),
    file_kind: str = Form("bin"),
    token: str = Form(),
):
    ctx = _ctx()
    client = _client_host(request)
    ctx.logger.info(
        "upload request type=%s path=%s file_kind=%s filename=%s client=%s",
        storage_type,
        storage_path,
        file_kind,
        file.filename,
        client,
    )
    if not token:
        ctx.logger.warning(
            "upload denied (token missing) type=%s path=%s client=%s",
            storage_type,
            storage_path,
            client,
        )
        return _error_response(401, "Token not found")
    if not await anyio.to_thread.run_sync(ctx.tools.check_token, "upload_file", token):
        ctx.logger.warning("upload denied (token) type=%s path=%s client=%s", storage_type, storage_path, client)
        return _error_response(403, "Access denied")
    if not ctx.tools.is_allowed_upload_type(storage_type):
        ctx.logger.warning("upload invalid type=%s path=%s client=%s", storage_type, storage_path, client)
        return _error_response(400, "Invalid type")
    normalized_file_kind = ctx.tools.normalize_file_kind(file_kind, default="")
    if not normalized_file_kind:
        ctx.logger.warning(
            "upload invalid file_kind=%s type=%s path=%s client=%s",
            file_kind,
            storage_type,
            storage_path,
            client,
        )
        return _error_response(400, "Invalid file kind")
    if storage_type == "avatar" and normalized_file_kind != "img":
        return _error_response(400, "Avatar requires image file kind")

    base_dir = os.path.join(ctx.main_dir, storage_type)
    try:
        real_path = ctx.tools.safe_path(base_dir, storage_path)
    except ValueError:
        ctx.logger.warning("upload path traversal type=%s path=%s client=%s", storage_type, storage_path, client)
        return _error_response(423, "Access denied")
    os.makedirs(os.path.dirname(real_path), exist_ok=True)

    if normalized_file_kind == "img":
        if not storage_path.lower().endswith(".webp"):
            return _error_response(400, "Image storage path must end with .webp")
        raw_bytes = await file.read()
        try:
            webp_bytes = await anyio.to_thread.run_sync(ctx.tools.image_bytes_to_webp, raw_bytes)
        except ValueError:
            return _error_response(400, "Image expected")

        def _write_bytes_sync() -> None:
            with open(real_path, "wb") as out_file:
                out_file.write(webp_bytes)

        await anyio.to_thread.run_sync(_write_bytes_sync)
    else:
        await anyio.to_thread.run_sync(ctx.tools.copy_fileobj_to_path, file.file, real_path)
    ctx.logger.info(
        "upload saved type=%s path=%s file_kind=%s client=%s",
        storage_type,
        storage_path,
        normalized_file_kind,
        client,
    )
    return storage_path


@router.delete(
    "/delete",
    tags=["Files"],
    summary="Delete file from Storage (internal)",
    description=(
        "Internal delete endpoint for Manager. " "Deletes file and empty parent folders. Requires delete token."
    ),
    status_code=200,
    response_class=PlainTextResponse,
    response_model=str,
    responses={
        200: {
            "description": "File deleted successfully",
            "content": {"text/plain": {"example": "File deleted"}},
            "model": str,
        },
        401: {
            "description": "Token not found",
            "content": {"text/plain": {"example": "Token not found"}},
            "model": str,
        },
        400: {
            "description": "Invalid type",
            "content": {"text/plain": {"example": "Invalid type"}},
            "model": str,
        },
        404: {
            "description": "File not found on server",
            "content": {"text/plain": {"example": "File not found"}},
            "model": str,
        },
    },
)
async def delete(
    request: Request,
    storage_type: str = Form(alias="type"),
    storage_path: str = Form(alias="path"),
    token: str = Form(),
):
    ctx = _ctx()
    client = _client_host(request)
    ctx.logger.info("delete request type=%s path=%s client=%s", storage_type, storage_path, client)
    if not token:
        ctx.logger.warning(
            "delete denied (token missing) type=%s path=%s client=%s",
            storage_type,
            storage_path,
            client,
        )
        return _error_response(401, "Token not found")
    if not await anyio.to_thread.run_sync(ctx.tools.check_token, "delete_file", token):
        ctx.logger.warning("delete denied (token) type=%s path=%s client=%s", storage_type, storage_path, client)
        return _error_response(403, "Access denied")
    if not ctx.tools.is_allowed_type(storage_type):
        ctx.logger.warning("delete invalid type=%s path=%s client=%s", storage_type, storage_path, client)
        return _error_response(400, "Invalid type")

    base_dir = os.path.join(ctx.main_dir, storage_type)
    try:
        real_path = ctx.tools.safe_path(base_dir, storage_path)
    except ValueError:
        ctx.logger.warning("delete path traversal type=%s path=%s client=%s", storage_type, storage_path, client)
        return _error_response(403, "Access denied")

    def delete_file_and_parent_folders(file_path: str, root_dir: str):
        if not os.path.isfile(file_path):
            ctx.logger.info("delete not found type=%s path=%s client=%s", storage_type, storage_path, client)
            return _error_response(404, "File not found")
        os.remove(file_path)
        folder_path = os.path.dirname(file_path)
        root_dir = os.path.abspath(root_dir)
        while folder_path and os.path.commonpath([folder_path, root_dir]) == root_dir and folder_path != root_dir:
            if not os.listdir(folder_path):
                os.rmdir(folder_path)
                folder_path = os.path.dirname(folder_path)
            else:
                break

        ctx.logger.info("delete ok type=%s path=%s client=%s", storage_type, storage_path, client)
        return _error_response(200, "File deleted")

    return await anyio.to_thread.run_sync(delete_file_and_parent_folders, real_path, base_dir)
