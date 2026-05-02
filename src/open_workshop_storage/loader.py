from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress

from . import app as legacy
from .service_factory import build_service_app, clone_router_subset

LOADER_FILE_PATHS = {"/upload", "/delete"}


def _include_loader_file_route(path: str) -> bool:
    return path in LOADER_FILE_PATHS


def _include_transfer_route(path: str) -> bool:
    return path.startswith("/transfer/")


@asynccontextmanager
async def lifespan(_app):
    try:
        legacy.tools.ensure_7z_available()
    except Exception as exc:
        legacy.logger.error("7z dependency missing: %s", exc)
        raise

    await legacy._start_job_event_listener()
    cleanup_task = asyncio.create_task(legacy._cleanup_loop())
    try:
        yield
    finally:
        cleanup_task.cancel()
        with suppress(asyncio.CancelledError):
            await cleanup_task
        await legacy._stop_job_event_listener()


app = build_service_app(
    title="Open Workshop Loader",
    context_provider=legacy._build_service_context,
    lifespan=lifespan,
    healthz_url="/loader/healthz",
    docs_url="/loader/",
    openapi_url="/loader/openapi.json",
    routers=[
        clone_router_subset(legacy.file_router, _include_loader_file_route),
        clone_router_subset(legacy.transfer_router, _include_transfer_route),
    ],
)
