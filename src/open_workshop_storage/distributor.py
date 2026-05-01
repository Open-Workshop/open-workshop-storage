from __future__ import annotations

from contextlib import asynccontextmanager

from . import app as legacy
from .service_factory import build_service_app, clone_router_subset

DISTRIBUTOR_FILE_PATHS = {"/blurhashes", "/download/{type}/{path:path}"}


def _include_distributor_route(path: str) -> bool:
    return path in DISTRIBUTOR_FILE_PATHS


@asynccontextmanager
async def lifespan(_app):
    try:
        legacy.tools.ensure_7z_available()
    except Exception as exc:
        legacy.logger.error("7z dependency missing: %s", exc)
        raise
    yield


app = build_service_app(
    title="Open Workshop Distributor",
    context_provider=legacy._build_service_context,
    lifespan=lifespan,
    healthz_url="/distributor/healthz",
    docs_url="/distributor/",
    openapi_url="/distributor/openapi.json",
    routers=[clone_router_subset(legacy.file_router, _include_distributor_route)],
)
