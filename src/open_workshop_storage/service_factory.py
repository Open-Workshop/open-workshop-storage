from __future__ import annotations

from contextvars import ContextVar
from typing import Callable, Sequence

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute, APIWebSocketRoute
from fastapi.responses import PlainTextResponse

from .core.context import ServiceContext
from .observability.uptrace import setup_uptrace_telemetry

SERVICE_CONTEXT: ContextVar[ServiceContext | None] = ContextVar("open_workshop_storage_service_context", default=None)


def get_current_service_context() -> ServiceContext:
    ctx = SERVICE_CONTEXT.get()
    if ctx is None:
        raise RuntimeError("service context is not configured")
    return ctx


class ServiceContextMiddleware:
    def __init__(self, app, context_provider: Callable[[], ServiceContext]):
        self.app = app
        self.context_provider = context_provider

    async def __call__(self, scope, receive, send):
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return

        token = SERVICE_CONTEXT.set(self.context_provider())
        try:
            await self.app(scope, receive, send)
        finally:
            SERVICE_CONTEXT.reset(token)


def clone_router_subset(source: APIRouter, include_path: Callable[[str], bool]) -> APIRouter:
    target = APIRouter()
    for route in source.routes:
        if isinstance(route, APIRoute):
            if not include_path(route.path):
                continue
            for method in sorted(route.methods or []):
                target.add_api_route(
                    route.path,
                    route.endpoint,
                    response_model=route.response_model,
                    status_code=route.status_code,
                    tags=route.tags,
                    dependencies=route.dependencies,
                    summary=route.summary,
                    description=route.description,
                    response_description=route.response_description,
                    responses=route.responses,
                    deprecated=route.deprecated,
                    methods=[method],
                    operation_id=route.operation_id,
                    response_model_include=route.response_model_include,
                    response_model_exclude=route.response_model_exclude,
                    response_model_by_alias=route.response_model_by_alias,
                    response_model_exclude_unset=route.response_model_exclude_unset,
                    response_model_exclude_defaults=route.response_model_exclude_defaults,
                    response_model_exclude_none=route.response_model_exclude_none,
                    include_in_schema=route.include_in_schema,
                    response_class=route.response_class,
                    name=route.name,
                    callbacks=route.callbacks,
                    openapi_extra=route.openapi_extra,
                    generate_unique_id_function=route.generate_unique_id_function,
                    strict_content_type=route.strict_content_type,
                )
            continue

        if isinstance(route, APIWebSocketRoute) and include_path(route.path):
            target.add_api_websocket_route(
                route.path,
                route.endpoint,
                name=route.name,
                dependencies=route.dependencies,
            )
    return target


def build_service_app(
    *,
    title: str,
    context_provider: Callable[[], ServiceContext],
    lifespan,
    routers: Sequence[APIRouter],
    healthz_url: str = "/healthz",
    docs_url: str | None = "/",
    openapi_url: str | None = "/openapi.json",
) -> FastAPI:
    app = FastAPI(
        title=title,
        contact={
            "name": "GitHub",
            "url": "https://github.com/Open-Workshop",
        },
        license_info={
            "name": "MPL-2.0 license",
            "identifier": "MPL-2.0",
        },
        docs_url=docs_url,
        openapi_url=openapi_url,
        lifespan=lifespan,
    )
    setup_uptrace_telemetry(app)
    app.add_middleware(ServiceContextMiddleware, context_provider=context_provider)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=False,
    )

    @app.get(healthz_url, include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.middleware("http")
    async def modify_header(request, call_next):
        if request.method == "OPTIONS":
            response = PlainTextResponse(status_code=200, content="OK")
        else:
            response = await call_next(request)
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization,X-File-Name"
        response.headers["Access-Control-Expose-Headers"] = "Content-Type,Content-Disposition"
        return response

    for router in routers:
        app.include_router(router)

    return app
