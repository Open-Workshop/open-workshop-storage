"""OpenTelemetry setup for exporting traces to Uptrace."""
from __future__ import annotations

import atexit
import logging
import os
from urllib.parse import parse_qs, urlparse

from fastapi import FastAPI


_LOG = logging.getLogger(__name__)
_INSTRUMENTED = False
_DEFAULT_FASTAPI_EXCLUDED_URLS = (
    r"^.*/docs$,^.*/openapi\.json$,^/favicon\.ico$,^/robots\.txt$"
)


def _parse_dsn(dsn: str):
    parsed = urlparse(dsn)
    if not parsed.scheme or not parsed.hostname:
        raise ValueError("UPTRACE_DSN must be a valid URL.")
    return parsed


def _read_setting(key: str, default: str | None = None) -> str | None:
    value = os.getenv(key)
    if value is not None and str(value).strip():
        return str(value).strip()

    try:
        import ow_config

        config_value = getattr(ow_config, key, None)
        if config_value is not None and str(config_value).strip():
            return str(config_value).strip()
    except Exception:
        pass

    return default


def _dsn_to_otlp_trace_endpoint(dsn: str) -> str:
    parsed = _parse_dsn(dsn)

    host = parsed.hostname
    if parsed.port:
        host = f"{host}:{parsed.port}"

    return f"{parsed.scheme}://{host}/v1/traces"


def _dsn_to_otlp_grpc_endpoint(dsn: str) -> str:
    parsed = _parse_dsn(dsn)
    query = parse_qs(parsed.query)

    host = parsed.hostname
    grpc_port = query.get("grpc", [None])[0]
    if grpc_port:
        host = f"{host}:{grpc_port}"
    elif parsed.port:
        host = f"{host}:{parsed.port}"

    return f"{parsed.scheme}://{host}"


def _fastapi_server_request_hook(span: object, scope: dict) -> None:
    """Add useful attributes to FastAPI server spans."""
    try:
        if not span or not span.is_recording():
            return

        path = str(scope.get("path", "/"))
        query_bytes = scope.get("query_string", b"")
        target = path
        if isinstance(query_bytes, (bytes, bytearray)) and query_bytes:
            target = f"{path}?{query_bytes.decode('latin-1')}"

        endpoint = scope.get("endpoint")
        endpoint_name = getattr(endpoint, "__name__", None) if endpoint else None

        span.set_attribute("http.target", target)
        if endpoint_name:
            span.set_attribute("fastapi.endpoint", endpoint_name)
    except Exception:
        _LOG.exception("Failed to enrich FastAPI request span.")


def _parse_fastapi_exclude_spans(value: str | None) -> list[str] | None:
    if value is None:
        return ["receive", "send"]

    normalized = [item.strip().lower() for item in value.split(",") if item.strip()]
    allowed = []
    for item in normalized:
        if item in {"receive", "send"} and item not in allowed:
            allowed.append(item)

    return allowed or None


def _aiohttp_span_name(params: object) -> str:
    try:
        method = str(getattr(params, "method", "HTTP")).upper()
        url = getattr(params, "url", None)
        path = getattr(url, "path", "") or "/"
        return f"{method} {path}"
    except Exception:
        return "HTTP"


def _aiohttp_request_hook(span: object, params: object) -> None:
    """Add route-like attributes for outbound aiohttp spans."""
    try:
        if not span or not span.is_recording():
            return

        method = str(getattr(params, "method", "HTTP")).upper()
        url = getattr(params, "url", None)
        path = getattr(url, "path", "") or "/"
        query_string = getattr(url, "query_string", "")

        span.update_name(f"{method} {path}")
        span.set_attribute("http.route", path)
        span.set_attribute("http_route", path)
        span.set_attribute("http.target", f"{path}?{query_string}" if query_string else path)
    except Exception:
        _LOG.exception("Failed to enrich aiohttp client span.")


def setup_uptrace_telemetry(app: FastAPI) -> bool:
    """Initialize OpenTelemetry + Uptrace integration.

    Returns True when telemetry is configured, otherwise False.
    """
    global _INSTRUMENTED

    if _INSTRUMENTED or getattr(app, "_uptrace_telemetry_enabled", False):
        return True

    dsn = _read_setting("UPTRACE_DSN")
    if not dsn:
        _LOG.info("UPTRACE_DSN is not configured, telemetry is disabled.")
        return False

    service_name = _read_setting("OTEL_SERVICE_NAME", "open-workshop-storage")
    service_version = _read_setting("OTEL_SERVICE_VERSION", "dev")
    service_environment = _read_setting("OTEL_DEPLOYMENT_ENVIRONMENT", "production")
    traces_endpoint = _read_setting("UPTRACE_OTLP_TRACES_URL")
    grpc_endpoint = _read_setting("UPTRACE_OTLP_GRPC_URL")
    protocol = (_read_setting("UPTRACE_OTLP_PROTOCOL") or "").lower().strip()
    fastapi_excluded_urls = _read_setting(
        "UPTRACE_FASTAPI_EXCLUDED_URLS",
        _DEFAULT_FASTAPI_EXCLUDED_URLS,
    )
    fastapi_exclude_spans = _parse_fastapi_exclude_spans(
        _read_setting("UPTRACE_FASTAPI_EXCLUDE_SPANS", "receive,send")
    )

    try:
        from opentelemetry import trace
        from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        if not protocol:
            parsed = _parse_dsn(dsn)
            has_grpc_query = bool(parse_qs(parsed.query).get("grpc"))
            protocol = "grpc" if (grpc_endpoint or has_grpc_query) else "http"

        if protocol == "grpc":
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter as OTLPGrpcSpanExporter,
            )

            exporter = OTLPGrpcSpanExporter(
                endpoint=grpc_endpoint or _dsn_to_otlp_grpc_endpoint(dsn),
                headers=(("uptrace-dsn", dsn),),
            )
        elif protocol == "http":
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter as OTLPHttpSpanExporter,
            )

            exporter = OTLPHttpSpanExporter(
                endpoint=traces_endpoint or _dsn_to_otlp_trace_endpoint(dsn),
                headers={"uptrace-dsn": dsn},
            )
        else:
            raise ValueError("UPTRACE_OTLP_PROTOCOL must be 'http' or 'grpc'.")

        provider = TracerProvider(
            resource=Resource.create(
                {
                    "service.name": service_name,
                    "service.version": service_version,
                    "deployment.environment": service_environment,
                }
            )
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        fastapi_instrumentor = FastAPIInstrumentor()
        try:
            fastapi_instrumentor.instrument_app(
                app,
                excluded_urls=fastapi_excluded_urls,
                server_request_hook=_fastapi_server_request_hook,
                exclude_spans=fastapi_exclude_spans,
            )
        except TypeError:
            # Compatibility with versions without `exclude_spans`.
            fastapi_instrumentor.instrument_app(
                app,
                excluded_urls=fastapi_excluded_urls,
                server_request_hook=_fastapi_server_request_hook,
            )

        aiohttp_instrumentor = AioHttpClientInstrumentor()
        try:
            aiohttp_instrumentor.instrument(
                span_name=_aiohttp_span_name,
                request_hook=_aiohttp_request_hook,
            )
        except TypeError:
            # Compatibility with older instrumentation versions.
            aiohttp_instrumentor.instrument(span_name=_aiohttp_span_name)
        atexit.register(_shutdown_provider, provider)

        _INSTRUMENTED = True
        setattr(app, "_uptrace_telemetry_enabled", True)
        _LOG.info("Uptrace telemetry enabled for service %s via %s.", service_name, protocol)
        return True
    except ImportError:
        _LOG.exception(
            "OpenTelemetry packages are missing. Install dependencies from requirements.txt."
        )
        return False
    except Exception:
        _LOG.exception("Failed to initialize Uptrace telemetry.")
        return False


def _shutdown_provider(provider: object) -> None:
    try:
        provider.shutdown()  # type: ignore[attr-defined]
    except Exception:
        _LOG.exception("Failed to shutdown telemetry provider cleanly.")
