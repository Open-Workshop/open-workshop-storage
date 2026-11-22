//! Optional request traces. Paths use route templates and omit query strings.
use axum::body::{Body, HttpBody};
use axum::extract::MatchedPath;
use axum::http::{HeaderMap, Request, Uri};
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::{FutureExt, SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing::Instrument;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

struct Headers<'a>(&'a HeaderMap);
impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}

pub fn init(service: &'static str) -> Option<SdkTracerProvider> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").ok()?;
    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .with_timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(exporter) => exporter,
        Err(error) => {
            tracing::warn!(%error, "failed to initialize OpenTelemetry exporter");
            return None;
        }
    };
    let resource = Resource::builder().with_attributes([
        KeyValue::new("service.name", service),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("host.name", "storage"),
        KeyValue::new("host.hostname", "storage"),
        KeyValue::new("deployment.environment", "production"),
        KeyValue::new("process.pid", i64::from(std::process::id())),
    ]).build();
    let provider = SdkTracerProvider::builder().with_resource(resource).with_batch_exporter(exporter).build();
    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(provider.clone());
    Some(provider)
}

/// Standard client attributes, excluding URL credentials and query parameters.
pub fn client_http_attributes(uri: &Uri) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    if let (Some(scheme), Some(host)) = (uri.scheme_str(), uri.host()) {
        let port = uri.port_u16().or(match scheme { "http" => Some(80), "https" => Some(443), _ => None });
        attributes.push(KeyValue::new("server.address", host.to_owned()));
        if let Some(port) = port { attributes.push(KeyValue::new("server.port", i64::from(port))); }
        let display_host = if host.contains(':') && !host.starts_with('[') { format!("[{host}]") } else { host.to_owned() };
        let explicit_port = uri.port_u16().map(|p| format!(":{p}")).unwrap_or_default();
        attributes.push(KeyValue::new("url.full", format!("{scheme}://{display_host}{explicit_port}{}", uri.path())));
    }
    attributes
}

/// Keep the server span open until Hyper has consumed or dropped the body.
struct TracedBody {
    inner: Body,
    context: Option<Context>,
    transfer_context: Option<Context>,
    log_scope: tracing::Span,
    sent_bytes: u64,
    expected_bytes: Option<u64>,
    completed: bool,
}

impl TracedBody {
    fn finish(&mut self) {
        if let Some(context) = self.transfer_context.take() {
            let span = context.span();
            span.set_attribute(KeyValue::new("ow.transfer.bytes_sent", self.sent_bytes as i64));
            if let Some(expected) = self.expected_bytes {
                if expected > 0 {
                    let percent = (self.sent_bytes as f64 / expected as f64 * 100.0).min(100.0);
                    span.set_attribute(KeyValue::new("ow.transfer.percent_sent", percent));
                }
            }
            let complete = self.completed || self.expected_bytes == Some(self.sent_bytes);
            span.set_attribute(KeyValue::new("ow.transfer.result", if complete { "complete" } else { "interrupted" }));
            span.end();
        }
        if let Some(context) = self.context.take() {
            let _guard = context.attach();
            let _scope = self.log_scope.enter();
            Context::current().span().set_attribute(KeyValue::new("http.response.body.size", self.sent_bytes as i64));
            tracing::info!(bytes = self.sent_bytes, "HTTP response body finished");
            Context::current().span().end();
        }
    }
}

impl HttpBody for TracedBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() { self.sent_bytes = self.sent_bytes.saturating_add(data.len() as u64); }
                if self.inner.is_end_stream() { self.completed = true; self.finish(); }
            }
            Poll::Ready(Some(Err(_))) => {
                if let Some(context) = &self.context { context.span().set_status(Status::error("HTTP response body failed")); }
                self.finish();
            }
            Poll::Ready(None) => { self.completed = true; self.finish(); },
            Poll::Pending => {}
        }
        result
    }

    fn is_end_stream(&self) -> bool { self.inner.is_end_stream() }
    fn size_hint(&self) -> http_body::SizeHint { self.inner.size_hint() }
}

impl Drop for TracedBody {
    fn drop(&mut self) { self.finish(); }
}

/// Extract IDs only from documented storage paths, never from query parameters.
fn file_attributes(path: &str, expected: Option<u64>, content_type: Option<&str>) -> Vec<KeyValue> {
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    let mut result = Vec::new();
    if parts.len() < 3 || parts[0] != "download" { return result; }
    result.push(KeyValue::new("ow.storage.type", parts[1].to_owned()));
    if let Some(size) = expected.and_then(|n| i64::try_from(n).ok()) { result.push(KeyValue::new("file.size", size)); }
    if let Some(mime) = content_type { result.push(KeyValue::new("http.response.mime_type", mime.to_owned())); }
    let filename = parts.last().copied().unwrap_or("");
    if let Some((stem, ext)) = filename.rsplit_once('.') {
        if !ext.is_empty() && ext.len() <= 12 && ext.bytes().all(|b| b.is_ascii_alphanumeric()) {
            result.push(KeyValue::new("file.extension", ext.to_ascii_lowercase()));
        }
        if parts.len() == 5 && matches!(parts[2], "mods" | "modpacks" | "games" | "profiles")
            && !parts[3].is_empty() && parts[3].bytes().all(|b| b.is_ascii_digit()) {
            let kind = match parts[2] { "mods" => "mod", "modpacks" => "modpack", "games" => "game", _ => "user" };
            result.push(KeyValue::new("ow.owner.type", kind));
            result.push(KeyValue::new("ow.owner.id", parts[3].to_owned()));
            if !stem.is_empty() && stem.len() <= 64 && stem.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
                result.push(KeyValue::new("ow.resource.id", stem.to_owned()));
            }
        } else if parts.len() == 3 && parts[1] == "avatar" && !stem.is_empty() && stem.bytes().all(|b| b.is_ascii_digit()) {
            result.push(KeyValue::new("ow.owner.type", "user"));
            result.push(KeyValue::new("ow.owner.id", stem.to_owned()));
        }
    }
    result
}

pub async fn trace_request(req: Request<Body>, next: Next) -> Response {
    // Keep proxy-originated health checks connected on the service map, while
    // avoiding standalone traces for local probes without a trace context.
    if req.uri().path() == "/healthz" && !req.headers().contains_key("traceparent") {
        return next.run(req).await;
    }
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let scheme = req.headers().get("x-forwarded-proto").and_then(|v| v.to_str().ok())
        .filter(|v| *v == "http" || *v == "https").unwrap_or("http").to_owned();
    let authority = req.headers().get("x-forwarded-host").or_else(|| req.headers().get("host"))
        .and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<axum::http::uri::Authority>().ok());
    let route = req.extensions().get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let parent = global::get_text_map_propagator(|propagator| propagator.extract(&Headers(req.headers())));
    let tracer = global::tracer("open-workshop-storage");
    let mut attributes = vec![
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("url.path", path.clone()),
        KeyValue::new("url.scheme", scheme.clone()),
    ];
    if let Some(authority) = authority {
        attributes.push(KeyValue::new("server.address", authority.host().to_owned()));
        let port = authority.port_u16().unwrap_or(if scheme == "https" { 443 } else { 80 });
        attributes.push(KeyValue::new("server.port", i64::from(port)));
    }
    let span = tracer.span_builder(format!("{method} {route}"))
        .with_kind(SpanKind::Server)
        .with_attributes(attributes)
        .start_with_context(&tracer, &parent);
    let context = Context::current_with_span(span);
    let span_context = context.span().span_context().clone();
    let log_scope = tracing::info_span!(
        "request",
        trace_id = %span_context.trace_id(),
        span_id = %span_context.span_id(),
    );
    let response = async move {
        let response = next.run(req).await;
        tracing::info!(status = response.status().as_u16(), "HTTP response headers ready");
        response
    }.with_context(context.clone()).instrument(log_scope.clone()).await;
    let span = context.span();
    span.set_attribute(KeyValue::new("http.response.status_code", i64::from(response.status().as_u16())));
    if response.status().is_server_error() {
        span.set_status(Status::error("HTTP server error"));
    }
    let expected_bytes = response.headers().get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()).and_then(|value| value.parse::<u64>().ok());
    let content_type = response.headers().get(axum::http::header::CONTENT_TYPE).and_then(|value| value.to_str().ok());
    let transfer_context = if method == "GET" && route == "/download/:storage_type/*path" && response.status().is_success() {
        let span = tracer.span_builder("Send file to Nginx")
            .with_kind(SpanKind::Internal)
            .with_attributes(file_attributes(&path, expected_bytes, content_type))
            .start_with_context(&tracer, &context);
        Some(context.with_span(span))
    } else { None };
    let (parts, body) = response.into_parts();
    let traced_body = TracedBody { inner: body, context: Some(context.clone()), transfer_context, log_scope, sent_bytes: 0, expected_bytes, completed: false };
    Response::from_parts(parts, Body::new(traced_body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outgoing_http_attributes_include_destination_and_remove_query() {
        let uri: Uri = "https://access.openworkshop.miskler.ru/mod/42?token=secret".parse().unwrap();
        let attrs = client_http_attributes(&uri);
        let get = |key: &str| attrs.iter().find(|v| v.key.as_str() == key).unwrap().value.to_string();
        assert_eq!(get("server.address"), "access.openworkshop.miskler.ru");
        assert_eq!(get("server.port"), "443");
        assert_eq!(get("url.full"), "https://access.openworkshop.miskler.ru/mod/42");

        let uri: Uri = "https://user:password@example.org/path?token=secret".parse().unwrap();
        let attrs = client_http_attributes(&uri);
        assert!(attrs.iter().any(|v| v.key.as_str() == "url.full" && v.value.to_string() == "https://example.org/path"));

        let uri: Uri = "http://127.0.0.1:7776/internal/storage/transfer-completions".parse().unwrap();
        let attrs = client_http_attributes(&uri);
        assert!(attrs.iter().any(|v| v.key.as_str() == "server.port" && v.value.to_string() == "7776"));
    }
}
