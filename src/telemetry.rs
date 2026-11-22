//! Optional request traces. Paths use route templates and omit query strings.
use axum::body::Body;
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

pub async fn trace_request(req: Request<Body>, next: Next) -> Response {
    if req.uri().path() == "/healthz" {
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
        KeyValue::new("url.path", path),
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
        tracing::info!(status = response.status().as_u16(), "HTTP request completed");
        response
    }.with_context(context.clone()).instrument(log_scope).await;
    let span = context.span();
    span.set_attribute(KeyValue::new("http.response.status_code", i64::from(response.status().as_u16())));
    if response.status().is_server_error() {
        span.set_status(Status::error("HTTP server error"));
    }
    span.end();
    response
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
