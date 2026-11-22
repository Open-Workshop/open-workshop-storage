//! Optional request traces. Paths use route templates and omit query strings.
use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::{HeaderMap, Request};
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
    let resource = Resource::builder_empty().with_attributes([
        KeyValue::new("service.name", service),
        KeyValue::new("host.name", "storage"),
        KeyValue::new("host.hostname", "storage"),
        KeyValue::new("deployment.environment", "production"),
    ]).build();
    let provider = SdkTracerProvider::builder().with_resource(resource).with_batch_exporter(exporter).build();
    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(provider.clone());
    Some(provider)
}

pub async fn trace_request(req: Request<Body>, next: Next) -> Response {
    if req.uri().path() == "/healthz" {
        return next.run(req).await;
    }
    let method = req.method().as_str().to_owned();
    let route = req.extensions().get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let parent = global::get_text_map_propagator(|propagator| propagator.extract(&Headers(req.headers())));
    let tracer = global::tracer("open-workshop-storage");
    let span = tracer.span_builder(format!("{method} {route}"))
        .with_kind(SpanKind::Server)
        .with_attributes(vec![
            KeyValue::new("http.request.method", method),
            KeyValue::new("http.route", route),
        ])
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
