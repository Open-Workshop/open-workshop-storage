use std::sync::{Arc, Mutex};
use axum::{body::Body, middleware, routing::get, Router};
use bytes::Bytes;
use http_body_util::BodyExt;
use opentelemetry::global;
use opentelemetry_sdk::{error::OTelSdkResult, propagation::TraceContextPropagator, trace::{SdkTracerProvider, SpanData, SpanExporter}};
use tokio::sync::mpsc;
use tower::ServiceExt;

#[derive(Debug, Clone)]
struct Capture(Arc<Mutex<Vec<SpanData>>>);
impl SpanExporter for Capture {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}

#[tokio::test]
async fn server_span_stays_open_until_streamed_body_finishes() {
    let spans = Arc::new(Mutex::new(Vec::<SpanData>::new()));
    let provider = SdkTracerProvider::builder().with_simple_exporter(Capture(spans.clone())).build();
    global::set_tracer_provider(provider.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());
    let (sender, receiver) = mpsc::channel::<Bytes>(4);
    let receiver = Arc::new(tokio::sync::Mutex::new(Some(receiver)));
    let app = Router::new().route("/download/:storage_type/*path", get(move || {
        let receiver = receiver.clone();
        async move {
            let receiver = receiver.lock().await.take().unwrap();
            let stream = futures_util::stream::unfold(receiver, |mut rx| async move {
                rx.recv().await.map(|bytes| (Ok::<_, std::io::Error>(bytes), rx))
            });
            axum::http::Response::builder().header("content-length", "5")
                .header("content-type", "application/octet-stream")
                .body(Body::from_stream(stream)).unwrap()
        }
    })).layer(middleware::from_fn(open_workshop_storage::telemetry::trace_request));
    let response = app.oneshot(axum::http::Request::builder().uri("/download/archive/mods/7515/main.zip").body(Body::empty()).unwrap()).await.unwrap();
    assert!(spans.lock().unwrap().is_empty(), "span ended before response body was read");
    let mut body = response.into_body();
    sender.send(Bytes::from_static(b"hello")).await.unwrap();
    assert_eq!(body.frame().await.unwrap().unwrap().into_data().unwrap(), "hello");
    assert!(spans.lock().unwrap().is_empty(), "span ended before streaming completed");
    drop(sender);
    assert!(body.frame().await.is_none());
    let shared_spans = spans.clone();
    let spans = spans.lock().unwrap();
    assert_eq!(spans.len(), 2);
    let transfer = spans.iter().find(|s| s.name == "Send file to Nginx").unwrap();
    for (key, value) in [("ow.owner.type", "mod"), ("ow.owner.id", "7515"), ("ow.resource.id", "main"),
                         ("file.extension", "zip"), ("file.size", "5"), ("ow.transfer.bytes_sent", "5"),
                         ("ow.transfer.percent_sent", "100"), ("ow.transfer.result", "complete")] {
        assert!(transfer.attributes.iter().any(|v| v.key.as_str() == key && v.value.to_string() == value), "missing {key}");
    }
    let server = spans.iter().find(|s| s.name.starts_with("GET ")).unwrap();
    assert_eq!(transfer.parent_span_id, server.span_context.span_id());
    drop(spans);

    let (sender, receiver) = mpsc::channel::<Bytes>(4);
    let receiver = Arc::new(tokio::sync::Mutex::new(Some(receiver)));
    let app = Router::new().route("/download/:storage_type/*path", get(move || {
        let receiver = receiver.clone();
        async move {
            let rx = receiver.lock().await.take().unwrap();
            let stream = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|bytes| (Ok::<_, std::io::Error>(bytes), rx))
            });
            axum::http::Response::builder().header("content-length", "5")
                .body(Body::from_stream(stream)).unwrap()
        }
    })).layer(middleware::from_fn(open_workshop_storage::telemetry::trace_request));
    let response = app.oneshot(axum::http::Request::builder().uri("/download/resource/mods/42/9.webp").body(Body::empty()).unwrap()).await.unwrap();
    let mut body = response.into_body();
    sender.send(Bytes::from_static(b"ab")).await.unwrap();
    assert_eq!(body.frame().await.unwrap().unwrap().into_data().unwrap(), "ab");
    drop(body);
    let spans = shared_spans.lock().unwrap();
    let transfer = spans.iter().rev().find(|s| s.name == "Send file to Nginx").unwrap();
    for (key, value) in [("ow.owner.id", "42"), ("ow.resource.id", "9"), ("file.extension", "webp"),
                         ("ow.transfer.bytes_sent", "2"), ("ow.transfer.percent_sent", "40"), ("ow.transfer.result", "interrupted")] {
        assert!(transfer.attributes.iter().any(|v| v.key.as_str() == key && v.value.to_string() == value), "missing {key}");
    }
    let before = spans.len();
    drop(spans);
    let app = Router::new().route("/healthz", get(|| async { "ok" }))
        .layer(middleware::from_fn(open_workshop_storage::telemetry::trace_request));
    let response = app.clone().oneshot(axum::http::Request::builder().uri("/healthz").body(Body::empty()).unwrap()).await.unwrap();
    drop(response);
    assert_eq!(shared_spans.lock().unwrap().len(), before);
    let response = app.oneshot(axum::http::Request::builder().uri("/healthz")
        .header("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
        .body(Body::empty()).unwrap()).await.unwrap();
    drop(response);
    let spans = shared_spans.lock().unwrap();
    let health = spans.iter().find(|s| s.name == "GET /healthz").unwrap();
    assert_eq!(health.parent_span_id.to_string(), "2222222222222222");
}
