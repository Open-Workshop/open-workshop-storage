use open_workshop_storage::telemetry;

#[tokio::test]
async fn exporter_initializes_and_shuts_down_outside_the_async_runtime() {
    // A local, closed port is sufficient: this test doesn't emit any spans.
    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:1/v1/traces");
    let provider = tokio::task::spawn_blocking(|| telemetry::init("storage-test"))
        .await
        .expect("telemetry initialization task panicked")
        .expect("telemetry exporter should initialize");
    tokio::task::spawn_blocking(move || provider.shutdown())
        .await
        .expect("telemetry shutdown task panicked")
        .expect("telemetry shutdown failed");
}


#[tokio::test]
async fn request_logs_include_incoming_trace_and_current_span() {
    use axum::{body::Body, http::{Request, StatusCode}, middleware, routing::get, Router};
    use opentelemetry::global;
    use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider};
    use std::{io::{self, Write}, sync::{Arc, Mutex}};
    use tower::ServiceExt;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct Output(Arc<Mutex<Vec<u8>>>);
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }
    impl<'a> MakeWriter<'a> for Output {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer { self.clone() }
    }

    let out = Output::default();
    let subscriber = tracing_subscriber::fmt().with_ansi(false).with_writer(out.clone()).finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let provider = SdkTracerProvider::builder().build();
    global::set_tracer_provider(provider);
    global::set_text_map_propagator(TraceContextPropagator::new());
    let router = Router::new().route("/test/:id", get(|| async {
        tracing::info!("handler event");
        "ok"
    })).layer(middleware::from_fn(telemetry::trace_request));
    let request = Request::builder()
        .uri("/test/123")
        .header("traceparent", "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01")
        .body(Body::empty()).unwrap();
    assert_eq!(router.oneshot(request).await.unwrap().status(), StatusCode::OK);
    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    assert!(text.contains("trace_id=1234567890abcdef1234567890abcdef span_id="), "{text}");
    assert!(text.contains("handler event"), "{text}");
    assert!(text.contains("HTTP response body finished"), "{text}");
}
