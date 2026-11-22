mod common;

use std::sync::Arc;
use axum::{extract::Extension, http::{HeaderMap, StatusCode}, routing::post, Router};
use opentelemetry::{global, Context};
use opentelemetry::trace::{FutureExt, TraceContextExt, Tracer};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider};
use serde_json::{Map, Value};
use tokio::sync::Mutex;

#[tokio::test]
async fn manager_callback_propagates_the_transfer_trace() {
    let captured = Arc::new(Mutex::new(None::<String>));
    let app = Router::new().route("/callback", post(
        |headers: HeaderMap, Extension(captured): Extension<Arc<Mutex<Option<String>>>>| async move {
            *captured.lock().await = headers.get("traceparent")
                .and_then(|v| v.to_str().ok()).map(str::to_owned);
            StatusCode::OK
        }
    )).layer(Extension(captured.clone()));
    let server = common::spawn_server(app).await;
    let dir = common::temp_dir("callback-tracing");
    let mut config = common::test_config(dir.path());
    config.manager_transfer_callback_url = Some(format!("{}/callback", server.base_url));
    let state = common::test_state_with_config(config);

    let provider = SdkTracerProvider::builder().build();
    global::set_tracer_provider(provider.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());
    let parent = global::tracer("test").start("transfer");
    let context = Context::current_with_span(parent);
    let trace_id = context.span().span_context().trace_id().to_string();
    let payload: Map<String, Value> = [("job_id".to_owned(), Value::String("test".into()))]
        .into_iter().collect();
    state.notify_manager(payload).with_context(context.clone()).await;

    let header = captured.lock().await.clone().expect("callback traceparent");
    assert!(header.starts_with(&format!("00-{trace_id}-")), "{header}");
    assert_ne!(&header[36..52], &context.span().span_context().span_id().to_string());
    context.span().end();
    provider.shutdown().unwrap();
}
