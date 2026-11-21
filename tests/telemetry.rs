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
