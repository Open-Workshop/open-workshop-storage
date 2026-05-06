mod common;

use axum::body::Body;
use axum::http::{Method, StatusCode};

use common::{call, response_text, temp_dir, test_state};
use open_workshop_storage::web::{build_distributor_router, build_loader_router};

#[tokio::test]
async fn distributor_router_exposes_public_routes_only() {
    let dir = temp_dir("service-distributor");
    let state = test_state(dir.path());
    let app = build_distributor_router(state);

    let health = call(&app, Method::GET, "/healthz", Body::empty()).await;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(response_text(health).await, r#"{"status":"ok"}"#);

    let docs = call(&app, Method::GET, "/", Body::empty()).await;
    assert_eq!(docs.status(), StatusCode::OK);
    let docs_text = response_text(docs).await;
    assert!(docs_text.contains("swagger-ui"));

    assert_eq!(
        call(&app, Method::GET, "/transfer/start", Body::empty())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, Method::POST, "/upload", Body::empty())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, Method::DELETE, "/delete", Body::empty())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn loader_router_exposes_ingest_routes_only() {
    let dir = temp_dir("service-loader");
    let state = test_state(dir.path());
    let app = build_loader_router(state);

    let health = call(&app, Method::GET, "/healthz", Body::empty()).await;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(response_text(health).await, r#"{"status":"ok"}"#);

    let docs = call(&app, Method::GET, "/", Body::empty()).await;
    assert_eq!(docs.status(), StatusCode::OK);
    let docs_text = response_text(docs).await;
    assert!(docs_text.contains("swagger-ui"));

    assert_eq!(
        call(
            &app,
            Method::GET,
            "/download/archive/mods/123/main.zip",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, Method::POST, "/blurhashes", Body::empty())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
}
