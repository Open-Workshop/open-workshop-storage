mod common;

use axum::body::Body;
use axum::http::{Method, StatusCode};
use common::{call, response_bytes, response_text, temp_dir, test_state};
use open_workshop_storage::web::{build_distributor_router, build_loader_router};
use serde_json::Value;

async fn json_from_response(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response_bytes(response).await).expect("parse openapi json")
}

#[tokio::test]
async fn distributor_swagger_ui_and_openapi_are_exposed() {
    let dir = temp_dir("openapi-distributor");
    let state = test_state(dir.path());
    let app = build_distributor_router(state);

    let docs = call(&app, Method::GET, "/", Body::empty()).await;
    assert_eq!(docs.status(), StatusCode::OK);
    let docs_text = response_text(docs).await;
    assert!(docs_text.contains("swagger-ui"));

    let openapi =
        json_from_response(call(&app, Method::GET, "/openapi.json", Body::empty()).await).await;
    let paths = openapi["paths"].as_object().expect("paths object");
    assert!(paths.contains_key("/healthz"));
    assert!(paths.contains_key("/blurhashes"));
    assert!(paths.contains_key("/download/{storage_type}/{path}"));
    assert_eq!(openapi["servers"][0]["url"], "./");
}

#[tokio::test]
async fn loader_swagger_ui_and_openapi_are_exposed() {
    let dir = temp_dir("openapi-loader");
    let state = test_state(dir.path());
    let app = build_loader_router(state);

    let docs = call(&app, Method::GET, "/", Body::empty()).await;
    assert_eq!(docs.status(), StatusCode::OK);
    let docs_text = response_text(docs).await;
    assert!(docs_text.contains("swagger-ui"));

    let openapi =
        json_from_response(call(&app, Method::GET, "/openapi.json", Body::empty()).await).await;
    let paths = openapi["paths"].as_object().expect("paths object");
    assert!(paths.contains_key("/healthz"));
    assert!(paths.contains_key("/upload"));
    assert!(paths.contains_key("/delete"));
    assert!(paths.contains_key("/transfer/start"));
    assert!(paths.contains_key("/transfer/upload"));
    assert!(paths.contains_key("/transfer/ws/{job_id}"));
    assert_eq!(openapi["servers"][0]["url"], "./");
}
