use std::sync::Arc;

use axum::body::Body;
use axum::extract::Extension;
use axum::http::{HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Router;

use crate::handlers::{files, transfers};
use crate::runtime::AppState;

async fn cors_middleware(req: axum::http::Request<Body>, next: Next) -> Response {
    if req.method() == Method::OPTIONS {
        let mut response = Response::new(Body::from("OK"));
        response.headers_mut().insert(
            axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        );
        response.headers_mut().insert(
            axum::http::header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET,POST,PUT,DELETE,OPTIONS,HEAD"),
        );
        response.headers_mut().insert(
            axum::http::header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("Content-Type,Authorization,X-File-Name"),
        );
        response.headers_mut().insert(
            axum::http::header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("Content-Type,Content-Disposition,X-Unpacked-Bytes"),
        );
        return response;
    }

    let mut response = next.run(req).await;
    response.headers_mut().insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response.headers_mut().insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET,POST,PUT,DELETE,OPTIONS,HEAD"),
    );
    response.headers_mut().insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type,Authorization,X-File-Name"),
    );
    response.headers_mut().insert(
        axum::http::header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("Content-Type,Content-Disposition,X-Unpacked-Bytes"),
    );
    response
}

async fn healthz(Extension(_state): Extension<Arc<AppState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({"status": "ok"})),
    )
}

async fn distributor_docs(Extension(_state): Extension<Arc<AppState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        "Open Workshop Distributor\n\nHealth: /healthz\nEndpoints: /blurhashes, /download/:storage_type/*path",
    )
}

async fn loader_docs(Extension(_state): Extension<Arc<AppState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        "Open Workshop Loader\n\nHealth: /healthz\nEndpoints: /upload, /delete, /transfer/*",
    )
}

fn base_router() -> Router {
    Router::new().route("/healthz", get(healthz))
}

pub fn build_distributor_router(state: Arc<AppState>) -> Router {
    base_router()
        .route("/", get(distributor_docs))
        .route("/blurhashes", post(files::blurhashes))
        .route(
            "/download/:storage_type/*path",
            get(files::download).head(files::download),
        )
        .layer(axum::extract::Extension(state))
        .layer(middleware::from_fn(cors_middleware))
}

pub fn build_loader_router(state: Arc<AppState>) -> Router {
    base_router()
        .route("/", get(loader_docs))
        .route("/upload", post(files::upload))
        .route("/delete", delete(files::delete))
        .route(
            "/transfer/start",
            get(transfers::transfer_start_get).post(transfers::transfer_start_post),
        )
        .route("/transfer/upload", post(transfers::transfer_upload))
        .route("/transfer/ws/:job_id", get(transfers::transfer_ws))
        .route("/transfer/repack", post(transfers::transfer_repack))
        .route("/transfer/move", post(transfers::transfer_move))
        .layer(axum::extract::Extension(state))
        .layer(middleware::from_fn(cors_middleware))
}
