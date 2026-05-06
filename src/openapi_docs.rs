#![allow(dead_code)]

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Extension, OriginalUri};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Json;
use axum::Router;
use serde::{Deserialize, Serialize};
use utoipa::openapi::server::Server;
use utoipa::{Modify, OpenApi, ToSchema};
use utoipa_swagger_ui::{serve, Config as SwaggerConfig};

type OpenApiSpec = utoipa::openapi::OpenApi;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HealthResponseDoc {
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlurhashBatchRequestDoc {
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlurhashItemDoc {
    pub path: String,
    pub blurhash: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlurhashBatchResponseDoc {
    pub items: Vec<BlurhashItemDoc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UploadMultipartRequestDoc {
    #[schema(value_type = String, format = Binary)]
    pub file: Vec<u8>,
    #[serde(rename = "type")]
    pub storage_type: String,
    pub path: String,
    pub file_kind: String,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteRequestDoc {
    #[serde(rename = "type")]
    pub storage_type: String,
    pub path: String,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransferStartFormDoc {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransferStartResponseDoc {
    pub job_id: String,
    pub status: String,
    pub ws_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransferUploadResponseDoc {
    pub job_id: String,
    pub bytes: u64,
    pub total: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransferRepackFormDoc {
    pub job_id: String,
    #[serde(rename = "format")]
    pub pack_format: Option<String>,
    pub compression_level: Option<i64>,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransferRepackResponseDoc {
    pub job_id: String,
    pub packed_bytes: Option<u64>,
    pub packed_path: Option<String>,
    pub unpacked_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransferMoveFormDoc {
    pub job_id: String,
    #[serde(rename = "type")]
    pub storage_type: String,
    #[serde(rename = "path")]
    pub target_path: String,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransferMoveResponseDoc {
    pub job_id: String,
    pub final_path: String,
    pub final_bytes: u64,
}

#[allow(dead_code)]
#[utoipa::path(
    get,
    path = "/healthz",
    responses(
        (status = 200, description = "Service is healthy", body = HealthResponseDoc)
    )
)]
async fn healthz_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    post,
    path = "/blurhashes",
    request_body(content = BlurhashBatchRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "BlurHash batch generated", body = BlurhashBatchResponseDoc),
        (status = 400, description = "Invalid request body")
    )
)]
async fn blurhashes_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    get,
    path = "/download/{storage_type}/{path}",
    params(
        ("storage_type" = String, Path, description = "Storage root to read from"),
        ("path" = String, Path, description = "Relative path inside the selected storage root"),
        ("filename" = Option<String>, Query, description = "Optional safe filename prefix for the download name")
    ),
    responses(
        (status = 200, description = "File streamed successfully", body = Vec<u8>),
        (status = 400, description = "Invalid storage type"),
        (status = 403, description = "Access denied"),
        (status = 404, description = "File not found"),
        (status = 423, description = "Path traversal blocked"),
        (status = 503, description = "Access service unavailable")
    )
)]
async fn download_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    head,
    path = "/download/{storage_type}/{path}",
    params(
        ("storage_type" = String, Path, description = "Storage root to read from"),
        ("path" = String, Path, description = "Relative path inside the selected storage root"),
        ("filename" = Option<String>, Query, description = "Optional safe filename prefix for the download name")
    ),
    responses(
        (status = 200, description = "File metadata returned for HEAD requests"),
        (status = 400, description = "Invalid storage type"),
        (status = 403, description = "Access denied"),
        (status = 404, description = "File not found"),
        (status = 423, description = "Path traversal blocked"),
        (status = 503, description = "Access service unavailable")
    )
)]
async fn download_head_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    post,
    path = "/upload",
    request_body(content = UploadMultipartRequestDoc, content_type = "multipart/form-data"),
    responses(
        (status = 201, description = "File uploaded", body = String),
        (status = 400, description = "Invalid type, path, or file kind"),
        (status = 401, description = "Token missing"),
        (status = 403, description = "Invalid token or blocked path"),
        (status = 423, description = "Path traversal blocked")
    )
)]
async fn upload_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    delete,
    path = "/delete",
    request_body(content = DeleteRequestDoc, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "File deleted", body = String),
        (status = 400, description = "Invalid type"),
        (status = 401, description = "Token missing"),
        (status = 403, description = "Invalid token or blocked path"),
        (status = 404, description = "File not found")
    )
)]
async fn delete_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    get,
    path = "/transfer/start",
    params(
        ("token" = String, Query, description = "Transfer JWT")
    ),
    responses(
        (status = 200, description = "Transfer started or existing job returned", body = TransferStartResponseDoc),
        (status = 400, description = "Invalid job id or download URL"),
        (status = 401, description = "Token missing"),
        (status = 403, description = "Access denied")
    )
)]
async fn transfer_start_get_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    post,
    path = "/transfer/start",
    request_body(content = TransferStartFormDoc, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "Transfer started or existing job returned", body = TransferStartResponseDoc),
        (status = 400, description = "Invalid job id or download URL"),
        (status = 401, description = "Token missing"),
        (status = 403, description = "Access denied")
    )
)]
async fn transfer_start_post_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    post,
    path = "/transfer/upload",
    params(
        ("token" = Option<String>, Query, description = "Transfer JWT query fallback"),
        ("Authorization" = Option<String>, Header, description = "Bearer transfer JWT"),
        ("filename" = Option<String>, Query, description = "Optional filename override"),
        ("size" = Option<u64>, Query, description = "Optional upload size hint"),
        ("X-File-Name" = Option<String>, Header, description = "Optional filename override"),
        ("X-File-Size" = Option<u64>, Header, description = "Optional upload size hint"),
        ("X-Upload-Size" = Option<u64>, Header, description = "Optional upload size hint")
    ),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses(
        (status = 200, description = "Upload completed", body = TransferUploadResponseDoc),
        (status = 401, description = "Token missing"),
        (status = 403, description = "Access denied"),
        (status = 413, description = "Upload exceeds size limits"),
        (status = 408, description = "Upload timed out"),
        (status = 429, description = "Storage busy")
    )
)]
async fn transfer_upload_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    post,
    path = "/transfer/repack",
    request_body(content = TransferRepackFormDoc, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "Archive repacked", body = TransferRepackResponseDoc),
        (status = 400, description = "Invalid job id, unsupported format, or encrypted archive"),
        (status = 401, description = "Token missing"),
        (status = 403, description = "Access denied"),
        (status = 404, description = "Job or source file not found"),
        (status = 413, description = "Archive too large"),
        (status = 429, description = "Storage busy")
    )
)]
async fn transfer_repack_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    post,
    path = "/transfer/move",
    request_body(content = TransferMoveFormDoc, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "Packed file moved", body = TransferMoveResponseDoc),
        (status = 400, description = "Invalid job id or storage type"),
        (status = 401, description = "Token missing"),
        (status = 403, description = "Access denied"),
        (status = 404, description = "Job or packed file not found")
    )
)]
async fn transfer_move_doc() {}

#[allow(dead_code)]
#[utoipa::path(
    get,
    path = "/transfer/ws/{job_id}",
    params(
        ("job_id" = String, Path, description = "Transfer job id"),
        ("token" = String, Query, description = "Transfer JWT")
    ),
    responses(
        (status = 101, description = "WebSocket upgrade"),
        (status = 401, description = "Token missing"),
        (status = 403, description = "Access denied")
    )
)]
async fn transfer_ws_doc() {}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        healthz_doc,
        blurhashes_doc,
        download_doc,
        download_head_doc
    ),
    modifiers(&DistributorModifier)
)]
struct DistributorApiDoc;

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        healthz_doc,
        upload_doc,
        delete_doc,
        transfer_start_get_doc,
        transfer_start_post_doc,
        transfer_upload_doc,
        transfer_repack_doc,
        transfer_move_doc,
        transfer_ws_doc
    ),
    modifiers(&LoaderModifier)
)]
struct LoaderApiDoc;

struct DistributorModifier;

impl Modify for DistributorModifier {
    fn modify(&self, openapi: &mut OpenApiSpec) {
        openapi.info.title = "Open Workshop Storage Distributor".to_string();
        openapi.info.description =
            Some("Public distributor API for downloads and BlurHash generation".to_string());
        openapi.servers = Some(vec![Server::new("./")]);
    }
}

struct LoaderModifier;

impl Modify for LoaderModifier {
    fn modify(&self, openapi: &mut OpenApiSpec) {
        openapi.info.title = "Open Workshop Storage Loader".to_string();
        openapi.info.description = Some("Loader API for uploads and transfer jobs".to_string());
        openapi.servers = Some(vec![Server::new("./")]);
    }
}

fn swagger_config() -> Arc<SwaggerConfig<'static>> {
    Arc::new(SwaggerConfig::from("openapi.json"))
}

fn swagger_file_response(file_path: &str, config: Arc<SwaggerConfig<'static>>) -> Response {
    match serve(file_path, config) {
        Ok(Some(file)) => Response::builder()
            .status(StatusCode::OK)
            .header(
                CONTENT_TYPE,
                HeaderValue::from_str(&file.content_type)
                    .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
            )
            .body(Body::from(file.bytes.into_owned()))
            .unwrap_or_else(|err| {
                (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
            }),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn swagger_ui_root(Extension(config): Extension<Arc<SwaggerConfig<'static>>>) -> Response {
    swagger_file_response("", config)
}

async fn swagger_ui_asset(
    OriginalUri(original_uri): OriginalUri,
    Extension(config): Extension<Arc<SwaggerConfig<'static>>>,
) -> Response {
    let path = original_uri.path().trim_start_matches('/');
    swagger_file_response(path, config)
}

async fn openapi_json(Extension(openapi): Extension<Arc<OpenApiSpec>>) -> Json<Arc<OpenApiSpec>> {
    Json(openapi)
}

fn docs_router(openapi: Arc<OpenApiSpec>) -> Router {
    let config = swagger_config();
    Router::new()
        .route("/", get(swagger_ui_root))
        .route("/openapi.json", get(openapi_json))
        .route("/swagger-ui/openapi.json", get(openapi_json))
        .fallback(any(swagger_ui_asset))
        .layer(Extension(config))
        .layer(Extension(openapi))
}

pub fn distributor_docs_router() -> Router {
    docs_router(Arc::new(DistributorApiDoc::openapi()))
}

pub fn loader_docs_router() -> Router {
    docs_router(Arc::new(LoaderApiDoc::openapi()))
}
