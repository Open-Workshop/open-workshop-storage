use std::time::Duration;

use axum::http::header::{CONTENT_TYPE, COOKIE};
use axum::http::{HeaderMap, HeaderValue, Request, Response, Uri};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::StatusCode;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ModDownloadAccessResult {
    pub allowed: bool,
    pub reason: String,
    pub reason_code: String,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct AccessServiceError {
    pub message: String,
    pub status_code: Option<u16>,
    pub response_text: Option<String>,
}

fn extract_cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookie_header = headers.get(COOKIE)?.to_str().ok()?;
    for pair in cookie_header.split(';') {
        let (key, value) = pair.trim().split_once('=')?;
        if key.trim() == name {
            return Some(value.trim().to_string());
        }
    }
    None
}

fn session_cookies(headers: &HeaderMap) -> Option<String> {
    let mut pairs = Vec::new();
    if let Some(access_token) = extract_cookie_value(headers, "accessToken") {
        pairs.push(format!("accessToken={access_token}"));
    }
    if let Some(refresh_token) = extract_cookie_value(headers, "refreshToken") {
        pairs.push(format!("refreshToken={refresh_token}"));
    }
    if pairs.is_empty() {
        None
    } else {
        Some(pairs.join("; "))
    }
}

fn extract_error_message(response_text: &str) -> String {
    let text = response_text.trim();
    if text.is_empty() {
        return "Access service unavailable".to_string();
    }
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
        if let Some(map) = json.as_object() {
            for key in ["detail", "message", "reason", "title"] {
                if let Some(value) = map.get(key).and_then(|value| value.as_str()) {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        return trimmed.to_string();
                    }
                }
            }
        } else if let Some(value) = json.as_str() {
            if !value.trim().is_empty() {
                return value.trim().to_string();
            }
        }
    }
    text.to_string()
}

async fn read_response_text(response: Response<Incoming>) -> Result<String, AccessServiceError> {
    let status = response.status();
    let body = response.into_body();
    let bytes = body.collect().await.map_err(|err| AccessServiceError {
        message: if status == StatusCode::OK {
            "Access service unavailable".to_string()
        } else {
            err.to_string()
        },
        status_code: Some(status.as_u16()),
        response_text: None,
    })?;
    Ok(String::from_utf8_lossy(&bytes.to_bytes()).to_string())
}

pub async fn resolve_mod_download_access(
    request_headers: &HeaderMap,
    mod_id: u64,
    access_service_url: &str,
    timeout_seconds: u64,
) -> Result<ModDownloadAccessResult, AccessServiceError> {
    let url = format!("{}/mod/{mod_id}", access_service_url.trim_end_matches('/'));
    let uri: Uri = url.parse::<Uri>().map_err(|err| AccessServiceError {
        message: err.to_string(),
        status_code: Some(503),
        response_text: None,
    })?;
    let timeout = Duration::from_secs(timeout_seconds.max(1));

    let mut connector = HttpConnector::new();
    connector.enforce_http(false);
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build(connector);

    let mut request_builder = Request::post(uri)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from_static(b"{}")))
        .map_err(|err| AccessServiceError {
            message: err.to_string(),
            status_code: Some(503),
            response_text: None,
        })?;

    if let Some(cookie_header) = session_cookies(request_headers) {
        request_builder.headers_mut().insert(
            COOKIE,
            cookie_header
                .parse::<HeaderValue>()
                .map_err(|err| AccessServiceError {
                    message: err.to_string(),
                    status_code: Some(503),
                    response_text: None,
                })?,
        );
    }

    let response = match tokio::time::timeout(timeout, client.request(request_builder)).await {
        Ok(Ok(response)) => response,
        Ok(Err(_err)) => {
            return Err(AccessServiceError {
                message: "Access service unavailable".to_string(),
                status_code: Some(503),
                response_text: None,
            });
        }
        Err(_) => {
            return Err(AccessServiceError {
                message: "Access service unavailable".to_string(),
                status_code: Some(503),
                response_text: None,
            });
        }
    };

    if response.status() != StatusCode::OK {
        let status_code = response.status().as_u16();
        let response_text = read_response_text(response).await.unwrap_or_default();
        return Err(AccessServiceError {
            message: extract_error_message(&response_text),
            status_code: Some(status_code),
            response_text: Some(response_text),
        });
    }

    let response_text = read_response_text(response)
        .await
        .map_err(|err| AccessServiceError {
            message: if err.message == "Access service unavailable" {
                "Access service unavailable".to_string()
            } else {
                "Access service returned invalid JSON".to_string()
            },
            status_code: Some(502),
            response_text: None,
        })?;

    let payload: serde_json::Value =
        serde_json::from_str(&response_text).map_err(|_| AccessServiceError {
            message: "Access service returned invalid JSON".to_string(),
            status_code: Some(502),
            response_text: None,
        })?;

    let download = payload
        .get("download")
        .and_then(|value| value.as_object())
        .ok_or_else(|| AccessServiceError {
            message: "Access service returned unexpected response".to_string(),
            status_code: Some(502),
            response_text: None,
        })?;

    let allowed = download
        .get("value")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let reason = download
        .get("reason")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| {
            if allowed {
                "Мод доступен для скачивания.".to_string()
            } else {
                "Access denied".to_string()
            }
        });
    let reason_code = download
        .get("reason_code")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| {
            if allowed {
                "public".to_string()
            } else {
                "forbidden".to_string()
            }
        });

    Ok(ModDownloadAccessResult {
        allowed,
        reason,
        reason_code,
    })
}
