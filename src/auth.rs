use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use bcrypt::verify;
use hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::Sha256;

use crate::config::AppConfig;

type HmacSha256 = Hmac<Sha256>;

const TRANSFER_JWT_ALG: &str = "HS256";

pub fn check_token(config: &AppConfig, token_name: &str, token: &str) -> bool {
    let Some(stored) = config.token_hash(token_name) else {
        return false;
    };
    verify(token, stored).unwrap_or(false)
}

pub fn is_safe_job_id(job_id: &str) -> bool {
    let len = job_id.len();
    if !(8..=128).contains(&len) {
        return false;
    }
    job_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn b64url_encode(data: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(data)
}

fn b64url_decode(data: &str) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(data.as_bytes()).ok()
}

fn sign(secret: &str, signing_input: &str) -> Option<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(signing_input.as_bytes());
    let result = mac.finalize().into_bytes();
    Some(b64url_encode(&result))
}

fn now_unix_seconds() -> Option<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs() as i64)
}

fn aud_matches(value: &Value, audience: &str) -> bool {
    match value {
        Value::String(text) => text == audience,
        Value::Array(values) => values.iter().any(|item| item.as_str() == Some(audience)),
        _ => false,
    }
}

pub fn decode_transfer_jwt(
    config: &AppConfig,
    token: &str,
    audience: &str,
) -> Option<Map<String, Value>> {
    let secret = config.transfer_jwt_secret.as_deref()?;
    let mut parts = token.split('.');
    let header = parts.next()?;
    let payload = parts.next()?;
    let signature = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let signing_input = format!("{header}.{payload}");
    let expected_signature = sign(secret, &signing_input)?;
    if expected_signature != signature {
        return None;
    }

    let header_bytes = b64url_decode(header)?;
    let header_value: Value = serde_json::from_slice(&header_bytes).ok()?;
    if header_value
        .get("alg")
        .and_then(|value| value.as_str())
        .filter(|alg| *alg == TRANSFER_JWT_ALG)
        .is_none()
    {
        return None;
    }

    let payload_bytes = b64url_decode(payload)?;
    let payload_value: Value = serde_json::from_slice(&payload_bytes).ok()?;
    let map = payload_value.as_object()?.clone();

    let now = now_unix_seconds()?;
    let exp = map.get("exp")?.as_i64()?;
    if exp < now {
        return None;
    }
    let aud = map.get("aud")?;
    if !aud_matches(aud, audience) {
        return None;
    }
    Some(map)
}

pub fn encode_transfer_jwt(
    config: &AppConfig,
    payload: &Map<String, Value>,
    audience: &str,
    ttl_seconds: u64,
) -> Option<String> {
    let secret = config.transfer_jwt_secret.as_deref()?;
    let now = now_unix_seconds()?;
    let mut token_payload = payload.clone();
    token_payload.insert("aud".to_string(), Value::String(audience.to_string()));
    token_payload.insert("iss".to_string(), Value::String("storage".to_string()));
    token_payload.insert("iat".to_string(), Value::from(now));
    token_payload.insert("exp".to_string(), Value::from(now + ttl_seconds as i64));

    let header = serde_json::json!({
        "alg": TRANSFER_JWT_ALG,
        "typ": "JWT",
    });
    let header_part = b64url_encode(&serde_json::to_vec(&header).ok()?);
    let payload_part = b64url_encode(&serde_json::to_vec(&Value::Object(token_payload)).ok()?);
    let signing_input = format!("{header_part}.{payload_part}");
    let signature_part = sign(secret, &signing_input)?;
    Some(format!("{signing_input}.{signature_part}"))
}
