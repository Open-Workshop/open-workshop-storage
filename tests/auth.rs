mod common;

use serde_json::{Map, Value};

use common::{temp_dir, test_config, transfer_token, DELETE_TOKEN, MANAGE_TOKEN, UPLOAD_TOKEN};
use open_workshop_storage::auth::{check_token, decode_transfer_jwt, is_safe_job_id};

#[test]
fn checks_bcrypt_tokens_and_job_ids() {
    let dir = temp_dir("auth-tokens");
    let config = test_config(dir.path());

    assert!(check_token(&config, "upload_file", UPLOAD_TOKEN));
    assert!(check_token(&config, "delete_file", DELETE_TOKEN));
    assert!(check_token(&config, "storage_manage_token", MANAGE_TOKEN));
    assert!(!check_token(&config, "upload_file", "wrong-token"));
    assert!(is_safe_job_id("abcDEF123_-"));
    assert!(!is_safe_job_id("bad/job/id"));
}

#[test]
fn transfer_jwt_round_trip_works() {
    let dir = temp_dir("auth-jwt");
    let config = test_config(dir.path());
    let mut payload = Map::new();
    payload.insert("job_id".to_string(), Value::String("abcd1234".to_string()));
    payload.insert(
        "download_url".to_string(),
        Value::String("https://example.com/archive.zip".to_string()),
    );

    let token = transfer_token(&config, &payload, "storage", 60);
    let decoded = decode_transfer_jwt(&config, &token, "storage").expect("decode jwt");

    assert_eq!(
        decoded.get("job_id").and_then(Value::as_str),
        Some("abcd1234")
    );
    assert_eq!(
        decoded.get("download_url").and_then(Value::as_str),
        Some("https://example.com/archive.zip")
    );
    assert_eq!(decoded.get("aud").and_then(Value::as_str), Some("storage"));
}

#[test]
fn existing_bcrypt_hashes_remain_valid_after_upgrade() {
    let hash = "$2b$04$abcdefghijklmnopqrstuu6idm4fKzWcovIJIpYNHcl9xI4Dwfk92";
    assert!(bcrypt::verify("upload-token", hash).unwrap());
    assert!(!bcrypt::verify("wrong-token", hash).unwrap());
}

#[test]
fn previously_issued_hs256_tokens_remain_valid() {
    let dir = temp_dir("legacy-jwt");
    let config = test_config(dir.path());
    let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJzdG9yYWdlIiwiZXhwIjo0MTAyNDQ0ODAwLCJqb2JfaWQiOiJmaXh0dXJlMTIzIn0.Vn56WgHmNh3EELlQFLngP500ehhWG25FsbKzgIvd5MQ";
    let claims = decode_transfer_jwt(&config, token, "storage").expect("legacy token");
    assert_eq!(claims.get("job_id").and_then(Value::as_str), Some("fixture123"));
    assert!(decode_transfer_jwt(&config, token, "other").is_none());
}
