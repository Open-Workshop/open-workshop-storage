mod common;

use axum::body::Body;
use axum::http::{header::CONTENT_DISPOSITION, header::CONTENT_LENGTH, Method, StatusCode};
use open_workshop_storage::web::build_distributor_router;

use common::{call, response_bytes, temp_dir, test_state};

#[tokio::test]
async fn download_response_sets_content_disposition_and_length() {
    let dir = temp_dir("download-headers");
    let state = test_state(dir.path());
    let file_path = state.main_dir.join("resource/mods/123/sample.bin");
    std::fs::create_dir_all(file_path.parent().expect("file parent")).expect("create parent");
    let expected = b"hello world".to_vec();
    std::fs::write(&file_path, &expected).expect("write file");

    let app = build_distributor_router(state);
    let response = call(
        &app,
        Method::GET,
        "/download/resource/mods/123/sample.bin?filename=Downloadable_Mod",
        Body::empty(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        Some(r#"attachment; filename="Downloadable_Mod.bin""#.to_string())
    );
    assert_eq!(
        response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        Some(expected.len().to_string())
    );
    assert_eq!(response_bytes(response).await.to_vec(), expected);
}

#[tokio::test]
async fn download_head_response_sets_content_length() {
    let dir = temp_dir("download-head");
    let state = test_state(dir.path());
    let file_path = state.main_dir.join("resource/mods/123/sample.bin");
    std::fs::create_dir_all(file_path.parent().expect("file parent")).expect("create parent");
    let expected = b"hello world".to_vec();
    std::fs::write(&file_path, &expected).expect("write file");

    let app = build_distributor_router(state);
    let response = call(
        &app,
        Method::HEAD,
        "/download/resource/mods/123/sample.bin?filename=Downloadable_Mod",
        Body::empty(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        Some(expected.len().to_string())
    );
    assert!(response_bytes(response).await.is_empty());
}
