mod common;

use common::{spawn_test_server, test_client, unique_stream_name};

/// Validates that GET responses include security headers.
#[tokio::test]
async fn test_get_security_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create and append
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("data")
        .send()
        .await
        .unwrap();

    // GET request
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    // Verify security headers
    assert_eq!(
        response
            .headers()
            .get("X-Content-Type-Options")
            .unwrap()
            .to_str()
            .unwrap(),
        "nosniff"
    );
    assert_eq!(
        response
            .headers()
            .get("Cross-Origin-Resource-Policy")
            .unwrap()
            .to_str()
            .unwrap(),
        "cross-origin"
    );
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-store"
    );
}

/// Validates that PUT responses include security headers.
#[tokio::test]
async fn test_put_security_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    // Verify security headers
    assert_eq!(
        response
            .headers()
            .get("X-Content-Type-Options")
            .unwrap()
            .to_str()
            .unwrap(),
        "nosniff"
    );
    assert_eq!(
        response
            .headers()
            .get("Cross-Origin-Resource-Policy")
            .unwrap()
            .to_str()
            .unwrap(),
        "cross-origin"
    );
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-store"
    );
}

/// Validates that HEAD responses include security headers.
#[tokio::test]
async fn test_head_security_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    // Verify security headers
    assert_eq!(
        response
            .headers()
            .get("X-Content-Type-Options")
            .unwrap()
            .to_str()
            .unwrap(),
        "nosniff"
    );
    assert_eq!(
        response
            .headers()
            .get("Cross-Origin-Resource-Policy")
            .unwrap()
            .to_str()
            .unwrap(),
        "cross-origin"
    );
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-store"
    );
}

/// Validates that POST responses include security headers.
#[tokio::test]
async fn test_post_security_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("data")
        .send()
        .await
        .unwrap();

    // Verify security headers
    assert_eq!(
        response
            .headers()
            .get("X-Content-Type-Options")
            .unwrap()
            .to_str()
            .unwrap(),
        "nosniff"
    );
    assert_eq!(
        response
            .headers()
            .get("Cross-Origin-Resource-Policy")
            .unwrap()
            .to_str()
            .unwrap(),
        "cross-origin"
    );
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-store"
    );
}

/// Validates that DELETE responses include security headers.
#[tokio::test]
async fn test_delete_security_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let response = client
        .delete(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    // Verify security headers
    assert_eq!(
        response
            .headers()
            .get("X-Content-Type-Options")
            .unwrap()
            .to_str()
            .unwrap(),
        "nosniff"
    );
    assert_eq!(
        response
            .headers()
            .get("Cross-Origin-Resource-Policy")
            .unwrap()
            .to_str()
            .unwrap(),
        "cross-origin"
    );
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-store"
    );
}

/// Validates that error responses include security headers.
#[tokio::test]
async fn test_error_response_security_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Try to read non-existent stream (404 error)
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);

    // Verify security headers even on errors
    assert_eq!(
        response
            .headers()
            .get("X-Content-Type-Options")
            .unwrap()
            .to_str()
            .unwrap(),
        "nosniff"
    );
    assert_eq!(
        response
            .headers()
            .get("Cross-Origin-Resource-Policy")
            .unwrap()
            .to_str()
            .unwrap(),
        "cross-origin"
    );
}
