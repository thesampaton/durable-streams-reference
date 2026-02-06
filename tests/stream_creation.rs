mod common;

use common::{spawn_test_server, test_client, unique_stream_name};

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that PUT /stream/{name} with Content-Type returns 201 Created
/// with Location, Content-Type, and Stream-Next-Offset headers.
#[tokio::test]
async fn test_create_stream_returns_201() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 201, "Expected 201 Created");

    // Check Location header
    let location = response
        .headers()
        .get("location")
        .expect("Missing Location header")
        .to_str()
        .unwrap();
    assert_eq!(location, format!("/v1/stream/{stream_name}"));

    // Check Content-Type header
    let content_type = response
        .headers()
        .get("content-type")
        .expect("Missing Content-Type header")
        .to_str()
        .unwrap();
    assert_eq!(content_type, "text/plain");

    // Check Stream-Next-Offset header
    let next_offset = response
        .headers()
        .get("Stream-Next-Offset")
        .expect("Missing Stream-Next-Offset header")
        .to_str()
        .unwrap();
    assert_eq!(next_offset, "0000000000000000_0000000000000000");

    // Check security headers
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

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that recreating a stream with matching config returns 200 OK.
#[tokio::test]
async fn test_idempotent_create_returns_200() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream
    let response1 = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(response1.status(), 201);

    // Recreate with same config
    let response2 = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response2.status(),
        200,
        "Expected 200 OK for idempotent create"
    );

    // Headers should still be present
    assert!(response2.headers().get("content-type").is_some());
    assert!(response2.headers().get("Stream-Next-Offset").is_some());
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that creating a stream with different config returns 409 Conflict.
#[tokio::test]
async fn test_config_mismatch_returns_409() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with text/plain
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    // Try to recreate with different content-type
    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 409, "Expected 409 Conflict");
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that PUT with non-empty body returns 400 Bad Request.
#[tokio::test]
async fn test_put_with_body_returns_400() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("some body data")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400, "Expected 400 Bad Request");
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that PUT without Content-Type returns 400 Bad Request.
#[tokio::test]
async fn test_missing_content_type_returns_400() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400, "Expected 400 Bad Request");
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that Content-Type comparison is case-insensitive.
#[tokio::test]
async fn test_content_type_case_insensitive() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create with lowercase
    let response1 = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(response1.status(), 201);

    // Recreate with uppercase (should be idempotent)
    let response2 = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "TEXT/PLAIN")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response2.status(),
        200,
        "Expected 200 for case-insensitive match"
    );
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that charset parameter is stripped from Content-Type.
#[tokio::test]
async fn test_content_type_charset_stripped() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create with charset
    let response1 = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain; charset=utf-8")
        .send()
        .await
        .unwrap();
    assert_eq!(response1.status(), 201);

    // Response should have normalized content-type
    let content_type = response1
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "text/plain");

    // Recreate without charset (should be idempotent)
    let response2 = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(response2.status(), 200);
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies TTL validation rejects leading zeros.
#[tokio::test]
async fn test_ttl_validation_leading_zeros() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "0123") // Leading zero
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        400,
        "Expected 400 for leading zeros in TTL"
    );
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies TTL validation rejects floats.
#[tokio::test]
async fn test_ttl_validation_floats() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "3600.5") // Float
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400, "Expected 400 for float TTL");
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies TTL validation rejects scientific notation.
#[tokio::test]
async fn test_ttl_validation_scientific_notation() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "1e3") // Scientific notation
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        400,
        "Expected 400 for scientific notation in TTL"
    );
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that providing both TTL and Expires-At returns 400.
#[tokio::test]
async fn test_both_ttl_and_expires_at_returns_400() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "3600")
        .header("Stream-Expires-At", "2025-12-31T23:59:59Z")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        400,
        "Expected 400 for both TTL and Expires-At"
    );
}

/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that valid TTL is accepted.
#[tokio::test]
async fn test_valid_ttl() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "3600")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201, "Expected 201 for valid TTL");
}

/// Validates spec: 01-stream-lifecycle.md#stream-metadata
///
/// Verifies that HEAD returns stream metadata with correct headers.
#[tokio::test]
async fn test_head_returns_metadata() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // HEAD request
    let response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200, "Expected 200 OK");

    // Verify headers
    let content_type = response
        .headers()
        .get("content-type")
        .expect("Missing Content-Type header")
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/json");

    let next_offset = response
        .headers()
        .get("Stream-Next-Offset")
        .expect("Missing Stream-Next-Offset header")
        .to_str()
        .unwrap();
    assert_eq!(next_offset, "0000000000000000_0000000000000000");

    let cache_control = response
        .headers()
        .get("cache-control")
        .expect("Missing Cache-Control header")
        .to_str()
        .unwrap();
    assert_eq!(cache_control, "no-store");

    // Stream-Closed should be absent (stream is open)
    assert!(response.headers().get("Stream-Closed").is_none());

    // Body should be empty (HEAD request)
    let body_bytes = response.bytes().await.unwrap();
    assert!(body_bytes.is_empty(), "HEAD response should have no body");
}

/// Validates spec: 01-stream-lifecycle.md#stream-metadata
///
/// Verifies that HEAD returns 404 for non-existent stream.
#[tokio::test]
async fn test_head_nonexistent_returns_404() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        404,
        "Expected 404 for non-existent stream"
    );
}

/// Validates spec: 01-stream-lifecycle.md#stream-metadata
///
/// Verifies that HEAD includes TTL metadata when stream has TTL.
#[tokio::test]
async fn test_head_includes_ttl_metadata() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with TTL
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "7200")
        .send()
        .await
        .unwrap();

    // HEAD request
    let response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    // Should have Stream-TTL (remaining seconds)
    let ttl = response
        .headers()
        .get("Stream-TTL")
        .expect("Missing Stream-TTL header")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();

    // TTL should be close to 7200 (within a second or two)
    assert!(ttl >= 7198 && ttl <= 7200, "TTL should be close to 7200");

    // Should have Stream-Expires-At
    assert!(response.headers().get("Stream-Expires-At").is_some());
}

/// Validates spec: 01-stream-lifecycle.md#stream-metadata
///
/// Verifies that HEAD includes Stream-Closed for closed stream.
#[tokio::test]
async fn test_head_includes_closed_flag() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create closed stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    // HEAD request
    let response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed header")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");
}
