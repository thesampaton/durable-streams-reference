mod common;

use common::{spawn_test_server, test_client, unique_stream_name};

/// Validates spec: 02-append-semantics.md#append-data
///
/// Verifies that POST appends data and returns 204 with Stream-Next-Offset.
#[tokio::test]
async fn test_append_returns_204() {
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

    // Append data
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("Hello, world!")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204, "Expected 204 No Content");

    // Check Stream-Next-Offset header
    let next_offset = response
        .headers()
        .get("Stream-Next-Offset")
        .expect("Missing Stream-Next-Offset header")
        .to_str()
        .unwrap();

    // After appending 13 bytes, next offset should be 0000000000000001_000000000000000d
    assert_eq!(next_offset, "0000000000000001_000000000000000d");

    // Body should be empty
    let body = response.bytes().await.unwrap();
    assert!(body.is_empty(), "POST response should have no body");
}

/// Validates spec: 02-append-semantics.md#append-data
///
/// Verifies that multiple appends generate monotonic offsets.
#[tokio::test]
async fn test_multiple_appends_monotonic_offsets() {
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

    // First append (5 bytes: "first")
    let response1 = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("first")
        .send()
        .await
        .unwrap();
    let offset1 = response1
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(offset1, "0000000000000001_0000000000000005");

    // Second append (6 bytes: "second")
    let response2 = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("second")
        .send()
        .await
        .unwrap();
    let offset2 = response2
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(offset2, "0000000000000002_000000000000000b");

    // Third append (5 bytes: "third")
    let response3 = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("third")
        .send()
        .await
        .unwrap();
    let offset3 = response3
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(offset3, "0000000000000003_0000000000000010");
}

/// Validates spec: 02-append-semantics.md#content-type-validation
///
/// Verifies that content-type mismatch returns 409.
#[tokio::test]
async fn test_append_content_type_mismatch_returns_409() {
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

    // Try to append with application/json
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"{"key": "value"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 409, "Expected 409 Conflict");
}

/// Validates spec: 02-append-semantics.md#content-type-validation
///
/// Verifies that content-type comparison is case-insensitive.
#[tokio::test]
async fn test_append_content_type_case_insensitive() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with lowercase
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    // Append with uppercase
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "TEXT/PLAIN")
        .body("data")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        204,
        "Expected 204 for case-insensitive match"
    );
}

/// Validates spec: 02-append-semantics.md#content-type-validation
///
/// Verifies that charset parameter is ignored.
#[tokio::test]
async fn test_append_content_type_ignores_charset() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream without charset
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    // Append with charset
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain; charset=utf-8")
        .body("data")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        204,
        "Expected 204 when charset is ignored"
    );
}

/// Validates spec: 02-append-semantics.md#append-data
///
/// Verifies that empty body without Stream-Closed returns 400.
#[tokio::test]
async fn test_append_empty_body_without_closed_returns_400() {
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

    // Try to append empty body without Stream-Closed
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        400,
        "Expected 400 for empty body without Stream-Closed"
    );
}

/// Validates spec: 02-append-semantics.md#stream-closure
///
/// Verifies that Stream-Closed: true closes the stream.
#[tokio::test]
async fn test_close_stream_without_data() {
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

    // Close stream without data
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204, "Expected 204 for close operation");

    // Verify stream is closed via HEAD
    let head_response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let closed = head_response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed header")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");
}

/// Validates spec: 02-append-semantics.md#stream-closure
///
/// Verifies that closing with data appends and then closes.
#[tokio::test]
async fn test_close_stream_with_data() {
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

    // Close stream with final message
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .body("goodbye")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204, "Expected 204 for close with data");

    let next_offset = response
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap();

    // After appending 7 bytes, next offset should be after the message
    assert_eq!(next_offset, "0000000000000001_0000000000000007");
}

/// Validates spec: 02-append-semantics.md#behavior-after-closure
///
/// Verifies that appending to closed stream returns 409.
#[tokio::test]
async fn test_append_to_closed_stream_returns_409() {
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

    // Close stream
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    // Try to append to closed stream
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("more data")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 409, "Expected 409 Conflict");

    // Verify Stream-Closed header in error response
    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed header in error")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");
}

/// Validates spec: 02-append-semantics.md#append-data
///
/// Verifies that appending to non-existent stream returns 404.
#[tokio::test]
async fn test_append_to_nonexistent_stream_returns_404() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Try to append without creating stream
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("data")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404, "Expected 404 Not Found");
}
