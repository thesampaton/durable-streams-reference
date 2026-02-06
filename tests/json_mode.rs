mod common;

use common::{spawn_test_server, spawn_test_server_with_limits, test_client, unique_stream_name};

/// Validates spec: 06-json-mode.md#single-json-value
///
/// Single JSON value appends as one message.
#[tokio::test]
async fn test_single_json_value() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create JSON stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // Append single JSON object
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"{"event":"click","x":100}"#)
        .send()
        .await
        .unwrap();

    // Read should wrap in array
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, r#"[{"event":"click","x":100}]"#);
}

/// Validates spec: 06-json-mode.md#json-array-flattening
///
/// JSON array flattens into multiple messages.
#[tokio::test]
async fn test_array_flattening() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create JSON stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // Append JSON array
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"[{"event":"click","x":100},{"event":"scroll","y":50}]"#)
        .send()
        .await
        .unwrap();

    // Read should return flattened messages wrapped in array
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(
        body,
        r#"[{"event":"click","x":100},{"event":"scroll","y":50}]"#
    );
}

/// Validates spec: 06-json-mode.md#empty-array-rejection
///
/// Empty arrays return 400.
#[tokio::test]
async fn test_empty_array_rejection() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create JSON stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // Append empty array
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body("[]")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
}

/// Validates spec: 06-json-mode.md#invalid-json
///
/// Invalid JSON returns 400.
#[tokio::test]
async fn test_invalid_json() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create JSON stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // Append invalid JSON
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body("{invalid json}")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
}

/// Validates spec: 06-json-mode.md#nested-arrays
///
/// Nested arrays are preserved (only top-level flattened).
#[tokio::test]
async fn test_nested_arrays_preserved() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create JSON stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // Append array with nested arrays
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"[{"tags":["a","b"]},{"items":[1,2,3]}]"#)
        .send()
        .await
        .unwrap();

    // Read should preserve nested arrays
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, r#"[{"tags":["a","b"]},{"items":[1,2,3]}]"#);
}

/// Validates spec: 06-json-mode.md#empty-stream
///
/// Reading from empty JSON stream returns empty array.
#[tokio::test]
async fn test_empty_stream_returns_empty_array() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create JSON stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // Read without appending
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, "[]");
}

/// Validates spec: 06-json-mode.md#mixed-batching
///
/// Mixed batching (single values and arrays) concatenates correctly.
#[tokio::test]
async fn test_mixed_batching() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create JSON stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // Append single value
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"{"a":1}"#)
        .send()
        .await
        .unwrap();

    // Append array
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"[{"b":2},{"c":3}]"#)
        .send()
        .await
        .unwrap();

    // Append another single value
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"{"d":4}"#)
        .send()
        .await
        .unwrap();

    // Read should concatenate all messages
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, r#"[{"a":1},{"b":2},{"c":3},{"d":4}]"#);
}

/// Validates spec: 06-json-mode.md#non-json-content-types
///
/// Non-JSON streams do not perform flattening/wrapping.
#[tokio::test]
async fn test_non_json_no_flattening() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create text/plain stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    // Append JSON-looking text
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body(r#"[{"a":1},{"b":2}]"#)
        .send()
        .await
        .unwrap();

    // Read should return literal string (no flattening)
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, r#"[{"a":1},{"b":2}]"#); // Literal, no wrapping in extra array
}

/// Validates spec: 06-json-mode.md#charset-parameter
///
/// Charset parameter is ignored (normalized away).
#[tokio::test]
async fn test_json_charset_ignored() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create JSON stream with charset
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .send()
        .await
        .unwrap();

    // Append with different charset format (should work)
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"{"test":true}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204);

    // Read should work
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, r#"[{"test":true}]"#);
}

/// Validates atomicity for JSON array appends when limits are exceeded.
#[tokio::test]
async fn test_json_array_append_is_atomic_on_limit_error() {
    // Set a very small stream limit so the batch fails as a whole.
    let (base_url, _port) = spawn_test_server_with_limits(1024 * 1024, 20).await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create JSON stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // First item is small, second item pushes the batch over the limit.
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"["12345","12345678901234567890"]"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 413);

    // Batch failure must not partially commit anything.
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, "[]");
}
