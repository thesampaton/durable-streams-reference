mod common;

use common::{spawn_test_server, test_client, unique_stream_name};

/// Helper: create a stream and return its name
async fn setup_stream(base_url: &str, client: &reqwest::Client) -> String {
    let name = unique_stream_name();
    client
        .put(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    name
}

/// Validates spec: 08-stream-closure.md#close-without-data
///
/// POST close-only (empty body + Stream-Closed: true) returns 204
/// with Stream-Closed: true header.
#[tokio::test]
async fn test_close_response_includes_stream_closed_header() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204);

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed header on close response")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");

    assert!(
        response.headers().get("Stream-Next-Offset").is_some(),
        "Missing Stream-Next-Offset on close response"
    );
}

/// Validates spec: 08-stream-closure.md#close-with-data
///
/// POST close-with-data returns 204 with Stream-Closed: true header.
#[tokio::test]
async fn test_close_with_data_response_includes_stream_closed_header() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .body("final message")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204);

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed header on close-with-data response")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");

    // Next offset should reflect the appended message (13 bytes)
    let next_offset = response
        .headers()
        .get("Stream-Next-Offset")
        .expect("Missing Stream-Next-Offset")
        .to_str()
        .unwrap();
    assert_eq!(next_offset, "0000000000000001_000000000000000d");
}

/// Validates spec: 08-stream-closure.md#idempotent-close
///
/// Closing an already-closed stream returns 204 with Stream-Closed: true.
#[tokio::test]
async fn test_idempotent_close_returns_204_with_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // First close
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    // Second close (idempotent)
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204);

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed on idempotent close")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");

    assert!(response.headers().get("Stream-Next-Offset").is_some());
}

/// Validates spec: 08-stream-closure.md#create-closed
///
/// PUT with Stream-Closed: true returns Stream-Closed: true in the
/// 201 response.
#[tokio::test]
async fn test_put_created_closed_response_includes_stream_closed() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = unique_stream_name();

    let response = client
        .put(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed on PUT create-closed response")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");
}

/// Validates spec: 08-stream-closure.md#create-closed
///
/// Idempotent PUT on a closed stream returns 200 with Stream-Closed: true.
#[tokio::test]
async fn test_put_idempotent_recreate_closed_includes_header() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = unique_stream_name();

    // Create closed
    client
        .put(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    // Idempotent recreate
    let response = client
        .put(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed on idempotent PUT of closed stream")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");
}

/// Validates spec: 08-stream-closure.md#error-precedence
///
/// Appending to a closed stream returns 409 with both Stream-Closed
/// and Stream-Next-Offset.
#[tokio::test]
async fn test_closed_stream_reject_includes_next_offset() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Append some data
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .body("data")
        .send()
        .await
        .unwrap();

    // Close
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    // Attempt append to closed stream
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .body("rejected")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 409);

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed on 409 response")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");

    let next_offset = response
        .headers()
        .get("Stream-Next-Offset")
        .expect("Missing Stream-Next-Offset on 409 response")
        .to_str()
        .unwrap();
    // After appending "data" (4 bytes), next_offset should be at seq 1
    assert_eq!(next_offset, "0000000000000001_0000000000000004");
}

/// Validates spec: 08-stream-closure.md#requirements
///
/// Stream-Closed: false (non-"true" value) does not close the stream.
#[tokio::test]
async fn test_close_with_non_true_value_ignored() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Append with Stream-Closed: false
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "false")
        .body("still open")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204);

    // Stream should NOT have Stream-Closed header in response
    assert!(
        response.headers().get("Stream-Closed").is_none(),
        "Stream-Closed should not be present when value is 'false'"
    );

    // Verify stream is still open by appending more data
    let response2 = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .body("more data")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response2.status(),
        204,
        "Stream should still accept appends"
    );
}

/// Validates spec: 08-stream-closure.md#read-mode-behavior
///
/// Reading an open stream with data returns Stream-Up-To-Date: true
/// but does NOT include Stream-Closed, verifying the header is only
/// emitted when the stream is actually closed.
///
/// Note: The server returns all messages from the requested offset to
/// the end of the stream in a single response — pagination is the
/// client's responsibility (resume from Stream-Next-Offset). Because
/// of this, `at_tail` is always true for data reads, so a true
/// "mid-stream on closed" scenario (`at_tail=false`, `closed=true`) cannot
/// occur at the HTTP layer. This test covers the `closed` guard
/// dimension instead: `at_tail=true`, `closed=false` → no Stream-Closed.
#[tokio::test]
async fn test_read_open_stream_omits_closed_header() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Append messages but do NOT close
    for msg in &["first", "second", "third"] {
        client
            .post(format!("{base_url}/v1/stream/{name}"))
            .header("Content-Type", "text/plain")
            .body(*msg)
            .send()
            .await
            .unwrap();
    }

    // Read all messages — at_tail=true, closed=false
    let response = client
        .get(format!("{base_url}/v1/stream/{name}?offset=-1"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let up_to_date = response
        .headers()
        .get("Stream-Up-To-Date")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(up_to_date, "true", "Should be at tail after reading all");

    assert!(
        response.headers().get("Stream-Closed").is_none(),
        "Stream-Closed must not be present on an open stream"
    );
}

/// Validates spec: 08-stream-closure.md#read-mode-behavior
///
/// Reading a closed stream at the tail includes Stream-Closed: true.
#[tokio::test]
async fn test_read_at_tail_includes_closed_header() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Append a message
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .body("hello")
        .send()
        .await
        .unwrap();

    // Close the stream
    let close_response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    let final_offset = close_response
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Read from the final offset (at tail, no messages ahead)
    let response = client
        .get(format!("{base_url}/v1/stream/{name}?offset={final_offset}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let up_to_date = response
        .headers()
        .get("Stream-Up-To-Date")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(up_to_date, "true");

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed at tail of closed stream")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");
}
