mod common;

use common::{
    spawn_test_server, spawn_test_server_with_timeout, test_client, test_client_with_timeout,
    unique_stream_name,
};

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// When data exists at the requested offset, long-poll returns
/// 200 immediately with the data (same as catch-up).
#[tokio::test]
async fn test_long_poll_returns_immediately_when_data_exists() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream and append data
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("hello")
        .send()
        .await
        .unwrap();

    // Long-poll with data available should return 200 immediately
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=long-poll"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.text().await.unwrap();
    assert_eq!(body, "hello");
}

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// Long-poll catches up first: if client is behind tail, returns
/// historical data immediately instead of waiting.
#[tokio::test]
async fn test_long_poll_catches_up_first() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream and append multiple messages
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("first")
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("second")
        .send()
        .await
        .unwrap();

    // Long-poll from start should return all data immediately
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=long-poll"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.text().await.unwrap();
    assert_eq!(body, "firstsecond");
}

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// Long-poll waits for new data and returns 200 when it arrives.
#[tokio::test]
async fn test_long_poll_waits_and_returns_on_new_data() {
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

    let url = format!("{base_url}/v1/stream/{stream_name}");
    let poll_url = format!("{url}?offset=-1&live=long-poll");
    let append_url = url.clone();

    // Start long-poll in background
    let poll_handle = tokio::spawn({
        let client = test_client();
        async move { client.get(&poll_url).send().await.unwrap() }
    });

    // Wait a moment then append data
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    client
        .post(&append_url)
        .header("Content-Type", "text/plain")
        .body("arrived")
        .send()
        .await
        .unwrap();

    // Long-poll should return with the new data
    let response = poll_handle.await.unwrap();
    assert_eq!(response.status(), 200);

    let body = response.text().await.unwrap();
    assert_eq!(body, "arrived");
}

/// Validates spec: 03-read-modes.md#timeout
///
/// Long-poll returns 204 when timeout expires with no new data.
#[tokio::test]
async fn test_long_poll_timeout_returns_204() {
    let (base_url, _port) = spawn_test_server_with_timeout(std::time::Duration::from_secs(1)).await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create empty stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    // Long-poll should timeout and return 204
    let start = std::time::Instant::now();
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=long-poll"
        ))
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(response.status(), 204, "Expected 204 No Content on timeout");
    assert!(
        elapsed >= std::time::Duration::from_millis(900),
        "Should wait at least ~1s, waited {elapsed:?}"
    );
}

/// Validates spec: 03-read-modes.md#closed-stream-at-tail
///
/// When stream is closed and client is at tail, long-poll MUST
/// immediately return 204 with Stream-Closed: true (no waiting).
#[tokio::test]
async fn test_long_poll_closed_stream_at_tail_returns_204_immediately() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream, append, and close
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .body("final")
        .send()
        .await
        .unwrap();

    // Get the next offset (tail position)
    let read_response = client
        .get(format!("{base_url}/v1/stream/{stream_name}?offset=-1"))
        .send()
        .await
        .unwrap();

    let next_offset = read_response
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Consume the body to avoid connection issues
    let _ = read_response.text().await.unwrap();

    // Long-poll from tail of closed stream should return immediately
    let start = std::time::Instant::now();
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset={next_offset}&live=long-poll"
        ))
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(response.status(), 204, "Expected 204 No Content");
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "Should return immediately for closed stream, took {elapsed:?}"
    );

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed header")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");
}

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// Long-poll on non-existent stream returns 404.
#[tokio::test]
async fn test_long_poll_nonexistent_stream_returns_404() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?live=long-poll&offset=-1"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// Invalid `live` parameter value returns 400.
#[tokio::test]
async fn test_long_poll_invalid_live_param_returns_400() {
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
        .get(format!("{base_url}/v1/stream/{stream_name}?live=invalid"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400, "Expected 400 for invalid live mode");
}

/// Validates spec: 03-read-modes.md#stream-cursor
///
/// Long-poll responses include Stream-Cursor header.
#[tokio::test]
async fn test_long_poll_includes_stream_cursor() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream and append data
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

    // Long-poll should include Stream-Cursor
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=long-poll"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let cursor = response
        .headers()
        .get("Stream-Cursor")
        .expect("Missing Stream-Cursor header on long-poll response")
        .to_str()
        .unwrap();

    // Cursor should be non-empty and digits-only (opaque monotonic counter)
    assert!(!cursor.is_empty(), "Stream-Cursor should not be empty");
    assert!(
        cursor.chars().all(|c| c.is_ascii_digit()),
        "Stream-Cursor should be digits only, got: {cursor}"
    );
}

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// 204 response includes correct headers: Stream-Next-Offset,
/// Stream-Up-To-Date, and Stream-Cursor.
#[tokio::test]
async fn test_long_poll_204_includes_correct_headers() {
    let (base_url, _port) = spawn_test_server_with_timeout(std::time::Duration::from_secs(1)).await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with some data
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("existing")
        .send()
        .await
        .unwrap();

    // Get next offset (tail position)
    let read_response = client
        .get(format!("{base_url}/v1/stream/{stream_name}?offset=-1"))
        .send()
        .await
        .unwrap();

    let next_offset = read_response
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let _ = read_response.text().await.unwrap();

    // Long-poll from tail, wait for timeout
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset={next_offset}&live=long-poll"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204);

    // Verify required headers
    let resp_next_offset = response
        .headers()
        .get("Stream-Next-Offset")
        .expect("204 must include Stream-Next-Offset")
        .to_str()
        .unwrap();
    assert_eq!(resp_next_offset, next_offset);

    let up_to_date = response
        .headers()
        .get("Stream-Up-To-Date")
        .expect("204 must include Stream-Up-To-Date")
        .to_str()
        .unwrap();
    assert_eq!(up_to_date, "true");

    let cursor = response
        .headers()
        .get("Stream-Cursor")
        .expect("204 must include Stream-Cursor")
        .to_str()
        .unwrap();
    assert!(!cursor.is_empty());

    // Stream-Closed should NOT be present (stream is open)
    assert!(
        response.headers().get("Stream-Closed").is_none(),
        "Stream-Closed should not be present on open stream"
    );
}

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// Long-poll response includes all standard read headers (`ETag`, etc.)
/// when returning data.
#[tokio::test]
async fn test_long_poll_200_includes_all_read_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream and append data
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

    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=long-poll"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    // All standard read headers must be present
    assert!(
        response.headers().get("Content-Type").is_some(),
        "Missing Content-Type"
    );
    assert!(
        response.headers().get("Stream-Next-Offset").is_some(),
        "Missing Stream-Next-Offset"
    );
    assert!(
        response.headers().get("Stream-Up-To-Date").is_some(),
        "Missing Stream-Up-To-Date"
    );
    assert!(response.headers().get("ETag").is_some(), "Missing ETag");
    assert!(
        response.headers().get("Stream-Cursor").is_some(),
        "Missing Stream-Cursor"
    );
    assert!(
        response.headers().get("Cache-Control").is_some(),
        "Missing Cache-Control"
    );
}

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// Long-poll wakes up and returns 204 with Stream-Closed when
/// stream is closed mid-wait.
#[tokio::test]
async fn test_long_poll_wakes_on_stream_close() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client_with_timeout(10);
    let stream_name = unique_stream_name();

    // Create stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let url = format!("{base_url}/v1/stream/{stream_name}");

    // Start long-poll in background
    let poll_handle = tokio::spawn({
        let poll_url = format!("{url}?offset=-1&live=long-poll");
        let client = test_client_with_timeout(10);
        async move { client.get(&poll_url).send().await.unwrap() }
    });

    // Wait a moment then close the stream
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    client
        .post(&url)
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .body("")
        .send()
        .await
        .unwrap();

    // Long-poll should wake up and return 204 with Stream-Closed
    let response = poll_handle.await.unwrap();
    assert_eq!(response.status(), 204);

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Should include Stream-Closed after close event")
        .to_str()
        .unwrap();
    assert_eq!(closed, "true");
}

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// Regression: long-poll from offset=now must deliver data that arrives
/// during the wait. The sentinel `now` resolves to the current tail on
/// the initial read; re-reads after wake must use the resolved concrete
/// offset, not the raw sentinel (which would always return empty).
#[tokio::test]
async fn test_long_poll_from_now_sentinel_delivers_data() {
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

    let url = format!("{base_url}/v1/stream/{stream_name}");

    // Start long-poll from `now` in background
    let poll_handle = tokio::spawn({
        let poll_url = format!("{url}?offset=now&live=long-poll");
        let client = test_client();
        async move { client.get(&poll_url).send().await.unwrap() }
    });

    // Wait a moment then append data
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    client
        .post(&url)
        .header("Content-Type", "text/plain")
        .body("after-now")
        .send()
        .await
        .unwrap();

    // Long-poll should wake and return 200 with the new data
    let response = poll_handle.await.unwrap();
    assert_eq!(
        response.status(),
        200,
        "Long-poll from now should return 200 when data arrives"
    );

    let body = response.text().await.unwrap();
    assert_eq!(body, "after-now");
}

/// Validates spec: 03-read-modes.md#long-poll-mode
///
/// Catch-up mode (no live param) is unaffected by long-poll changes;
/// it does NOT include Stream-Cursor header.
#[tokio::test]
async fn test_catch_up_mode_unchanged() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream and append data
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

    // Regular catch-up GET (no live param)
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}?offset=-1"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    // Catch-up mode should NOT have Stream-Cursor
    assert!(
        response.headers().get("Stream-Cursor").is_none(),
        "Catch-up mode should not include Stream-Cursor"
    );

    let body = response.text().await.unwrap();
    assert_eq!(body, "data");
}
