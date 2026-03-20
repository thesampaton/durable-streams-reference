mod common;

use common::{spawn_test_server, test_client, unique_stream_name};

/// Validates spec: 03-read-modes.md#catch-up-mode
///
/// Verifies that GET returns 200 with data and headers.
#[tokio::test]
async fn test_read_returns_200() {
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
        .body("Hello, world!")
        .send()
        .await
        .unwrap();

    // Read data
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200, "Expected 200 OK");

    // Check headers
    let content_type = response
        .headers()
        .get("content-type")
        .expect("Missing Content-Type header")
        .to_str()
        .unwrap();
    assert_eq!(content_type, "text/plain");

    let next_offset = response
        .headers()
        .get("Stream-Next-Offset")
        .expect("Missing Stream-Next-Offset header")
        .to_str()
        .unwrap();
    assert_eq!(next_offset, "0000000000000001_000000000000000d");

    let up_to_date = response
        .headers()
        .get("Stream-Up-To-Date")
        .expect("Missing Stream-Up-To-Date header")
        .to_str()
        .unwrap();
    assert_eq!(up_to_date, "true");

    let etag = response
        .headers()
        .get("etag")
        .expect("Missing ETag header")
        .to_str()
        .unwrap();
    assert_eq!(etag, "\"-1:0000000000000001_000000000000000d\"");

    let cache_control = response
        .headers()
        .get("cache-control")
        .expect("Missing Cache-Control header")
        .to_str()
        .unwrap();
    assert_eq!(cache_control, "no-store");

    // Check body
    let body = response.text().await.unwrap();
    assert_eq!(body, "Hello, world!");
}

/// Validates spec: 03-read-modes.md#catch-up-mode
///
/// Verifies that reading empty stream returns empty body.
#[tokio::test]
async fn test_read_empty_stream() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create empty stream
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    // Read empty stream
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.text().await.unwrap();
    assert!(body.is_empty(), "Empty stream should return empty body");
}

/// Validates spec: 03-read-modes.md#catch-up-mode
///
/// Verifies that multiple messages are concatenated.
#[tokio::test]
async fn test_read_multiple_messages_concatenated() {
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

    // Append multiple messages
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

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("third")
        .send()
        .await
        .unwrap();

    // Read all messages
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let body = response.text().await.unwrap();
    assert_eq!(body, "firstsecondthird", "Messages should be concatenated");
}

/// Validates spec: 03-read-modes.md#offset-sentinels
///
/// Verifies that offset=-1 reads from start.
#[tokio::test]
async fn test_read_from_start_sentinel() {
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

    // Read with explicit -1 sentinel
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}?offset=-1"))
        .send()
        .await
        .unwrap();

    let body = response.text().await.unwrap();
    assert_eq!(body, "data");
}

/// Validates spec: 03-read-modes.md#offset-sentinels
///
/// Verifies that offset=now reads from tail (empty).
#[tokio::test]
async fn test_read_from_now_sentinel() {
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

    // Read from tail (now)
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}?offset=now"))
        .send()
        .await
        .unwrap();

    let up_to_date = response
        .headers()
        .get("Stream-Up-To-Date")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let body = response.text().await.unwrap();
    assert!(
        body.is_empty(),
        "Reading from 'now' should return empty body"
    );
    assert_eq!(up_to_date, "true");
}

/// Validates spec: 03-read-modes.md#resumable-reads
///
/// Verifies that reads can resume from specific offset.
#[tokio::test]
async fn test_resumable_reads() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream and append messages
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

    // First read - get all data
    let response1 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let body1 = response1.text().await.unwrap();
    assert_eq!(body1, "firstsecond");

    // Append more data
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("third")
        .send()
        .await
        .unwrap();

    // Resume from offset after "second" (offset 0000000000000002_000000000000000b)
    let response2 = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=0000000000000002_000000000000000b"
        ))
        .send()
        .await
        .unwrap();

    let body2 = response2.text().await.unwrap();
    assert_eq!(
        body2, "third",
        "Should only return new data after resume offset"
    );
}

/// Validates spec: 03-read-modes.md#read-your-writes-consistency
///
/// Verifies that data is immediately readable after append.
#[tokio::test]
async fn test_read_your_writes() {
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
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("immediate")
        .send()
        .await
        .unwrap();

    // Immediately read - should see the data
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let body = response.text().await.unwrap();
    assert_eq!(body, "immediate", "Read-your-writes must be consistent");
}

/// Validates spec: 03-read-modes.md#stream-closed-semantics
///
/// Verifies that Stream-Closed header is present when at tail of closed stream.
#[tokio::test]
async fn test_read_closed_stream_at_tail() {
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

    // Append and close
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-Closed", "true")
        .body("final")
        .send()
        .await
        .unwrap();

    // Read from start - should get all data and see Stream-Closed
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let closed = response
        .headers()
        .get("Stream-Closed")
        .expect("Missing Stream-Closed header")
        .to_str()
        .unwrap()
        .to_string();

    let etag = response
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let body = response.text().await.unwrap();
    assert_eq!(body, "final");
    assert_eq!(closed, "true");
    assert!(
        etag.ends_with(":c\""),
        "ETag should have :c suffix for closed stream at tail"
    );
}

/// Validates spec: 03-read-modes.md#etag-and-caching
///
/// Verifies that If-None-Match returns 304 when `ETag` matches.
#[tokio::test]
async fn test_if_none_match_returns_304() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream and append
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

    // First read - get ETag
    let response1 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let etag = response1.headers().get("etag").unwrap().to_str().unwrap();

    // Second read with If-None-Match
    let response2 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .header("If-None-Match", etag)
        .send()
        .await
        .unwrap();

    assert_eq!(response2.status(), 304, "Expected 304 Not Modified");

    // Should still have Stream-Next-Offset and Stream-Up-To-Date
    assert!(response2.headers().get("Stream-Next-Offset").is_some());
    assert!(response2.headers().get("Stream-Up-To-Date").is_some());
}

/// Validates spec: 03-read-modes.md#catch-up-mode
///
/// Verifies that reading non-existent stream returns 404.
#[tokio::test]
async fn test_read_nonexistent_stream_returns_404() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404, "Expected 404 Not Found");
}

/// Validates spec: 03-read-modes.md#catch-up-mode
///
/// Verifies that invalid offset format returns 400.
#[tokio::test]
async fn test_invalid_offset_returns_400() {
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

    // Try to read with invalid offset
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}?offset=invalid"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400, "Expected 400 Bad Request");
}

/// Regression test: Verifies response headers match body snapshot under concurrent appends
///
/// This test prevents reintroduction of a bug where calling `storage.head()` after
/// `storage.read()` could return offsets from a newer snapshot than the body,
/// breaking resumable reads when a concurrent append lands between the calls.
#[tokio::test]
async fn test_response_headers_match_body_snapshot() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream and append initial data
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("message1")
        .send()
        .await
        .unwrap();

    // Read and capture response headers
    let response1 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let next_offset1 = response1
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let etag1 = response1
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let body1 = response1.text().await.unwrap();
    assert_eq!(body1, "message1");

    // Verify next_offset points after message1
    assert_eq!(next_offset1, "0000000000000001_0000000000000008");

    // Verify ETag reflects the range we actually read
    assert_eq!(etag1, "\"-1:0000000000000001_0000000000000008\"");

    // Append more data (simulating concurrent producer)
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("message2")
        .send()
        .await
        .unwrap();

    // Read again from start
    let response2 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let next_offset2 = response2
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let etag2 = response2
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let body2 = response2.text().await.unwrap();
    assert_eq!(body2, "message1message2");

    // Verify next_offset now points after message2
    assert_eq!(next_offset2, "0000000000000002_0000000000000010");

    // Verify new ETag reflects both messages
    assert_eq!(etag2, "\"-1:0000000000000002_0000000000000010\"");

    // Critical: Resume from first read's next_offset should give us only message2
    let response3 = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset={next_offset1}"
        ))
        .send()
        .await
        .unwrap();

    let body3 = response3.text().await.unwrap();
    assert_eq!(
        body3, "message2",
        "Resuming from first read's next_offset should return only new data, \
         proving the offset was consistent with the first read's body"
    );
}
