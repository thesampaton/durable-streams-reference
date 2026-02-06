mod common;

use common::{spawn_test_server, test_client, unique_stream_name};

/// Validates spec: 07-caching-etag.md#format
///
/// Verifies ETag format with start sentinel (-1) offset.
#[tokio::test]
async fn test_etag_format_start_sentinel() {
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

    // Read with default offset (-1)
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let etag = response
        .headers()
        .get("etag")
        .expect("Missing ETag header")
        .to_str()
        .unwrap();

    // ETag should be "-1:{next_offset}"
    assert!(
        etag.starts_with("\"-1:"),
        "ETag should start with \"-1: but was {etag}"
    );
    assert!(etag.ends_with('"'), "ETag should end with quote");
    assert!(
        !etag.ends_with(":c\""),
        "Open stream should not have :c suffix"
    );
}

/// Validates spec: 07-caching-etag.md#format
///
/// Verifies ETag format with now sentinel offset.
#[tokio::test]
async fn test_etag_format_now_sentinel() {
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

    // Read with now sentinel
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}?offset=now"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let etag = response
        .headers()
        .get("etag")
        .expect("Missing ETag header")
        .to_str()
        .unwrap();

    // ETag should start with "now:
    assert!(
        etag.starts_with("\"now:"),
        "ETag should start with \"now: but was {etag}"
    );
}

/// Validates spec: 07-caching-etag.md#format
///
/// Verifies ETag format with specific hex offset.
#[tokio::test]
async fn test_etag_format_specific_offset() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream and append two messages
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

    // Read from specific offset (after "first": seq=1, bytes=5)
    let offset = "0000000000000001_0000000000000005";
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset={offset}"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let etag = response
        .headers()
        .get("etag")
        .expect("Missing ETag header")
        .to_str()
        .unwrap();

    // ETag should be "{offset}:{next_offset}"
    assert!(
        etag.starts_with(&format!("\"{offset}:")),
        "ETag start should be the requested offset, got {etag}"
    );

    let body = response.text().await.unwrap();
    assert_eq!(body, "second");
}

/// Validates spec: 07-caching-etag.md#format
///
/// Verifies ETag includes :c suffix for closed stream at tail.
#[tokio::test]
async fn test_etag_closed_stream_at_tail() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create, append, and close
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

    // Read from start — should be at tail of closed stream
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let etag = response.headers().get("etag").unwrap().to_str().unwrap();

    assert!(
        etag.ends_with(":c\""),
        "ETag should have :c suffix for closed stream at tail, got {etag}"
    );
}

/// Validates spec: 07-caching-etag.md#if-none-match
///
/// Verifies that matching If-None-Match returns 304 Not Modified.
#[tokio::test]
async fn test_if_none_match_matching_returns_304() {
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

    // First read — capture ETag
    let response1 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let etag = response1
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Second read with If-None-Match
    let response2 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .header("If-None-Match", &etag)
        .send()
        .await
        .unwrap();

    assert_eq!(response2.status(), 304, "Expected 304 Not Modified");
}

/// Validates spec: 07-caching-etag.md#if-none-match
///
/// Verifies that 304 response includes required metadata headers.
#[tokio::test]
async fn test_304_includes_metadata_headers() {
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

    // Get ETag
    let response1 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let etag = response1
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let next_offset = response1
        .headers()
        .get("Stream-Next-Offset")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // 304 response
    let response2 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .header("If-None-Match", &etag)
        .send()
        .await
        .unwrap();

    assert_eq!(response2.status(), 304);

    // Must include Stream-Next-Offset
    let r_next_offset = response2
        .headers()
        .get("Stream-Next-Offset")
        .expect("304 must include Stream-Next-Offset")
        .to_str()
        .unwrap();
    assert_eq!(r_next_offset, next_offset);

    // Must include Stream-Up-To-Date
    let up_to_date = response2
        .headers()
        .get("Stream-Up-To-Date")
        .expect("304 must include Stream-Up-To-Date")
        .to_str()
        .unwrap();
    assert_eq!(up_to_date, "true");

    // Must include Cache-Control: no-store
    let cache_control = response2
        .headers()
        .get("cache-control")
        .expect("304 must include Cache-Control")
        .to_str()
        .unwrap();
    assert_eq!(cache_control, "no-store");
}

/// Validates spec: 07-caching-etag.md#if-none-match
///
/// Verifies that 304 response has no body.
#[tokio::test]
async fn test_304_has_no_body() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

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

    let response1 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let etag = response1
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let response2 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .header("If-None-Match", &etag)
        .send()
        .await
        .unwrap();

    assert_eq!(response2.status(), 304);
    let body = response2.text().await.unwrap();
    assert!(body.is_empty(), "304 response must have no body");
}

/// Validates spec: 07-caching-etag.md#if-none-match
///
/// Verifies that non-matching If-None-Match returns 200 with data.
#[tokio::test]
async fn test_if_none_match_non_matching_returns_200() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

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

    // Send a fabricated ETag that won't match
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .header("If-None-Match", "\"bogus:etag\"")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        200,
        "Non-matching ETag should return 200"
    );
    let body = response.text().await.unwrap();
    assert_eq!(body, "data");
}

/// Validates spec: 07-caching-etag.md#if-none-match
///
/// Verifies that stale ETag returns 200 when new data has been appended.
#[tokio::test]
async fn test_stale_etag_returns_200_after_append() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

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

    // Capture ETag
    let response1 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let old_etag = response1
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Append more data — ETag should now differ
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("second")
        .send()
        .await
        .unwrap();

    // Send old ETag — should get 200 with all data
    let response2 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .header("If-None-Match", &old_etag)
        .send()
        .await
        .unwrap();

    assert_eq!(
        response2.status(),
        200,
        "Stale ETag should return 200 with new data"
    );

    let new_etag = response2
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    assert_ne!(old_etag, new_etag, "ETag should change after new data");

    let body = response2.text().await.unwrap();
    assert_eq!(body, "firstsecond");
}

/// Validates spec: 07-caching-etag.md#cache-control
///
/// Verifies Cache-Control: no-store on 200 GET response.
#[tokio::test]
async fn test_cache_control_on_200() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let cache_control = response
        .headers()
        .get("cache-control")
        .expect("200 GET must include Cache-Control")
        .to_str()
        .unwrap();
    assert_eq!(cache_control, "no-store");
}

/// Validates spec: 07-caching-etag.md#cache-control
///
/// Verifies Cache-Control: no-store on error responses.
#[tokio::test]
async fn test_cache_control_on_error() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // 404 for non-existent stream
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
    let cache_control = response
        .headers()
        .get("cache-control")
        .expect("Error responses must include Cache-Control")
        .to_str()
        .unwrap();
    assert_eq!(cache_control, "no-store");
}

/// Validates spec: 07-caching-etag.md#format
///
/// Verifies ETag changes when new data is appended.
#[tokio::test]
async fn test_etag_changes_after_append() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("msg1")
        .send()
        .await
        .unwrap();

    let response1 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();
    let etag1 = response1
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Append more data
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("msg2")
        .send()
        .await
        .unwrap();

    let response2 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();
    let etag2 = response2
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    assert_ne!(etag1, etag2, "ETag must change when stream content changes");
}

/// Validates spec: 07-caching-etag.md#if-none-match
///
/// Verifies 304 works correctly for closed stream with :c ETag.
#[tokio::test]
async fn test_304_with_closed_stream_etag() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

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

    // First read captures ETag with :c suffix
    let response1 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let etag = response1
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(etag.ends_with(":c\""), "Should have :c suffix");

    // Send same ETag back — should get 304
    let response2 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .header("If-None-Match", &etag)
        .send()
        .await
        .unwrap();

    assert_eq!(
        response2.status(),
        304,
        "Matching closed-stream ETag should return 304"
    );
}

/// Validates spec: 07-caching-etag.md#format
///
/// Verifies ETag on empty stream read (at tail with no messages).
#[tokio::test]
async fn test_etag_on_empty_stream() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let etag = response
        .headers()
        .get("etag")
        .expect("Empty stream GET must include ETag")
        .to_str()
        .unwrap();

    // Start and end offsets should be the same (no messages)
    assert!(
        etag.starts_with("\"-1:"),
        "ETag start should be -1 sentinel"
    );
}
