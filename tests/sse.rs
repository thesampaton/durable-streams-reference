mod common;

use common::{spawn_test_server, test_client_with_timeout, unique_stream_name};
use std::time::Duration;

/// A parsed SSE event with event type and data.
#[derive(Debug, Clone)]
struct SseEvent {
    event_type: String,
    data: String,
}

/// Parse SSE text into structured events.
///
/// Handles the SSE wire format: `event:` lines set the type,
/// `data:` lines are concatenated with newlines, and blank lines
/// delimit events. Comment lines (`:`) are ignored.
fn parse_sse_events(text: &str) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut current_type = String::new();
    let mut current_data = Vec::<String>::new();

    for line in text.lines() {
        if line.is_empty() {
            // Blank line = event boundary
            if !current_data.is_empty() || !current_type.is_empty() {
                events.push(SseEvent {
                    event_type: std::mem::take(&mut current_type),
                    data: current_data.join("\n"),
                });
                current_data.clear();
            }
        } else if let Some(value) = line.strip_prefix("event:") {
            current_type = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("data:") {
            // SSE spec: if there's a space after "data:", skip it
            let trimmed = if value.starts_with(' ') {
                &value[1..]
            } else {
                value
            };
            current_data.push(trimmed.to_string());
        } else if line.starts_with(':') {
            // Comment line (keep-alive), ignore
        }
    }

    // Flush trailing event (if stream ended without final blank line)
    if !current_data.is_empty() || !current_type.is_empty() {
        events.push(SseEvent {
            event_type: current_type,
            data: current_data.join("\n"),
        });
    }

    events
}

/// Helper to create a stream and optionally append data.
async fn setup_stream(base_url: &str, name: &str, content_type: &str, messages: &[&str]) {
    let client = reqwest::Client::new();
    client
        .put(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", content_type)
        .send()
        .await
        .unwrap();

    for msg in messages {
        client
            .post(format!("{base_url}/v1/stream/{name}"))
            .header("Content-Type", content_type)
            .body(msg.to_string())
            .send()
            .await
            .unwrap();
    }
}

/// Helper to close a stream via POST with `Stream-Closed: true`.
async fn close_stream(base_url: &str, name: &str, content_type: &str) {
    let client = reqwest::Client::new();
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", content_type)
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// SSE responses MUST use Content-Type: text/event-stream.
#[tokio::test]
async fn test_sse_content_type() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["hello"]).await;
    close_stream(&base_url, &stream_name, "text/plain").await;

    let client = test_client_with_timeout(5);
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let ct = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        ct.starts_with("text/event-stream"),
        "Expected text/event-stream, got {ct}"
    );
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// Each stored message produces an `event: data` event.
#[tokio::test]
async fn test_sse_data_events_for_stored_messages() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["msg1", "msg2"]).await;
    close_stream(&base_url, &stream_name, "text/plain").await;

    let client = test_client_with_timeout(5);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let events = parse_sse_events(&body);
    let data_events: Vec<_> = events.iter().filter(|e| e.event_type == "data").collect();
    assert_eq!(
        data_events.len(),
        2,
        "Expected 2 data events, got {}",
        data_events.len()
    );
    assert_eq!(data_events[0].data, "msg1");
    assert_eq!(data_events[1].data, "msg2");
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// Control event MUST include `streamNextOffset` after data events.
#[tokio::test]
async fn test_sse_control_includes_next_offset() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["hello"]).await;
    close_stream(&base_url, &stream_name, "text/plain").await;

    let client = test_client_with_timeout(5);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let events = parse_sse_events(&body);
    let control_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "control")
        .collect();
    assert!(
        !control_events.is_empty(),
        "Expected at least one control event"
    );

    let control_json: serde_json::Value = serde_json::from_str(&control_events[0].data).unwrap();
    assert!(
        control_json.get("streamNextOffset").is_some(),
        "Control event must include streamNextOffset"
    );
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// When the client is caught up, control event MUST include `upToDate: true`.
/// Uses an open stream so `upToDate` is the relevant field (not `streamClosed`).
#[tokio::test]
async fn test_sse_control_event_up_to_date() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["data"]).await;

    // Stream stays open. Close after a brief delay so SSE terminates.
    let base_url_clone = base_url.clone();
    let stream_name_clone = stream_name.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        close_stream(&base_url_clone, &stream_name_clone, "text/plain").await;
    });

    let client = test_client_with_timeout(5);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let events = parse_sse_events(&body);
    let control_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "control")
        .collect();
    assert!(!control_events.is_empty());

    // First control event is emitted when caught up at tail on open stream
    let first_control: serde_json::Value = serde_json::from_str(&control_events[0].data).unwrap();
    assert_eq!(
        first_control.get("upToDate"),
        Some(&serde_json::json!(true)),
        "Control event must include upToDate: true when caught up"
    );
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// SSE on non-existent stream returns 404 before streaming starts.
#[tokio::test]
async fn test_sse_nonexistent_stream_404() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();

    let client = test_client_with_timeout(5);
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// `offset=now` skips historical data; first control event includes
/// `streamNextOffset` at tail and `upToDate: true`.
#[tokio::test]
async fn test_sse_offset_now_skips_history() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["old_data"]).await;
    close_stream(&base_url, &stream_name, "text/plain").await;

    let client = test_client_with_timeout(5);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=now&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let events = parse_sse_events(&body);
    let data_events: Vec<_> = events.iter().filter(|e| e.event_type == "data").collect();
    // No historical data should be sent with offset=now
    assert_eq!(
        data_events.len(),
        0,
        "Expected no data events with offset=now"
    );

    let control_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "control")
        .collect();
    assert!(!control_events.is_empty(), "Expected control event");
    let control: serde_json::Value = serde_json::from_str(&control_events[0].data).unwrap();
    assert!(control.get("streamNextOffset").is_some());
    // Stream is closed, so we should get streamClosed
    assert_eq!(control.get("streamClosed"), Some(&serde_json::json!(true)));
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// SSE waits for new data appended after connection is established.
#[tokio::test]
async fn test_sse_waits_for_new_data() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &[]).await;

    let base_url_clone = base_url.clone();
    let stream_name_clone = stream_name.clone();

    // Spawn a task that appends data after a brief delay, then closes
    let append_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let client = reqwest::Client::new();
        client
            .post(format!("{base_url_clone}/v1/stream/{stream_name_clone}"))
            .header("Content-Type", "text/plain")
            .body("new_data")
            .send()
            .await
            .unwrap();

        // Close the stream so SSE connection terminates
        tokio::time::sleep(Duration::from_millis(100)).await;
        client
            .post(format!("{base_url_clone}/v1/stream/{stream_name_clone}"))
            .header("Content-Type", "text/plain")
            .header("Stream-Closed", "true")
            .send()
            .await
            .unwrap();
    });

    let client = test_client_with_timeout(10);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    append_handle.await.unwrap();

    let events = parse_sse_events(&body);
    let data_events: Vec<_> = events.iter().filter(|e| e.event_type == "data").collect();

    // Should have received the appended data
    assert!(
        data_events.iter().any(|e| e.data == "new_data"),
        "Expected to receive 'new_data' event, got: {data_events:?}"
    );
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// `offset=-1` starts from beginning and returns all historical data.
#[tokio::test]
async fn test_sse_offset_start_sentinel() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["first", "second"]).await;
    close_stream(&base_url, &stream_name, "text/plain").await;

    let client = test_client_with_timeout(5);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let events = parse_sse_events(&body);
    let data_events: Vec<_> = events.iter().filter(|e| e.event_type == "data").collect();
    assert_eq!(data_events.len(), 2);
    assert_eq!(data_events[0].data, "first");
    assert_eq!(data_events[1].data, "second");
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// SSE works with application/json content type streams.
/// JSON messages are sent as-is (not base64 encoded).
#[tokio::test]
async fn test_sse_json_content_type() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();

    let client = reqwest::Client::new();
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body(r#"{"key":"value"}"#)
        .send()
        .await
        .unwrap();

    close_stream(&base_url, &stream_name, "application/json").await;

    let reader = test_client_with_timeout(5);
    let response = reader
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap();

    // Should NOT have base64 encoding header
    assert!(
        response.headers().get("stream-sse-data-encoding").is_none(),
        "JSON streams should not have stream-sse-data-encoding header"
    );

    let body = response.text().await.unwrap();
    let events = parse_sse_events(&body);
    let data_events: Vec<_> = events.iter().filter(|e| e.event_type == "data").collect();
    assert!(!data_events.is_empty(), "Expected data events");

    // Data should be parseable as JSON array wrapping the original message
    let parsed: serde_json::Value = serde_json::from_str(&data_events[0].data).unwrap();
    assert!(
        parsed.is_array(),
        "SSE JSON data should be wrapped in array"
    );
    assert_eq!(parsed[0]["key"], "value");
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// SSE responses MUST include security headers: x-content-type-options: nosniff.
#[tokio::test]
async fn test_sse_security_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["data"]).await;
    close_stream(&base_url, &stream_name, "text/plain").await;

    let client = test_client_with_timeout(5);
    let response = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let nosniff = response
        .headers()
        .get("x-content-type-options")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(nosniff.as_deref(), Some("nosniff"));
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// When stream is closed, final control event includes `streamClosed: true`
/// and connection terminates.
#[tokio::test]
async fn test_sse_closed_stream() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["final_msg"]).await;
    close_stream(&base_url, &stream_name, "text/plain").await;

    let client = test_client_with_timeout(5);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let events = parse_sse_events(&body);
    let control_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "control")
        .collect();
    assert!(!control_events.is_empty());

    let last_control: serde_json::Value =
        serde_json::from_str(&control_events.last().unwrap().data).unwrap();
    assert_eq!(
        last_control.get("streamClosed"),
        Some(&serde_json::json!(true)),
        "Final control event must include streamClosed: true"
    );
    // streamCursor should be absent when closed
    assert!(
        last_control.get("streamCursor").is_none(),
        "streamCursor should be omitted when stream is closed"
    );
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// Binary streams (application/octet-stream) must include
/// `stream-sse-data-encoding: base64` header and base64-encode data.
#[tokio::test]
async fn test_sse_binary_base64_encoding() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();

    let client = reqwest::Client::new();
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/octet-stream")
        .send()
        .await
        .unwrap();

    // Append raw binary data
    let binary_data = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06];
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/octet-stream")
        .body(binary_data.clone())
        .send()
        .await
        .unwrap();

    // Close stream
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/octet-stream")
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();

    let reader = test_client_with_timeout(5);
    let response = reader
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap();

    // Must have the encoding header
    let encoding = response
        .headers()
        .get("stream-sse-data-encoding")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(encoding.as_deref(), Some("base64"));

    let body = response.text().await.unwrap();
    let events = parse_sse_events(&body);
    let data_events: Vec<_> = events.iter().filter(|e| e.event_type == "data").collect();
    assert!(!data_events.is_empty(), "Expected data events");

    // Decode and verify
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&data_events[0].data)
        .expect("Data should be valid base64");
    assert_eq!(decoded, binary_data);
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// Data containing newlines must be correctly split across multiple `data:` lines
/// per the SSE wire format. The SSE parser must reconstruct the original data.
#[tokio::test]
async fn test_sse_newlines_in_data() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(
        &base_url,
        &stream_name,
        "text/plain",
        &["line1\nline2\nline3"],
    )
    .await;
    close_stream(&base_url, &stream_name, "text/plain").await;

    let client = test_client_with_timeout(5);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let events = parse_sse_events(&body);
    let data_events: Vec<_> = events.iter().filter(|e| e.event_type == "data").collect();
    assert_eq!(data_events.len(), 1, "Expected 1 data event");
    assert_eq!(
        data_events[0].data, "line1\nline2\nline3",
        "Newlines in data must be preserved via multi-line data: fields"
    );
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// `offset=now` on an open stream should skip history and wait for new data.
/// When data arrives, it is emitted as a data event.
#[tokio::test]
async fn test_sse_offset_now_open_stream_waits() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["old_data"]).await;

    let base_url_clone = base_url.clone();
    let stream_name_clone = stream_name.clone();

    // Append new data + close after delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let client = reqwest::Client::new();
        client
            .post(format!("{base_url_clone}/v1/stream/{stream_name_clone}"))
            .header("Content-Type", "text/plain")
            .body("new_data")
            .send()
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        close_stream(&base_url_clone, &stream_name_clone, "text/plain").await;
    });

    let client = test_client_with_timeout(10);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=now&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let events = parse_sse_events(&body);
    let data_events: Vec<_> = events.iter().filter(|e| e.event_type == "data").collect();

    // Should NOT have old_data (offset=now skips history)
    assert!(
        !data_events.iter().any(|e| e.data == "old_data"),
        "offset=now should skip historical data"
    );
    // Should have new_data (appended after SSE started)
    assert!(
        data_events.iter().any(|e| e.data == "new_data"),
        "Expected to receive 'new_data', got: {data_events:?}"
    );
}

/// Validates spec: 03-read-modes.md#sse-mode (PROTOCOL.md §5.8)
///
/// Control events include `streamCursor` when stream is open.
#[tokio::test]
async fn test_sse_control_includes_cursor_when_open() {
    let (base_url, _port) = spawn_test_server().await;
    let stream_name = unique_stream_name();
    setup_stream(&base_url, &stream_name, "text/plain", &["data"]).await;

    // Don't close — stream stays open. We need it to close eventually
    // so the SSE connection terminates. Close after a brief delay.
    let base_url_clone = base_url.clone();
    let stream_name_clone = stream_name.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        close_stream(&base_url_clone, &stream_name_clone, "text/plain").await;
    });

    let client = test_client_with_timeout(5);
    let body = client
        .get(format!(
            "{base_url}/v1/stream/{stream_name}?offset=-1&live=sse"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let events = parse_sse_events(&body);
    let control_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "control")
        .collect();
    assert!(
        control_events.len() >= 2,
        "Expected at least 2 control events (initial + close)"
    );

    // First control event should have streamCursor (stream was open)
    let first_control: serde_json::Value = serde_json::from_str(&control_events[0].data).unwrap();
    assert!(
        first_control.get("streamCursor").is_some(),
        "First control event should include streamCursor when stream is open"
    );
    assert_eq!(
        first_control.get("upToDate"),
        Some(&serde_json::json!(true)),
        "First control should have upToDate when at tail"
    );

    // Last control event should have streamClosed and no cursor
    let last_control: serde_json::Value =
        serde_json::from_str(&control_events.last().unwrap().data).unwrap();
    assert_eq!(
        last_control.get("streamClosed"),
        Some(&serde_json::json!(true))
    );
    assert!(last_control.get("streamCursor").is_none());
}
