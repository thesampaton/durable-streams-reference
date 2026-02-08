mod common;

use common::{spawn_test_server, test_client, unique_stream_name};

/// Helper: create a stream and return its URL
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

/// Validates spec: 05-producer-sequencing.md#bootstrap-and-restart-flows
///
/// Verifies basic producer append returns 200 with echoed headers.
#[tokio::test]
async fn test_producer_basic_append_returns_200() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("hello")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200, "Producer append should return 200");

    // Echoed producer headers
    let epoch = response
        .headers()
        .get("Producer-Epoch")
        .expect("Missing Producer-Epoch")
        .to_str()
        .unwrap();
    assert_eq!(epoch, "0");

    let seq = response
        .headers()
        .get("Producer-Seq")
        .expect("Missing Producer-Seq")
        .to_str()
        .unwrap();
    assert_eq!(seq, "0");

    // Stream-Next-Offset should be present
    assert!(response.headers().get("Stream-Next-Offset").is_some());

    // Cache-Control should be present
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

/// Validates spec: 05-producer-sequencing.md#sequence-validation
///
/// Verifies sequential appends (seq 0, 1, 2) all return 200.
#[tokio::test]
async fn test_producer_sequential_appends() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    for seq in 0..3 {
        let response = client
            .post(format!("{base_url}/v1/stream/{name}"))
            .header("Content-Type", "text/plain")
            .header("Producer-Id", "prod-1")
            .header("Producer-Epoch", "0")
            .header("Producer-Seq", seq.to_string())
            .body(format!("msg{seq}"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200, "seq {seq} should return 200");

        let resp_seq = response
            .headers()
            .get("Producer-Seq")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(resp_seq, seq.to_string());
    }

    // Verify all data was appended
    let read = client
        .get(format!("{base_url}/v1/stream/{name}"))
        .send()
        .await
        .unwrap();
    let body = read.text().await.unwrap();
    assert_eq!(body, "msg0msg1msg2");
}

/// Validates spec: 05-producer-sequencing.md#sequence-validation
///
/// Verifies duplicate detection returns 204 No Content.
#[tokio::test]
async fn test_producer_duplicate_returns_204() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // First append
    let response1 = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(response1.status(), 200);

    // Duplicate
    let response2 = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(response2.status(), 204, "Duplicate should return 204");

    // Producer headers still present on 204
    assert!(response2.headers().get("Producer-Epoch").is_some());
    assert!(response2.headers().get("Producer-Seq").is_some());

    // Data should appear only once
    let read = client
        .get(format!("{base_url}/v1/stream/{name}"))
        .send()
        .await
        .unwrap();
    let body = read.text().await.unwrap();
    assert_eq!(body, "hello");
}

/// Validates spec: 05-producer-sequencing.md#sequence-validation
///
/// Verifies sequence gap returns 409 with expected/received headers.
#[tokio::test]
async fn test_producer_sequence_gap_returns_409() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // seq 0
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("msg0")
        .send()
        .await
        .unwrap();

    // Skip to seq 5 (gap)
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "5")
        .body("msg5")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 409, "Sequence gap should return 409");

    let expected = response
        .headers()
        .get("Producer-Expected-Seq")
        .expect("Missing Producer-Expected-Seq")
        .to_str()
        .unwrap();
    assert_eq!(expected, "1");

    let received = response
        .headers()
        .get("Producer-Received-Seq")
        .expect("Missing Producer-Received-Seq")
        .to_str()
        .unwrap();
    assert_eq!(received, "5");
}

/// Validates spec: 05-producer-sequencing.md#epoch-validation
///
/// Verifies epoch fencing returns 403 with current epoch.
#[tokio::test]
async fn test_producer_epoch_fencing_returns_403() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Establish epoch 2
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "2")
        .header("Producer-Seq", "0")
        .body("msg")
        .send()
        .await
        .unwrap();

    // Try with stale epoch 0
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("zombie")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 403, "Stale epoch should return 403");

    let epoch = response
        .headers()
        .get("Producer-Epoch")
        .expect("Missing Producer-Epoch on 403")
        .to_str()
        .unwrap();
    assert_eq!(epoch, "2", "Should return server's current epoch");
}

/// Validates spec: 05-producer-sequencing.md#epoch-validation
///
/// Verifies epoch bump resets sequence and returns 200.
#[tokio::test]
async fn test_producer_epoch_bump_resets_seq() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Epoch 0, seq 0..2
    for seq in 0..3 {
        client
            .post(format!("{base_url}/v1/stream/{name}"))
            .header("Content-Type", "text/plain")
            .header("Producer-Id", "prod-1")
            .header("Producer-Epoch", "0")
            .header("Producer-Seq", seq.to_string())
            .body(format!("e0s{seq}"))
            .send()
            .await
            .unwrap();
    }

    // Epoch 1, seq 0
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "1")
        .header("Producer-Seq", "0")
        .body("e1s0")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("Producer-Epoch")
            .unwrap()
            .to_str()
            .unwrap(),
        "1"
    );
}

/// Validates spec: 05-producer-sequencing.md#epoch-validation
///
/// Verifies epoch bump with seq != 0 returns 400.
#[tokio::test]
async fn test_producer_epoch_bump_nonzero_seq_returns_400() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Establish epoch 0
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("msg")
        .send()
        .await
        .unwrap();

    // Try epoch 1 with seq 5 (must start at 0)
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "1")
        .header("Producer-Seq", "5")
        .body("bad")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
}

/// Validates spec: 05-producer-sequencing.md#header-validation
///
/// Verifies partial producer headers return 400.
#[tokio::test]
async fn test_producer_partial_headers_returns_400() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Only Producer-Id (missing epoch and seq)
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .body("msg")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
}

/// Validates spec: 05-producer-sequencing.md#header-validation
///
/// Verifies empty Producer-Id returns 400.
#[tokio::test]
async fn test_producer_empty_id_returns_400() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("msg")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
}

/// Validates spec: 05-producer-sequencing.md#header-validation
///
/// Verifies non-integer epoch returns 400.
#[tokio::test]
async fn test_producer_non_integer_epoch_returns_400() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "abc")
        .header("Producer-Seq", "0")
        .body("msg")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
}

/// Validates spec: 05-producer-sequencing.md#concurrency
///
/// Verifies multiple independent producers to the same stream.
#[tokio::test]
async fn test_producer_multiple_producers_independent() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Producer A seq 0
    let r1 = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "producer-a")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("a0")
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);

    // Producer B seq 0 (independent sequence)
    let r2 = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "producer-b")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("b0")
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);

    // Producer A seq 1
    let r3 = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "producer-a")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "1")
        .body("a1")
        .send()
        .await
        .unwrap();
    assert_eq!(r3.status(), 200);

    // Verify all 3 messages
    let read = client
        .get(format!("{base_url}/v1/stream/{name}"))
        .send()
        .await
        .unwrap();
    let body = read.text().await.unwrap();
    assert_eq!(body, "a0b0a1");
}

/// Validates spec: 05-producer-sequencing.md#stream-closure-with-producers
///
/// Verifies producer close with final append is atomic.
#[tokio::test]
async fn test_producer_close_with_append() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .header("Stream-Closed", "true")
        .body("final")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("Stream-Closed")
            .unwrap()
            .to_str()
            .unwrap(),
        "true"
    );

    // Verify stream is closed and has data
    let read = client
        .get(format!("{base_url}/v1/stream/{name}"))
        .send()
        .await
        .unwrap();
    let closed_header = read
        .headers()
        .get("Stream-Closed")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let body = read.text().await.unwrap();
    assert_eq!(body, "final");
    assert_eq!(closed_header, "true");
}

/// Validates spec: 05-producer-sequencing.md#stream-closure-with-producers
///
/// Verifies duplicate of closing append returns 204 with Stream-Closed.
#[tokio::test]
async fn test_producer_duplicate_close_returns_204() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Close with append
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .header("Stream-Closed", "true")
        .body("final")
        .send()
        .await
        .unwrap();

    // Retry same request (duplicate)
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .header("Stream-Closed", "true")
        .body("final")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204, "Duplicate close should return 204");
    assert_eq!(
        response
            .headers()
            .get("Stream-Closed")
            .unwrap()
            .to_str()
            .unwrap(),
        "true"
    );
}

/// Validates spec: 05-producer-sequencing.md#stream-closure-with-producers
///
/// Verifies new sequence to closed stream returns 409 with Stream-Closed.
#[tokio::test]
async fn test_producer_append_to_closed_stream_returns_409() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // Append and close
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .header("Stream-Closed", "true")
        .body("final")
        .send()
        .await
        .unwrap();

    // New sequence to closed stream
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "1")
        .body("more")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        409,
        "New append to closed stream should return 409"
    );
    assert_eq!(
        response
            .headers()
            .get("Stream-Closed")
            .unwrap()
            .to_str()
            .unwrap(),
        "true"
    );
}

/// Validates spec: 05-producer-sequencing.md#response-codes
///
/// Verifies non-producer append still returns 204 (no regression).
#[tokio::test]
async fn test_non_producer_append_still_returns_204() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .body("hello")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        204,
        "Non-producer append should still return 204"
    );
}

/// Validates spec: 05-producer-sequencing.md#bootstrap-and-restart-flows
///
/// Verifies new producer with nonzero seq returns gap error.
#[tokio::test]
async fn test_producer_new_with_nonzero_seq_returns_409() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "3")
        .body("msg")
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        409,
        "New producer starting at nonzero seq should be rejected"
    );

    assert_eq!(
        response
            .headers()
            .get("Producer-Expected-Seq")
            .unwrap()
            .to_str()
            .unwrap(),
        "0"
    );
}

/// Validates spec: 05-producer-sequencing.md#producer-headers
///
/// Verifies producer close without body works.
#[tokio::test]
async fn test_producer_close_without_body() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let name = setup_stream(&base_url, &client).await;

    // First append some data
    client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "0")
        .body("data")
        .send()
        .await
        .unwrap();

    // Close without body
    let response = client
        .post(format!("{base_url}/v1/stream/{name}"))
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "prod-1")
        .header("Producer-Epoch", "0")
        .header("Producer-Seq", "1")
        .header("Stream-Closed", "true")
        .body("")
        .send()
        .await
        .unwrap();

    // Close-only with producer: 204 (no content appended, only state updated)
    assert_eq!(response.status(), 204);
    assert_eq!(
        response
            .headers()
            .get("Stream-Closed")
            .unwrap()
            .to_str()
            .unwrap(),
        "true"
    );
}
