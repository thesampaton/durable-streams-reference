mod common;

use common::{HttpTestBackend, spawn_test_server_for_backend, test_client, unique_stream_name};

// File-durable uses the same HTTP/router layer as memory; backend parity for
// file behavior is enforced in direct storage contract tests.
const BACKENDS: [HttpTestBackend; 2] = [HttpTestBackend::Memory, HttpTestBackend::Acid];

#[tokio::test]
async fn parity_create_idempotency_and_config_mismatch() {
    for backend in BACKENDS {
        let (base_url, _port) = spawn_test_server_for_backend(backend).await;
        let client = test_client();
        let stream = unique_stream_name();

        let create = client
            .put(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .send()
            .await
            .expect("create request failed");
        assert_eq!(create.status(), 201);

        let idempotent = client
            .put(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .send()
            .await
            .expect("idempotent create request failed");
        assert_eq!(idempotent.status(), 200);

        let mismatch = client
            .put(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "application/json")
            .send()
            .await
            .expect("config mismatch request failed");
        assert_eq!(mismatch.status(), 409);
    }
}

#[tokio::test]
async fn parity_append_read_and_offset_resume() {
    for backend in BACKENDS {
        let (base_url, _port) = spawn_test_server_for_backend(backend).await;
        let client = test_client();
        let stream = unique_stream_name();

        client
            .put(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .send()
            .await
            .expect("create request failed");

        let append1 = client
            .post(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .body("first")
            .send()
            .await
            .expect("append1 request failed");
        assert_eq!(append1.status(), 204);

        let next_offset = append1
            .headers()
            .get("Stream-Next-Offset")
            .expect("missing Stream-Next-Offset")
            .to_str()
            .expect("invalid offset header")
            .to_string();

        let append2 = client
            .post(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .body("second")
            .send()
            .await
            .expect("append2 request failed");
        assert_eq!(append2.status(), 204);

        let read_all = client
            .get(format!("{base_url}/v1/stream/{stream}?offset=-1"))
            .send()
            .await
            .expect("read-all request failed");
        assert_eq!(read_all.status(), 200);
        assert_eq!(
            read_all.text().await.expect("read body failed"),
            "firstsecond"
        );

        let resumed = client
            .get(format!(
                "{base_url}/v1/stream/{stream}?offset={next_offset}"
            ))
            .send()
            .await
            .expect("resumed read request failed");
        assert_eq!(resumed.status(), 200);
        assert_eq!(resumed.text().await.expect("resumed body failed"), "second");
    }
}

#[tokio::test]
async fn parity_producer_duplicate_gap_and_fencing() {
    for backend in BACKENDS {
        let (base_url, _port) = spawn_test_server_for_backend(backend).await;
        let client = test_client();
        let stream = unique_stream_name();

        client
            .put(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .send()
            .await
            .expect("create request failed");

        let accepted = client
            .post(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .header("Producer-Id", "p1")
            .header("Producer-Epoch", "0")
            .header("Producer-Seq", "0")
            .body("msg0")
            .send()
            .await
            .expect("producer append request failed");
        assert_eq!(accepted.status(), 200);

        let duplicate = client
            .post(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .header("Producer-Id", "p1")
            .header("Producer-Epoch", "0")
            .header("Producer-Seq", "0")
            .body("msg0")
            .send()
            .await
            .expect("producer duplicate request failed");
        assert_eq!(duplicate.status(), 204);

        let gap = client
            .post(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .header("Producer-Id", "p1")
            .header("Producer-Epoch", "0")
            .header("Producer-Seq", "5")
            .body("msg5")
            .send()
            .await
            .expect("producer gap request failed");
        assert_eq!(gap.status(), 409);

        let establish_epoch_two = client
            .post(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .header("Producer-Id", "p1")
            .header("Producer-Epoch", "2")
            .header("Producer-Seq", "0")
            .body("epoch2")
            .send()
            .await
            .expect("producer epoch setup failed");
        assert_eq!(establish_epoch_two.status(), 200);

        let fenced = client
            .post(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .header("Producer-Id", "p1")
            .header("Producer-Epoch", "1")
            .header("Producer-Seq", "0")
            .body("zombie")
            .send()
            .await
            .expect("producer fenced request failed");
        assert_eq!(fenced.status(), 403);
    }
}

#[tokio::test]
async fn parity_close_and_ttl_expiry() {
    for backend in BACKENDS {
        let (base_url, _port) = spawn_test_server_for_backend(backend).await;
        let client = test_client();
        let stream = unique_stream_name();

        let create = client
            .put(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .send()
            .await
            .expect("create request failed");
        assert_eq!(create.status(), 201);

        let close = client
            .post(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .header("Stream-Closed", "true")
            .send()
            .await
            .expect("close request failed");
        assert_eq!(close.status(), 204);

        let append_closed = client
            .post(format!("{base_url}/v1/stream/{stream}"))
            .header("Content-Type", "text/plain")
            .body("after-close")
            .send()
            .await
            .expect("append after close failed");
        assert_eq!(append_closed.status(), 409);

        let ttl_stream = unique_stream_name();
        let create_ttl = client
            .put(format!("{base_url}/v1/stream/{ttl_stream}"))
            .header("Content-Type", "text/plain")
            .header("Stream-TTL", "1")
            .send()
            .await
            .expect("create ttl stream failed");
        assert_eq!(create_ttl.status(), 201);

        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

        let read_expired = client
            .get(format!("{base_url}/v1/stream/{ttl_stream}"))
            .send()
            .await
            .expect("read expired stream failed");
        assert_eq!(read_expired.status(), 404);
    }
}
