mod common;

use common::{spawn_test_server_acid, test_client, unique_stream_name};

#[tokio::test]
async fn test_http_smoke_acid_mode_create_append_read() {
    let (base_url, _port) = spawn_test_server_acid().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    let create = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create.status(), 201);

    let append = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("acid message")
        .send()
        .await
        .expect("append request failed");
    assert_eq!(append.status(), 204);

    let read = client
        .get(format!("{base_url}/v1/stream/{stream_name}?offset=-1"))
        .send()
        .await
        .expect("read request failed");
    assert_eq!(read.status(), 200);
    let body = read.text().await.expect("read body failed");
    assert_eq!(body, "acid message");
}
