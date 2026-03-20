mod common;

use common::{spawn_test_server, test_client};

/// Validates: Server startup and health check endpoint
///
/// Verifies that the server starts successfully and the /healthz endpoint
/// returns 200 OK with body "ok".
#[tokio::test]
async fn test_health_check() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();

    let response = client
        .get(format!("{base_url}/healthz"))
        .send()
        .await
        .expect("Failed to send health check request");

    assert_eq!(response.status(), 200, "Expected 200 OK status");

    let body = response.text().await.expect("Failed to read response body");

    assert_eq!(body, "ok", "Expected body to be 'ok'");
}

/// Validates: Server binds to configured port
///
/// Verifies that the server successfully binds to port and is accessible.
#[tokio::test]
async fn test_server_starts() {
    let (base_url, port) = spawn_test_server().await;
    let client = test_client();

    // Verify server is accessible
    let response = client
        .get(format!("{base_url}/healthz"))
        .send()
        .await
        .expect("Failed to connect to server");

    assert!(
        response.status().is_success(),
        "Server should respond successfully on port {port}",
    );
}
