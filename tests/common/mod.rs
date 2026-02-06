use durable_streams_rust_server::storage::memory::InMemoryStorage;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::net::TcpListener;

/// Global counter for generating unique stream names in tests
static STREAM_COUNTER: AtomicU16 = AtomicU16::new(0);

/// Generate a unique stream name for testing
///
/// Uses a global atomic counter to ensure unique names across all tests.
pub fn unique_stream_name() -> String {
    let id = STREAM_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("test-stream-{id}")
}

/// Spawn a test server on a random available port
///
/// Returns the bound address and port number.
pub async fn spawn_test_server() -> (String, u16) {
    spawn_test_server_with_limits(1024 * 1024 * 100, 1024 * 1024 * 10).await
}

/// Spawn a test server with custom memory limits.
pub async fn spawn_test_server_with_limits(
    max_total_bytes: u64,
    max_stream_bytes: u64,
) -> (String, u16) {
    // Bind to port 0 to get a random available port
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind test server");

    let addr = listener.local_addr().expect("Failed to get local addr");
    let port = addr.port();

    let storage = Arc::new(InMemoryStorage::new(max_total_bytes, max_stream_bytes));

    // Build and spawn server
    let app = durable_streams_rust_server::router::build_router(storage);

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("Test server failed");
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (format!("http://127.0.0.1:{port}"), port)
}

/// Create an HTTP client for testing
pub fn test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("Failed to build test client")
}
