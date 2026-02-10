#![allow(dead_code)]

use durable_streams_reference::config::{Config, StorageMode};
use durable_streams_reference::storage::{Storage, acid::AcidStorage, memory::InMemoryStorage};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

/// Global counter for generating unique stream names in tests
static STREAM_COUNTER: AtomicU16 = AtomicU16::new(0);
static STORAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

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
    let config = Config {
        max_memory_bytes: max_total_bytes,
        max_stream_bytes,
        ..Config::default()
    };
    spawn_test_server_with_config(config).await
}

/// Spawn a test server with a custom long-poll timeout.
pub async fn spawn_test_server_with_timeout(timeout: Duration) -> (String, u16) {
    let config = Config {
        long_poll_timeout: timeout,
        ..Config::default()
    };
    spawn_test_server_with_config(config).await
}

/// Spawn a test server with a full Config.
async fn spawn_test_server_with_config(config: Config) -> (String, u16) {
    let storage = Arc::new(InMemoryStorage::new(
        config.max_memory_bytes,
        config.max_stream_bytes,
    ));
    spawn_test_server_with_storage(storage, config).await
}

/// Spawn a test server in acid mode (sharded redb).
pub async fn spawn_test_server_acid() -> (String, u16) {
    let storage_dir = unique_acid_storage_dir();
    let config = Config {
        storage_mode: StorageMode::Acid,
        data_dir: storage_dir.to_string_lossy().into_owned(),
        acid_shard_count: 16,
        ..Config::default()
    };
    let storage = Arc::new(
        AcidStorage::new(
            &config.data_dir,
            config.acid_shard_count,
            config.max_memory_bytes,
            config.max_stream_bytes,
        )
        .expect("Failed to initialize acid test storage"),
    );
    spawn_test_server_with_storage(storage, config).await
}

fn unique_acid_storage_dir() -> PathBuf {
    let seq = STORAGE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    std::env::temp_dir().join(format!("ds-acid-http-test-{pid}-{ts}-{seq}"))
}

async fn spawn_test_server_with_storage<S>(storage: Arc<S>, config: Config) -> (String, u16)
where
    S: Storage + 'static,
{
    // Bind to port 0 to get a random available port
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind test server");

    let addr = listener.local_addr().expect("Failed to get local addr");
    let port = addr.port();

    // Build and spawn server
    let app = durable_streams_reference::router::build_router(storage, &config);

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

/// Create an HTTP client with a custom timeout for long-poll tests
pub fn test_client_with_timeout(timeout_secs: u64) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build()
        .expect("Failed to build test client")
}
