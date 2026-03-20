#![allow(dead_code)]
// Shared across many independent integration-test crates; each crate only uses
// a subset of helpers, so items appear unused when compiled per-test target.

use durable_streams_server::config::{Config, StorageMode};
use durable_streams_server::protocol::error::Result;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::protocol::producer::ProducerHeaders;
use durable_streams_server::storage::{
    CreateStreamResult, CreateWithDataResult, ProducerAppendResult, ReadResult, Storage,
    StreamConfig, StreamMetadata, acid::AcidStorage, file::FileStorage, memory::InMemoryStorage,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

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

#[derive(Debug, Clone, Copy)]
pub enum StorageTestBackend {
    Memory,
    FileDurable,
    Acid,
}

impl StorageTestBackend {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::FileDurable => "file-durable",
            Self::Acid => "acid",
        }
    }
}

pub enum TestStorage {
    Memory(InMemoryStorage),
    File(FileStorage),
    Acid(AcidStorage),
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TestStorage>();
};

impl Storage for TestStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        match self {
            Self::Memory(inner) => inner.create_stream(name, config),
            Self::File(inner) => inner.create_stream(name, config),
            Self::Acid(inner) => inner.create_stream(name, config),
        }
    }

    fn append(&self, name: &str, data: bytes::Bytes, content_type: &str) -> Result<Offset> {
        match self {
            Self::Memory(inner) => inner.append(name, data, content_type),
            Self::File(inner) => inner.append(name, data, content_type),
            Self::Acid(inner) => inner.append(name, data, content_type),
        }
    }

    fn batch_append(
        &self,
        name: &str,
        messages: Vec<bytes::Bytes>,
        content_type: &str,
        seq: Option<&str>,
    ) -> Result<Offset> {
        match self {
            Self::Memory(inner) => inner.batch_append(name, messages, content_type, seq),
            Self::File(inner) => inner.batch_append(name, messages, content_type, seq),
            Self::Acid(inner) => inner.batch_append(name, messages, content_type, seq),
        }
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        match self {
            Self::Memory(inner) => inner.read(name, from_offset),
            Self::File(inner) => inner.read(name, from_offset),
            Self::Acid(inner) => inner.read(name, from_offset),
        }
    }

    fn delete(&self, name: &str) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.delete(name),
            Self::File(inner) => inner.delete(name),
            Self::Acid(inner) => inner.delete(name),
        }
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        match self {
            Self::Memory(inner) => inner.head(name),
            Self::File(inner) => inner.head(name),
            Self::Acid(inner) => inner.head(name),
        }
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.close_stream(name),
            Self::File(inner) => inner.close_stream(name),
            Self::Acid(inner) => inner.close_stream(name),
        }
    }

    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<bytes::Bytes>,
        content_type: &str,
        producer: &ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult> {
        match self {
            Self::Memory(inner) => inner.append_with_producer(
                name,
                messages,
                content_type,
                producer,
                should_close,
                seq,
            ),
            Self::File(inner) => inner.append_with_producer(
                name,
                messages,
                content_type,
                producer,
                should_close,
                seq,
            ),
            Self::Acid(inner) => inner.append_with_producer(
                name,
                messages,
                content_type,
                producer,
                should_close,
                seq,
            ),
        }
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<bytes::Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        match self {
            Self::Memory(inner) => {
                inner.create_stream_with_data(name, config, messages, should_close)
            }
            Self::File(inner) => {
                inner.create_stream_with_data(name, config, messages, should_close)
            }
            Self::Acid(inner) => {
                inner.create_stream_with_data(name, config, messages, should_close)
            }
        }
    }

    fn exists(&self, name: &str) -> bool {
        match self {
            Self::Memory(inner) => inner.exists(name),
            Self::File(inner) => inner.exists(name),
            Self::Acid(inner) => inner.exists(name),
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        match self {
            Self::Memory(inner) => inner.subscribe(name),
            Self::File(inner) => inner.subscribe(name),
            Self::Acid(inner) => inner.subscribe(name),
        }
    }
}

impl TestStorage {
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        match self {
            Self::Memory(inner) => inner.total_bytes(),
            Self::File(inner) => inner.total_bytes(),
            Self::Acid(inner) => inner.total_bytes(),
        }
    }
}

pub struct TestStorageHandle {
    pub storage: TestStorage,
    _storage_dir: Option<PathBuf>,
}

#[must_use]
pub fn create_test_storage(backend: StorageTestBackend) -> TestStorageHandle {
    create_test_storage_with_limits(backend, 1024 * 1024, 100 * 1024)
}

#[must_use]
pub fn create_test_storage_with_limits(
    backend: StorageTestBackend,
    max_total_bytes: u64,
    max_stream_bytes: u64,
) -> TestStorageHandle {
    match backend {
        StorageTestBackend::Memory => TestStorageHandle {
            storage: TestStorage::Memory(InMemoryStorage::new(max_total_bytes, max_stream_bytes)),
            _storage_dir: None,
        },
        StorageTestBackend::FileDurable => {
            let storage_dir = unique_storage_dir("file");
            let storage = FileStorage::new(&storage_dir, max_total_bytes, max_stream_bytes, true)
                .expect("failed to initialize test file storage");
            TestStorageHandle {
                storage: TestStorage::File(storage),
                _storage_dir: Some(storage_dir),
            }
        }
        StorageTestBackend::Acid => {
            let storage_dir = unique_storage_dir("acid");
            let storage = AcidStorage::new(&storage_dir, 16, max_total_bytes, max_stream_bytes)
                .expect("failed to initialize test acid storage");
            TestStorageHandle {
                storage: TestStorage::Acid(storage),
                _storage_dir: Some(storage_dir),
            }
        }
    }
}

fn unique_storage_dir(prefix: &str) -> PathBuf {
    let seq = STORAGE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    std::env::temp_dir().join(format!("ds-{prefix}-storage-test-{pid}-{ts}-{seq}"))
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

#[derive(Debug, Clone, Copy)]
pub enum HttpTestBackend {
    Memory,
    Acid,
}

/// Spawn a test server for a specific backend under test.
pub async fn spawn_test_server_for_backend(backend: HttpTestBackend) -> (String, u16) {
    match backend {
        HttpTestBackend::Memory => spawn_test_server_with_config(Config::default()).await,
        HttpTestBackend::Acid => spawn_test_server_acid().await,
    }
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
    let storage_dir = unique_storage_dir("acid-http");
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
    let app = durable_streams_server::router::build_router(storage, &config);

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
