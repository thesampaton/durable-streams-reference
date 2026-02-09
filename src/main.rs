use durable_streams_reference::{
    config::Config,
    router,
    storage::{Storage, file::FileStorage, memory::InMemoryStorage},
};
use std::env;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration
    let config = Config::from_env();
    let addr = format!("0.0.0.0:{}", config.port);

    tracing::info!("Starting durable streams server on {}", addr);
    tracing::info!(
        "Max memory: {} bytes, Max per stream: {} bytes",
        config.max_memory_bytes,
        config.max_stream_bytes
    );
    let storage_backend = env::var("STORAGE_BACKEND").unwrap_or_else(|_| "memory".to_string());
    tracing::info!("Storage backend: {}", storage_backend);

    if storage_backend.eq_ignore_ascii_case("file") {
        let root_dir = env::var("STORAGE_DIR").unwrap_or_else(|_| "./data/streams".to_string());
        let sync_on_append = env::var("FILE_STORAGE_SYNC_ON_APPEND")
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(false);
        let storage = Arc::new(
            FileStorage::new(
                root_dir,
                config.max_memory_bytes,
                config.max_stream_bytes,
                sync_on_append,
            )
            .unwrap_or_else(|e| panic!("Failed to initialize file storage: {e}")),
        );
        serve(storage, &config, &addr).await;
    } else {
        let storage = Arc::new(InMemoryStorage::new(
            config.max_memory_bytes,
            config.max_stream_bytes,
        ));
        serve(storage, &config, &addr).await;
    }
}

async fn serve<S: Storage + 'static>(storage: Arc<S>, config: &Config, addr: &str) {
    let app = router::build_router(storage, config);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {addr}: {e}"));

    tracing::info!("Server listening on {}", addr);
    tracing::info!("Health check: http://{}/healthz", addr);
    tracing::info!("Protocol base: http://{}/v1/stream/", addr);

    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| panic!("Server error: {e}"));
}
