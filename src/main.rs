use durable_streams_reference::{
    config::Config,
    router,
    storage::{Storage, file::FileStorage, memory::InMemoryStorage},
};
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
    tracing::info!("Storage mode: {}", config.storage_mode.as_str());

    if config.storage_mode.uses_file_backend() {
        let sync_on_append = config.storage_mode.sync_on_append();
        tracing::info!(
            "File storage dir: {}, sync on append: {}",
            config.data_dir,
            sync_on_append
        );
        let storage = Arc::new(
            FileStorage::new(
                &config.data_dir,
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
