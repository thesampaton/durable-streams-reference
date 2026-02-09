use durable_streams_reference::{config::Config, router, storage::memory::InMemoryStorage};
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

    // Create storage
    let storage = Arc::new(InMemoryStorage::new(
        config.max_memory_bytes,
        config.max_stream_bytes,
    ));

    // Build router
    let app = router::build_router(storage, &config);

    // Bind and serve
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {addr}: {e}"));

    tracing::info!("Server listening on {}", addr);
    tracing::info!("Health check: http://{}/healthz", addr);
    tracing::info!("Protocol base: http://{}/v1/stream/", addr);

    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| panic!("Server error: {e}"));
}
