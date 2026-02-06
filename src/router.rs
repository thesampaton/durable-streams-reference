use crate::handlers;
use axum::{Router, routing::get};

/// Build the application router
///
/// Routes:
/// - GET /healthz - Health check (outside protocol namespace)
/// - /v1/stream/* - Protocol routes (placeholder for now)
pub fn build_router() -> Router {
    Router::new()
        .route("/healthz", get(handlers::health::health_check))
        .nest("/v1/stream", protocol_routes())
}

/// Protocol routes under /v1/stream
///
/// Placeholder for now. Will be populated with PUT, POST, GET, DELETE, HEAD
/// handlers for stream operations.
fn protocol_routes() -> Router {
    Router::new()
    // Routes will be added here as we implement each handler
}
