use axum::http::StatusCode;

/// Health check endpoint
///
/// Returns 200 OK with body "ok" to indicate the server is running.
/// This endpoint is outside the protocol namespace (/v1/stream) as it's
/// an infrastructure concern, not part of the durable streams protocol.
pub async fn health_check() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok")
}
