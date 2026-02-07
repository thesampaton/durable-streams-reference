use crate::config::{Config, LongPollTimeout, SseIdleClose};
use crate::{handlers, middleware, storage::Storage};
use axum::{Extension, Router, middleware as axum_middleware, routing::get};
use std::sync::Arc;

/// Build the application router with storage state
///
/// Routes:
/// - GET /healthz - Health check (outside protocol namespace)
/// - /v1/stream/* - Protocol routes
pub fn build_router<S: Storage + 'static>(storage: Arc<S>, config: &Config) -> Router {
    Router::new()
        .route("/healthz", get(handlers::health::health_check))
        .nest("/v1/stream", protocol_routes(storage, config))
}

/// Protocol routes under /v1/stream
///
/// All protocol routes have security headers applied via middleware.
fn protocol_routes<S: Storage + 'static>(storage: Arc<S>, config: &Config) -> Router {
    Router::new()
        .route(
            "/{name}",
            get(handlers::get::read_stream::<S>)
                .put(handlers::put::create_stream::<S>)
                .head(handlers::head::stream_metadata::<S>)
                .post(handlers::post::append_data::<S>)
                .delete(handlers::delete::delete_stream::<S>),
        )
        .layer(Extension(SseIdleClose(config.sse_idle_close_secs)))
        .layer(Extension(LongPollTimeout(config.long_poll_timeout)))
        .layer(axum_middleware::from_fn(
            middleware::security::add_security_headers,
        ))
        .with_state(storage)
}
