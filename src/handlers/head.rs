use crate::protocol::{error::Result, headers::names};
use crate::storage::Storage;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use std::sync::Arc;

/// HEAD handler for stream metadata
///
/// Returns stream metadata in headers without a body.
///
/// # Errors
///
/// Returns 404 if stream doesn't exist or has expired.
///
/// # Panics
///
/// Panics if validated header values fail to parse into `HeaderValue`,
/// which should never happen with valid inputs.
pub async fn stream_metadata<S: Storage>(
    State(storage): State<Arc<S>>,
    Path(name): Path<String>,
) -> Result<Response> {
    // Get stream metadata
    let metadata = storage.head(&name)?;

    // Build response headers
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        metadata.config.content_type.parse().unwrap(),
    );
    headers.insert(
        names::STREAM_NEXT_OFFSET,
        metadata.next_offset.to_string().parse().unwrap(),
    );
    headers.insert("cache-control", "no-store".parse().unwrap());

    // Include Stream-Closed if stream is closed
    if metadata.closed {
        headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
    }

    // Include TTL metadata if applicable
    if let Some(expires_at) = metadata.config.expires_at {
        // Calculate remaining TTL
        let now = Utc::now();
        let remaining_seconds = (expires_at - now).num_seconds();

        // Only include if not expired (positive remaining time)
        if remaining_seconds > 0 {
            headers.insert(
                names::STREAM_TTL,
                remaining_seconds.to_string().parse().unwrap(),
            );
            headers.insert(
                names::STREAM_EXPIRES_AT,
                expires_at.to_rfc3339().parse().unwrap(),
            );
        }
    }

    Ok((StatusCode::OK, headers).into_response())
}
