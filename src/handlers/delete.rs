use crate::protocol::error::Result;
use crate::storage::Storage;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// DELETE handler for deleting streams
///
/// Deletes a stream and all its data. Idempotent - returns 204 even if
/// stream doesn't exist.
///
/// # Errors
///
/// Returns error only for internal storage failures (not for missing streams).
///
/// # Panics
///
/// Panics if hardcoded header value fails to parse, which should never happen.
pub async fn delete_stream<S: Storage>(
    State(storage): State<Arc<S>>,
    Path(name): Path<String>,
) -> Result<Response> {
    // Delete stream (idempotent - no error if doesn't exist)
    storage.delete(&name)?;

    let mut response_headers = HeaderMap::new();
    response_headers.insert("cache-control", "no-store".parse().unwrap());

    Ok((StatusCode::NO_CONTENT, response_headers).into_response())
}
