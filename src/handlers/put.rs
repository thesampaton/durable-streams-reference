use crate::protocol::error::{Error, Result};
use crate::protocol::headers::{self, names};
use crate::storage::{Storage, StreamConfig};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use std::sync::Arc;

/// PUT handler for creating streams
///
/// Creates a new stream with the specified configuration.
/// Returns 201 Created for new streams, 200 OK for idempotent recreates.
///
/// # Errors
///
/// Returns error if Content-Type is missing/empty, body is non-empty,
/// both TTL and Expires-At are provided, TTL format is invalid, or stream
/// exists with different configuration.
///
/// # Panics
///
/// Panics if validated content-type or offset strings fail to parse into
/// header values, which should never happen with valid inputs.
pub async fn create_stream<S: Storage>(
    State(storage): State<Arc<S>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    // Reject non-empty body (PUT should have no body)
    let body_bytes =
        axum::body::to_bytes(body, usize::MAX)
            .await
            .map_err(|e| Error::InvalidHeader {
                header: "Content-Length".to_string(),
                reason: format!("Failed to read body: {e}"),
            })?;

    if !body_bytes.is_empty() {
        return Err(Error::InvalidHeader {
            header: "Content-Length".to_string(),
            reason: "PUT requests must have empty body".to_string(),
        });
    }

    // Parse Content-Type (required)
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Error::InvalidHeader {
            header: "Content-Type".to_string(),
            reason: "missing required header".to_string(),
        })?;

    if content_type.trim().is_empty() {
        return Err(Error::InvalidHeader {
            header: "Content-Type".to_string(),
            reason: "empty value".to_string(),
        });
    }

    // Normalize content type (lowercase, strip charset)
    let normalized_ct = headers::normalize_content_type(content_type);

    // Parse optional TTL
    let ttl_seconds =
        if let Some(ttl_value) = headers.get(names::STREAM_TTL).and_then(|v| v.to_str().ok()) {
            Some(headers::parse_ttl(ttl_value)?)
        } else {
            None
        };

    // Parse optional Expires-At
    let expires_at = if let Some(expires_value) = headers
        .get(names::STREAM_EXPIRES_AT)
        .and_then(|v| v.to_str().ok())
    {
        Some(headers::parse_expires_at(expires_value)?)
    } else {
        None
    };

    // Reject both TTL and Expires-At
    if ttl_seconds.is_some() && expires_at.is_some() {
        return Err(Error::ConflictingExpiration);
    }

    // Parse optional Stream-Closed
    let created_closed = headers
        .get(names::STREAM_CLOSED)
        .and_then(|v| v.to_str().ok())
        .is_some_and(headers::parse_bool);

    // Build stream config
    let mut config = StreamConfig::new(normalized_ct.clone());

    if let Some(ttl) = ttl_seconds {
        // Calculate absolute expiration from TTL
        let expires_at =
            Utc::now() + chrono::Duration::seconds(i64::try_from(ttl).unwrap_or(i64::MAX));
        config = config.with_expires_at(expires_at);
        config = config.with_ttl(ttl);
    } else if let Some(expires) = expires_at {
        config = config.with_expires_at(expires);
    }

    if created_closed {
        config = config.with_created_closed(true);
    }

    // Check if stream existed before create
    let existed_before = storage.exists(&name);

    // Create stream (returns Ok if idempotent, Err(ConfigMismatch) if conflict)
    match storage.create_stream(&name, config) {
        Ok(()) => {} // Success (new or idempotent)
        Err(Error::ConfigMismatch) => {
            // Stream exists with different config
            return Err(Error::ConfigMismatch);
        }
        Err(e) => return Err(e),
    }

    let is_new = !existed_before;

    // Get metadata for response headers
    let metadata = storage.head(&name)?;

    // Build response
    let status = if is_new {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };

    let mut response_headers = HeaderMap::new();
    response_headers.insert("content-type", normalized_ct.parse().unwrap());
    response_headers.insert(
        names::STREAM_NEXT_OFFSET,
        metadata.next_offset.to_string().parse().unwrap(),
    );
    // Location header (only for 201 Created, but include for both per spec behavior)
    let location = format!("/v1/stream/{name}");
    response_headers.insert("location", location.parse().unwrap());

    if metadata.closed {
        response_headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
    }

    Ok((status, response_headers).into_response())
}
