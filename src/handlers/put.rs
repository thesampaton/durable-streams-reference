use crate::protocol::error::{Error, Result};
use crate::protocol::headers::{self, names};
use crate::protocol::json_mode;
use crate::storage::{CreateStreamResult, CreateWithDataResult, Storage, StreamConfig};
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
/// Optionally accepts a body with initial data for the stream.
///
/// # Errors
///
/// Returns error if Content-Type is explicitly provided but empty,
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
    // Read body (PUT may include initial data)
    let body_bytes =
        axum::body::to_bytes(body, usize::MAX)
            .await
            .map_err(|e| Error::InvalidHeader {
                header: "Content-Length".to_string(),
                reason: format!("Failed to read body: {e}"),
            })?;

    // Parse Content-Type: optional, defaults to application/octet-stream
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());

    // Reject explicitly-provided but empty Content-Type
    if let Some(ct) = content_type
        && ct.trim().is_empty()
    {
        return Err(Error::InvalidHeader {
            header: "Content-Type".to_string(),
            reason: "empty value".to_string(),
        });
    }

    let normalized_ct = content_type.map_or_else(
        || "application/octet-stream".to_string(),
        headers::normalize_content_type,
    );

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

    // Parse body into messages BEFORE creating the stream so that
    // failures (e.g. invalid JSON) never leave an orphaned stream.
    let messages = if body_bytes.is_empty() {
        vec![]
    } else if json_mode::is_json_content_type(&normalized_ct) {
        // Empty JSON arrays produce no messages — that's fine for PUT
        json_mode::process_append(&body_bytes)?
    } else {
        vec![body_bytes]
    };

    // Atomic create + append + close. If commit_messages fails (e.g.
    // memory limit), the stream is never inserted. If created_closed,
    // the entry is closed before it becomes visible to other operations.
    let CreateWithDataResult {
        status: create_status,
        next_offset,
        closed,
    } = storage.create_stream_with_data(&name, config, messages, created_closed)?;

    let status = if matches!(create_status, CreateStreamResult::Created) {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };

    // Build absolute Location URL
    let location = build_location_url(&headers, &name);

    let mut response_headers = HeaderMap::new();
    response_headers.insert("content-type", normalized_ct.parse().unwrap());
    response_headers.insert(
        names::STREAM_NEXT_OFFSET,
        next_offset.to_string().parse().unwrap(),
    );
    response_headers.insert("location", location.parse().unwrap());

    if closed {
        response_headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
    }

    Ok((status, response_headers).into_response())
}

/// Build an absolute Location URL from request headers.
///
/// Uses `Host` header for the authority and `X-Forwarded-Proto` for the scheme.
/// Falls back to `http` and `localhost` when headers are absent.
fn build_location_url(headers: &HeaderMap, name: &str) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");

    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");

    format!("{scheme}://{host}/v1/stream/{name}")
}
