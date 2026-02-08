use crate::protocol::error::{Error, Result};
use crate::protocol::headers::{self, names};
use crate::protocol::json_mode;
use crate::storage::{CreateStreamResult, Storage, StreamConfig};
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

    // Create stream (returns created-vs-existing status atomically)
    // Note: create_stream stores config for idempotent checks but does NOT
    // set the closed flag — the handler closes explicitly after any appends.
    let create_result = match storage.create_stream(&name, config) {
        Ok(result) => result,
        Err(Error::ConfigMismatch) => {
            return Err(Error::ConfigMismatch);
        }
        Err(e) => return Err(e),
    };
    let is_new = matches!(create_result, CreateStreamResult::Created);

    // If new stream and body is non-empty, append initial data
    if is_new && !body_bytes.is_empty() {
        let messages = if json_mode::is_json_content_type(&normalized_ct) {
            // Empty JSON arrays produce no messages — that's fine for PUT
            json_mode::process_append(&body_bytes)?
        } else {
            vec![body_bytes]
        };

        if !messages.is_empty() {
            storage.batch_append(&name, messages, &normalized_ct, None)?;
        }
    }

    // Close stream after appending data (if created_closed)
    if is_new && created_closed {
        storage.close_stream(&name)?;
    }

    // Get metadata for response headers (snapshot after any appends)
    let metadata = storage.head(&name)?;

    let status = if is_new {
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
        metadata.next_offset.to_string().parse().unwrap(),
    );
    response_headers.insert("location", location.parse().unwrap());

    if metadata.closed {
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
