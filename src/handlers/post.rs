use crate::protocol::error::{Error, Result};
use crate::protocol::headers::{self, names};
use crate::storage::Storage;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// POST handler for appending data to streams
///
/// Appends data to an existing stream and returns the next offset.
/// Can also close a stream with the Stream-Closed header.
///
/// # Errors
///
/// Returns error if stream doesn't exist, content-type mismatches,
/// stream is closed, or body is empty without Stream-Closed header.
///
/// # Panics
///
/// Panics if validated offset strings fail to parse into header values,
/// which should never happen with valid inputs.
pub async fn append_data<S: Storage>(
    State(storage): State<Arc<S>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    // Read body
    let body_bytes =
        axum::body::to_bytes(body, usize::MAX)
            .await
            .map_err(|e| Error::InvalidHeader {
                header: "Content-Length".to_string(),
                reason: format!("Failed to read body: {e}"),
            })?;

    // Parse Content-Type (required)
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Error::InvalidHeader {
            header: "Content-Type".to_string(),
            reason: "missing required header".to_string(),
        })?;

    // Normalize content type (lowercase, strip charset)
    let normalized_ct = headers::normalize_content_type(content_type);

    // Parse optional Stream-Closed
    let should_close = headers
        .get(names::STREAM_CLOSED)
        .and_then(|v| v.to_str().ok())
        .is_some_and(headers::parse_bool);

    // Validate body
    if body_bytes.is_empty() && !should_close {
        return Err(Error::InvalidHeader {
            header: "Content-Length".to_string(),
            reason: "empty body requires Stream-Closed header".to_string(),
        });
    }

    // Append data if body is non-empty
    let next_offset = if body_bytes.is_empty() {
        // No data to append, just get current next offset
        storage.head(&name)?.next_offset
    } else {
        // Attempt append - catch StreamClosed to add headers
        match storage.append(&name, body_bytes, &normalized_ct) {
            Ok(_offset) => {
                // Success - get next offset
                storage.head(&name)?.next_offset
            }
            Err(Error::StreamClosed) => {
                // Stream is closed - return 409 with Stream-Closed header
                let metadata = storage.head(&name)?;
                let mut error_headers = HeaderMap::new();
                error_headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
                error_headers.insert(
                    names::STREAM_NEXT_OFFSET,
                    metadata.next_offset.to_string().parse().unwrap(),
                );
                return Ok(
                    (StatusCode::CONFLICT, error_headers, "Stream is closed").into_response()
                );
            }
            Err(e) => return Err(e),
        }
    };

    // Close stream if requested
    if should_close {
        storage.close_stream(&name)?;
    }

    // Build response headers
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        names::STREAM_NEXT_OFFSET,
        next_offset.to_string().parse().unwrap(),
    );

    Ok((StatusCode::NO_CONTENT, response_headers).into_response())
}
