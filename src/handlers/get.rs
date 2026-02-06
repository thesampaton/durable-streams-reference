use crate::protocol::error::Result;
use crate::protocol::headers::names;
use crate::protocol::offset::Offset;
use crate::storage::Storage;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::{BufMut, BytesMut};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;

/// Query parameters for GET requests
#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    /// Starting offset (default: -1, start of stream)
    #[serde(default = "default_offset")]
    offset: String,
}

fn default_offset() -> String {
    "-1".to_string()
}

/// GET handler for reading stream data (catch-up mode)
///
/// Reads data from a stream starting at the specified offset.
/// Returns concatenated message data with resumption headers.
///
/// # Errors
///
/// Returns error if stream doesn't exist, offset is invalid,
/// or storage operation fails.
///
/// # Panics
///
/// Panics if validated header values fail to parse into `HeaderValue`,
/// which should never happen with valid inputs.
pub async fn read_stream<S: Storage>(
    State(storage): State<Arc<S>>,
    Path(name): Path<String>,
    Query(query): Query<ReadQuery>,
    headers: HeaderMap,
) -> Result<Response> {
    // Parse offset (supports sentinels: -1, now, and hex format)
    let offset = Offset::from_str(&query.offset)?;

    // Parse If-None-Match for 304 optimization
    let if_none_match = headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    // Get content type first (immutable stream config, safe to fetch separately)
    let metadata = storage.head(&name)?;
    let content_type = metadata.config.content_type.clone();

    // Read from storage (single snapshot for offsets/closed state)
    // IMPORTANT: All offset/closed metadata in the response MUST come from read_result,
    // not from a separate head() call. Under concurrency, a write could land between
    // read() and head(), causing Stream-Next-Offset/ETag to reflect newer state than
    // the response body. This would break resumable reads by skipping unread data.
    let read_result = storage.read(&name, &offset)?;

    // Generate ETag: "{start_offset}:{end_offset}" or "{start_offset}:{end_offset}:c"
    // Use read_result.next_offset (not metadata.next_offset) for consistency with body
    let start_etag = query.offset;
    let end_offset = read_result.next_offset.to_string();
    let is_closed_at_tail = read_result.closed && read_result.at_tail;
    let etag = if is_closed_at_tail {
        format!("\"{start_etag}:{end_offset}:c\"")
    } else {
        format!("\"{start_etag}:{end_offset}\"")
    };

    // Check If-None-Match for 304 Not Modified
    if let Some(ref client_etag) = if_none_match
        && client_etag == &etag
    {
        // ETag matches - return 304
        let mut response_headers = HeaderMap::new();
        response_headers.insert(
            names::STREAM_NEXT_OFFSET,
            read_result.next_offset.to_string().parse().unwrap(),
        );
        response_headers.insert(names::STREAM_UP_TO_DATE, "true".parse().unwrap());
        return Ok((StatusCode::NOT_MODIFIED, response_headers).into_response());
    }

    // Concatenate message data (Bytes end-to-end, zero-copy!)
    let body = if read_result.messages.is_empty() {
        bytes::Bytes::new()
    } else {
        let total_len: usize = read_result.messages.iter().map(|m| m.data.len()).sum();

        let mut buf = BytesMut::with_capacity(total_len);
        for message in &read_result.messages {
            buf.put(message.data.clone());
        }
        buf.freeze()
    };

    // Build response headers
    let mut response_headers = HeaderMap::new();
    response_headers.insert("content-type", content_type.parse().unwrap());
    response_headers.insert(
        names::STREAM_NEXT_OFFSET,
        read_result.next_offset.to_string().parse().unwrap(),
    );
    response_headers.insert(
        names::STREAM_UP_TO_DATE,
        (if read_result.at_tail { "true" } else { "false" })
            .parse()
            .unwrap(),
    );
    response_headers.insert("etag", etag.parse().unwrap());
    response_headers.insert("cache-control", "no-store".parse().unwrap());

    // Include Stream-Closed only when stream is closed AND at tail
    if is_closed_at_tail {
        response_headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
    }

    Ok((StatusCode::OK, response_headers, body).into_response())
}
