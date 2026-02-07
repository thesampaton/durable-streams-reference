use crate::config::LongPollTimeout;
use crate::protocol::cursor;
use crate::protocol::error::{Error, Result};
use crate::protocol::headers::names;
use crate::protocol::json_mode;
use crate::protocol::offset::Offset;
use crate::storage::{ReadResult, Storage};
use axum::{
    Extension,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::{BufMut, BytesMut};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

/// Query parameters for GET requests
#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    /// Starting offset (default: -1, start of stream)
    #[serde(default = "default_offset")]
    offset: String,
    /// Live mode: "long-poll" for long-polling
    live: Option<String>,
    /// Cursor echoed from previous long-poll response. Parsed by axum/serde
    /// so the query param is accepted, but the server doesn't use it — it
    /// exists for CDN intermediaries to collapse identical polling requests.
    #[allow(dead_code)]
    cursor: Option<String>,
}

fn default_offset() -> String {
    "-1".to_string()
}

/// GET handler for reading stream data
///
/// Supports two modes:
/// - Catch-up (no `live` param): immediate read, returns all available data
/// - Long-poll (`live=long-poll`): waits for new data at tail
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
    Extension(LongPollTimeout(timeout)): Extension<LongPollTimeout>,
    headers: HeaderMap,
) -> Result<Response> {
    // Validate live parameter
    if let Some(ref live) = query.live
        && live != "long-poll"
    {
        return Err(Error::InvalidHeader {
            header: "live".to_string(),
            reason: format!("unsupported live mode: {live}"),
        });
    }

    let offset = Offset::from_str(&query.offset)?;
    let if_none_match = headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    // Get content type (immutable stream config, safe to fetch separately)
    let metadata = storage.head(&name)?;
    let content_type = metadata.config.content_type.clone();

    if query.live.is_some() {
        read_long_poll(
            &storage,
            &name,
            &offset,
            &query.offset,
            if_none_match.as_ref(),
            &content_type,
            timeout,
        )
        .await
    } else {
        read_catch_up(
            &storage,
            &name,
            &offset,
            &query.offset,
            if_none_match.as_ref(),
            &content_type,
        )
    }
}

/// Catch-up mode: immediate read of all available data.
fn read_catch_up<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    offset: &Offset,
    raw_offset: &str,
    if_none_match: Option<&String>,
    content_type: &str,
) -> Result<Response> {
    // Read from storage (single snapshot for offsets/closed state)
    let read_result = storage.read(name, offset)?;

    // Check 304 Not Modified
    let etag = generate_etag(raw_offset, &read_result);
    if let Some(client_etag) = if_none_match
        && client_etag == &etag
    {
        return Ok(build_304_response(&read_result));
    }

    build_data_response(&read_result, content_type, &etag, None)
}

/// Long-poll mode: wait for new data at tail, return immediately if data exists.
async fn read_long_poll<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    offset: &Offset,
    raw_offset: &str,
    if_none_match: Option<&String>,
    content_type: &str,
    timeout: Duration,
) -> Result<Response> {
    // Subscribe BEFORE read to avoid missing notifications between read and subscribe
    let mut receiver = storage
        .subscribe(name)
        .ok_or_else(|| Error::NotFound(name.to_string()))?;

    let read_result = storage.read(name, offset)?;

    // Check 304 Not Modified (same as catch-up)
    let etag = generate_etag(raw_offset, &read_result);
    if let Some(client_etag) = if_none_match
        && client_etag == &etag
    {
        return Ok(build_304_response(&read_result));
    }

    // Data available → return immediately (like catch-up + cursor)
    if !read_result.messages.is_empty() {
        let cursor_val = cursor::generate(&read_result.next_offset);
        return build_data_response(&read_result, content_type, &etag, Some(&cursor_val));
    }

    // At tail + closed → immediate 204 (MUST NOT wait)
    if read_result.closed && read_result.at_tail {
        return Ok(build_204_response(&read_result.next_offset, true));
    }

    // At tail + open → wait for notification or timeout.
    // Capture the concrete tail offset for re-reads. The original `offset`
    // may be a sentinel (e.g. `now`) which always returns empty on re-read;
    // using the resolved position ensures we pick up data that arrived.
    let tail_offset = read_result.next_offset.clone();
    let tail_offset_str = tail_offset.to_string();

    tokio::select! {
        _ = receiver.recv() => {
            // Data or close event — re-read from resolved tail position
            handle_long_poll_wake(storage, name, &tail_offset, &tail_offset_str, content_type)
        }
        () = tokio::time::sleep(timeout) => {
            // Timeout — return 204 with current position
            let read_result = storage.read(name, &tail_offset)?;
            let is_closed = read_result.closed && read_result.at_tail;
            Ok(build_204_response(&read_result.next_offset, is_closed))
        }
    }
}

/// Handle wake-up from broadcast in long-poll mode.
///
/// Re-reads from storage to get the actual data that triggered the notification.
fn handle_long_poll_wake<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    offset: &Offset,
    raw_offset: &str,
    content_type: &str,
) -> Result<Response> {
    let read_result = storage.read(name, offset)?;

    if read_result.messages.is_empty() {
        // Woke up but no data (e.g., stream was closed)
        let is_closed = read_result.closed && read_result.at_tail;
        return Ok(build_204_response(&read_result.next_offset, is_closed));
    }

    let etag = generate_etag(raw_offset, &read_result);
    let cursor_val = cursor::generate(&read_result.next_offset);
    build_data_response(&read_result, content_type, &etag, Some(&cursor_val))
}

/// Generate `ETag` from read result.
///
/// Format: `"{start_offset}:{end_offset}"` or `"{start_offset}:{end_offset}:c"` if closed at tail.
fn generate_etag(start_offset: &str, read_result: &ReadResult) -> String {
    let end_offset = read_result.next_offset.to_string();
    if read_result.closed && read_result.at_tail {
        format!("\"{start_offset}:{end_offset}:c\"")
    } else {
        format!("\"{start_offset}:{end_offset}\"")
    }
}

/// Build a 304 Not Modified response.
fn build_304_response(read_result: &ReadResult) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        names::STREAM_NEXT_OFFSET,
        read_result.next_offset.to_string().parse().unwrap(),
    );
    headers.insert(names::STREAM_UP_TO_DATE, "true".parse().unwrap());
    (StatusCode::NOT_MODIFIED, headers).into_response()
}

/// Build a 200 OK response with message data.
///
/// If `cursor` is `Some`, includes `Stream-Cursor` header (long-poll mode).
fn build_data_response(
    read_result: &ReadResult,
    content_type: &str,
    etag: &str,
    cursor_val: Option<&str>,
) -> Result<Response> {
    let body = build_body(read_result, content_type)?;

    let mut headers = HeaderMap::new();
    headers.insert("content-type", content_type.parse().unwrap());
    headers.insert(
        names::STREAM_NEXT_OFFSET,
        read_result.next_offset.to_string().parse().unwrap(),
    );
    headers.insert(
        names::STREAM_UP_TO_DATE,
        (if read_result.at_tail { "true" } else { "false" })
            .parse()
            .unwrap(),
    );
    headers.insert("etag", etag.parse().unwrap());

    let is_closed_at_tail = read_result.closed && read_result.at_tail;
    if is_closed_at_tail {
        headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
    }

    if let Some(c) = cursor_val {
        headers.insert(names::STREAM_CURSOR, c.parse().unwrap());
    }

    Ok((StatusCode::OK, headers, body).into_response())
}

/// Build a 204 No Content response for long-poll timeout or closed stream.
fn build_204_response(next_offset: &Offset, is_closed: bool) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        names::STREAM_NEXT_OFFSET,
        next_offset.to_string().parse().unwrap(),
    );
    headers.insert(names::STREAM_UP_TO_DATE, "true".parse().unwrap());

    let cursor_val = cursor::generate(next_offset);
    if is_closed {
        headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
        // Cursor MAY be omitted when closed per spec, but including it is harmless
    }
    headers.insert(names::STREAM_CURSOR, cursor_val.parse().unwrap());

    (StatusCode::NO_CONTENT, headers).into_response()
}

/// Build response body from read result messages.
fn build_body(read_result: &ReadResult, content_type: &str) -> Result<bytes::Bytes> {
    if json_mode::is_json_content_type(content_type) {
        let message_data: Vec<_> = read_result
            .messages
            .iter()
            .map(|m| m.data.clone())
            .collect();
        json_mode::wrap_read(&message_data)
    } else if read_result.messages.is_empty() {
        Ok(bytes::Bytes::new())
    } else {
        let total_len: usize = read_result.messages.iter().map(|m| m.data.len()).sum();
        let mut buf = BytesMut::with_capacity(total_len);
        for message in &read_result.messages {
            buf.put(message.data.clone());
        }
        Ok(buf.freeze())
    }
}
