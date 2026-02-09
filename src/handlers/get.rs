use crate::config::{LongPollTimeout, SseIdleClose};
use crate::protocol::cursor;
use crate::protocol::error::{Error, Result};
use crate::protocol::headers::names;
use crate::protocol::json_mode;
use crate::protocol::offset::Offset;
use crate::protocol::sse::{self, ControlPayload};
use crate::storage::{ReadResult, Storage};
use axum::{
    Extension,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::{BufMut, BytesMut};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Query parameters for GET requests
#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    /// Starting offset (None when not provided; defaults vary by mode)
    offset: Option<String>,
    /// Live mode: "long-poll" for long-polling
    live: Option<String>,
    /// Cursor echoed from previous long-poll response. Parsed by axum/serde
    /// so the query param is accepted, but the server doesn't use it — it
    /// exists for CDN intermediaries to collapse identical polling requests.
    #[allow(dead_code)]
    cursor: Option<String>,
}

/// GET handler for reading stream data
///
/// Supports three modes:
/// - Catch-up (no `live` param): immediate read, returns all available data
/// - Long-poll (`live=long-poll`): waits for new data at tail
/// - SSE (`live=sse`): streaming Server-Sent Events
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
pub async fn read_stream<S: Storage + 'static>(
    State(storage): State<Arc<S>>,
    Path(name): Path<String>,
    Query(query): Query<ReadQuery>,
    Extension(LongPollTimeout(timeout)): Extension<LongPollTimeout>,
    Extension(SseIdleClose(idle_close_secs)): Extension<SseIdleClose>,
    headers: HeaderMap,
) -> Result<Response> {
    // Resolve offset: live modes require explicit offset, catch-up defaults to "-1"
    let raw_offset = if let Some(ref live) = query.live {
        match query.offset {
            Some(ref o) => o.clone(),
            None => {
                return Err(Error::InvalidHeader {
                    header: "offset".to_string(),
                    reason: format!("offset query parameter is required for live={live} mode"),
                });
            }
        }
    } else {
        query.offset.clone().unwrap_or_else(|| "-1".to_string())
    };

    let offset = Offset::from_str(&raw_offset)?;
    let metadata = storage.head(&name)?;
    let content_type = metadata.config.content_type.clone();

    if let Some(ref live) = query.live {
        match live.as_str() {
            "long-poll" => {
                let if_none_match = headers.get("if-none-match").and_then(|v| v.to_str().ok());
                read_long_poll(
                    &storage,
                    &name,
                    &offset,
                    &raw_offset,
                    if_none_match,
                    &content_type,
                    timeout,
                )
                .await
            }
            "sse" => read_sse(storage, name, &offset, &content_type, idle_close_secs),
            other => Err(Error::InvalidHeader {
                header: "live".to_string(),
                reason: format!("unsupported live mode: {other}"),
            }),
        }
    } else {
        let if_none_match = headers.get("if-none-match").and_then(|v| v.to_str().ok());
        read_catch_up(
            &storage,
            &name,
            &offset,
            &raw_offset,
            if_none_match,
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
    if_none_match: Option<&str>,
    content_type: &str,
) -> Result<Response> {
    // Read from storage (single snapshot for offsets/closed state)
    let read_result = storage.read(name, offset)?;

    // Check 304 Not Modified
    let etag = generate_etag(raw_offset, &read_result);
    if let Some(client_etag) = if_none_match
        && client_etag == etag
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
    if_none_match: Option<&str>,
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
        && client_etag == etag
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

/// SSE mode: stream data as Server-Sent Events (PROTOCOL.md §5.8).
///
/// Validates preconditions (stream existence, offset) eagerly before
/// starting the stream. Uses raw byte streaming for full control over
/// the SSE wire format. Once streaming begins, errors are silently
/// dropped (SSE has no error frame).
fn read_sse<S: Storage + 'static>(
    storage: Arc<S>,
    name: String,
    offset: &Offset,
    content_type: &str,
    idle_close_secs: u64,
) -> Result<Response> {
    let is_binary = sse::is_binary_content_type(content_type);
    let is_json = json_mode::is_json_content_type(content_type);

    // Subscribe before read to avoid missing notifications
    let receiver = storage
        .subscribe(&name)
        .ok_or_else(|| Error::NotFound(name.clone()))?;

    // Initial read to get current state
    let read_result = storage.read(&name, offset)?;

    let byte_stream = build_sse_byte_stream(
        storage,
        name,
        read_result,
        receiver,
        is_binary,
        is_json,
        idle_close_secs,
    );

    let body = Body::from_stream(byte_stream);

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "text/event-stream".parse().unwrap());

    if is_binary {
        headers.insert("stream-sse-data-encoding", "base64".parse().unwrap());
    }

    Ok((StatusCode::OK, headers, body).into_response())
}

/// Build a byte stream that yields raw SSE frame strings.
///
/// Manages keep-alive, idle timeout, and the subscribe-before-read pattern.
fn build_sse_byte_stream<S: Storage + 'static>(
    storage: Arc<S>,
    name: String,
    initial_read: ReadResult,
    mut receiver: tokio::sync::broadcast::Receiver<()>,
    is_binary: bool,
    is_json: bool,
    idle_close_secs: u64,
) -> impl futures_util::stream::Stream<Item = std::result::Result<String, std::convert::Infallible>> + Send
{
    async_stream::stream! {
        let read_result = initial_read;

        // Emit initial data + control
        for msg in &read_result.messages {
            yield Ok(sse::format_data_frame(msg, is_binary, is_json));
        }

        let control = build_sse_control(&read_result);
        yield Ok(sse::format_control_frame(&control));

        // If closed at tail, we're done
        if read_result.closed && read_result.at_tail {
            return;
        }

        // Enter wait loop at tail
        let mut tail_offset = read_result.next_offset;
        let idle_timeout = if idle_close_secs > 0 {
            Some(Duration::from_secs(idle_close_secs))
        } else {
            None
        };
        let mut idle_deadline = idle_timeout.map(|timeout| Instant::now() + timeout);

        let keepalive_interval = Duration::from_secs(15);

        loop {
            tokio::select! {
                recv_result = receiver.recv() => {
                    match recv_result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // New data or we fell behind — re-read from tail
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // Channel closed — final read + emit + end
                            if let Ok(rr) = storage.read(&name, &tail_offset) {
                                for msg in &rr.messages {
                                    yield Ok(sse::format_data_frame(msg, is_binary, is_json));
                                }
                                let ctrl = build_sse_control(&rr);
                                yield Ok(sse::format_control_frame(&ctrl));
                            }
                            return;
                        }
                    }
                }
                () = tokio::time::sleep(keepalive_interval) => {
                    // Keep-alive comment frame
                    yield Ok(sse::format_keepalive_frame().to_string());
                    continue;
                }
                () = async {
                    match idle_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Idle close per PROTOCOL.md §5.8
                    return;
                }
            }

            // Re-read from tail position
            let Ok(rr) = storage.read(&name, &tail_offset) else {
                return;
            };

            for msg in &rr.messages {
                yield Ok(sse::format_data_frame(msg, is_binary, is_json));
            }

            let ctrl = build_sse_control(&rr);
            yield Ok(sse::format_control_frame(&ctrl));

            // Keep idle-close tied to stream activity (new data), not keepalive ticks.
            if !rr.messages.is_empty()
                && let Some(timeout) = idle_timeout
            {
                idle_deadline = Some(Instant::now() + timeout);
            }

            if rr.closed && rr.at_tail {
                return;
            }

            tail_offset = rr.next_offset;
        }
    }
}

/// Build a `ControlPayload` from a `ReadResult`.
fn build_sse_control(read_result: &ReadResult) -> ControlPayload {
    let is_closed_at_tail = read_result.closed && read_result.at_tail;

    ControlPayload {
        stream_next_offset: read_result.next_offset.to_string(),
        stream_cursor: if is_closed_at_tail {
            None
        } else {
            Some(cursor::generate(&read_result.next_offset))
        },
        up_to_date: if read_result.at_tail {
            Some(true)
        } else {
            None
        },
        stream_closed: if is_closed_at_tail { Some(true) } else { None },
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
    let end_offset = read_result.next_offset.as_str();
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
        axum::http::HeaderValue::from_bytes(read_result.next_offset.as_str().as_bytes()).unwrap(),
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
        axum::http::HeaderValue::from_bytes(read_result.next_offset.as_str().as_bytes()).unwrap(),
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
        axum::http::HeaderValue::from_bytes(next_offset.as_str().as_bytes()).unwrap(),
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
        json_mode::wrap_read_iter(read_result.messages.iter())
    } else if read_result.messages.is_empty() {
        Ok(bytes::Bytes::new())
    } else {
        let total_len: usize = read_result.messages.iter().map(bytes::Bytes::len).sum();
        let mut buf = BytesMut::with_capacity(total_len);
        for message in &read_result.messages {
            buf.put(message.clone());
        }
        Ok(buf.freeze())
    }
}
