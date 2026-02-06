use crate::protocol::error::{Error, Result};
use crate::protocol::headers::{self, names};
use crate::protocol::json_mode;
use crate::protocol::producer;
use crate::storage::{ProducerAppendResult, Storage};
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
/// Supports idempotent producer semantics when Producer-Id/Epoch/Seq headers
/// are provided.
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

    // Parse optional producer headers
    let producer_headers = producer::parse_producer_headers(&headers)?;

    // Validate body (empty body requires Stream-Closed)
    if body_bytes.is_empty() && !should_close {
        return Err(Error::InvalidHeader {
            header: "Content-Length".to_string(),
            reason: "empty body requires Stream-Closed header".to_string(),
        });
    }

    // Prepare messages for append
    let messages = if body_bytes.is_empty() {
        vec![]
    } else if json_mode::is_json_content_type(&normalized_ct) {
        json_mode::process_append(&body_bytes)?
    } else {
        vec![body_bytes]
    };

    // Route to producer or non-producer append path
    if let Some(ref prod) = producer_headers {
        handle_producer_append(
            &storage,
            &name,
            messages,
            &normalized_ct,
            prod,
            should_close,
        )
    } else {
        handle_non_producer_append(&storage, &name, messages, &normalized_ct, should_close)
    }
}

/// Non-producer append path.
///
/// Always returns 204 No Content on success.
fn handle_non_producer_append<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    messages: Vec<bytes::Bytes>,
    content_type: &str,
    should_close: bool,
) -> Result<Response> {
    let next_offset = if messages.is_empty() {
        storage.head(name)?.next_offset
    } else {
        match storage.batch_append(name, messages, content_type) {
            Ok(next_offset) => next_offset,
            Err(Error::StreamClosed) => {
                return stream_closed_response(storage, name);
            }
            Err(e) => return Err(e),
        }
    };

    if should_close {
        storage.close_stream(name)?;
    }

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        names::STREAM_NEXT_OFFSET,
        next_offset.to_string().parse().unwrap(),
    );

    Ok((StatusCode::NO_CONTENT, response_headers).into_response())
}

/// Producer append path with idempotent sequencing.
///
/// Returns 200 OK for accepted appends, 204 No Content for duplicates.
fn handle_producer_append<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    messages: Vec<bytes::Bytes>,
    content_type: &str,
    producer: &producer::ProducerHeaders,
    should_close: bool,
) -> Result<Response> {
    match storage.append_with_producer(name, messages, content_type, producer, should_close) {
        Ok(result) => {
            let (status, epoch, seq, next_offset, closed) = match result {
                ProducerAppendResult::Accepted {
                    epoch,
                    seq,
                    next_offset,
                    closed,
                } => (StatusCode::OK, epoch, seq, next_offset, closed),
                ProducerAppendResult::Duplicate {
                    epoch,
                    seq,
                    next_offset,
                    closed,
                } => (StatusCode::NO_CONTENT, epoch, seq, next_offset, closed),
            };

            let mut response_headers = HeaderMap::new();
            response_headers.insert(
                names::STREAM_NEXT_OFFSET,
                next_offset.to_string().parse().unwrap(),
            );
            response_headers.insert(names::PRODUCER_EPOCH, epoch.to_string().parse().unwrap());
            response_headers.insert(names::PRODUCER_SEQ, seq.to_string().parse().unwrap());

            if closed {
                response_headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
            }

            Ok((status, response_headers).into_response())
        }
        Err(Error::StreamClosed) => {
            let metadata = storage.head(name)?;
            let mut error_headers = HeaderMap::new();
            error_headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
            error_headers.insert(
                names::STREAM_NEXT_OFFSET,
                metadata.next_offset.to_string().parse().unwrap(),
            );
            Ok((StatusCode::CONFLICT, error_headers, "Stream is closed").into_response())
        }
        Err(Error::EpochFenced { current, .. }) => {
            let mut error_headers = HeaderMap::new();
            error_headers.insert(names::PRODUCER_EPOCH, current.to_string().parse().unwrap());
            Ok((
                StatusCode::FORBIDDEN,
                error_headers,
                "Producer epoch fenced",
            )
                .into_response())
        }
        Err(Error::SequenceGap { expected, actual }) => {
            let mut error_headers = HeaderMap::new();
            error_headers.insert(
                names::PRODUCER_EXPECTED_SEQ,
                expected.to_string().parse().unwrap(),
            );
            error_headers.insert(
                names::PRODUCER_RECEIVED_SEQ,
                actual.to_string().parse().unwrap(),
            );
            Ok((StatusCode::CONFLICT, error_headers, "Producer sequence gap").into_response())
        }
        Err(e) => Err(e),
    }
}

/// Build the 409 Conflict response for a closed stream.
fn stream_closed_response<S: Storage>(storage: &Arc<S>, name: &str) -> Result<Response> {
    let metadata = storage.head(name)?;
    let mut error_headers = HeaderMap::new();
    error_headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
    error_headers.insert(
        names::STREAM_NEXT_OFFSET,
        metadata.next_offset.to_string().parse().unwrap(),
    );
    Ok((StatusCode::CONFLICT, error_headers, "Stream is closed").into_response())
}
