pub mod file;
pub mod memory;

use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tokio::sync::broadcast;

/// Stream configuration
///
/// Immutable configuration set at stream creation time.
///
/// Custom `PartialEq`: when `ttl_seconds` is `Some`, `expires_at` is
/// derived from `Utc::now()` and will drift between requests.  The
/// comparison therefore ignores `expires_at` in that case.  When
/// `ttl_seconds` is `None` and `expires_at` was set directly (via
/// `Expires-At` header), the parsed timestamp is stable so we compare it.
#[derive(Debug, Clone, Eq, serde::Serialize, serde::Deserialize)]
pub struct StreamConfig {
    /// Content-Type header value (normalized, lowercase)
    pub content_type: String,
    /// Time-to-live in seconds (optional)
    pub ttl_seconds: Option<u64>,
    /// Absolute expiration time (optional)
    pub expires_at: Option<DateTime<Utc>>,
    /// Whether the stream was created closed
    pub created_closed: bool,
}

impl PartialEq for StreamConfig {
    fn eq(&self, other: &Self) -> bool {
        self.content_type == other.content_type
            && self.ttl_seconds == other.ttl_seconds
            && self.created_closed == other.created_closed
            && if self.ttl_seconds.is_some() {
                // TTL-derived expires_at drifts with Utc::now(); skip comparison
                true
            } else {
                self.expires_at == other.expires_at
            }
    }
}

impl StreamConfig {
    /// Create a new stream config with required fields
    #[must_use]
    pub fn new(content_type: String) -> Self {
        Self {
            content_type,
            ttl_seconds: None,
            expires_at: None,
            created_closed: false,
        }
    }

    /// Set TTL in seconds
    #[must_use]
    pub fn with_ttl(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = Some(ttl_seconds);
        self
    }

    /// Set absolute expiration time
    #[must_use]
    pub fn with_expires_at(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Set created closed flag
    #[must_use]
    pub fn with_created_closed(mut self, created_closed: bool) -> Self {
        self.created_closed = created_closed;
        self
    }
}

/// A message in a stream
#[derive(Debug, Clone)]
pub struct Message {
    /// Message offset (unique identifier within stream)
    pub offset: Offset,
    /// Message data
    pub data: Bytes,
    /// Byte length (for memory tracking)
    pub byte_len: u64,
}

impl Message {
    /// Create a new message
    #[must_use]
    pub fn new(offset: Offset, data: Bytes) -> Self {
        let byte_len = u64::try_from(data.len()).unwrap_or(u64::MAX);
        Self {
            offset,
            data,
            byte_len,
        }
    }
}

/// Read result from storage
#[derive(Debug)]
pub struct ReadResult {
    /// Messages read
    pub messages: Vec<Message>,
    /// Next offset to read from (for resumption)
    pub next_offset: Offset,
    /// Whether we're at the end of the stream
    pub at_tail: bool,
    /// Whether the stream is closed
    pub closed: bool,
}

/// Stream metadata
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    /// Stream configuration
    pub config: StreamConfig,
    /// Next offset that will be assigned
    pub next_offset: Offset,
    /// Whether the stream is closed
    pub closed: bool,
    /// Total bytes stored in this stream
    pub total_bytes: u64,
    /// Number of messages in the stream
    pub message_count: u64,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
}

/// Result of create-stream operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateStreamResult {
    /// A new stream was created.
    Created,
    /// Stream already existed with matching config (idempotent create).
    AlreadyExists,
}

/// Result of atomic create-with-data operation.
///
/// Bundles creation status with a metadata snapshot taken under the
/// same lock hold so the handler never needs a separate `head()` call.
#[derive(Debug)]
pub struct CreateWithDataResult {
    /// Whether the stream was newly created or already existed.
    pub status: CreateStreamResult,
    /// Next offset (for `Stream-Next-Offset` response header).
    pub next_offset: Offset,
    /// Whether the stream is closed (for `Stream-Closed` response header).
    pub closed: bool,
}

/// Result of an append with producer sequencing.
///
/// Includes a snapshot of stream state taken atomically with the operation
/// so handlers never need a separate `head()` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerAppendResult {
    /// New data accepted (200 OK)
    Accepted {
        epoch: u64,
        seq: u64,
        next_offset: Offset,
        closed: bool,
    },
    /// Duplicate detected, data already persisted (204 No Content)
    Duplicate {
        epoch: u64,
        seq: u64,
        next_offset: Offset,
        closed: bool,
    },
}

// ── Shared constants and validation helpers ─────────────────────────
//
// Extracted from InMemoryStorage/FileStorage to avoid duplication.
// Both implementations delegate to these for content-type, seq,
// producer, and expiry validation.

/// Duration after which stale producer state is cleaned up (7 days).
pub(crate) const PRODUCER_STATE_TTL_SECS: i64 = 7 * 24 * 60 * 60;

/// Broadcast channel capacity for long-poll/SSE notifications.
/// Small because notifications are hints (no payload), not data delivery.
pub(crate) const NOTIFY_CHANNEL_CAPACITY: usize = 16;

/// Per-producer state tracked within a stream.
///
/// Shared between storage implementations. Includes serde derives
/// for the file-backed storage which persists this to disk.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProducerState {
    pub epoch: u64,
    pub last_seq: u64,
    pub updated_at: DateTime<Utc>,
}

/// Outcome of producer validation before any mutation.
pub(crate) enum ProducerCheck {
    /// Request is valid; proceed with append.
    Accept,
    /// Request is a duplicate; return idempotent success.
    Duplicate { epoch: u64, seq: u64 },
}

/// Check if a stream has expired based on its configuration.
pub(crate) fn is_stream_expired(config: &StreamConfig) -> bool {
    config
        .expires_at
        .is_some_and(|expires_at| Utc::now() >= expires_at)
}

/// Validate content-type matches the stream's configured type (case-insensitive).
pub(crate) fn validate_content_type(stream_ct: &str, request_ct: &str) -> Result<()> {
    if !request_ct.eq_ignore_ascii_case(stream_ct) {
        return Err(Error::ContentTypeMismatch {
            expected: stream_ct.to_string(),
            actual: request_ct.to_string(),
        });
    }
    Ok(())
}

/// Validate Stream-Seq ordering and return the pending value to commit.
///
/// Returns `Err(SeqOrderingViolation)` if the new seq is not strictly
/// greater than the last seq (lexicographic comparison).
pub(crate) fn validate_seq(
    last_seq: Option<&str>,
    new_seq: Option<&str>,
) -> Result<Option<String>> {
    if let Some(new) = new_seq {
        if let Some(last) = last_seq
            && new <= last
        {
            return Err(Error::SeqOrderingViolation {
                last: last.to_string(),
                received: new.to_string(),
            });
        }
        return Ok(Some(new.to_string()));
    }
    Ok(None)
}

/// Remove producer state entries older than `PRODUCER_STATE_TTL_SECS`.
pub(crate) fn cleanup_stale_producers(producers: &mut HashMap<String, ProducerState>) {
    let cutoff = Utc::now()
        - chrono::TimeDelta::try_seconds(PRODUCER_STATE_TTL_SECS)
            .expect("7 days fits in TimeDelta");
    producers.retain(|_, state| state.updated_at > cutoff);
}

/// Validate producer epoch/sequence against existing state.
///
/// Implements the standard validation order:
///   1. Epoch fencing (403)
///   2. Duplicate detection (204) — before closed check so retries work
///   3. Closed check (409) — blocks new sequences on closed streams
///   4. Gap / epoch-bump validation
pub(crate) fn check_producer(
    existing: Option<&ProducerState>,
    producer: &ProducerHeaders,
    stream_closed: bool,
) -> Result<ProducerCheck> {
    if let Some(state) = existing {
        if producer.epoch < state.epoch {
            return Err(Error::EpochFenced {
                current: state.epoch,
                received: producer.epoch,
            });
        }

        if producer.epoch == state.epoch && producer.seq <= state.last_seq {
            return Ok(ProducerCheck::Duplicate {
                epoch: state.epoch,
                seq: state.last_seq,
            });
        }

        // Not a duplicate — if stream is closed, reject
        if stream_closed {
            return Err(Error::StreamClosed);
        }

        if producer.epoch > state.epoch {
            if producer.seq != 0 {
                return Err(Error::InvalidProducerState(
                    "new epoch must start at seq 0".to_string(),
                ));
            }
        } else if producer.seq > state.last_seq + 1 {
            return Err(Error::SequenceGap {
                expected: state.last_seq + 1,
                actual: producer.seq,
            });
        }
    } else {
        // New producer
        if stream_closed {
            return Err(Error::StreamClosed);
        }
        if producer.seq != 0 {
            return Err(Error::SequenceGap {
                expected: 0,
                actual: producer.seq,
            });
        }
    }
    Ok(ProducerCheck::Accept)
}

/// Storage trait for stream persistence
///
/// Methods are intentionally sync (not async) to keep the core logic simple.
/// Async boundaries are at the handler layer and notification layer.
///
/// Implementations must be thread-safe (Send + Sync).
///
/// Error conditions are documented inline rather than in separate sections
/// to avoid repetitive documentation on internal trait methods.
#[allow(clippy::missing_errors_doc)]
pub trait Storage: Send + Sync {
    /// Create a new stream
    ///
    /// Returns whether the stream was newly created or already existed.
    ///
    /// Returns `Err(Error::ConfigMismatch)` if stream exists with different config.
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult>;

    /// Append data to a stream
    ///
    /// Generates and returns the offset for this message.
    /// Offsets must be monotonically increasing.
    ///
    /// Returns `Err(Error::StreamClosed)` if stream is closed.
    /// Returns `Err(Error::ContentTypeMismatch)` if content type doesn't match.
    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset>;

    /// Append multiple messages atomically
    ///
    /// All messages are validated and committed as a single atomic operation.
    /// Either all messages are appended successfully, or none are.
    /// Returns the next offset (the offset that will be assigned to the
    /// next message appended after this batch).
    ///
    /// If `seq` is `Some`, validates lexicographic ordering against the
    /// stream's last seq and updates it on success.
    ///
    /// Returns `Err(Error::StreamClosed)` if stream is closed.
    /// Returns `Err(Error::ContentTypeMismatch)` if content type doesn't match.
    /// Returns `Err(Error::SeqOrderingViolation)` if seq <= last seq.
    /// Returns `Err(Error::MemoryLimitExceeded)` if batch would exceed limits.
    fn batch_append(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        seq: Option<&str>,
    ) -> Result<Offset>;

    /// Read messages from a stream starting at offset
    ///
    /// If `from_offset` is `Offset::start()`, reads from beginning.
    /// If `from_offset` is `Offset::now()`, returns empty result at tail.
    ///
    /// Returns `Err(Error::NotFound)` if stream doesn't exist.
    /// Returns `Err(Error::InvalidOffset)` if offset is invalid.
    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult>;

    /// Delete a stream
    ///
    /// Returns `Ok(())` on successful deletion.
    /// Returns `Err(Error::NotFound)` if stream doesn't exist.
    fn delete(&self, name: &str) -> Result<()>;

    /// Get stream metadata
    ///
    /// Returns `Err(Error::NotFound)` if stream doesn't exist.
    fn head(&self, name: &str) -> Result<StreamMetadata>;

    /// Close a stream
    ///
    /// Prevents further appends.
    /// Returns `Ok(())` if already closed (idempotent).
    /// Returns `Err(Error::NotFound)` if stream doesn't exist.
    fn close_stream(&self, name: &str) -> Result<()>;

    /// Append messages with producer sequencing (atomic validation + append)
    ///
    /// Validates producer epoch/sequence, appends data if accepted, and
    /// optionally closes the stream — all within a single lock hold.
    ///
    /// Returns `ProducerAppendResult::Accepted` for new data (200 OK).
    /// Returns `ProducerAppendResult::Duplicate` for already-seen seq (204).
    /// Returns `Err(EpochFenced)` if epoch < current (403).
    /// Returns `Err(SequenceGap)` if seq > expected (409).
    /// Returns `Err(InvalidProducerState)` if epoch bump with seq != 0 (400).
    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        producer: &ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult>;

    /// Atomically create a stream with optional initial data and close.
    ///
    /// Creates the stream, appends `messages` (if non-empty), and closes
    /// (if `should_close`) — all before the entry becomes visible to other
    /// operations. If `commit_messages` fails (e.g. memory limit), the
    /// stream is never created.
    ///
    /// For idempotent recreates (`AlreadyExists`), the body and close
    /// flag are ignored and existing metadata is returned.
    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult>;

    /// Check if a stream exists
    fn exists(&self, name: &str) -> bool;

    /// Subscribe to notifications for new data on a stream.
    ///
    /// Returns a broadcast receiver that fires when data is appended
    /// or the stream is closed. Returns `None` if the stream does not
    /// exist or has expired.
    ///
    /// The method itself is sync; the handler awaits on the receiver.
    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>>;
}
