pub mod memory;

use crate::protocol::error::Result;
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use bytes::Bytes;
use chrono::{DateTime, Utc};
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
#[derive(Debug, Clone, Eq)]
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
