pub mod memory;

use crate::protocol::error::Result;
use crate::protocol::offset::Offset;
use bytes::Bytes;
use chrono::{DateTime, Utc};

/// Stream configuration
///
/// Immutable configuration set at stream creation time.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Returns `Ok(())` for idempotent creates with matching config.
    /// Returns `Err(Error::ConfigMismatch)` if stream exists with different config.
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<()>;

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
    /// Returns the offset of the last appended message.
    ///
    /// Returns `Err(Error::StreamClosed)` if stream is closed.
    /// Returns `Err(Error::ContentTypeMismatch)` if content type doesn't match.
    /// Returns `Err(Error::MemoryLimitExceeded)` if batch would exceed limits.
    fn batch_append(&self, name: &str, messages: Vec<Bytes>, content_type: &str) -> Result<Offset>;

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
    /// Returns `Ok(())` even if stream doesn't exist (idempotent).
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

    /// Check if a stream exists
    fn exists(&self, name: &str) -> bool;
}
