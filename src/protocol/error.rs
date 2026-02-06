use thiserror::Error;

/// Single error type for all storage and protocol operations
///
/// Maps to HTTP status codes in handlers. Following the single error enum
/// pattern to avoid error type proliferation.
#[derive(Debug, Error)]
pub enum Error {
    /// Stream not found (404)
    #[error("Stream not found: {0}")]
    NotFound(String),

    /// Stream already exists with different config (409)
    #[error("Stream already exists with different configuration")]
    ConfigMismatch,

    /// Stream already exists (used for idempotent operations)
    #[error("Stream already exists: {0}")]
    AlreadyExists(String),

    /// Invalid offset format (400)
    #[error("Invalid offset format: {0}")]
    InvalidOffset(String),

    /// Invalid stream name (400)
    #[error("Invalid stream name: {0}")]
    InvalidStreamName(String),

    /// Content type mismatch (409)
    #[error("Content type mismatch: expected {expected}, got {actual}")]
    ContentTypeMismatch { expected: String, actual: String },

    /// Stream is closed (409)
    #[error("Stream is closed")]
    StreamClosed,

    /// Producer sequence regression (409)
    #[error("Producer sequence regression: expected > {expected}, got {actual}")]
    SequenceRegression { expected: u64, actual: u64 },

    /// Producer sequence gap (409)
    #[error("Producer sequence gap: expected {expected}, got {actual}")]
    SequenceGap { expected: u64, actual: u64 },

    /// Producer epoch fenced (403)
    #[error("Producer epoch fenced: current {current}, received {received}")]
    EpochFenced { current: u64, received: u64 },

    /// Invalid producer state (400)
    #[error("Invalid producer state: {0}")]
    InvalidProducerState(String),

    /// Memory limit exceeded (413)
    #[error("Memory limit exceeded")]
    MemoryLimitExceeded,

    /// Stream size limit exceeded (413)
    #[error("Stream size limit exceeded")]
    StreamSizeLimitExceeded,

    /// Invalid TTL format (400)
    #[error("Invalid TTL format: {0}")]
    InvalidTtl(String),

    /// Both TTL and Expires-At provided (400)
    #[error("Cannot specify both TTL and Expires-At")]
    ConflictingExpiration,

    /// Stream has expired (404)
    #[error("Stream has expired")]
    StreamExpired,

    /// Invalid JSON (400)
    #[error("Invalid JSON: {0}")]
    InvalidJson(String),

    /// Empty request body when data expected (400)
    #[error("Empty request body")]
    EmptyBody,

    /// Invalid header value (400)
    #[error("Invalid header value for {header}: {reason}")]
    InvalidHeader { header: String, reason: String },
}

impl Error {
    /// Map error to HTTP status code
    ///
    /// This is the single place where errors are mapped to status codes.
    /// Handlers should use this method to determine the response code.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::NotFound(_) | Self::StreamExpired => 404,
            Self::ConfigMismatch
            | Self::ContentTypeMismatch { .. }
            | Self::StreamClosed
            | Self::SequenceRegression { .. }
            | Self::SequenceGap { .. } => 409,
            Self::EpochFenced { .. } => 403,
            Self::MemoryLimitExceeded | Self::StreamSizeLimitExceeded => 413,
            Self::AlreadyExists(_)
            | Self::InvalidOffset(_)
            | Self::InvalidStreamName(_)
            | Self::InvalidProducerState(_)
            | Self::InvalidTtl(_)
            | Self::ConflictingExpiration
            | Self::InvalidJson(_)
            | Self::EmptyBody
            | Self::InvalidHeader { .. } => 400,
        }
    }
}

/// Result type alias for storage and protocol operations
pub type Result<T> = std::result::Result<T, Error>;

/// Convert Error to HTTP response
impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = axum::http::StatusCode::from_u16(self.status_code())
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);

        let body = self.to_string();

        (status, body).into_response()
    }
}
