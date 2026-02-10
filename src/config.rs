use std::env;
use std::time::Duration;

/// Storage runtime mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    /// In-memory backend.
    Memory,
    /// File backend without fsync/fdatasync on every append.
    FileFast,
    /// File backend with fsync/fdatasync on every append.
    FileDurable,
    /// ACID backend using sharded redb databases.
    Acid,
}

impl StorageMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::FileFast => "file-fast",
            Self::FileDurable => "file-durable",
            Self::Acid => "acid",
        }
    }

    #[must_use]
    pub fn uses_file_backend(self) -> bool {
        matches!(self, Self::FileFast | Self::FileDurable)
    }

    #[must_use]
    pub fn sync_on_append(self) -> bool {
        matches!(self, Self::FileDurable)
    }
}

/// Server configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// Port to bind the server to
    pub port: u16,
    /// Maximum total memory usage in bytes
    pub max_memory_bytes: u64,
    /// Maximum bytes per stream
    pub max_stream_bytes: u64,
    /// CORS allowed origins (comma-separated, "*" for all)
    pub cors_origins: String,
    /// Long-poll timeout duration
    pub long_poll_timeout: Duration,
    /// SSE reconnect interval in seconds (0 disables).
    ///
    /// Matches Caddy's `sse_reconnect_interval`. Connections are closed after
    /// this many idle seconds to enable CDN request collapsing.
    pub sse_reconnect_interval_secs: u64,
    /// Selected storage mode
    pub storage_mode: StorageMode,
    /// Root directory for file/acid-backed storage.
    ///
    /// Matches Caddy's `data_dir`.
    pub data_dir: String,
    /// Number of shards for acid/redb storage mode.
    pub acid_shard_count: usize,
    /// Optional TLS certificate path (PEM). Requires `tls_key_path`.
    pub tls_cert_path: Option<String>,
    /// Optional TLS private key path (PEM or PKCS#8). Requires `tls_cert_path`.
    pub tls_key_path: Option<String>,
}

impl Config {
    /// Load configuration from environment variables with sensible defaults
    #[must_use]
    pub fn from_env() -> Self {
        Self::from_lookup(|key| env::var(key).ok())
    }

    /// Build config from an arbitrary key→value lookup function.
    ///
    /// This is the pure core of `from_env()` — testable without touching
    /// the process environment.
    #[must_use]
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let long_poll_secs: u64 = get("LONG_POLL_TIMEOUT_SECS")
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        let sse_reconnect_interval_secs: u64 = get("SSE_RECONNECT_INTERVAL_SECS")
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);
        let storage_mode = Self::parse_storage_mode(&get);
        let acid_shard_count = Self::parse_acid_shard_count(&get);

        Self {
            port: get("PORT").and_then(|s| s.parse().ok()).unwrap_or(4437),
            max_memory_bytes: get("MAX_MEMORY_BYTES")
                .and_then(|s| s.parse().ok())
                .unwrap_or(100 * 1024 * 1024), // 100 MB default
            max_stream_bytes: get("MAX_STREAM_BYTES")
                .and_then(|s| s.parse().ok())
                .unwrap_or(10 * 1024 * 1024), // 10 MB default
            cors_origins: get("CORS_ORIGINS").unwrap_or_else(|| "*".to_string()),
            long_poll_timeout: Duration::from_secs(long_poll_secs),
            sse_reconnect_interval_secs,
            storage_mode,
            data_dir: get("DATA_DIR").unwrap_or_else(|| "./data/streams".to_string()),
            acid_shard_count,
            tls_cert_path: get("TLS_CERT_PATH"),
            tls_key_path: get("TLS_KEY_PATH"),
        }
    }

    /// Validate configuration invariants before server startup.
    ///
    /// # Errors
    ///
    /// Returns an error string when config is internally inconsistent.
    pub fn validate(&self) -> std::result::Result<(), String> {
        match (&self.tls_cert_path, &self.tls_key_path) {
            (Some(_), Some(_)) | (None, None) => Ok(()),
            (Some(_), None) => Err(
                "TLS_CERT_PATH is set but TLS_KEY_PATH is missing; both must be set together"
                    .to_string(),
            ),
            (None, Some(_)) => Err(
                "TLS_KEY_PATH is set but TLS_CERT_PATH is missing; both must be set together"
                    .to_string(),
            ),
        }
    }

    /// True when direct TLS termination is enabled on this server.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
    }

    fn parse_storage_mode(get: &impl Fn(&str) -> Option<String>) -> StorageMode {
        if let Some(mode) = get("STORAGE_MODE")
            && let Some(parsed) = Self::parse_storage_mode_value(&mode)
        {
            return parsed;
        }

        // Backward-compatible fallback for legacy envs.
        let backend = get("STORAGE_BACKEND").unwrap_or_else(|| "memory".to_string());
        if backend.eq_ignore_ascii_case("file") {
            let explicit_sync = get("FILE_STORAGE_SYNC_ON_APPEND").map(|v| Self::parse_bool(&v));
            return if explicit_sync.unwrap_or(true) {
                StorageMode::FileDurable
            } else {
                StorageMode::FileFast
            };
        }

        StorageMode::Memory
    }

    fn parse_storage_mode_value(raw: &str) -> Option<StorageMode> {
        match raw.to_ascii_lowercase().as_str() {
            "memory" => Some(StorageMode::Memory),
            "file" | "file-durable" | "durable" => Some(StorageMode::FileDurable),
            "file-fast" | "fast" => Some(StorageMode::FileFast),
            "acid" | "redb" => Some(StorageMode::Acid),
            _ => None,
        }
    }

    fn parse_acid_shard_count(get: &impl Fn(&str) -> Option<String>) -> usize {
        const DEFAULT: usize = 16;
        const MIN: usize = 1;
        const MAX: usize = 256;

        let Some(raw) = get("ACID_SHARD_COUNT") else {
            return DEFAULT;
        };
        let Ok(value) = raw.parse::<usize>() else {
            return DEFAULT;
        };
        if !(MIN..=MAX).contains(&value) {
            return DEFAULT;
        }
        if !value.is_power_of_two() {
            return DEFAULT;
        }
        value
    }

    fn parse_bool(raw: &str) -> bool {
        matches!(raw, "1" | "true" | "TRUE" | "True")
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 4437,
            max_memory_bytes: 100 * 1024 * 1024,
            max_stream_bytes: 10 * 1024 * 1024,
            cors_origins: "*".to_string(),
            long_poll_timeout: Duration::from_secs(30),
            sse_reconnect_interval_secs: 60,
            storage_mode: StorageMode::Memory,
            data_dir: "./data/streams".to_string(),
            acid_shard_count: 16,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

/// Typed wrapper for long-poll timeout, injected via axum `Extension`.
#[derive(Debug, Clone, Copy)]
pub struct LongPollTimeout(pub Duration);

/// Typed wrapper for SSE reconnect interval in seconds (0 = disabled).
///
/// Matches Caddy's `sse_reconnect_interval`. Injected via axum `Extension`.
#[derive(Debug, Clone, Copy)]
pub struct SseReconnectInterval(pub u64);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Helper: build a lookup function from key-value pairs.
    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.port, 4437);
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_stream_bytes, 10 * 1024 * 1024);
        assert_eq!(config.cors_origins, "*");
        assert_eq!(config.long_poll_timeout, Duration::from_secs(30));
        assert_eq!(config.sse_reconnect_interval_secs, 60);
        assert_eq!(config.storage_mode, StorageMode::Memory);
        assert_eq!(config.data_dir, "./data/streams");
        assert_eq!(config.acid_shard_count, 16);
        assert_eq!(config.tls_cert_path, None);
        assert_eq!(config.tls_key_path, None);
    }

    #[test]
    fn test_from_lookup_uses_defaults_when_no_vars() {
        let config = Config::from_lookup(|_| None);
        assert_eq!(config.port, 4437);
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_stream_bytes, 10 * 1024 * 1024);
        assert_eq!(config.cors_origins, "*");
        assert_eq!(config.long_poll_timeout, Duration::from_secs(30));
        assert_eq!(config.sse_reconnect_interval_secs, 60);
        assert_eq!(config.storage_mode, StorageMode::Memory);
        assert_eq!(config.data_dir, "./data/streams");
        assert_eq!(config.acid_shard_count, 16);
        assert_eq!(config.tls_cert_path, None);
        assert_eq!(config.tls_key_path, None);
    }

    #[test]
    fn test_from_lookup_parses_all_vars() {
        let get = lookup(&[
            ("PORT", "8080"),
            ("MAX_MEMORY_BYTES", "200000000"),
            ("MAX_STREAM_BYTES", "20000000"),
            ("CORS_ORIGINS", "https://example.com"),
            ("LONG_POLL_TIMEOUT_SECS", "5"),
            ("SSE_RECONNECT_INTERVAL_SECS", "120"),
            ("STORAGE_MODE", "file-fast"),
            ("DATA_DIR", "/tmp/ds-store"),
            ("ACID_SHARD_COUNT", "32"),
            ("TLS_CERT_PATH", "/tmp/cert.pem"),
            ("TLS_KEY_PATH", "/tmp/key.pem"),
        ]);
        let config = Config::from_lookup(get);
        assert_eq!(config.port, 8080);
        assert_eq!(config.max_memory_bytes, 200_000_000);
        assert_eq!(config.max_stream_bytes, 20_000_000);
        assert_eq!(config.cors_origins, "https://example.com");
        assert_eq!(config.long_poll_timeout, Duration::from_secs(5));
        assert_eq!(config.sse_reconnect_interval_secs, 120);
        assert_eq!(config.storage_mode, StorageMode::FileFast);
        assert_eq!(config.data_dir, "/tmp/ds-store");
        assert_eq!(config.acid_shard_count, 32);
        assert_eq!(config.tls_cert_path.as_deref(), Some("/tmp/cert.pem"));
        assert_eq!(config.tls_key_path.as_deref(), Some("/tmp/key.pem"));
    }

    #[test]
    fn test_from_lookup_ignores_unparseable_values() {
        let get = lookup(&[
            ("PORT", "not-a-number"),
            ("MAX_MEMORY_BYTES", ""),
            ("MAX_STREAM_BYTES", "-1"),
            ("LONG_POLL_TIMEOUT_SECS", "abc"),
            ("SSE_RECONNECT_INTERVAL_SECS", "xyz"),
        ]);
        let config = Config::from_lookup(get);
        // All fall back to defaults because the values don't parse
        assert_eq!(config.port, 4437);
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_stream_bytes, 10 * 1024 * 1024);
        assert_eq!(config.long_poll_timeout, Duration::from_secs(30));
        assert_eq!(config.sse_reconnect_interval_secs, 60);
        assert_eq!(config.storage_mode, StorageMode::Memory);
        assert_eq!(config.acid_shard_count, 16);
        assert_eq!(config.tls_cert_path, None);
        assert_eq!(config.tls_key_path, None);
    }

    #[test]
    fn test_from_lookup_partial_override() {
        let get = lookup(&[("PORT", "9090")]);
        let config = Config::from_lookup(get);
        assert_eq!(config.port, 9090);
        // Everything else stays at defaults
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_stream_bytes, 10 * 1024 * 1024);
        assert_eq!(config.cors_origins, "*");
        assert_eq!(config.long_poll_timeout, Duration::from_secs(30));
        assert_eq!(config.sse_reconnect_interval_secs, 60);
        assert_eq!(config.storage_mode, StorageMode::Memory);
        assert_eq!(config.acid_shard_count, 16);
        assert_eq!(config.tls_cert_path, None);
        assert_eq!(config.tls_key_path, None);
    }

    #[test]
    fn test_from_env_delegates_to_from_lookup() {
        // Proves from_env() uses the same parsing path as from_lookup
        // by reading the real env through both and comparing field-by-field.
        let via_env = Config::from_env();
        let via_lookup = Config::from_lookup(|key| env::var(key).ok());
        assert_eq!(via_env.port, via_lookup.port);
        assert_eq!(via_env.max_memory_bytes, via_lookup.max_memory_bytes);
        assert_eq!(via_env.max_stream_bytes, via_lookup.max_stream_bytes);
        assert_eq!(via_env.cors_origins, via_lookup.cors_origins);
        assert_eq!(via_env.long_poll_timeout, via_lookup.long_poll_timeout);
        assert_eq!(
            via_env.sse_reconnect_interval_secs,
            via_lookup.sse_reconnect_interval_secs
        );
        assert_eq!(via_env.storage_mode, via_lookup.storage_mode);
        assert_eq!(via_env.data_dir, via_lookup.data_dir);
        assert_eq!(via_env.acid_shard_count, via_lookup.acid_shard_count);
        assert_eq!(via_env.tls_cert_path, via_lookup.tls_cert_path);
        assert_eq!(via_env.tls_key_path, via_lookup.tls_key_path);
    }

    #[test]
    fn test_validate_tls_pair_ok_when_both_absent_or_present() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());
        assert!(!config.tls_enabled());

        config.tls_cert_path = Some("/tmp/cert.pem".to_string());
        config.tls_key_path = Some("/tmp/key.pem".to_string());
        assert!(config.validate().is_ok());
        assert!(config.tls_enabled());
    }

    #[test]
    fn test_validate_tls_pair_rejects_partial_configuration() {
        let mut config = Config::default();
        config.tls_cert_path = Some("/tmp/cert.pem".to_string());
        assert!(config.validate().is_err());

        config.tls_cert_path = None;
        config.tls_key_path = Some("/tmp/key.pem".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_storage_mode_legacy_env_uses_durable_default_for_file() {
        let get = lookup(&[("STORAGE_BACKEND", "file")]);
        let config = Config::from_lookup(get);
        assert_eq!(config.storage_mode, StorageMode::FileDurable);
    }

    #[test]
    fn test_storage_mode_legacy_env_allows_explicit_fast_override() {
        let get = lookup(&[
            ("STORAGE_BACKEND", "file"),
            ("FILE_STORAGE_SYNC_ON_APPEND", "false"),
        ]);
        let config = Config::from_lookup(get);
        assert_eq!(config.storage_mode, StorageMode::FileFast);
    }

    #[test]
    fn test_storage_mode_acid_aliases() {
        let config = Config::from_lookup(lookup(&[("STORAGE_MODE", "acid")]));
        assert_eq!(config.storage_mode, StorageMode::Acid);

        let config = Config::from_lookup(lookup(&[("STORAGE_MODE", "redb")]));
        assert_eq!(config.storage_mode, StorageMode::Acid);
    }

    #[test]
    fn test_acid_shard_count_default() {
        let config = Config::from_lookup(|_| None);
        assert_eq!(config.acid_shard_count, 16);
    }

    #[test]
    fn test_acid_shard_count_valid_values() {
        let config = Config::from_lookup(lookup(&[("ACID_SHARD_COUNT", "1")]));
        assert_eq!(config.acid_shard_count, 1);

        let config = Config::from_lookup(lookup(&[("ACID_SHARD_COUNT", "256")]));
        assert_eq!(config.acid_shard_count, 256);
    }

    #[test]
    fn test_acid_shard_count_invalid_values_fall_back_to_default() {
        let config = Config::from_lookup(lookup(&[("ACID_SHARD_COUNT", "0")]));
        assert_eq!(config.acid_shard_count, 16);

        let config = Config::from_lookup(lookup(&[("ACID_SHARD_COUNT", "3")]));
        assert_eq!(config.acid_shard_count, 16);

        let config = Config::from_lookup(lookup(&[("ACID_SHARD_COUNT", "300")]));
        assert_eq!(config.acid_shard_count, 16);

        let config = Config::from_lookup(lookup(&[("ACID_SHARD_COUNT", "abc")]));
        assert_eq!(config.acid_shard_count, 16);
    }

    #[test]
    fn test_long_poll_timeout_newtype() {
        let timeout = LongPollTimeout(Duration::from_secs(10));
        assert_eq!(timeout.0, Duration::from_secs(10));
    }

    #[test]
    fn test_sse_reconnect_interval_newtype() {
        let interval = SseReconnectInterval(120);
        assert_eq!(interval.0, 120);
    }
}
