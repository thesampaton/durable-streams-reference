use std::env;
use std::time::Duration;

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
        }
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
        }
    }
}

/// Typed wrapper for long-poll timeout, injected via axum `Extension`.
#[derive(Debug, Clone, Copy)]
pub struct LongPollTimeout(pub Duration);

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
    }

    #[test]
    fn test_from_lookup_uses_defaults_when_no_vars() {
        let config = Config::from_lookup(|_| None);
        assert_eq!(config.port, 4437);
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_stream_bytes, 10 * 1024 * 1024);
        assert_eq!(config.cors_origins, "*");
        assert_eq!(config.long_poll_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_from_lookup_parses_all_vars() {
        let get = lookup(&[
            ("PORT", "8080"),
            ("MAX_MEMORY_BYTES", "200000000"),
            ("MAX_STREAM_BYTES", "20000000"),
            ("CORS_ORIGINS", "https://example.com"),
            ("LONG_POLL_TIMEOUT_SECS", "5"),
        ]);
        let config = Config::from_lookup(get);
        assert_eq!(config.port, 8080);
        assert_eq!(config.max_memory_bytes, 200_000_000);
        assert_eq!(config.max_stream_bytes, 20_000_000);
        assert_eq!(config.cors_origins, "https://example.com");
        assert_eq!(config.long_poll_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_from_lookup_ignores_unparseable_values() {
        let get = lookup(&[
            ("PORT", "not-a-number"),
            ("MAX_MEMORY_BYTES", ""),
            ("MAX_STREAM_BYTES", "-1"),
            ("LONG_POLL_TIMEOUT_SECS", "abc"),
        ]);
        let config = Config::from_lookup(get);
        // All fall back to defaults because the values don't parse
        assert_eq!(config.port, 4437);
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_stream_bytes, 10 * 1024 * 1024);
        assert_eq!(config.long_poll_timeout, Duration::from_secs(30));
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
    }

    #[test]
    fn test_long_poll_timeout_newtype() {
        let timeout = LongPollTimeout(Duration::from_secs(10));
        assert_eq!(timeout.0, Duration::from_secs(10));
    }
}
