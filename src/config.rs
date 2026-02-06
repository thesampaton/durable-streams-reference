use std::env;

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
}

impl Config {
    /// Load configuration from environment variables with sensible defaults
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            port: env::var("PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(4437),
            max_memory_bytes: env::var("MAX_MEMORY_BYTES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100 * 1024 * 1024), // 100 MB default
            max_stream_bytes: env::var("MAX_STREAM_BYTES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10 * 1024 * 1024), // 10 MB default
            cors_origins: env::var("CORS_ORIGINS").unwrap_or_else(|_| "*".to_string()),
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.port, 4437);
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_stream_bytes, 10 * 1024 * 1024);
        assert_eq!(config.cors_origins, "*");
    }

    #[test]
    fn test_from_env_uses_defaults() {
        // Clear any env vars that might interfere
        // Safety: Tests run in isolated environment, single-threaded
        unsafe {
            env::remove_var("PORT");
            env::remove_var("MAX_MEMORY_BYTES");
            env::remove_var("MAX_STREAM_BYTES");
            env::remove_var("CORS_ORIGINS");
        }

        let config = Config::from_env();
        assert_eq!(config.port, 4437);
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_stream_bytes, 10 * 1024 * 1024);
        assert_eq!(config.cors_origins, "*");
    }

    #[test]
    fn test_from_env_respects_env_vars() {
        // Safety: Tests run in isolated environment, single-threaded
        unsafe {
            env::set_var("PORT", "8080");
            env::set_var("MAX_MEMORY_BYTES", "200000000");
            env::set_var("MAX_STREAM_BYTES", "20000000");
            env::set_var("CORS_ORIGINS", "https://example.com");
        }

        let config = Config::from_env();
        assert_eq!(config.port, 8080);
        assert_eq!(config.max_memory_bytes, 200_000_000);
        assert_eq!(config.max_stream_bytes, 20_000_000);
        assert_eq!(config.cors_origins, "https://example.com");

        // Clean up
        // Safety: Tests run in isolated environment, single-threaded
        unsafe {
            env::remove_var("PORT");
            env::remove_var("MAX_MEMORY_BYTES");
            env::remove_var("MAX_STREAM_BYTES");
            env::remove_var("CORS_ORIGINS");
        }
    }
}
