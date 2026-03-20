use figment::{
    Figment,
    providers::{Format, Toml},
};
use axum::http::HeaderValue;
use serde::Deserialize;
use std::env;
use std::path::PathBuf;
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
    /// Default log filter when `RUST_LOG` is not explicitly set.
    pub rust_log: String,
}

#[derive(Debug, Clone)]
pub struct ConfigLoadOptions {
    pub config_dir: PathBuf,
    pub profile: String,
    pub config_override: Option<PathBuf>,
}

impl Default for ConfigLoadOptions {
    fn default() -> Self {
        Self {
            config_dir: PathBuf::from("config"),
            profile: "default".to_string(),
            config_override: None,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct SettingsFile {
    server: ServerSettingsFile,
    limits: LimitsSettingsFile,
    http: HttpSettingsFile,
    storage: StorageSettingsFile,
    tls: TlsSettingsFile,
    log: LogSettingsFile,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ServerSettingsFile {
    port: Option<u16>,
    long_poll_timeout_secs: Option<u64>,
    sse_reconnect_interval_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct LimitsSettingsFile {
    max_memory_bytes: Option<u64>,
    max_stream_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct HttpSettingsFile {
    cors_origins: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct StorageSettingsFile {
    mode: Option<String>,
    data_dir: Option<String>,
    acid_shard_count: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct TlsSettingsFile {
    cert_path: Option<String>,
    key_path: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct LogSettingsFile {
    rust_log: Option<String>,
}

impl Config {
    /// Load configuration from `DS_*` environment variables with sensible defaults.
    ///
    /// Used by tests and as a simple entry point when TOML layering is not needed.
    /// # Errors
    ///
    /// Returns an error when any `DS_*` environment variable is present but invalid.
    pub fn from_env() -> Result<Self, String> {
        let mut config = Self::default();
        config.apply_env_overrides(&|key| env::var(key).ok())?;
        Ok(config)
    }

    /// Load configuration from layered TOML files plus environment overrides.
    ///
    /// Order (later wins):
    /// 1. built-in defaults
    /// 2. `config/default.toml` (if present)
    /// 3. `config/<profile>.toml` (if present)
    /// 4. `config/local.toml` (if present)
    /// 5. `--config <path>` override file (if provided)
    /// 6. `DS_*` env vars
    ///
    /// # Errors
    ///
    /// Returns an error when config files cannot be parsed or an explicit
    /// override file path does not exist/read.
    pub fn from_sources(options: &ConfigLoadOptions) -> Result<Self, String> {
        let get = |key: &str| env::var(key).ok();
        Self::from_sources_with_lookup(options, &get)
    }

    fn from_sources_with_lookup(
        options: &ConfigLoadOptions,
        get: &impl Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let mut figment = Figment::new();

        let default_path = options.config_dir.join("default.toml");
        if default_path.is_file() {
            figment = figment.merge(Toml::file(&default_path));
        }

        let profile_path = options
            .config_dir
            .join(format!("{}.toml", options.profile.trim()));
        if profile_path.is_file() {
            figment = figment.merge(Toml::file(&profile_path));
        }

        let local_path = options.config_dir.join("local.toml");
        if local_path.is_file() {
            figment = figment.merge(Toml::file(&local_path));
        }

        if let Some(override_path) = &options.config_override {
            if !override_path.is_file() {
                return Err(format!(
                    "config override file not found: '{}'",
                    override_path.display()
                ));
            }
            figment = figment.merge(Toml::file(override_path));
        }

        let settings: SettingsFile = figment
            .extract()
            .map_err(|e| format!("failed to parse TOML config: {e}"))?;

        let mut config = Self::apply_file_settings(settings)?;
        config.apply_env_overrides(get)?;
        Ok(config)
    }

    fn apply_file_settings(settings: SettingsFile) -> Result<Self, String> {
        let mut config = Self::default();

        if let Some(port) = settings.server.port {
            config.port = port;
        }
        if let Some(long_poll_timeout_secs) = settings.server.long_poll_timeout_secs {
            config.long_poll_timeout = Duration::from_secs(long_poll_timeout_secs);
        }
        if let Some(sse_reconnect_interval_secs) = settings.server.sse_reconnect_interval_secs {
            config.sse_reconnect_interval_secs = sse_reconnect_interval_secs;
        }

        if let Some(max_memory_bytes) = settings.limits.max_memory_bytes {
            config.max_memory_bytes = max_memory_bytes;
        }
        if let Some(max_stream_bytes) = settings.limits.max_stream_bytes {
            config.max_stream_bytes = max_stream_bytes;
        }

        if let Some(cors_origins) = settings.http.cors_origins {
            config.cors_origins = cors_origins;
        }

        if let Some(mode) = settings.storage.mode {
            config.storage_mode = Self::parse_storage_mode_value(&mode)
                .ok_or_else(|| format!("invalid storage.mode value: '{mode}'"))?;
        }
        if let Some(data_dir) = settings.storage.data_dir {
            config.data_dir = data_dir;
        }
        if let Some(acid_shard_count) = settings.storage.acid_shard_count {
            if Self::valid_acid_shard_count(acid_shard_count) {
                config.acid_shard_count = acid_shard_count;
            } else {
                return Err(format!(
                    "invalid storage.acid_shard_count value: '{acid_shard_count}' (must be power-of-two in 1..=256)"
                ));
            }
        }

        config.tls_cert_path = settings.tls.cert_path;
        config.tls_key_path = settings.tls.key_path;

        if let Some(rust_log) = settings.log.rust_log {
            config.rust_log = rust_log;
        }

        Ok(config)
    }

    /// Apply `DS_*` environment variable overrides on top of current config.
    fn apply_env_overrides(&mut self, get: &impl Fn(&str) -> Option<String>) -> Result<(), String> {
        if let Some(port) = get("DS_SERVER__PORT") {
            self.port = port
                .parse()
                .map_err(|_| "invalid DS_SERVER__PORT value".to_string())?;
        }
        if let Some(long_poll_timeout_secs) = get("DS_SERVER__LONG_POLL_TIMEOUT_SECS") {
            self.long_poll_timeout = Duration::from_secs(
                long_poll_timeout_secs
                    .parse()
                    .map_err(|_| "invalid DS_SERVER__LONG_POLL_TIMEOUT_SECS value".to_string())?,
            );
        }
        if let Some(sse_reconnect_interval_secs) = get("DS_SERVER__SSE_RECONNECT_INTERVAL_SECS") {
            self.sse_reconnect_interval_secs = sse_reconnect_interval_secs.parse().map_err(|_| {
                "invalid DS_SERVER__SSE_RECONNECT_INTERVAL_SECS value".to_string()
            })?;
        }

        if let Some(max_memory_bytes) = get("DS_LIMITS__MAX_MEMORY_BYTES") {
            self.max_memory_bytes = max_memory_bytes
                .parse()
                .map_err(|_| "invalid DS_LIMITS__MAX_MEMORY_BYTES value".to_string())?;
        }
        if let Some(max_stream_bytes) = get("DS_LIMITS__MAX_STREAM_BYTES") {
            self.max_stream_bytes = max_stream_bytes
                .parse()
                .map_err(|_| "invalid DS_LIMITS__MAX_STREAM_BYTES value".to_string())?;
        }

        if let Some(cors_origins) = get("DS_HTTP__CORS_ORIGINS") {
            self.cors_origins = cors_origins;
        }

        if let Some(storage_mode) = get("DS_STORAGE__MODE") {
            self.storage_mode = Self::parse_storage_mode_value(&storage_mode)
                .ok_or_else(|| format!("invalid DS_STORAGE__MODE value: '{storage_mode}'"))?;
        }

        if let Some(data_dir) = get("DS_STORAGE__DATA_DIR") {
            self.data_dir = data_dir;
        }

        if let Some(acid_shard_count) = get("DS_STORAGE__ACID_SHARD_COUNT") {
            let parsed = acid_shard_count
                .parse::<usize>()
                .map_err(|_| "invalid DS_STORAGE__ACID_SHARD_COUNT value".to_string())?;
            if !Self::valid_acid_shard_count(parsed) {
                return Err(format!(
                    "invalid DS_STORAGE__ACID_SHARD_COUNT value: '{acid_shard_count}' (must be power-of-two in 1..=256)"
                ));
            }
            self.acid_shard_count = parsed;
        }

        if let Some(cert_path) = get("DS_TLS__CERT_PATH") {
            self.tls_cert_path = Some(cert_path);
        }
        if let Some(key_path) = get("DS_TLS__KEY_PATH") {
            self.tls_key_path = Some(key_path);
        }

        if let Some(rust_log) = get("DS_LOG__RUST_LOG") {
            self.rust_log = rust_log;
        }

        Ok(())
    }

    /// Validate configuration invariants before server startup.
    ///
    /// # Errors
    ///
    /// Returns an error string when config is internally inconsistent.
    pub fn validate(&self) -> std::result::Result<(), String> {
        let tls_pair_result = match (&self.tls_cert_path, &self.tls_key_path) {
            (Some(_), Some(_)) | (None, None) => Ok(()),
            (Some(_), None) => Err(
                "tls.cert_path is set but tls.key_path is missing; both must be set together"
                    .to_string(),
            ),
            (None, Some(_)) => Err(
                "tls.key_path is set but tls.cert_path is missing; both must be set together"
                    .to_string(),
            ),
        };
        tls_pair_result?;

        Self::validate_cors_origins(&self.cors_origins)?;

        Ok(())
    }

    fn validate_cors_origins(origins: &str) -> Result<(), String> {
        if origins == "*" {
            return Ok(());
        }

        let mut parsed_any = false;
        for origin in origins.split(',').map(str::trim) {
            if origin.is_empty() {
                return Err("http.cors_origins contains an empty origin entry".to_string());
            }
            HeaderValue::from_str(origin)
                .map_err(|_| format!("invalid http.cors_origins entry: '{origin}'"))?;
            parsed_any = true;
        }

        if !parsed_any {
            return Err("http.cors_origins must be '*' or a non-empty comma-separated list".to_string());
        }

        Ok(())
    }

    /// True when direct TLS termination is enabled on this server.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
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

    fn valid_acid_shard_count(value: usize) -> bool {
        (1..=256).contains(&value) && value.is_power_of_two()
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
            rust_log: "info".to_string(),
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
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Helper: build a lookup function from key-value pairs.
    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    fn temp_config_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("ds-config-tests-{}-{}", std::process::id(), id));
        fs::create_dir_all(&path).expect("create temp config dir");
        path
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
        assert_eq!(config.rust_log, "info");
    }

    #[test]
    fn test_from_env_uses_defaults_when_no_ds_vars() {
        // from_env reads real env; in test context no DS_* vars are set
        let config = Config::from_env().expect("config from env");
        assert_eq!(config.port, 4437);
        assert_eq!(config.storage_mode, StorageMode::Memory);
        assert_eq!(config.rust_log, "info");
    }

    #[test]
    fn test_env_overrides_parse_all_ds_vars() {
        let mut config = Config::default();
        let get = lookup(&[
            ("DS_SERVER__PORT", "8080"),
            ("DS_LIMITS__MAX_MEMORY_BYTES", "200000000"),
            ("DS_LIMITS__MAX_STREAM_BYTES", "20000000"),
            ("DS_HTTP__CORS_ORIGINS", "https://example.com"),
            ("DS_SERVER__LONG_POLL_TIMEOUT_SECS", "5"),
            ("DS_SERVER__SSE_RECONNECT_INTERVAL_SECS", "120"),
            ("DS_STORAGE__MODE", "file-fast"),
            ("DS_STORAGE__DATA_DIR", "/tmp/ds-store"),
            ("DS_STORAGE__ACID_SHARD_COUNT", "32"),
            ("DS_TLS__CERT_PATH", "/tmp/cert.pem"),
            ("DS_TLS__KEY_PATH", "/tmp/key.pem"),
            ("DS_LOG__RUST_LOG", "debug"),
        ]);
        config.apply_env_overrides(&get).expect("apply env overrides");
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
        assert_eq!(config.rust_log, "debug");
    }

    #[test]
    fn test_env_overrides_reject_unparseable_values() {
        let mut config = Config::default();
        let get = lookup(&[
            ("DS_SERVER__PORT", "not-a-number"),
            ("DS_LIMITS__MAX_MEMORY_BYTES", ""),
            ("DS_SERVER__LONG_POLL_TIMEOUT_SECS", "abc"),
        ]);
        let err = config
            .apply_env_overrides(&get)
            .expect_err("invalid env override should fail");
        assert_eq!(err, "invalid DS_SERVER__PORT value");
        assert_eq!(config.port, 4437);
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.long_poll_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_env_overrides_partial() {
        let mut config = Config::default();
        let get = lookup(&[("DS_SERVER__PORT", "9090")]);
        config.apply_env_overrides(&get).expect("apply env overrides");
        assert_eq!(config.port, 9090);
        // Everything else stays at defaults
        assert_eq!(config.storage_mode, StorageMode::Memory);
        assert_eq!(config.rust_log, "info");
    }

    #[test]
    fn test_from_sources_file_layers_and_env_override() {
        let config_dir = temp_config_dir();
        fs::write(
            config_dir.join("default.toml"),
            r#"
                [server]
                port = 4437
                [storage]
                mode = "memory"
                [log]
                rust_log = "warn"
            "#,
        )
        .expect("write default.toml");

        fs::write(
            config_dir.join("dev.toml"),
            r#"
                [server]
                port = 7777
                [storage]
                mode = "file-fast"
                data_dir = "/tmp/dev-store"
            "#,
        )
        .expect("write dev.toml");

        fs::write(
            config_dir.join("local.toml"),
            r"
                [server]
                port = 8888
            ",
        )
        .expect("write local.toml");

        let options = ConfigLoadOptions {
            config_dir,
            profile: "dev".to_string(),
            config_override: None,
        };

        // DS_SERVER__PORT env override wins over all TOML layers
        let env = lookup(&[("DS_SERVER__PORT", "9999"), ("DS_LOG__RUST_LOG", "debug")]);
        let config = Config::from_sources_with_lookup(&options, &env).expect("config from sources");

        assert_eq!(config.port, 9999);
        assert_eq!(config.storage_mode, StorageMode::FileFast);
        assert_eq!(config.data_dir, "/tmp/dev-store");
        assert_eq!(config.rust_log, "debug");
    }

    #[test]
    fn test_from_sources_env_overrides_toml() {
        let config_dir = temp_config_dir();
        fs::write(
            config_dir.join("default.toml"),
            r#"
                [server]
                port = 4437
                [storage]
                mode = "memory"
            "#,
        )
        .expect("write default.toml");

        let options = ConfigLoadOptions {
            config_dir,
            profile: "default".to_string(),
            config_override: None,
        };

        let env = lookup(&[
            ("DS_SERVER__PORT", "12345"),
            ("DS_STORAGE__MODE", "acid"),
            ("DS_STORAGE__ACID_SHARD_COUNT", "32"),
            ("DS_TLS__CERT_PATH", "/tmp/cert.pem"),
            ("DS_TLS__KEY_PATH", "/tmp/key.pem"),
        ]);
        let config = Config::from_sources_with_lookup(&options, &env).expect("config from sources");

        assert_eq!(config.port, 12345);
        assert_eq!(config.storage_mode, StorageMode::Acid);
        assert_eq!(config.acid_shard_count, 32);
        assert_eq!(config.tls_cert_path.as_deref(), Some("/tmp/cert.pem"));
        assert_eq!(config.tls_key_path.as_deref(), Some("/tmp/key.pem"));
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
        let mut config = Config {
            tls_cert_path: Some("/tmp/cert.pem".to_string()),
            ..Config::default()
        };
        assert!(config.validate().is_err());

        config.tls_cert_path = None;
        config.tls_key_path = Some("/tmp/key.pem".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_storage_mode_aliases() {
        let mut config = Config::default();
        config
            .apply_env_overrides(&lookup(&[("DS_STORAGE__MODE", "acid")]))
            .expect("apply env overrides");
        assert_eq!(config.storage_mode, StorageMode::Acid);

        let mut config = Config::default();
        config
            .apply_env_overrides(&lookup(&[("DS_STORAGE__MODE", "redb")]))
            .expect("apply env overrides");
        assert_eq!(config.storage_mode, StorageMode::Acid);
    }

    #[test]
    fn test_acid_shard_count_valid_values() {
        let mut config = Config::default();
        config
            .apply_env_overrides(&lookup(&[("DS_STORAGE__ACID_SHARD_COUNT", "1")]))
            .expect("apply env overrides");
        assert_eq!(config.acid_shard_count, 1);

        let mut config = Config::default();
        config
            .apply_env_overrides(&lookup(&[("DS_STORAGE__ACID_SHARD_COUNT", "256")]))
            .expect("apply env overrides");
        assert_eq!(config.acid_shard_count, 256);
    }

    #[test]
    fn test_acid_shard_count_invalid_values_return_error() {
        let mut config = Config::default();
        let err = config
            .apply_env_overrides(&lookup(&[("DS_STORAGE__ACID_SHARD_COUNT", "0")]))
            .expect_err("invalid shard count should fail");
        assert_eq!(
            err,
            "invalid DS_STORAGE__ACID_SHARD_COUNT value: '0' (must be power-of-two in 1..=256)"
        );
        assert_eq!(config.acid_shard_count, 16);

        let mut config = Config::default();
        let err = config
            .apply_env_overrides(&lookup(&[("DS_STORAGE__ACID_SHARD_COUNT", "3")]))
            .expect_err("invalid shard count should fail");
        assert_eq!(
            err,
            "invalid DS_STORAGE__ACID_SHARD_COUNT value: '3' (must be power-of-two in 1..=256)"
        );
        assert_eq!(config.acid_shard_count, 16);

        let mut config = Config::default();
        let err = config
            .apply_env_overrides(&lookup(&[("DS_STORAGE__ACID_SHARD_COUNT", "abc")]))
            .expect_err("invalid shard count should fail");
        assert_eq!(err, "invalid DS_STORAGE__ACID_SHARD_COUNT value");
        assert_eq!(config.acid_shard_count, 16);
    }

    #[test]
    fn test_env_overrides_reject_invalid_storage_mode() {
        let mut config = Config::default();
        let err = config
            .apply_env_overrides(&lookup(&[("DS_STORAGE__MODE", "memroy")]))
            .expect_err("invalid storage mode should fail");
        assert_eq!(err, "invalid DS_STORAGE__MODE value: 'memroy'");
    }

    #[test]
    fn test_validate_rejects_invalid_cors_origins() {
        let config = Config {
            cors_origins: "https://good.example, ,https://other.example".to_string(),
            ..Config::default()
        };
        assert_eq!(
            config.validate().expect_err("invalid cors origins should fail"),
            "http.cors_origins contains an empty origin entry"
        );
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
