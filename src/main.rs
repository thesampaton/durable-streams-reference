use axum_server::{Handle, tls_rustls::RustlsConfig};
use durable_streams_rust_server::{
    config::{Config, ConfigLoadOptions, StorageMode},
    router,
    storage::{Storage, acid::AcidStorage, file::FileStorage, memory::InMemoryStorage},
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

struct CliArgs {
    profile: String,
    config_override: Option<PathBuf>,
}

impl CliArgs {
    fn parse() -> Result<Self, String> {
        let mut profile = String::from("default");
        let mut config_override: Option<PathBuf> = None;

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                "--profile" => {
                    let value = args
                        .next()
                        .ok_or_else(|| "missing value for --profile".to_string())?;
                    profile = value;
                }
                "--config" => {
                    let value = args
                        .next()
                        .ok_or_else(|| "missing value for --config".to_string())?;
                    config_override = Some(PathBuf::from(value));
                }
                _ if arg.starts_with("--profile=") => {
                    profile = arg.trim_start_matches("--profile=").to_string();
                }
                _ if arg.starts_with("--config=") => {
                    config_override = Some(PathBuf::from(arg.trim_start_matches("--config=")));
                }
                _ => {
                    return Err(format!("unknown argument: {arg}"));
                }
            }
        }

        Ok(Self {
            profile,
            config_override,
        })
    }
}

fn print_usage() {
    eprintln!("Usage: durable-streams-rust-server [--profile <name>] [--config <path>]");
    eprintln!("  --profile <name>  Loads config/<name>.toml after config/default.toml");
    eprintln!("  --config <path>   Loads an extra TOML override file last");
}

struct AppRuntime {
    config: Config,
    addr: SocketAddr,
}

impl AppRuntime {
    fn new(config: Config) -> Result<Self, String> {
        let addr = format!("0.0.0.0:{}", config.port)
            .parse::<SocketAddr>()
            .map_err(|e| format!("failed to parse bind address: {e}"))?;
        Ok(Self { config, addr })
    }

    /// Log startup diagnostics. Extend this with pre-warming, cert reload
    /// setup, or other one-time initialisation when needed.
    fn provision(&self) {
        tracing::info!("Starting durable streams server on {}", self.addr);
        tracing::info!(
            "Max memory: {} bytes, Max per stream: {} bytes",
            self.config.max_memory_bytes,
            self.config.max_stream_bytes
        );
        tracing::info!("Storage mode: {}", self.config.storage_mode.as_str());
        if self.config.tls_enabled() {
            tracing::info!("Transport: direct TLS enabled");
        } else {
            tracing::info!("Transport: plain HTTP (terminate TLS at proxy/edge)");
        }
    }

    fn validate(&self) -> Result<(), String> {
        self.config.validate()?;

        if let (Some(cert), Some(key)) = (&self.config.tls_cert_path, &self.config.tls_key_path) {
            ensure_regular_file(cert)?;
            ensure_regular_file(key)?;
        }

        Ok(())
    }

    fn cleanup() {
        tracing::info!("Runtime cleanup completed");
    }
}

fn ensure_regular_file(path: &str) -> Result<(), String> {
    let metadata = std::fs::metadata(Path::new(path))
        .map_err(|e| format!("failed to stat path '{path}': {e}"))?;
    if !metadata.is_file() {
        return Err(format!("path is not a regular file: '{path}'"));
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = match CliArgs::parse() {
        Ok(cli) => cli,
        Err(err) => {
            eprintln!("{err}");
            print_usage();
            std::process::exit(2);
        }
    };

    let load_options = ConfigLoadOptions {
        profile: cli.profile,
        config_override: cli.config_override,
        ..ConfigLoadOptions::default()
    };

    let config = match Config::from_sources(&load_options) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| config.rust_log.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    if let Err(err) = run(config).await {
        tracing::error!("{err}");
        std::process::exit(1);
    }
}

async fn run(config: Config) -> Result<(), String> {
    let runtime = AppRuntime::new(config)?;
    runtime.provision();
    runtime.validate()?;

    let serve_result = match runtime.config.storage_mode {
        StorageMode::Memory => {
            let storage = Arc::new(InMemoryStorage::new(
                runtime.config.max_memory_bytes,
                runtime.config.max_stream_bytes,
            ));
            serve(storage, &runtime).await
        }
        StorageMode::FileFast | StorageMode::FileDurable => {
            let sync_on_append = runtime.config.storage_mode.sync_on_append();
            tracing::info!(
                "File storage dir: {}, sync on append: {}",
                runtime.config.data_dir,
                sync_on_append
            );
            let storage = Arc::new(
                FileStorage::new(
                    &runtime.config.data_dir,
                    runtime.config.max_memory_bytes,
                    runtime.config.max_stream_bytes,
                    sync_on_append,
                )
                .map_err(|e| format!("Failed to initialize file storage: {e}"))?,
            );
            serve(storage, &runtime).await
        }
        StorageMode::Acid => {
            tracing::info!(
                "Acid storage dir: {}, shards: {}",
                runtime.config.data_dir,
                runtime.config.acid_shard_count
            );
            let storage = Arc::new(
                AcidStorage::new(
                    &runtime.config.data_dir,
                    runtime.config.acid_shard_count,
                    runtime.config.max_memory_bytes,
                    runtime.config.max_stream_bytes,
                )
                .map_err(|e| format!("Failed to initialize acid storage: {e}"))?,
            );
            serve(storage, &runtime).await
        }
    };

    AppRuntime::cleanup();
    serve_result
}

async fn serve<S: Storage + 'static>(storage: Arc<S>, runtime: &AppRuntime) -> Result<(), String> {
    let app = router::build_router(storage, &runtime.config);
    let handle = Handle::new();

    tracing::info!("Server listening on {}", runtime.addr);
    if runtime.config.tls_enabled() {
        tracing::info!("Health check: https://{}/healthz", runtime.addr);
        tracing::info!("Protocol base: https://{}/v1/stream/", runtime.addr);
    } else {
        tracing::info!("Health check: http://{}/healthz", runtime.addr);
        tracing::info!("Protocol base: http://{}/v1/stream/", runtime.addr);
    }

    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("Shutdown signal received, beginning graceful drain");
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
    });

    if let (Some(cert_path), Some(key_path)) =
        (&runtime.config.tls_cert_path, &runtime.config.tls_key_path)
    {
        let tls = RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| format!("failed to load TLS config: {e}"))?;
        axum_server::bind_rustls(runtime.addr, tls)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| format!("server error: {e}"))?;
    } else {
        axum_server::bind(runtime.addr)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| format!("server error: {e}"))?;
    }

    Ok(())
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!("Failed to install Ctrl+C handler: {e}");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                tracing::error!("Failed to install SIGTERM handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
