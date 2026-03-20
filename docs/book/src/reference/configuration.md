# Configuration

The DS server supports layered TOML configuration plus env-var overrides.

Load order (later wins):

1. built-in defaults
2. `config/default.toml` (if present)
3. `config/<profile>.toml` (if present; selected with `--profile`)
4. `config/local.toml` (if present; intended for local overrides)
5. `--config <path>` override file
6. environment variables

CLI options:

| Flag | Description |
|------|-------------|
| `--profile <name>` | Loads `config/<name>.toml` |
| `--config <path>` | Loads an additional TOML file last |

Sample config files in this repo:
- `config/default.toml` (baseline defaults)
- `config/dev.toml` (development overrides)
- `config/prod.toml` (production example)

Example:

```bash
cargo run -- --profile dev
cargo run -- --profile prod --config /etc/durable-streams/override.toml
```

Environment variables use the `DS_` prefix with double-underscore section separators (e.g., `DS_SERVER__PORT`).

## Server

| Variable | Default | Description |
|----------|---------|-------------|
| `DS_SERVER__PORT` | `4437` | TCP port to listen on |
| `DS_HTTP__CORS_ORIGINS` | `*` | Allowed CORS origins. `*` allows all. Multiple origins can be comma-separated (e.g., `https://app.example.com,https://admin.example.com`). |
| `RUST_LOG` | `info` | Log level filter ([tracing](https://docs.rs/tracing-subscriber) format: `debug`, `info`, `warn`, `error`, or per-module like `durable_streams=debug`). Takes precedence over `DS_LOG__RUST_LOG`. |
| `DS_LOG__RUST_LOG` | `info` | Default log level filter, applied through the TOML config layer. Overridden by `RUST_LOG` when both are set. |

## Transport (optional direct TLS)

| Variable | Default | Description |
|----------|---------|-------------|
| `DS_TLS__CERT_PATH` | _unset_ | Path to PEM certificate for direct TLS termination. Must be set together with `DS_TLS__KEY_PATH`. |
| `DS_TLS__KEY_PATH` | _unset_ | Path to PEM/PKCS#8 private key for direct TLS termination. Must be set together with `DS_TLS__CERT_PATH`. |

## Protocol

| Variable | Default | Description |
|----------|---------|-------------|
| `DS_SERVER__LONG_POLL_TIMEOUT_SECS` | `30` | How long to hold a long-poll request before returning `204 No Content`. Set lower (e.g., `2`) for fast-feedback testing. |
| `DS_SERVER__SSE_RECONNECT_INTERVAL_SECS` | `60` | SSE reconnect interval in seconds (matches Caddy's `sse_reconnect_interval`). Enables CDN request collapsing. Set to `0` to disable. |

## Memory limits

| Variable | Default | Description |
|----------|---------|-------------|
| `DS_LIMITS__MAX_MEMORY_BYTES` | `104857600` (100 MB) | Maximum total memory across all streams. Appends exceeding this return `413 Payload Too Large`. |
| `DS_LIMITS__MAX_STREAM_BYTES` | `10485760` (10 MB) | Maximum bytes per individual stream. |

## Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `DS_STORAGE__MODE` | `memory` | Storage backend mode: `memory`, `file-fast`, `file-durable`, `acid` (alias: `redb`). |
| `DS_STORAGE__DATA_DIR` | `./data/streams` | Root directory for persistent backends (`file-*`, `acid`). |
| `DS_STORAGE__ACID_SHARD_COUNT` | `16` | Number of redb shards when `DS_STORAGE__MODE=acid`; must be power-of-2 in `1..=256` (invalid values return an error). |

## Sync service (e2e stack)

These variables configure the sync service container, not the DS server itself:

| Variable | Default | Description |
|----------|---------|-------------|
| `ELECTRIC_URL` | `http://electric:3000` | Electric SQL Shape API base URL |
| `DS_SERVER_URL` | `http://server:4437` | DS server URL (internal Docker network) |
| `POSTGRES_URL` | `postgresql://postgres:password@postgres:5432/durable_streams` | Postgres connection string |

## Example

```bash
# Development (fast timeouts for testing)
DS_SERVER__LONG_POLL_TIMEOUT_SECS=2 DS_SERVER__SSE_RECONNECT_INTERVAL_SECS=5 cargo run

# Development profile from TOML
cargo run -- --profile dev

# Production (restricted CORS, custom port)
DS_SERVER__PORT=8080 DS_HTTP__CORS_ORIGINS=https://app.example.com cargo run

# Optional direct TLS (proxy->server encryption or direct serving)
DS_TLS__CERT_PATH=/etc/ds/tls/server.crt DS_TLS__KEY_PATH=/etc/ds/tls/server.key cargo run

# Debug logging
RUST_LOG=debug cargo run
```
