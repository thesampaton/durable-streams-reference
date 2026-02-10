# Configuration

The DS server is configured entirely through environment variables. All have sensible defaults for local development.

## Server

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `4437` | TCP port to listen on |
| `RUST_LOG` | `info` | Log level filter ([tracing](https://docs.rs/tracing-subscriber) format: `debug`, `info`, `warn`, `error`, or per-module like `durable_streams=debug`) |

## Protocol

| Variable | Default | Description |
|----------|---------|-------------|
| `LONG_POLL_TIMEOUT_SECS` | `30` | How long to hold a long-poll request before returning `204 No Content`. Set lower (e.g., `2`) for fast-feedback testing. |
| `SSE_RECONNECT_INTERVAL_SECS` | `60` | SSE reconnect interval in seconds (matches Caddy's `sse_reconnect_interval`). Enables CDN request collapsing. Set to `0` to disable. |

## Memory limits

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_MEMORY_BYTES` | `104857600` (100 MB) | Maximum total memory across all streams. Appends exceeding this return `413 Payload Too Large`. |
| `MAX_STREAM_BYTES` | `10485760` (10 MB) | Maximum bytes per individual stream. |

## CORS

| Variable | Default | Description |
|----------|---------|-------------|
| `CORS_ORIGINS` | `*` | Allowed CORS origins. `*` allows all. Multiple origins can be comma-separated (e.g., `https://app.example.com,https://admin.example.com`). |

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
LONG_POLL_TIMEOUT_SECS=2 SSE_RECONNECT_INTERVAL_SECS=5 cargo run

# Production (restricted CORS, custom port)
PORT=8080 CORS_ORIGINS=https://app.example.com cargo run

# Debug logging
RUST_LOG=debug cargo run
```
