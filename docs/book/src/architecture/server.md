# DS server

The durable streams server is the core component. It implements the [durable streams protocol](https://github.com/durable-streams/durable-streams) as an HTTP service: create streams, append messages, read them back with offset resumption, and subscribe for live delivery via long-poll or SSE.

## Design principles

- **Protocol-only.** The server implements the protocol and nothing else. No authentication, no database, no application logic. This makes it composable with external infrastructure.
- **Thin handlers.** HTTP handlers parse requests, validate inputs, call the storage layer, and format responses. No business logic in handlers.
- **Pluggable storage.** Streams can run in `memory`, `file-fast` / `file-durable`, or `acid` mode. `acid` uses sharded redb files for crash-safe ACID commits.
- **Content-agnostic.** The server treats all payloads as opaque bytes (or opaque JSON objects for JSON-mode streams). It does not parse or interpret message content.

## What it does

| Capability | How |
|------------|-----|
| Create / delete streams | PUT / DELETE on `/v1/stream/{name}` |
| Append messages | POST with matching Content-Type |
| Read (catch-up) | GET with offset, returns concatenated data |
| Read (long-poll) | GET with `live=long-poll`, waits for new data |
| Read (SSE) | GET with `live=sse`, streaming delivery |
| Producer idempotency | Producer-ID / Epoch / Seq headers for deduplication |
| JSON mode | Array flattening on append, array wrapping on read |
| TTL / expiry | Per-stream TTL with lazy expiration |
| Stream closure | Immutable close with conflict detection |
| ETag caching | Range-based ETags, 304 Not Modified |

## Configuration

The server is configured entirely through environment variables. See [Configuration](../reference/configuration.md) for the full list.

Key defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `DS_SERVER__PORT` | `4437` | Listen port |
| `DS_SERVER__LONG_POLL_TIMEOUT_SECS` | `30` | Long-poll timeout |
| `DS_SERVER__SSE_RECONNECT_INTERVAL_SECS` | `60` | SSE reconnect interval (0 to disable) |
| `DS_SERVER__CORS_ORIGINS` | `*` | Allowed CORS origins |
| `DS_STORAGE__MODE` | `memory` | Storage backend selection |
| `DS_STORAGE__DATA_DIR` | `./data/streams` | Persistent backend root directory |

## Storage backends

- `memory`: fastest and simplest, no restart durability.
- `file-fast` / `file-durable`: one file per stream append log; durable mode fsyncs each append.
- `acid`: sharded redb backend in `${DATA_DIR}/acid`.
  - Routing is deterministic: `shard = seahash(stream_name) & (shard_count - 1)`.
  - Each shard has an independent redb write lock, so different streams can write concurrently on different shards.
  - Every mutating operation is one redb transaction with immediate durability, providing crash-safe all-or-nothing commits.

## Security headers

The server applies security headers via middleware on all responses:

- `Cache-Control: no-store` (prevents intermediate caches from serving stale data)
- `X-Content-Type-Options: nosniff`
- `Cross-Origin-Resource-Policy: cross-origin`

SSE responses use `Cache-Control: no-cache` instead of `no-store`.

## Memory limits

Two configurable limits prevent unbounded memory growth:

- `DS_LIMITS__MAX_MEMORY_BYTES` (default 100 MB): total memory across all streams
- `DS_LIMITS__MAX_STREAM_BYTES` (default 10 MB): maximum size per stream

When limits are exceeded, appends return `413 Payload Too Large`.

## Health check

`GET /healthz` returns 200 with `"ok"`. This endpoint lives outside the `/v1/stream/` namespace and requires no authentication when behind a proxy.
