# Production

The Docker stack in this repository is a local development and testing tool. This page covers what to change for production.

## Authentication

Replace the test JWKS with a real identity provider:

- **Test setup:** Envoy validates JWTs against a local file (`e2e/fixtures/jwks.json`) with a committed RSA keypair.
- **Production:** Point Envoy's `remote_jwks` at your identity provider's JWKS endpoint (Auth0, Cognito, Keycloak, etc.). Update the `issuer` and `audiences` to match.

The DS server itself needs no changes. It is auth-agnostic by design.

## Persistent storage

The DS server stores streams in memory. For durability:

- **With the sync layer:** The Stream-to-PG direction already writes events into Postgres. On server restart, the sync service would need to resume from a saved offset (not yet implemented in the reference sync service).
- **Alternative:** Implement a persistent storage backend (Redis, SQLite, Postgres) behind the `Storage` trait. The server's storage layer is pluggable.

## Electric SQL configuration

- Pin the Electric SQL version in your deployment (the stack uses `electricsql/electric:1.4.2`).
- Configure Postgres replication slots carefully. The defaults (`max_wal_senders=10`, `max_replication_slots=10`) work for development but may need tuning for production workloads.
- Ensure `wal_level=logical` is set in your Postgres configuration. This is required for Electric's logical replication.

## Sync service resilience

The reference sync service (`e2e/sync/sync.mjs`) is a starting point, not production-ready:

- **Offset persistence:** The sync service should save its SSE offset (from the `id:` field) to a Postgres table so it can resume after restart without replaying the full stream.
- **Error handling:** Add retries with backoff for Postgres insert failures and SSE reconnection.
- **Scaling:** The sync service is a single process. For high-throughput streams, partition by stream name or run multiple instances with offset coordination.

## Memory limits

Tune the server's memory limits for your workload:

| Variable | Default | Notes |
|----------|---------|-------|
| `MAX_MEMORY_BYTES` | 100 MB | Total across all streams |
| `MAX_STREAM_BYTES` | 10 MB | Per stream |

In production with the sync layer, streams are consumed and can be deleted after sync. The in-memory store acts as a buffer, not long-term storage.

## Monitoring

- Log and alert on sync service errors, SSE reconnections, and PG insert failures.
- The sync service logs events to stdout; aggregate with your preferred log pipeline.
- Use the Envoy admin dashboard (port 9901 in dev) for proxy metrics and connection debugging.
- The DS server logs via `tracing` with configurable levels via `RUST_LOG`.

## CORS

The server defaults to `CORS_ORIGINS=*` (allow all). For production, restrict to your application's domain:

```bash
CORS_ORIGINS=https://app.example.com cargo run
```

Multiple origins can be comma-separated.

## TLS

The DS server does not terminate TLS. In production, terminate TLS at the proxy layer (Envoy, nginx, cloud load balancer) or at a CDN edge.
