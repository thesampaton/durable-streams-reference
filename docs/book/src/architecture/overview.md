# Architecture overview

The full deployment has five components. Each solves one problem and composes cleanly with the others.

```
                    ┌──────────────────────────────────────────────┐
                    │              Docker Compose Stack            │
                    │                                              │
 Client ──────────►│  Envoy (:8080)  ──►  DS Server (:4437)      │
 (browser/curl)    │  JWT validation      append-only log          │
                    │                      SSE delivery             │
                    │                          │                    │
                    │                     Sync Service              │
                    │                      ▲         │              │
                    │                      │         ▼              │
                    │              Electric SQL    Postgres         │
                    │              (WAL reader)    (durable store)  │
                    └──────────────────────────────────────────────┘
```

## Why each piece exists

| Component | Problem it solves |
|-----------|-------------------|
| **DS server** | Real-time append-only log with SSE, offset resumption, and producer idempotency. Configurable storage (memory, file, acid/redb). |
| **Envoy proxy** | JWT authentication. The DS server has no auth, so Envoy validates tokens and forwards the `sub` claim as `X-JWT-Sub`. |
| **Postgres** | Durable storage and SQL querying. Even with persistent storage modes, Postgres provides structured access, analytics, and cross-service visibility. |
| **Electric SQL** | Change data capture. Reads the Postgres WAL and exposes a Shape API that delivers table changes as a stream. |
| **Sync service** | Bidirectional bridge. Forwards Postgres changes into DS streams (PG-to-Stream) and DS stream events into Postgres (Stream-to-PG). |

## Data flows

### PG-to-Stream (structured data to real-time)

```
INSERT INTO items → Postgres WAL → Electric Shape API → Sync Service → DS Server → SSE to clients
```

An application writes structured data to Postgres. Electric picks up the WAL change, the sync service receives it via the Shape API, and POSTs it as JSON to a DS stream. Connected clients receive it instantly via SSE.

### Stream-to-PG (real-time events to durable storage)

```
Client POST → DS Server → SSE → Sync Service → INSERT INTO session_events
```

A client appends a session event (chat message, presence update) to a DS stream. The sync service consumes the stream via SSE and inserts each event into Postgres.

## What you can swap

The architecture is composable. Each piece can be replaced independently:

- **Auth proxy:** Envoy is one option. Any reverse proxy that validates JWTs and forwards claims works (nginx, Caddy, cloud load balancers).
- **Storage:** The DS server supports in-memory, file-based, and acid (redb) storage backends, configurable via `DS_STORAGE__MODE`.
- **Sync layer:** Electric SQL is the reference CDC tool. Any WAL reader (Debezium, custom logical replication) could feed the sync service.
- **Database:** Postgres is used here because Electric requires it. The sync service pattern works with any database that supports change notifications.

## Minimal vs. full stack

You do not need the full stack to use the DS server. The server runs standalone:

```bash
cargo run
```

This gives you a working durable streams server with no dependencies. The full stack adds auth, persistence, and sync for production-like deployments.

See [Docker Compose](../deployment/docker-compose.md) for running the full stack and [Quickstart](../quickstart.md) for the minimal server.
