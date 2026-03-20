# Sync layer

The DS server supports multiple storage backends (memory, file, acid/redb). For SQL querying and cross-service visibility, you need a sync layer that bridges DS streams and a database bidirectionally.

## The pattern

A sync service sits between the DS server and a database, copying data in both directions:

**Database-to-Stream:** A change data capture (CDC) tool watches the database for new rows. The sync service receives change events and POSTs them to a DS stream. Connected clients get the changes via SSE.

```
App writes to DB → CDC tool detects change → Sync service → POST to DS stream → SSE to clients
```

**Stream-to-Database:** The sync service subscribes to a DS stream via SSE. Each event is parsed and inserted into the database.

```
Client POSTs to DS stream → SSE → Sync service → INSERT into database
```

### Why both directions

Neither direction is optional for a production session system:

- **Without Stream-to-DB:** Session events live only in DS memory. Server restart loses everything. No SQL queries, no analytics.
- **Without DB-to-Stream:** Structured data changes (from CRUD APIs, admin tools, batch jobs) never reach connected clients in real time.

### Technology choices

The sync layer has three slots to fill:

| Slot | Role | Options |
|------|------|---------|
| **Database** | Durable storage with change notifications | Postgres, MySQL, MongoDB, any DB with CDC support |
| **CDC tool** | Watches DB changes, exposes them as a stream | Electric SQL, Debezium, custom logical replication |
| **Sync service** | Bridges CDC events and DS streams | Custom process (any language) |

The DS server is agnostic to all three. It only sees HTTP requests.

### Database requirements

For the CDC direction (DB-to-Stream), the database needs to support change notifications. For Postgres, this means:

- `wal_level=logical` for logical replication
- Sufficient `max_wal_senders` and `max_replication_slots`

### Sync service requirements

A sync service needs to:

1. Subscribe to the CDC tool's change feed
2. POST changes as JSON arrays to a DS stream (using [JSON mode](../protocol/json-mode.md))
3. Subscribe to a DS stream via SSE for the reverse direction
4. Parse SSE data events and insert into the database
5. (Production) persist its SSE offset so it can resume after restart

### Latency

Typical latencies in a local stack:

- **DB-to-Stream:** under 1 second. CDC replication latency is the bottleneck.
- **Stream-to-DB:** under 100ms. DS broadcast + SSE delivery + DB insert.

## In this repository

The e2e harness uses Postgres + Electric SQL + a Node.js sync service.

### Components

| Component | Image / location | Role |
|-----------|-----------------|------|
| Postgres | `postgres:17-alpine` (port 54321) | Source and sink tables |
| Electric SQL | `electricsql/electric:1.4.2` | Reads Postgres WAL, exposes Shape API |
| Sync service | `e2e/sync/sync.mjs` | Bridges Electric Shape API and DS streams |

### Postgres schema

```sql
-- Source table for DB-to-Stream (application writes here)
CREATE TABLE items (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Sink table for Stream-to-DB (sync service writes here)
CREATE TABLE session_events (
  id SERIAL PRIMARY KEY,
  stream_name TEXT NOT NULL,
  event_key TEXT,
  event_type TEXT,
  operation TEXT,
  payload JSONB NOT NULL,
  ds_offset TEXT,
  received_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Sync service configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ELECTRIC_URL` | `http://electric:3000` | Electric Shape API |
| `DS_SERVER_URL` | `http://server:4437` | DS server URL (internal, no auth). Use `https://...` when direct TLS is enabled on the DS server. |
| `POSTGRES_URL` | `postgresql://postgres:password@postgres:5432/durable_streams` | Postgres connection |

### Running

```bash
docker-compose --profile sync up -d --build
docker-compose logs -f sync-service
make integration-test-sessions
```

## Without the sync layer

The DS server works standalone. If you only need real-time delivery without durable storage:

```bash
cargo run
```

Add the sync layer when you need data surviving restarts, SQL queries, or structured data reaching clients in real time.
