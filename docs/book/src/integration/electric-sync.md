# Electric SQL sync

[Electric SQL](https://electric-sql.com/) is a CDC (change data capture) tool for Postgres. It reads the write-ahead log and exposes table changes as an HTTP stream called the Shape API. This page explains the general approach and how the e2e harness implements it.

## The pattern

### How Electric works

```
Application → INSERT → Postgres WAL → Electric (replication slot) → Shape API → Subscribers
```

1. An application writes to a Postgres table.
2. Postgres records the change in its WAL.
3. Electric connects to Postgres via a logical replication slot and reads new WAL entries.
4. Electric exposes changes through its Shape API: an HTTP endpoint that subscribers poll or stream.
5. A sync service subscribes to the Shape API and forwards changes wherever they need to go.

### Postgres requirements

Electric requires logical replication:

```
wal_level = logical
max_wal_senders = 10        # at least 1 per Electric instance
max_replication_slots = 10   # at least 1 per Electric instance
```

These are Postgres server-level settings, not per-database. Without `wal_level=logical`, Electric fails during startup. See [ecosystem interop CI-004](../project/gaps.md) for details.

### Shape API subscription

The Electric client library provides `ShapeStream` for subscribing to table changes:

```javascript
import { ShapeStream } from "@electric-sql/client";

const stream = new ShapeStream({
  url: `${electricUrl}/v1/shape`,
  params: { table: "my_table" },
});

stream.subscribe(async (messages) => {
  const inserts = messages
    .filter(m => m.headers.operation === "insert")
    .map(m => m.value);
  // Forward inserts to DS stream, message queue, etc.
});
```

Each message includes headers (`operation`: insert/update/delete) and the row `value`. The subscriber decides what to do with changes.

### Bridging to a DS stream

To get Postgres changes into a DS stream, the sync service POSTs each batch as a JSON array:

```javascript
if (inserts.length > 0) {
  await fetch(`${dsServerUrl}/v1/stream/${streamName}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(inserts),
  });
}
```

[JSON mode](../protocol/json-mode.md) flattens the array into individual messages. Connected clients receive each change via SSE.

### The reverse direction

Electric handles DB-to-Stream. For Stream-to-DB, the sync service subscribes to a DS stream via SSE and inserts events into Postgres. See [Durable sessions](sessions.md) for the full bidirectional pattern.

### Latency

DB-to-Stream latency depends primarily on Electric's replication lag. In local development this is typically under 1 second. In production it depends on WAL volume, replication slot configuration, and network latency to the Electric instance.

## In this repository

The e2e harness runs Electric as a Docker container connecting to a Postgres instance.

### Docker services

```yaml
postgres:
  image: postgres:17-alpine
  command: [postgres, -c, wal_level=logical, -c, max_wal_senders=10, -c, max_replication_slots=10]
  environment:
    POSTGRES_DB: durable_streams
    POSTGRES_PASSWORD: password
  ports: ["54321:5432"]

electric:
  image: electricsql/electric:1.4.2
  environment:
    DATABASE_URL: postgresql://postgres:password@postgres:5432/durable_streams
  depends_on: [postgres]
```

### Sync service

The reference sync service (`e2e/sync/sync.mjs`) subscribes to the `items` table via Electric and forwards changes to the `pg-items` DS stream.

| Variable | Default | Description |
|----------|---------|-------------|
| `ELECTRIC_URL` | `http://electric:3000` | Electric Shape API |
| `DS_SERVER_URL` | `http://server:4437` | DS server |
| `POSTGRES_URL` | `postgresql://postgres:password@postgres:5432/durable_streams` | Postgres |

### Running

```bash
docker-compose --profile sync up -d --build
docker-compose logs -f sync-service
make integration-test-sessions
```
