# Durable Sessions architecture

This document describes how to build collaborative real-time applications
(like AI chat sessions) using Durable Streams with bidirectional Postgres
sync. It is the reference architecture for the `e2e/sessions.test.mjs`
integration tests.

## Why this architecture

A durable session has two requirements that no single component satisfies
alone:

1. **Real-time delivery.** Users in a chat session need messages instantly.
   Polling a database is too slow. SSE or WebSockets are the transport.
2. **Durable querying.** Session history must survive server restarts and
   be queryable for analytics, moderation, and search. Postgres is the
   storage.

Durable Streams handles requirement 1: it is an append-only log with SSE
delivery, offset resumption, and producer idempotency. Postgres handles
requirement 2. The sync service bridges them.

```
          Real-time delivery                Durable querying
          ─────────────────                ──────────────────
Client  ◄──── SSE ────  DS Server  ◄──── sync ────  Postgres
Client  ────► POST ───► DS Server  ────► sync ────► Postgres
```

Neither direction is optional:

- **Without Stream→PG:** Session events live only in DS memory. Server
  restart loses everything. No SQL queries, no analytics.
- **Without PG→Stream:** Structured data changes (created by CRUD APIs,
  admin tools, or batch jobs) never reach connected clients in real time.

## Components

### DS server

The protocol server. Stores streams using a configurable backend (memory, file, or acid/redb), delivers data via HTTP GET
(catch-up), long-poll, or SSE. Does not understand session semantics — it
treats all payloads as opaque bytes (or opaque JSON objects for JSON-mode
streams).

Clients interact through an auth proxy (Envoy). The DS server itself has
no authentication.

### Envoy proxy

JWT validation layer. Validates tokens against a JWKS, forwards the `sub`
claim as `X-JWT-Sub`, and proxies all requests to the DS server. Configured
with long route/idle timeouts to support SSE streaming.

### Postgres

Persistent storage with `wal_level=logical` for Electric SQL replication.
Two tables:

- **`items`** — source table for structured data (PG→Stream direction).
  Application CRUD operations write here.
- **`session_events`** — sink table for session events (Stream→PG
  direction). The sync service writes here.

### Electric SQL

Reads the Postgres WAL and exposes a Shape API (HTTP endpoint that streams
table changes). The sync service subscribes to this API to forward changes
into DS streams.

### Sync service

A Node.js process that bridges both directions:

- **PG→Stream:** Subscribes to the Electric Shape API for the `items`
  table. On each change batch, POSTs a JSON array to the `pg-items` DS
  stream.
- **Stream→PG:** Connects to the `session-events` DS stream via SSE
  (`?live=sse&offset=-1`). On each data event, parses the JSON payload
  and INSERTs into the `session_events` Postgres table.

## Data flows

### Direction 1: PG→Stream (structured data to real-time)

```
1. Application INSERT INTO items (title, body) VALUES (...)
2. Postgres WAL records the insert
3. Electric SQL picks up the WAL change via replication slot
4. Electric Shape API delivers the change to subscribers
5. Sync service receives the change via ShapeStream
6. Sync service POSTs the change as JSON array to DS stream "pg-items"
7. DS server stores the message and broadcasts to SSE subscribers
8. Connected clients receive the change via SSE
```

**Latency:** Typically under 1 second end-to-end in the local Docker
stack. In production, Electric replication latency is the bottleneck.

### Direction 2: Stream→PG (real-time events to durable storage)

```
1. Client POSTs a session event (JSON array) to DS stream "session-events"
   through Envoy (authenticated)
2. DS server stores the message and broadcasts to SSE subscribers
3. Sync service receives the SSE data event on its live connection
4. Sync service parses the JSON and extracts event metadata
5. Sync service INSERTs into session_events table
6. Event is now queryable in Postgres
```

**Latency:** Typically under 100ms (DS broadcast + SSE delivery + PG
insert).

## Session event format

The DS server is content-agnostic. All structure lives in the JSON payload.
A session event typically includes:

```json
{
  "key": "user:alice",
  "type": "presence",
  "operation": "set",
  "value": { "status": "online", "lastSeen": "2026-02-09T12:00:00Z" }
}
```

Common event types in a collaborative AI chat:

| type | operation | description |
|------|-----------|-------------|
| `presence` | `set` / `remove` | User join/leave status |
| `chunk` | `append` | Chat message fragment (streaming AI response) |
| `message` | `set` | Complete chat message |
| `reaction` | `set` / `remove` | Emoji reaction on a message |
| `typing` | `set` / `remove` | Typing indicator |

The `key` field identifies the entity (user, message) and the `operation`
field describes the mutation. This follows the STATE-PROTOCOL pattern where
the stream is an ordered log of state mutations that can be replayed to
reconstruct current state.

## JSON-mode streams

Streams created with `Content-Type: application/json` operate in JSON mode:

- **POST body** must be a JSON array. Each element is stored as a separate
  message. The server splits the array during append.
- **GET response** wraps all stored messages in a JSON array.
- **SSE data events** wrap each message in a JSON array:
  `data:[{"key":"..."}]`. Consumers must unwrap the outer array.
  See `docs/ecosystem-interop.md` CI-003.
- **Non-producer POST** returns `204 No Content` (no sequence tracking).
  Producer POST returns `200 OK` for new data, `204` for duplicates.

## SSE consumption pattern

To consume a DS stream via SSE, use the `live=sse` query parameter with an
offset:

```
GET /v1/stream/{name}?live=sse&offset=-1
```

The server returns `Content-Type: text/event-stream` and keeps the
connection open. Events are:

- `event: data` — a stored message (JSON-wrapped for JSON streams)
- `event: control` — metadata: `streamNextOffset`, `streamCursor`,
  `upToDate`, `streamClosed`
- `id:` — the SSE event ID (use as offset for resumption)

The SSE frame format uses `data:value` (no space after colon). For
multi-line data, each line gets a separate `data:` prefix.

### Parsing SSE in Node.js

```javascript
const res = await fetch(`${url}?live=sse&offset=-1`);
const reader = res.body.getReader();
const decoder = new TextDecoder();
let buffer = "";
let currentEventType = null;

while (true) {
  const { done, value } = await reader.read();
  if (done) break;

  buffer += decoder.decode(value, { stream: true });
  const lines = buffer.split("\n");
  buffer = lines.pop(); // keep incomplete line

  for (const line of lines) {
    if (line.startsWith("event:")) {
      currentEventType = line.slice(6).trim();
    } else if (line.startsWith("data:") && currentEventType === "data") {
      const parsed = JSON.parse(line.slice(5));
      // JSON streams: unwrap the array
      const items = Array.isArray(parsed) ? parsed : [parsed];
      for (const item of items) {
        console.log("Received:", item);
      }
    } else if (line.startsWith("id:")) {
      const offset = line.slice(3).trim();
      // Save for resumption
    }
  }
}
```

## Producer semantics

For exactly-once delivery (e.g., ensuring a chat message isn't duplicated
on retry), use producer headers on POST:

```
Producer-ID: user-alice-tab-1
Producer-Epoch: 1
Producer-Seq: 0
```

- `Producer-ID` identifies the producer (unique per client/tab).
- `Producer-Epoch` increments on reconnect (fences old producers).
- `Producer-Seq` increments per message (0, 1, 2, ...).

The server returns `200 OK` for new data and `204 No Content` for
duplicate (same ID + epoch + seq). This makes retries safe.

Multiple producers can write to the same stream (e.g., user and AI
assistant each have their own Producer-ID). Messages are ordered by
arrival time, not by producer.

## Offset resumption

Clients can resume from where they left off by saving the offset from the
last read response:

```javascript
// First read
const res = await stream({ url, live: false, json: true });
const data = await res.text();
const savedOffset = res.offset;

// Later: resume from saved offset (only new messages)
const resumed = await stream({ url, offset: savedOffset, live: false });
```

This enables the "close tab, reopen, seamless resume" pattern that is
essential for chat applications.

## Postgres schema

```sql
-- Source table for PG→Stream sync
CREATE TABLE items (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Sink table for Stream→PG sync
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

The `session_events` table stores the full event payload as JSONB for
querying, plus extracted metadata fields for indexing and filtering.

## Running locally

```bash
# Full stack with sync (builds all containers, runs tests, tears down)
make integration-test-sessions

# Start the stack manually
docker-compose --profile sync up -d --build

# Check sync service logs
docker-compose logs -f sync-service

# Run tests against running stack
cd e2e && npm install
npx vitest run --reporter=verbose sessions.test.mjs

# Tear down
docker-compose --profile sync down
```

## Production considerations

This harness is a local development and testing tool. For production:

- **Auth:** Replace the test JWKS with a real identity provider.
- **Persistence:** The DS server defaults to in-memory storage. For durability
  beyond the Stream→PG sync, use `file-durable` or `acid` storage mode via `DS_STORAGE__MODE`.
- **Electric config:** Pin the Electric SQL version and configure
  replication slots carefully. See `docs/ecosystem-interop.md` CI-004.
- **Sync service resilience:** The sync service should track its SSE
  offset persistently (e.g., in a Postgres table) so it can resume after
  restart without replaying the full stream.
- **Scaling:** The sync service is a single process. For high-throughput
  streams, consider partitioning by stream name or running multiple
  instances with offset coordination.
- **Monitoring:** Log and alert on sync service errors, SSE reconnections,
  and PG insert failures. The sync service logs these events to stdout.

## Known gotchas

These are documented in detail in `docs/ecosystem-interop.md`:

| ID | Summary |
|----|---------|
| CI-001 | SSE transport masks stream Content-Type; pass `json: true` to client SDK |
| CI-002 | Session events are opaque to DS server; consumers must parse semantics |
| CI-003 | SSE data events for JSON streams are array-wrapped: `[{...}]` not `{...}` |
| CI-004 | Electric requires `wal_level=logical` in Postgres config |

## Cross-references

- `e2e/README.md` — operational guide for the test harness
- `e2e/sync/sync.mjs` — sync service implementation (reference code)
- `e2e/sessions.test.mjs` — integration tests (8 scenarios)
- `docs/decisions.md` — why we chose this architecture over Electric-only
- `docs/ecosystem-interop.md` — detailed interop observations
