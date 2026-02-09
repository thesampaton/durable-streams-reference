# Durable sessions

The durable sessions pattern combines DS streams for real-time delivery with a database for durable querying. It is the primary production use case: collaborative applications like AI chat where messages arrive instantly and are queryable forever.

## The pattern

### The two requirements

A durable session needs two things that no single component satisfies alone:

1. **Real-time delivery.** Users in a session need messages instantly. Polling a database is too slow. SSE is the transport.
2. **Durable querying.** Session history must survive server restarts and be queryable for analytics, moderation, and search. A database is the storage.

```
Client  ← SSE ───  DS Server  ← sync ──  Database
Client  ─ POST ──► DS Server  ─ sync ──► Database
```

A sync service bridges both directions. Without Stream-to-DB, events are lost on restart. Without DB-to-Stream, structured data changes never reach clients in real time.

### Session events

The DS server is content-agnostic. All session structure lives in the JSON payload. A typical event:

```json
{
  "key": "user:alice",
  "type": "presence",
  "operation": "set",
  "value": { "status": "online" }
}
```

Common event types for collaborative chat:

| type | operation | description |
|------|-----------|-------------|
| `presence` | `set` / `remove` | User join/leave |
| `chunk` | `append` | Chat message fragment (streaming AI response) |
| `message` | `set` | Complete chat message |
| `reaction` | `set` / `remove` | Emoji reaction |
| `typing` | `set` / `remove` | Typing indicator |

The `key` identifies the entity and `operation` describes the mutation. This follows the STATE-PROTOCOL pattern: the stream is an ordered log of state mutations that can be replayed to reconstruct current state.

### Building a session

**1. Create a JSON-mode stream** for the session:

```bash
curl -X PUT -H "Content-Type: application/json" \
  http://localhost:4437/v1/stream/session-events
```

**2. Append events** with producer idempotency for exactly-once delivery:

```bash
curl -X POST -H "Content-Type: application/json" \
  -H "Producer-ID: user-alice-tab-1" \
  -H "Producer-Epoch: 0" \
  -H "Producer-Seq: 0" \
  -d '[{"key":"msg:1","type":"message","operation":"set","value":{"text":"Hello!"}}]' \
  http://localhost:4437/v1/stream/session-events
```

**3. Subscribe for live updates** via SSE:

```bash
curl -N http://localhost:4437/v1/stream/session-events?offset=-1\&live=sse
```

**4. Resume after disconnect.** Save the offset from the last SSE control event and reconnect:

```bash
curl -N http://localhost:4437/v1/stream/session-events?offset=0000000000000001_0000000000000042\&live=sse
```

### Database schema design

The sync service needs a sink table for events. Design it to extract queryable fields from the JSON payload:

```sql
CREATE TABLE session_events (
  id SERIAL PRIMARY KEY,
  stream_name TEXT NOT NULL,
  event_key TEXT,          -- extracted from payload.key
  event_type TEXT,         -- extracted from payload.type
  operation TEXT,          -- extracted from payload.operation
  payload JSONB NOT NULL,  -- full event for querying
  ds_offset TEXT,          -- for deduplication
  received_at TIMESTAMPTZ DEFAULT NOW()
);
```

Once events are in the database, query them with SQL:

```sql
-- All messages in a session
SELECT * FROM session_events WHERE event_type = 'message' ORDER BY received_at;

-- Active users
SELECT event_key, payload->'value'->>'status'
FROM session_events
WHERE event_type = 'presence' AND operation = 'set'
ORDER BY received_at DESC;
```

### Multiple producers

Multiple producers can write to the same session stream. Each has independent epoch and sequence tracking:

- User Alice writes from her browser tab (`Producer-ID: alice-tab-1`)
- An AI assistant writes responses (`Producer-ID: ai-assistant`)
- A bot writes notifications (`Producer-ID: notification-bot`)

Messages are ordered by arrival time, not by producer.

## In this repository

The e2e harness implements the full durable sessions pattern with Postgres, Electric SQL, and a Node.js sync service.

### Harness architecture

```
Test Runner (host)
  │
  ├── PG-to-Stream: INSERT into items → Electric → sync-service → pg-items DS stream
  │
  └── Stream-to-PG: POST to session-events DS stream → sync-service SSE → session_events PG table
```

### Running

```bash
# Automated: build, start, test, tear down
make integration-test-sessions

# Manual
docker-compose --profile sync up -d --build
cd e2e && npm install
npx vitest run --reporter=verbose sessions.test.mjs
docker-compose --profile sync down
```

### Test scenarios

The `e2e/sessions.test.mjs` suite validates 8 scenarios:

**Session pattern (5 tests):**

1. Session lifecycle: create, write presence event, read back
2. Multi-producer chat: user + AI messages, verify ordering
3. SSE live subscription: subscribe, write, verify real-time delivery
4. Session recovery: write, save offset, write more, resume
5. Producer idempotency: retry same seq (204), next seq (200)

**Database sync (3 tests):**

6. PG-to-Stream: INSERT into Postgres, poll DS stream until it appears
7. Stream-to-PG: POST to DS stream, poll Postgres until row appears
8. Round trip: INSERT into PG, verify in DS stream
