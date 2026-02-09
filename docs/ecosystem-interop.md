# Ecosystem Interoperability & Ergonomics

Observations about how ecosystem components interact with each other in
practice. These are not protocol spec gaps — the protocol itself is fine. They
are rough edges, non-obvious requirements, or atypical patterns that developers
will encounter when integrating the pieces.

This covers two directions:

- **Downstream:** the official `@durable-streams/client` SDK talking to the
  server (content type hints, transport abstractions, API ergonomics)
- **Upstream:** the server talking to Electric-SQL / Postgres sync layer
  (connection semantics, event sourcing patterns, replication quirks)

The purpose of this log is to:

1. **Prevent repeat debugging.** If we hit it, the next implementer will too.
2. **Inform upstream feedback.** Patterns here may indicate ergonomics issues
   worth raising with component maintainers (client SDK, Electric-SQL, etc.).
3. **Guide our own documentation.** If a workaround is required, our tests
   and READMEs should demonstrate it explicitly.

## Downstream (client SDK)

### CI-001: SSE transport masks stream Content-Type, requires `json: true` hint

**Observed:** 2026-02-08
**Component:** `@durable-streams/client@0.2.1`
**Context:** e2e integration test using `stream()` + `subscribeJson()` over SSE

**What happened:**

A JSON-mode stream was created with `contentType: "application/json"` via
`DurableStream.create()`. When reading via `stream({ live: "sse" })`, the HTTP
response arrives with `Content-Type: text/event-stream` (standard SSE). The
client cannot infer the underlying stream content type from the transport
headers and throws:

```
DurableStreamError: JSON methods are only valid for JSON-mode streams.
Content-Type is "text/event-stream" and json hint was not set.
```

**Workaround:**

Pass `json: true` to the `stream()` call:

```javascript
const res = await stream({
  url,
  headers: authHeaders(),
  live: "sse",
  json: true,  // required: SSE transport masks the stream's real content type
});
```

**Why this is atypical:**

The client knows the stream's content type from the `DurableStream.create()`
call, but the `stream()` read function is stateless — it doesn't carry forward
metadata from creation. This is a leaky transport abstraction: the developer
must redundantly specify the content type hint because the SSE transport layer
overwrites it.

When using `DurableStream.connect()` followed by `handle.stream()`, the handle
may resolve this automatically (untested), but the standalone `stream()`
function does not.

**Upstream consideration:**

The client could potentially:
- Infer JSON mode from the stream's stored content type via a HEAD request
- Accept `contentType` as an option on `stream()` alongside `json`
- Document the `json` hint requirement prominently for SSE mode

---

## Upstream (Electric-SQL / Postgres)

### CI-002: STATE-PROTOCOL events are opaque JSON to the DS server

**Observed:** 2026-02-09
**Component:** DS server + sync service
**Context:** Building the bidirectional sync bridge for durable sessions

**What happened:**

SESSION/STATE-PROTOCOL events (presence, chat messages, user actions) are JSON
objects with fields like `key`, `type`, `operation`, and `value`. The DS server
treats these as opaque `application/json` payloads — it stores and delivers them
without parsing or validating the event structure.

This means the sync service must handle all semantic interpretation: extracting
`event_key`, `event_type`, and `operation` from the JSON payload for Postgres
persistence.

**Workaround:**

The sync service parses each SSE data event as JSON and extracts known fields
before INSERT:

```javascript
const eventKey = event.key || null;
const eventType = event.type || null;
const operation = event.operation || null;
const payload = JSON.stringify(event);
```

**Why this is atypical:**

Developers might expect the DS server to understand session event semantics or
provide structured metadata in headers. Instead, the protocol is intentionally
content-agnostic — all structure lives in the payload. This is the right design
(protocol stays simple) but means every consumer must parse events independently.

**Upstream consideration:**

No action needed — this is working as designed. The protocol's content-agnosticism
is a feature, not a bug. Document the pattern for implementers.

### CI-003: SSE data events for JSON streams are array-wrapped

**Observed:** 2026-02-09
**Component:** DS server SSE output (`src/protocol/sse.rs`)
**Context:** Building SSE consumer for Stream→PG sync

**What happened:**

For `application/json` streams, each SSE `event: data` wraps the stored message
in a JSON array: `data:[{"key":"..."}]` instead of `data:{"key":"..."}`. This is
per the conformance spec, but means SSE consumers must unwrap the array to get
individual items.

**Workaround:**

```javascript
const parsed = JSON.parse(data);
const items = Array.isArray(parsed) ? parsed : [parsed];
for (const item of items) { /* process */ }
```

**Why this is atypical:**

Developers writing SSE consumers for JSON streams would reasonably expect each
`data:` line to contain a single JSON object (the stored message). The array
wrapping is a protocol requirement but not immediately obvious from the SSE spec.

**Upstream consideration:**

This is working as designed per the protocol. Should be documented prominently
in the SSE section of integration guides.

---

### CI-004: Electric Shape API requires `wal_level=logical`

**Observed:** 2026-02-09
**Component:** `electricsql/electric:1.4` + `postgres:17-alpine`
**Context:** Setting up PG->DS sync via Electric Shape API

**What happened:**

Electric SQL requires Postgres to be configured with `wal_level=logical` for
replication slot creation. Without this, Electric fails silently or with
unhelpful errors during startup.

Additionally, `max_wal_senders` and `max_replication_slots` must be set high
enough (default of 10 each works for development).

**Workaround:**

Configure Postgres via command-line args in docker-compose:

```yaml
command:
  - postgres
  - -c
  - wal_level=logical
  - -c
  - max_wal_senders=10
  - -c
  - max_replication_slots=10
```

**Why this is atypical:**

Most Postgres Docker setups use default `wal_level=replica`. The requirement
for `logical` is well-documented in Electric's docs but easy to miss in a
docker-compose context where you might not think to override Postgres config.

**Upstream consideration:**

Electric's Docker documentation could include a complete docker-compose snippet
showing the required Postgres configuration.

---

## Observation entry format

When adding an observation, include:

1. **ID:** `CI-NNN` (sequential, shared across upstream/downstream)
2. **Observed:** Date (YYYY-MM-DD)
3. **Component:** Which ecosystem component and version
4. **Context:** What you were doing when you hit it
5. **What happened:** The error, unexpected behaviour, or non-obvious requirement
6. **Workaround:** How to make it work (code snippet preferred)
7. **Why this is atypical:** What a developer would reasonably expect vs. reality
8. **Upstream consideration:** Whether this warrants feedback to component maintainers

## Cross-references

- For protocol spec ambiguities, see `docs/gaps.md`
- For protocol implementation decisions, see `docs/decisions.md`
- For server-side implementation bugs, see `docs/postmortems.md`
