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

_No observations yet. This section will capture interop issues when the
Electric-SQL sync layer and Postgres persistence are integrated (e.g., event
sourcing patterns, replication semantics, connection lifecycle quirks)._

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
