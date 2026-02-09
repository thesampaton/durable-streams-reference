# Protocol Implementation Decisions

This document records all protocol-level implementation decisions made during
development. Each decision includes what was decided, why, and traceability to
spec sections or conformance tests (or explicit gap references).

## Decision Log

| Decision | Spec/Test Link | Rationale | Date |
|----------|----------------|-----------|------|
| Health check at `/healthz` outside `/v1/stream/` namespace | Gap: Not covered by conformance tests | Health checks are infrastructure concern, not part of the protocol API. Keeping separate namespaces prevents confusion and allows protocol versioning without affecting health checks. | 2026-02-06 |
| SSE event type `data` (not `message`) for stream content | PROTOCOL.md §5.8: "`data`: Emitted for each batch of data" | Matches upstream spec exactly. Standard SSE `message` type is browser default; protocol uses explicit `data` type. | 2026-02-07 |
| SSE control event uses typed `ControlPayload` struct with serde camelCase | PROTOCOL.md §5.8: "Field names use camelCase: `streamNextOffset`, `streamCursor`, `upToDate`, and `streamClosed`" | Typed struct prevents field name typos and ensures consistent serialization. | 2026-02-07 |
| Binary detection: everything not `text/*` or `application/json` | PROTOCOL.md §5.8: "For streams with content-type: text/* or application/json, data events carry UTF-8 text directly" | Matches upstream spec exactly. Note: `application/ndjson` is treated as binary per this rule. | 2026-02-07 |
| One `event: data` per stored message (not concatenated) | Conformance: `should send message events for stream data` | Each message is a distinct SSE event, preserving message boundaries. | 2026-02-07 |
| SSE idle close default 60s, configurable via `SSE_IDLE_CLOSE_SECS` | PROTOCOL.md §5.8: "Server SHOULD close connections roughly every ~60 seconds"; Gap: `docs/gaps.md#sse-idle-close` | Configurable to allow tuning. 0 disables idle close entirely. | 2026-02-07 |
| Idempotent close returns 204 (same as initial close) | PROTOCOL.md §4.1: "Closing already-closed stream succeeds"; §5.3 | 204 is the standard close response; idempotent replay should return the same status. | 2026-02-08 |
| `Stream-Closed: true` included in ALL success responses where stream is closed | PROTOCOL.md §5.1, §5.2, §5.3 | Consistent header presence lets clients detect closure without a separate HEAD. Applies to POST close, PUT create-closed, and idempotent replays. | 2026-02-08 |

| Replace Electric-only test with Sessions + bidirectional DB sync | N/A (architecture) | DS is lower-level than Electric; the sessions pattern is the primary production use case. Bidirectional PG sync validates the full data layer (DS for real-time delivery, PG for durable querying). The `integration-test-electric` stub is replaced by `integration-test-sessions` which tests both directions: PG->DS via Electric Shape API, and DS->PG via SSE consumer. | 2026-02-09 |
| CORS: allow all origins by default, configurable via `CORS_ORIGINS` | Gap: `docs/gaps.md` example entry (CORS not covered by conformance) | CORS is not part of the protocol spec or conformance tests. Default allow-all suits development; production deployments restrict via env var or auth proxy. Keeps CORS config out of protocol semantics. | 2026-02-09 |

## Decision Entry Format

When adding a decision, include:

1. **Decision:** Clear statement of what was decided (e.g., "Use 413 for memory limit exceeded")
2. **Spec/Test Link:** URL to spec section or conformance test name, or reference to gap in `docs/gaps.md`
3. **Rationale:** Why this choice was made, especially if multiple options existed
4. **Date:** When the decision was made (YYYY-MM-DD)

## Example Entry

| Decision | Spec/Test Link | Rationale | Date |
|----------|----------------|-----------|------|
| Use 413 for memory limit exceeded | Gap: `docs/gaps.md#memory-limit-status` | 413 Payload Too Large is standard HTTP; 507 Insufficient Storage is WebDAV-specific. Prefer standard status codes. | 2026-02-06 |
| Base path `/v1/stream/` | PROTOCOL.md#paths | Required by protocol specification | 2026-02-06 |
| Health check at `/healthz` outside base path | Gap: `docs/gaps.md#health-check-path` | Health checks are infrastructure concern, not protocol API. Keep separate from `/v1/stream/` namespace. | 2026-02-06 |

## Cross-References

- For ambiguities, see `docs/gaps.md`
- For blockers, see GitHub issues or `docs/blockers.md`
- For ecosystem interop issues (not protocol decisions), see `docs/ecosystem-interop.md`
- For version compatibility, see `docs/compatibility.md`
