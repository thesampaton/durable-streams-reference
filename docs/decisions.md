# Implementation Decisions

This document records implementation decisions in three categories:

- **Protocol decisions:** behavior constrained by the protocol spec or conformance tests.
- **Architecture decisions:** storage/runtime/integration choices that are not protocol semantics.
- **Operational decisions:** deployment/runtime defaults and infrastructure-facing conventions.

## Protocol Decision Log

| Decision | Spec/Test Link | Rationale | Date |
|----------|----------------|-----------|------|
| SSE event type `data` (not `message`) for stream content | PROTOCOL.md §5.8: "`data`: Emitted for each batch of data" | Matches upstream spec exactly. Standard SSE `message` type is browser default; protocol uses explicit `data` type. | 2026-02-07 |
| SSE control event uses typed `ControlPayload` struct with serde camelCase | PROTOCOL.md §5.8: "Field names use camelCase: `streamNextOffset`, `streamCursor`, `upToDate`, and `streamClosed`" | Typed struct prevents field name typos and ensures consistent serialization. | 2026-02-07 |
| Binary detection: everything not `text/*` or `application/json` | PROTOCOL.md §5.8: "For streams with content-type: text/* or application/json, data events carry UTF-8 text directly" | Matches upstream spec exactly. Note: `application/ndjson` is treated as binary per this rule. | 2026-02-07 |
| One `event: data` per stored message (not concatenated) | Conformance: `should send message events for stream data` | Each message is a distinct SSE event, preserving message boundaries. | 2026-02-07 |
| SSE reconnect interval default 60s, configurable via `SSE_RECONNECT_INTERVAL_SECS` (was `SSE_IDLE_CLOSE_SECS`) | PROTOCOL.md §5.8: "Server SHOULD close connections roughly every ~60 seconds"; Gap: `docs/gaps.md` | Configurable to allow tuning. 0 disables entirely. Renamed to match Caddy's `sse_reconnect_interval`. | 2026-02-07 |
| Idempotent close returns 204 (same as initial close) | PROTOCOL.md §4.1: "Closing already-closed stream succeeds"; §5.3 | 204 is the standard close response; idempotent replay should return the same status. | 2026-02-08 |
| `Stream-Closed: true` included in ALL success responses where stream is closed | PROTOCOL.md §5.1, §5.2, §5.3 | Consistent header presence lets clients detect closure without a separate HEAD. Applies to POST close, PUT create-closed, and idempotent replays. | 2026-02-08 |

## Architecture Decision Log

| Decision | Scope | Rationale | Date |
|----------|-------|-----------|------|
| `acid` backend uses sharded redb (`seahash-v1` routing + per-shard DB files) | Storage engine | A single redb file has one writer; sharding across `N` databases enables concurrent writes across streams while preserving per-stream order. redb provides crash-safe ACID transactions; `Durability::Immediate` prioritizes commit durability. Layout manifest (`layout.json`) freezes shard_count/hash policy per directory to prevent silent routing drift. | 2026-02-10 |
| No in-place migration from `file-*` format to `acid` format (current limitation) | Storage migration | Avoids risky format conversion in the first ACID iteration. `acid` currently targets new/empty storage directories until dedicated migration tooling is added. | 2026-02-10 |

## Operational Decision Log

| Decision | Scope | Rationale | Date |
|----------|-------|-----------|------|
| Health check at `/healthz` outside `/v1/stream/` namespace | Server routing | Health checks are infrastructure concern, not protocol API. Keeping separate namespaces avoids coupling infra probes to protocol versioning. | 2026-02-06 |
| Replace Electric-only test with Sessions + bidirectional DB sync | Test/deployment architecture | DS is lower-level than Electric; the sessions pattern is the primary production use case. Bidirectional PG sync validates both PG→DS and DS→PG paths. | 2026-02-09 |
| CORS: allow all origins by default, configurable via `CORS_ORIGINS` | Deployment defaults | CORS is not part of protocol semantics. Default allow-all suits development; production deployments restrict via env var or auth proxy. | 2026-02-09 |

## Decision Entry Format

When adding a decision, include:

1. **Decision:** clear statement of what was chosen.
2. **Category:** protocol vs architecture vs operational.
3. **Reference:** spec/test link for protocol, or subsystem/scope for architecture/operational.
4. **Rationale:** why this choice was made.
5. **Date:** when the decision was made (YYYY-MM-DD).

## Cross-References

- For protocol ambiguities, see `docs/gaps.md`
- For blockers, see GitHub issues or `docs/blockers.md`
- For ecosystem interop observations (not protocol decisions), see `docs/ecosystem-interop.md`
- For version compatibility, see `docs/compatibility.md`
