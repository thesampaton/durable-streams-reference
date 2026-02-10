# Decision log

Implementation decisions are recorded in [`docs/decisions.md`](https://github.com/thesampaton/durable-streams-reference/blob/trunk/docs/decisions.md) in three sections: **Protocol**, **Architecture**, and **Operational**.

## Current protocol decisions

| Decision | Rationale |
|----------|-----------|
| SSE event type `data` (not `message`) | Matches PROTOCOL.md exactly; `message` is the browser default, protocol uses explicit `data` type |
| SSE control event uses typed struct with camelCase | Prevents field name typos, ensures consistent serialization |
| Binary detection: everything not `text/*` or `application/json` | Matches PROTOCOL.md binary encoding rule |
| One `event: data` per stored message | Preserves message boundaries per conformance tests |
| SSE idle close default 60s, configurable | Spec says SHOULD ~60s; configurable allows tuning |
| Idempotent close returns 204 | Same status for initial and repeated close |
| `Stream-Closed: true` on all success responses for closed streams | Consistent header presence lets clients detect closure without HEAD |

## Current architecture decisions

| Decision | Rationale |
|----------|-----------|
| `acid` backend uses sharded redb with fixed hash routing | Preserves per-stream ordering while enabling concurrent writes across shards; manifest prevents silent routing drift |
| No in-place migration from `file-*` to `acid` (current limitation) | Keeps first ACID iteration safer; migration tooling can be added separately |

## Current operational decisions

| Decision | Rationale |
|----------|-----------|
| Health check at `/healthz` outside `/v1/stream/` | Health checks are infrastructure, not protocol API |
| Sessions + bidirectional DB sync replaces Electric-only test | Sessions pattern is the primary production use case |
| CORS: allow all origins by default, configurable via `DS_SERVER__CORS_ORIGINS` | Not part of protocol semantics; production restricts via env var/proxy |

## Decision format

Each decision in the full log includes:

1. **Decision:** clear statement of what was decided
2. **Category:** protocol, architecture, or operational
3. **Reference:** spec/test (protocol) or subsystem/scope (architecture/operational)
4. **Rationale:** why this choice was made
5. **Date:** when the decision was made

When ambiguous on protocol behavior, decisions reference the corresponding gap in [`docs/gaps.md`](https://github.com/thesampaton/durable-streams-reference/blob/trunk/docs/gaps.md).
