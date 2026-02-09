# Decision log

Protocol-level implementation decisions are recorded in [`docs/decisions.md`](https://github.com/thesampaton/durable-streams-rust-server/blob/trunk/docs/decisions.md). Each entry includes what was decided, why, and a link to the spec section or conformance test that drove the decision.

## Current decisions

| Decision | Rationale |
|----------|-----------|
| Health check at `/healthz` outside `/v1/stream/` | Health checks are infrastructure, not protocol API |
| SSE event type `data` (not `message`) | Matches PROTOCOL.md exactly; `message` is the browser default, protocol uses explicit `data` type |
| SSE control event uses typed struct with camelCase | Prevents field name typos, ensures consistent serialization |
| Binary detection: everything not `text/*` or `application/json` | Matches PROTOCOL.md binary encoding rule |
| One `event: data` per stored message | Preserves message boundaries per conformance tests |
| SSE idle close default 60s, configurable | Spec says SHOULD ~60s; configurable allows tuning |
| Idempotent close returns 204 | Same status for initial and repeated close |
| `Stream-Closed: true` on all success responses for closed streams | Consistent header presence lets clients detect closure without HEAD |
| Sessions + bidirectional DB sync replaces Electric-only test | Sessions pattern is the primary production use case |

## Decision format

Each decision in the full log includes:

1. **Decision:** clear statement of what was decided
2. **Spec/Test link:** URL to spec section or conformance test, or gap reference
3. **Rationale:** why this choice was made
4. **Date:** when the decision was made

When ambiguous, decisions reference the corresponding gap in [`docs/gaps.md`](https://github.com/thesampaton/durable-streams-rust-server/blob/trunk/docs/gaps.md).
