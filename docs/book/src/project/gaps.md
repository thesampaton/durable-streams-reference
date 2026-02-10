# Spec gaps

Ambiguities and edge cases not fully covered by the protocol spec or conformance tests are recorded in [`docs/gaps.md`](https://github.com/thesampaton/durable-streams-reference/blob/trunk/docs/gaps.md). This page summarizes the current gaps and known ecosystem interop observations.

## Active gaps

| Ambiguity | Our interpretation |
|-----------|-------------------|
| SSE idle close timing (~60s) | Default 60s, configurable via `DS_SERVER__SSE_RECONNECT_INTERVAL_SECS` (0 disables). Spec says SHOULD close roughly every ~60s. |

Gaps are not failures. They are explicit acknowledgments that the implementation operates beyond the spec's current coverage.

Non-protocol architecture and operational decisions are tracked in [`docs/decisions.md`](https://github.com/thesampaton/durable-streams-reference/blob/trunk/docs/decisions.md).

## Ecosystem interop observations

These are recorded in [`docs/ecosystem-interop.md`](https://github.com/thesampaton/durable-streams-reference/blob/trunk/docs/ecosystem-interop.md). They are not protocol gaps -- the protocol is fine. They are rough edges in ecosystem components that developers encounter during integration.

| ID | Summary | Component |
|----|---------|-----------|
| CI-001 | SSE transport masks stream Content-Type; `stream()` needs `json: true` hint | `@durable-streams/client` |
| CI-002 | Session events are opaque JSON; consumers must parse semantics | DS server + sync service |
| CI-003 | SSE data events for JSON streams are array-wrapped: `[{...}]` not `{...}` | DS server SSE output |
| CI-004 | Electric requires `wal_level=logical` in Postgres | Electric SQL + Postgres |

## Proposing upstream clarifications

When a gap needs upstream resolution:

1. Draft a proposed conformance test based on the clarifying test column in `docs/gaps.md`
2. Open an issue in [github.com/durable-streams/durable-streams](https://github.com/durable-streams/durable-streams)
3. Link the issue in `docs/gaps.md`
4. If accepted upstream, update `SPEC_VERSION.md` and move to resolved gaps
