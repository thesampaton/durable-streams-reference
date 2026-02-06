# Protocol Implementation Decisions

This document records all protocol-level implementation decisions made during
development. Each decision includes what was decided, why, and traceability to
spec sections or conformance tests (or explicit gap references).

## Decision Log

| Decision | Spec/Test Link | Rationale | Date |
|----------|----------------|-----------|------|
| _Decisions will be recorded here as implementation progresses_ | | | |

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
- For version compatibility, see `docs/compatibility.md`
