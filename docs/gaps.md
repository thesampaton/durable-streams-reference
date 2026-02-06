# Protocol Specification Gaps

This document records ambiguities, edge cases, and unspecified behaviour encountered
during implementation. Each gap includes our chosen interpretation, why it's
conservative, and what test would clarify it.

## Active Gaps

| Ambiguity | Interpretation | Why Conservative | Clarifying Test | Date |
|-----------|----------------|------------------|-----------------|------|
| _Gaps will be recorded here as implementation progresses_ | | | | |

## Gap Entry Format

When adding a gap, include:

1. **Ambiguity:** What is unclear or unspecified in the protocol spec/conformance tests
2. **Interpretation:** What we implemented / how we interpreted the ambiguity
3. **Why Conservative:** How this choice preserves forward compatibility and minimizes surprise
4. **Clarifying Test:** What conformance test or spec clarification would resolve this
5. **Date:** When the gap was recorded (YYYY-MM-DD)

## Example Entry

| Ambiguity | Interpretation | Why Conservative | Clarifying Test | Date |
|-----------|----------------|------------------|-----------------|------|
| CORS not covered by conformance | Allow all origins by default, configurable via `CORS_ORIGINS` env var | Aligns with typical development defaults, restrictable in production. Not adding protocol-level headers that might conflict with future spec. | Test OPTIONS preflight with various Origin headers; verify Access-Control-Allow-* headers | 2026-02-06 |
| Health check endpoint path | `/healthz` outside `/v1/stream/` namespace | Health checks are infrastructure, not protocol API. Separate namespaces prevent confusion. | N/A - health check is implementation detail | 2026-02-06 |

## Resolved Gaps

When a gap is resolved (spec updated, conformance test added, maintainer clarified),
move the entry here with a resolution note:

| Ambiguity | Resolution | Resolved By | Date Resolved |
|-----------|------------|-------------|---------------|
| _Resolved gaps will appear here_ | | | |

## Cross-References

- For decisions based on gaps, see `docs/decisions.md`
- For blockers related to gaps, see GitHub issues or `docs/blockers.md`
- For spec version pinning, see `SPEC_VERSION.md`

## Proposing Upstream Clarifications

When a gap needs upstream clarification:
1. Draft a proposed conformance test based on the "Clarifying Test" column
2. Open an issue in github.com/durable-streams/durable-streams with:
   - Description of the ambiguity
   - Current behaviour in this implementation
   - Proposed test or spec language
3. Link the issue in this document
4. If accepted upstream, update `SPEC_VERSION.md` and move to Resolved Gaps
