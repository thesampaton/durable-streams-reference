# Documentation Rules

This file governs all documentation in `docs/` and protocol specifications in `specs/`.

## Purpose

Documentation exists to help someone who wants to implement or integrate with the
durable streams protocol. Target audience: protocol newcomers, not Rust internals experts.

## Plain Language Requirement

- Use plain language. Avoid jargon unless it's protocol terminology.
- Define terms on first use.
- Example: "offset" needs explanation ("a cursor marking position in the stream"),
  not assumption ("obviously it's a lexicographically ordered position marker").
- Write for someone who has read README.md and wants to understand how things work.

## Every Decision Links to Spec/Test

All externally observable behaviour MUST be traceable:

- **Ideal:** Link to spec section URL with anchor: `https://github.com/durable-streams/durable-streams/blob/SHA/PROTOCOL.md#section-name`
- **Acceptable:** Link to conformance test that asserts it.
- **If neither exists:** Record as a gap in `docs/gaps.md`.

Do not document invented behaviour. If we implement something not in spec/tests,
it's either a bug or a gap that needs recording.

## Gaps Explicitly Recorded

`docs/gaps.md` is where ambiguities live. Every gap entry includes:

- **What is ambiguous:** Specific scenario or edge case not covered by spec/tests.
- **Chosen interpretation:** What we implemented.
- **Why it's conservative:** How this choice preserves forward compatibility and
  minimizes surprise.
- **Clarifying test:** What test would resolve the ambiguity (for upstream proposal).

Gaps are not failures. They're explicit acknowledgment that we're operating beyond
the spec's current coverage. Recording them is how we stay honest.

## Required Documentation Files

### `SPEC_VERSION.md` (repo root)

Pins the protocol spec and conformance test suite:
- Spec repo URL
- Pinned spec commit SHA
- Conformance test suite version (npm package version)
- Date pinned

### `docs/protocol-governance.md`

Canonical governance policy. Copied from `scratch/protocol-governance.md`. Defines:
- Decision hierarchy (conformance tests > spec > maintainer clarification > conservative fallback)
- Spec pinning requirements
- Traceability requirements
- Conservative fallback rules

### `docs/blocker-policy.md`

Canonical blocker policy. Copied from `scratch/blocker-policy.md`. Defines:
- What counts as a blocker
- Where to log blockers (GitHub issues preferred, `docs/blockers.md` fallback)
- Required issue format
- How to link blockers back to gaps/decisions

### `docs/compatibility.md`

Version compatibility matrix:

| Spec SHA | Server Version | Notes | Breaking Changes |
|----------|----------------|-------|------------------|
| abc123   | 0.1.0          | Initial implementation | N/A |

Updated whenever spec pin changes or server version increments.

### `docs/decisions.md`

Decision log for all protocol-level choices:

| Decision | Spec/Test Link | Rationale | Date |
|----------|----------------|-----------|------|
| Use 413 for memory limit exceeded | N/A (gap) | 413 is standard HTTP, 507 is WebDAV-specific | 2024-10-15 |

Each entry includes what was decided, why, and traceability link (or gap reference).

### `docs/gaps.md`

Ambiguity log:

| Ambiguity | Interpretation | Why Conservative | Clarifying Test |
|-----------|----------------|------------------|-----------------|
| CORS not covered by conformance | Allow all origins by default, configurable via `CORS_ORIGINS` | Aligns with typical development defaults, restrictable in production | Test OPTIONS preflight with various origin headers |

### `docs/ecosystem-interop.md`

Ecosystem interoperability and ergonomics observations. These are NOT protocol
spec gaps — the protocol is fine. They are rough edges, non-obvious requirements,
or atypical patterns in ecosystem components that developers will encounter
during integration.

Covers two directions:
- **Downstream:** `@durable-streams/client` SDK talking to the server
- **Upstream:** server talking to Electric-SQL / Postgres sync layer

Record an observation when:
- An ecosystem component requires a non-obvious workaround (e.g., redundant hints)
- An API behaves differently than a developer would reasonably expect
- A transport or sync abstraction leaks implementation details
- Integration required trial-and-error that documentation didn't prevent

Each entry includes: what happened, the workaround, why it's atypical, and
whether it warrants feedback to the component maintainers.

### `docs/blockers.md` (optional fallback)

If GitHub issues cannot be created via API, log blockers here with the same format
as the issue template. Include the exact `gh issue create` command to run.

## Documentation Style

- Use markdown with GitHub flavor (fenced code blocks, tables).
- Headers: sentence case, not title case ("How to deploy" not "How To Deploy").
- Code examples: always include language hint for syntax highlighting.
- File paths: use backticks: `src/main.rs`.
- Commands: use fenced blocks with `bash` hint.

## When to Update Docs

- **Spec pin changes:** Update `SPEC_VERSION.md` and `docs/compatibility.md`.
- **Protocol decision made:** Add entry to `docs/decisions.md` with links.
- **Ambiguity encountered:** Add entry to `docs/gaps.md` immediately.
- **Blocker hit:** Create issue or log in `docs/blockers.md`.

## Don't Document Implementation Details

- Document protocol behaviour, not Rust internals.
- Example: Document "streams are held in memory with configurable size limits"
  not "we use a `HashMap<String, Arc<RwLock<StreamEntry>>>` for storage".
- Implementation details live in code comments and `src/CLAUDE.md`, not protocol docs.

## Cross-References

Link between docs liberally:
- `docs/gaps.md` entries reference `docs/decisions.md` when a gap is resolved.
- `docs/decisions.md` references spec sections and conformance test names.
- `docs/blockers.md` entries link to gaps/decisions if spec-related.

## Maintenance

- Review `docs/gaps.md` periodically. When spec/tests clarify a gap, move the
  entry to `docs/decisions.md` with updated links.
- Keep `docs/compatibility.md` up to date. Don't let spec pin drift silently.
- Archive resolved blockers (comment them out or move to a "Resolved" section).
