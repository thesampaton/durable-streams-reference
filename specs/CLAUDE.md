# Specification Writing Rules

This file governs spec documents in `specs/`. Specs describe the durable streams
protocol contract as implemented by this server.

## Purpose

Specs are intermediate design artifacts between the upstream PROTOCOL.md and our
implementation. They serve to:

1. Break down the protocol into discrete, testable capabilities.
2. Clarify ambiguities through explicit interpretation (recorded as decisions/gaps).
3. Provide a clear contract for integration tests to validate against.

Specs are NOT the source of truth (that's the upstream spec + conformance tests).
They're our working interpretation of it.

## Spec Document Structure

Each spec document describes one discrete capability:

- `01-stream-lifecycle.md` — create, delete, metadata (PUT, DELETE, HEAD)
- `02-append-semantics.md` — appending data (POST)
- `03-read-modes.md` — catch-up, long-poll, SSE (GET with modes)
- `05-producer-sequencing.md` — idempotent producers, epoch fencing
- `06-json-mode.md` — JSON array flattening
- `07-caching-etag.md` — ETag generation, If-None-Match, Cache-Control
- `08-stream-closure.md` — closing streams, error precedence
- `09-ttl-expiry.md` — TTL and expiration behaviour

## Spec Document Format

```markdown
# Capability Name

Version: X.Y.Z (independent of crate version)
Status: draft | stable | deprecated

## Overview

Brief description of what this capability does in plain language.

## Requirements

Use RFC 2119 terminology (MUST, SHOULD, MAY):
- The server MUST ...
- The server SHOULD ...
- The server MAY ...

### Subsection (e.g., Create Stream)

Detailed requirements for this subsection:
- Request format
- Headers
- Response codes
- Edge cases

## Examples

Concrete examples of requests and responses.

## Conformance

List conformance test blocks this spec covers:
- Basic Stream Operations
- HTTP Protocol (PUT)
- Protocol Edge Cases

## Traceability

Link to upstream spec sections:
- PROTOCOL.md#stream-lifecycle
- Conformance test: `describe('Basic Stream Operations', ...)`

## Gaps

Any ambiguities or unspecified behaviour:
- "Spec silent on empty PUT body" → see docs/gaps.md#empty-put-body
```

## RFC 2119 Terminology (Required)

Use precise terminology to indicate requirement level:

- **MUST / MUST NOT / REQUIRED / SHALL / SHALL NOT:** Absolute requirement.
- **SHOULD / SHOULD NOT / RECOMMENDED:** Strong suggestion but exceptions allowed.
- **MAY / OPTIONAL:** Truly optional, implementation choice.

Do not use vague terms like "needs to", "has to", "probably", "generally".

## Derive from PROTOCOL.md, Not Invention

Every requirement in a spec document MUST be traceable to:
- Upstream PROTOCOL.md section, OR
- Conformance test assertion, OR
- Explicit gap recorded in `docs/gaps.md`.

Do not invent requirements because they "seem reasonable". If it's not in the
upstream spec or tests, it's a gap.

## Version Independently

Each spec document has its own version number (semver). This allows incremental
clarification without bumping the entire project version.

- **Patch bump (X.Y.Z+1):** Clarifications, typo fixes, no behaviour change.
- **Minor bump (X.Y+1.0):** New optional requirements, backward-compatible.
- **Major bump (X+1.0.0):** Breaking changes to requirements.

Track spec version in the document header and in `Cargo.toml` metadata (comment):
```toml
# Spec versions implemented:
# - 01-stream-lifecycle: 1.0.0
# - 02-append-semantics: 1.0.0
```

## Spec as Test Contract

Integration tests in `tests/` validate spec requirements. Each test references
the spec section it validates:

```rust
/// Validates spec: 01-stream-lifecycle.md#create-stream
#[tokio::test]
async fn test_create_stream() { ... }
```

When writing a spec, think about how it will be tested. If a requirement is not
testable via black-box HTTP interaction, it's probably an implementation detail,
not a protocol requirement.

## Keep Specs Close to Tests

Spec documents and their corresponding integration tests live in proximity:
- `specs/01-stream-lifecycle.md`
- `tests/stream_lifecycle.rs`

This makes it harder for specs and tests to drift apart. When updating a spec,
update the corresponding tests in the same commit.

## Plain Language for Protocol Newcomers

Write for someone who wants to implement the protocol in another language, not
for someone reading Rust source code.

- Define terms: "offset", "cursor", "epoch", "sequence number".
- Provide examples: show actual HTTP requests/responses.
- Avoid Rust-isms: don't reference `Result<T, E>` or `Option<T>` in specs.

## When Specs Are Ambiguous

If the upstream spec or conformance tests are ambiguous:
1. Record the ambiguity in `docs/gaps.md`.
2. Choose the most conservative interpretation (see protocol-governance.md).
3. Note the interpretation in the spec document with a reference to the gap.
4. Propose a clarifying test or upstream spec issue.

Example:
```markdown
## Edge Case: Empty PUT Body

The spec is silent on whether PUT with a body should be accepted.
Interpretation: Reject with 400 Bad Request (see docs/gaps.md#empty-put-body).
Rationale: Preserves semantic distinction between PUT (create) and POST (append).
```

## Maintenance

- When the upstream spec or conformance tests change, review affected spec documents.
- Update spec version and `docs/compatibility.md`.
- Keep specs focused. If a document exceeds ~100 lines, consider splitting it.
