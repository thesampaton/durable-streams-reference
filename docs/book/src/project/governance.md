# Protocol governance

This implementation strictly adheres to the durable streams protocol specification. This page summarizes the governance policies defined in full in [`docs/protocol-governance.md`](https://github.com/thesampaton/durable-streams-rust-server/blob/trunk/docs/protocol-governance.md).

## Default stance

- The spec and conformance tests are the source of truth.
- When ambiguous, take the least-committal interpretation that preserves forward compatibility.
- Never invent semantics because they "feel reasonable". If it is not in spec or tests, it is a gap.

## Decision hierarchy

1. **Conformance tests** -- behavioral truth. If a test asserts it, we implement it.
2. **Pinned PROTOCOL.md** -- textual truth at a specific spec commit SHA.
3. **Maintainer clarification** -- linked issue or PR in the spec repo.
4. **Conservative fallback** -- recorded as a spec gap. No unrecorded guesses.

## Spec pinning

The protocol spec is pinned by git commit SHA, not by branch. The conformance test suite is pinned by npm package version. See [`SPEC_VERSION.md`](https://github.com/thesampaton/durable-streams-rust-server/blob/trunk/SPEC_VERSION.md) for current pins:

- **Spec SHA:** `a347312a47ae510a4a2e3ee7a121d6c8d7d74e50`
- **Conformance:** `@durable-streams/server-conformance-tests@0.2.3`

## Traceability

Every externally observable behavior (paths, headers, status codes, framing) must link to:
- A spec section URL with anchor, or
- A conformance test that asserts it, or
- An explicit gap in [`docs/gaps.md`](https://github.com/thesampaton/durable-streams-rust-server/blob/trunk/docs/gaps.md)

## Conservative fallback rules

- **Paths:** Follow conformance defaults exactly.
- **Headers:** Emit only headers required by spec/tests.
- **Status codes:** Use only codes defined by spec/tests.
- **Ordering:** Preserve monotonicity, avoid replay/gap surprises.
- **Security:** Auth stays out of the server. Auth behavior lives in proxy config.

## Required artifacts

These files are maintained and checked in CI:

| File | Purpose |
|------|---------|
| `SPEC_VERSION.md` | Pinned spec SHA and conformance version |
| `docs/compatibility.md` | Version compatibility matrix |
| `docs/decisions.md` | Protocol decision log |
| `docs/gaps.md` | Spec ambiguity log |
| `docs/ecosystem-interop.md` | Ecosystem integration observations |
