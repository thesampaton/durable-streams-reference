# Protocol Governance

This document is normative. All protocol-level implementation decisions in this repository MUST comply with the policies below.

## Default stance

- The spec and conformance tests are the source of truth.
- If ambiguous, take the least-committal interpretation that preserves forward compatibility and minimises surprise.
- Never invent semantics because they "feel reasonable". If it's not in spec/tests, it's a gap.
- This is the software engineering equivalent of contra proferentem: ambiguity is resolved conservatively and recorded, and we push for upstream clarification.

## Decision hierarchy (strict)

1. **Conformance tests** — behavioural truth. If a test asserts it, we implement it.
2. **Pinned PROTOCOL.md at a specific spec commit SHA** — textual truth. If the spec says it but no test covers it, we implement it conservatively.
3. **Maintainer clarification** — linked issue or PR in the spec repo, treated as an addendum to the spec.
4. **Conservative fallback** — record as a spec gap explicitly. Do not ship unrecorded guesses.

## Spec pinning (hard requirement)

- Pin the protocol spec by git commit SHA (not `main`). Store the SHA in `SPEC_VERSION.md` at the repo root.
- Pin the conformance test suite package version (lockfile, plus explicit callout in `SPEC_VERSION.md`).
- The server must be able to state: "This implementation targets spec SHA X and conformance version Y."

## Traceability (hard requirement)

For every externally observable behaviour (paths, headers, status codes, mode semantics, framing):

- Link to the exact spec section (URL with `#anchor`), OR
- Link to the exact conformance test that asserts it.

If neither exists:

- Record it in `docs/gaps.md` as a spec ambiguity, including:
  - What is ambiguous
  - Chosen interpretation
  - Why it's conservative
  - How to reproduce / what test would clarify it

## Version mapping (hard requirement)

- The server advertises a protocol version as defined by the spec (header, response, docs — whatever the protocol requires).
- Maintain a compatibility matrix in `docs/compatibility.md`:
  - Spec SHA → supported behaviour deltas → server version range
- When the spec changes, either:
  - Update implementation + docs + compatibility matrix (and rerun conformance), OR
  - Explicitly mark "not supported yet" including the failing conformance note and reasoning

## Conservative fallback rules

- **Paths:** Follow conformance defaults exactly. If the spec is silent, make it configurable but keep the conformance path as the baked-in default.
- **Headers:** Emit only headers required by spec/tests. Do not add "helpful" headers that could become semantically loaded later, unless explicitly documented as non-protocol (e.g. health check responses).
- **Status codes:** Use only codes defined by spec/tests. If unspecified, prefer one documented generic behaviour over creative specificity.
- **Ordering/resume:** When in doubt, preserve monotonicity and avoid replay/gap surprises.
- **Security boundary:** Auth stays out of the server. Any auth-adjacent behaviour lives in proxy config/docs, not in protocol semantics.

## Required repo artifacts

These files MUST exist and be maintained:

### `SPEC_VERSION.md`

- Spec repo URL
- Pinned spec SHA
- Conformance test suite version
- Date pinned

### `docs/compatibility.md`

- Table: spec SHA | server version | notes | breaking changes

### `docs/decisions.md`

- Each decision includes: what + why + spec/test link

### `docs/gaps.md`

- Ambiguity list + repro notes + proposed clarifying tests
- Cross-reference any related blocker issues (see `docs/blocker-policy.md`)

### `docs/ecosystem-interop.md`

- Ecosystem interoperability and ergonomics observations (downstream and upstream)
- NOT protocol gaps — the protocol is fine; these are component integration rough edges
- **Downstream:** client SDK → server (transport abstractions, API hints, ergonomics)
- **Upstream:** server → Electric-SQL / Postgres (sync semantics, replication, event sourcing)
- Each entry: what happened, workaround, why atypical, upstream consideration

## Enforcement (CI)

CI MUST fail if any of the following are true:

- Conformance fails against the pinned spec
- Spec pin changed without updating `docs/compatibility.md`
- A protocol behaviour is implemented without a corresponding entry in `docs/decisions.md` linking to spec/tests (or `docs/gaps.md` if ambiguous)

The third check may start as manual review (PR checklist) and graduate to automated linting as the project matures. The first two must be automated from day one.
