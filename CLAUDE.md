# Durable Streams Rust Server

Reference implementation of the durable streams protocol, built on electric-sql.
Designed to coexist with separate auth proxy patterns so that adopters can
understand how to compose a working durable streams deployment.

## Build & Run Commands

- **Build:** `cargo build`
- **Run:** `cargo run`
- **Test all (unit + conformance):** `cargo test`
- **Unit tests only:** `cargo test --lib`
- **Conformance tests only:** `cargo test --test '*'`
- **Single test:** `cargo test test_name`
- **Lint:** `cargo clippy -- -D warnings`
- **Format:** `cargo fmt`
- **Format check:** `cargo fmt -- --check`

## Development Methodology

This project follows an implicit BDD pattern where the server conformance
specifications are the primary design artefact. The implementation exists
to pass those specifications.

### Conformance specifications

- Specs live in `specs/` as structured markdown, using plain language with
  must/should/may terminology to describe protocol behaviour.
- Each spec document describes a discrete capability (connection lifecycle,
  offset resumption, delivery guarantees, error responses, etc.).
- Specs carry their own version number independent of the crate version.
  `Cargo.toml` metadata tracks which spec version this implementation targets.
- Specs describe the durable streams protocol contract — not electric-sql
  internals. Electric-sql is an implementation choice of this server, not
  something the conformance suite should know about.

### Two test layers

1. **Unit tests (inner loop):** `#[cfg(test)] mod tests` in each source file.
   Test internal logic — parsing, state machines, stream mechanics. Fast, no I/O.
2. **Conformance tests (outer loop):** Integration tests in `tests/` that boot
   the server on a random port, make HTTP requests, and assert against the spec.
   These are the acceptance tests. Each test function maps to a specific spec
   requirement and includes a doc comment referencing the spec section it validates.

The conformance tests are deliberately black-box: they interact only through
the public HTTP interface so the same expectations could be validated against
any implementation of the protocol.

### Workflow loop

1. Start from a failing conformance test that expresses a spec requirement.
2. Write the minimum implementation to pass it.
3. Run `cargo clippy -- -D warnings` — fix all warnings before moving on.
4. Run `cargo test` to verify both unit and conformance tests pass.
5. Run `cargo fmt` before considering the task done.
6. If a spec requirement is ambiguous, clarify the spec document first, then
   write the test.

## Code Style & Conventions

- Clippy pedantic lints are enabled project-wide via `Cargo.toml` `[lints.clippy]`.
  Treat all warnings as errors.
- If a specific pedantic lint is genuinely inapplicable to a particular item,
  suppress it with a targeted `#[allow(clippy::...)]` on that item with a
  comment explaining why. Do not add blanket allows at the crate level.
- Use `thiserror` for library error types, `anyhow` for application-level
  error propagation (add when needed).
- Prefer returning `Result` over panicking. Reserve `unwrap()`/`expect()` for
  cases where invariants are provably upheld, and add a comment explaining why.
- Use `snake_case` for functions/variables, `CamelCase` for types/traits,
  `SCREAMING_SNAKE_CASE` for constants.
- Keep functions short and focused. Extract helpers when a function exceeds ~40 lines.
- Prefer strong typing over stringly-typed APIs. Use newtypes and enums to
  make invalid states unrepresentable.

## Interface Documentation

- All public HTTP endpoints, request/response shapes, and error codes must be
  documented in the corresponding spec file before implementation begins.
- Documentation uses plain language aimed at someone who wants to implement or
  integrate with the protocol, not at someone reading Rust source.
- Spec documents and their corresponding conformance tests live close together
  so they are harder to let drift apart.

## Git Conventions

- Never include `Co-Authored-By` trailers in commit messages.
- Write concise commit messages explaining *why*, not *what*.
- Separate logical changes into distinct commits.
- Spec changes, test additions, and implementation should be distinct commits
  where practical, making the BDD progression visible in history.

## Protocol Governance

This implementation strictly adheres to the durable streams protocol specification.
Governance policies are defined in detail in `docs/protocol-governance.md` and
`docs/blocker-policy.md` (canonical sources in `scratch/` until copied to `docs/`).

### Key Principles

- **Spec is truth:** Conformance tests and the pinned PROTOCOL.md are the source
  of truth. Never invent semantics. If it's not in spec/tests, it's a gap.
- **Conservative interpretation:** When ambiguous, take the least-committal
  interpretation that preserves forward compatibility.
- **Traceability:** Every observable behaviour must link to spec section or
  conformance test. Gaps recorded explicitly in `docs/gaps.md`.
- **Spec pinning:** Protocol spec pinned by git SHA in `SPEC_VERSION.md`.
  Conformance test suite version also pinned.
- **Decision tracking:** All protocol decisions recorded in `docs/decisions.md`
  with spec/test links.

### Blocker Policy

If blocked for >15 minutes (spec ambiguity, missing credentials, unclear choices,
CI flakiness), stop and log it. Create GitHub issue or write to `docs/blockers.md`
with context, observed/expected behaviour, repro steps, and proposed next step.
See `docs/blocker-policy.md` for required issue format.

### Directory-Specific Instructions

Subdirectories contain additional CLAUDE.md files with context-specific rules:
- `src/CLAUDE.md` — code conventions, typing rules, error handling
- `tests/CLAUDE.md` — test conventions, black-box testing requirements
- `docs/CLAUDE.md` — documentation rules, gap recording
- `specs/CLAUDE.md` — spec writing conventions, must/should/may terminology
# Tool Rules

## Allowed (no confirmation needed)
- Bash(cargo build*)
- Bash(cargo check*)
- Bash(cargo test*)
- Bash(cargo clippy*)
- Bash(cargo fmt*)
- Bash(cargo run*)
- Bash(cargo add*)
- Bash(cargo remove*)
- Bash(cargo doc*)
- Bash(cargo bench*)
- Bash(cargo tree*)
- Bash(cargo update*)
- Bash(rustup*)
- Bash(cat *)
- Bash(find *)
- Bash(grep *)
- Bash(rg *)
- Bash(ls *)
- Bash(head *)
- Bash(tail *)
- Bash(wc *)
- Bash(mkdir *)
- Bash(cp *)
- Bash(mv *)
- Bash(touch *)
- Read
- Write
- Edit

## Denied
- Bash(rm -rf /*)
- Bash(cargo publish*)