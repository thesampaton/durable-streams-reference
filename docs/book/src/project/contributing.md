# Contributing

## Development setup

```bash
git clone https://github.com/thesampaton/durable-streams-reference.git
cd durable-streams-reference
cargo build
cargo test
```

## BDD workflow

This project follows an implicit BDD pattern. The conformance specifications are the primary design artifact. The implementation exists to pass them.

### The loop

1. **Start from a failing conformance test** that expresses a spec requirement.
2. **Write the minimum implementation** to pass it.
3. **Run `cargo clippy -- -D warnings`** -- fix all warnings before moving on.
4. **Run `cargo test`** to verify both unit and conformance tests pass.
5. **Run `cargo fmt`** before considering the task done.
6. If a spec requirement is ambiguous, clarify the spec document first, then write the test.

### Two test layers

**Unit tests** (`#[cfg(test)] mod tests` in each source file): test internal logic -- parsing, state machines, stream mechanics. Fast, no I/O.

**Conformance tests** (`tests/` directory): boot the server on a random port, make HTTP requests, assert against the spec. These are acceptance tests. Each test maps to a specific spec requirement.

The conformance tests are deliberately black-box: they interact only through the public HTTP interface.

## Code style

- Clippy pedantic lints are enabled project-wide. Treat all warnings as errors.
- Use `thiserror` for error types.
- Prefer `Result` over panicking. Reserve `unwrap()`/`expect()` for provable invariants.
- `snake_case` for functions/variables, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep functions under ~40 lines. Extract helpers when they grow longer.
- Prefer strong typing over stringly-typed APIs. Newtypes and enums for invalid-state prevention.

## Commands

```bash
cargo build                    # compile
cargo test                     # unit + integration tests
cargo test --lib               # unit tests only
cargo test --test '*'          # integration tests only
cargo test test_name           # single test
cargo clippy -- -D warnings    # lint
cargo fmt                      # format
cargo fmt -- --check           # format check
make conformance               # external conformance suite
make integration-test          # Docker stack e2e tests
make integration-test-sessions # sessions + DB sync tests
make pgo-train                 # generate + merge PGO profile data
make release-pgo               # guarded profile-use release build
make pgo-benchmark             # benchmark profile-use build
make docs                      # build mdbook documentation
```

For release/performance work, prefer `make release-pgo` after `make pgo-train`
so artifacts are built from current benchmark-driven profiles.

## Documentation

- Protocol behavior is documented in `specs/` as structured markdown.
- Implementation decisions go in `docs/decisions.md`.
- Spec ambiguities go in `docs/gaps.md`.
- Ecosystem interop observations go in `docs/ecosystem-interop.md`.
- The mdbook documentation site lives in `docs/book/`. Build with `make docs`.

## Git conventions

- Write concise commit messages explaining *why*, not *what*.
- Separate logical changes into distinct commits.
- Spec changes, test additions, and implementation should be distinct commits where practical.

## Protocol governance

All protocol-level decisions must comply with the governance policies in [`docs/protocol-governance.md`](https://github.com/thesampaton/durable-streams-reference/blob/trunk/docs/protocol-governance.md). Key rule: never invent semantics. If it is not in spec or tests, record it as a gap.
