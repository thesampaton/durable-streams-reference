# Test Conventions

This file governs all test code in `tests/`. Integration tests here are development
scaffolding. The external conformance suite is the ultimate acceptance gate.

## Test Layers

### Unit Tests (`#[cfg(test)] mod tests` in source files)

- Fast, no I/O, no networking.
- Test internal logic: parsing, state machines, offset generation, validation.
- Mock storage and external dependencies.
- Run with `cargo test --lib`.
- Examples:
  - Offset format validation and ordering
  - Producer sequence validation logic
  - JSON array flattening
  - Error precedence rules

### Integration Tests (`tests/*.rs`)

- Boot the server on a random port, make real HTTP requests, assert responses.
- Black-box only: interact through public HTTP interface, no direct module access.
- Each test function maps to a specific spec requirement.
- Include doc comment referencing spec section: `/// Validates spec: 01-stream-lifecycle.md#create-stream`
- Run with `cargo test --test '*'` or `cargo test --test test_name`.
- Examples:
  - Create stream, verify 201 + Location header
  - Append data, read back, verify offset resumption
  - Long-poll timeout behaviour

### Backend parity policy

- Shared backend invariants belong in `tests/storage_backend_contract.rs`.
- The contract suite runs the same direct-storage assertions against
  `memory`, `file-durable`, and `acid`.
- Backend-specific storage tests stay in source files:
  - `src/storage/memory.rs`: in-memory concurrency specifics only.
  - `src/storage/file.rs`: filesystem/log recovery specifics only.
  - `src/storage/acid.rs`: shard/layout/corruption fail-fast specifics only.
- HTTP parity-critical coverage across backends belongs in
  `tests/http_backend_parity_subset.rs` and currently targets `memory` + `acid`.
- Keep assertions backend-neutral in shared suites; do not assert internals
  (file paths, shard IDs, lock shapes) outside backend-specific tests.

### External Conformance Suite (ultimate gate)

- `@durable-streams/server-conformance-tests` on npm (~195 tests).
- These are the acceptance tests. If we pass all of these, we conform.
- Run with `make conformance` or `npx @durable-streams/server-conformance-tests`.
- Our Rust integration tests exist to provide faster feedback during development.
  The external suite is the final authority.

## Black-Box Testing Requirement

Integration tests MUST NOT:
- Import server modules directly (no `use durable_streams_reference::*`)
- Inspect internal state (no accessing storage directly)
- Mock server behaviour (no fake servers, use the real binary)

Integration tests MUST:
- Start a real server process or in-process server via `axum::serve()`
- Use an HTTP client (reqwest) to make requests
- Assert only on observable HTTP behaviour: status codes, headers, body content

This ensures the same test expectations could validate any implementation of the protocol.

## Test Structure

Each integration test file corresponds to a spec document:
- `tests/stream_lifecycle.rs` → `specs/01-stream-lifecycle.md`
- `tests/append_semantics.rs` → `specs/02-append-semantics.md`
- `tests/read_modes.rs` → `specs/03-read-modes.md`

Within each file, test functions reference specific sections:
```rust
/// Validates spec: 01-stream-lifecycle.md#create-stream
#[tokio::test]
async fn test_create_stream() { ... }
```

## Test Helpers (`tests/common/mod.rs`)

Common test utilities:
- `spawn_test_server() -> (Server, u16)` — start server on random port
- `test_client(port: u16) -> Client` — HTTP client with base URL
- `unique_stream_name() -> String` — generate unique stream names to avoid collisions
- `shutdown_server(server: Server)` — clean shutdown

Keep helpers focused. Don't build a framework. Just reduce duplication.

Backend-aware helpers:
- `create_test_storage(...)` and `create_test_storage_with_limits(...)` for
  direct storage-contract tests.
- `spawn_test_server_for_backend(...)` for HTTP parity subsets.

## Test Naming

- Use descriptive names: `test_create_stream_returns_201_with_location`
- Avoid generic names: `test_1`, `test_basic`, `test_works`
- Pattern: `test_{operation}_{expected_behaviour}`

## Test Independence

- Each test must be independent. No shared state between tests.
- Use unique stream names per test to avoid collisions.
- Clean up resources (though server restart between tests is fine).

## Spec References (Required)

Every integration test MUST include a doc comment referencing the spec section it validates:
```rust
/// Validates spec: 01-stream-lifecycle.md#create-stream
///
/// Verifies that PUT /stream/{name} returns 201 Created with Location header
/// when creating a new stream.
#[tokio::test]
async fn test_create_stream() { ... }
```

If a test validates behaviour not covered by the spec, mark it as a gap:
```rust
/// Validates gap: docs/gaps.md#cors-preflight
///
/// CORS preflight not explicitly covered by conformance tests.
#[tokio::test]
async fn test_cors_preflight() { ... }
```

## Running Tests

- All tests: `cargo test`
- Unit only: `cargo test --lib`
- Integration only: `cargo test --test '*'`
- Single test: `cargo test test_name`
- Conformance: `make conformance`

## When Tests Fail

1. **Integration test fails:** Either the implementation is wrong or the test is
   wrong. Check the spec. Fix the implementation or update the test + spec.
2. **Conformance test fails:** The implementation is wrong (or we hit a blocker).
   Conformance tests are the source of truth. If ambiguous, record in `docs/gaps.md`
   and follow blocker policy.
3. **Unit test fails:** Internal logic is broken. Fix it before proceeding.

## No Test-Driven Refactoring

- Tests validate behaviour, not implementation details.
- Don't refactor internals to make tests easier. Refactor to make code clearer.
- If a test is hard to write, the interface might be wrong, but don't let test
  convenience drive design. The protocol drives design, tests validate it.
