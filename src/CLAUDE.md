# Source Code Conventions

This file governs all code in `src/`. Violations should be caught by clippy or code review.

## Strong Typing

- Use newtypes and enums to make invalid states unrepresentable.
- Prefer `Offset`, `ProducerEpoch`, `ProducerSeq` newtypes over raw strings/integers.
- Parse and validate at module boundaries. Internal functions assume validated types.
- Example: `Offset::from_str()` returns `Result<Offset, ParseError>`. Once you have
  an `Offset`, it's guaranteed valid. Never pass raw strings as offsets internally.

## Offset Invariants (Critical)

- Offsets MUST be monotonically increasing within a stream.
- Offset format: `{read_seq:016x}_{byte_offset:016x}` (zero-padded hex).
- Sentinels: `-1` (stream start), `now` (tail/live).
- Lexicographic ordering equals temporal ordering.
- Concurrent appends MUST be serialized per-stream to maintain monotonicity.
- Use per-stream `Mutex` for append operations, not just stream lookup `RwLock`.

## Storage Trait Contract

The `Storage` trait defines the core persistence interface. Implementations must:

- **Sync methods by default:** Most storage operations are sync. Wrap with
  `tokio::sync::RwLock`/`Mutex` at the server layer when called from async handlers.
- **Async only when necessary:** Notification/subscribe for long-poll/SSE is the
  one async boundary. Use `tokio::sync::broadcast` or equivalent.
- **Thread-safe:** All methods must be safe to call concurrently. Use interior
  mutability (`RwLock`, `Mutex`) as needed.
- **Atomicity:** Appends are atomic. Either the message is added with its offset
  or the operation fails. No partial writes.
- **Isolation:** Stream operations are isolated. Creating, deleting, or appending
  to one stream does not affect others (except global memory limits).

## Error Handling

- **Single error enum:** One `Error` enum with `thiserror` for all storage and
  protocol errors. Defined in `src/protocol/error.rs`.
- **Map to HTTP in one place:** Error → HTTP status code mapping happens in
  handlers, not in storage/protocol layers.
- **No anyhow in core logic:** Use `anyhow` only in main.rs for application-level
  error propagation if needed. Storage and protocol layers return `Result<T, Error>`.
- **Explicit over clever:** Don't use `?` chains that obscure what error is
  being propagated. If context is lost, use `.map_err()` to add it.
- **Invariant panics are bugs:** If you hit `expect()`, that's a bug in our code,
  not user input. Add a comment explaining why it's unreachable if invariants hold.

## Owned Boundary Types

- Module boundaries and API surfaces use owned types: `Bytes`, `String`, `Arc<T>`.
- Avoid borrowing gymnastics (`&str`, `&[u8]`) across module boundaries.
- Example: Handler receives `String` stream name, passes to storage as `String`.
  Storage stores in `HashMap<String, Stream>`. Don't fight the borrow checker.
- Within a module, use borrowing freely. But when crossing boundaries, own it.

## Ban Cleverness in Hot Path

- No iterator acrobatics that require three reads to understand.
- No lifetime tricks to save an allocation.
- No generic overengineering to handle "future extensibility".
- Boring and obvious beats clever and compact.
- Example: prefer `for` loop with explicit early return over `.find().and_then().or_else()`.

## Thin Protocol Layer (Handlers)

- Handlers are dumb: parse → validate → call core functions → format response.
- No business logic in handlers. Logic lives in `protocol/` or `storage/`.
- Handler responsibility: HTTP framing, header parsing, status code mapping.
- Example: `handlers/post.rs` parses `Producer-Seq` header, validates format,
  calls `storage.append()`, returns 204 with `Stream-Next-Offset` header.
  It does NOT implement producer sequencing logic. That's in `protocol/producer.rs`.

## Isolate Streaming Complexity

- SSE and long-poll logic lives in dedicated modules with tight types.
- `Stream<Item = ...>` types do not leak beyond `handlers/get.rs`.
- Use `tokio::sync::broadcast` for live notifications. Handlers subscribe.
- Keep the streaming state machine explicit and testable.

## Function Size Limits

- Keep functions under ~40 lines. If longer, extract helpers.
- One responsibility per function.
- If you need a comment block to explain what a section does, extract it.

## Testing Requirements

- Every invariant gets a unit test. Especially:
  - Offset monotonicity under concurrency
  - Producer sequencing conflict handling (gaps, duplicates, epoch fencing)
  - Cursor/offset resume boundaries
  - Error precedence (closed stream > content-type mismatch > sequence regression)
- Mock external dependencies in unit tests. Use real HTTP in integration tests.

## Clippy and Lints

- Clippy pedantic lints are enabled. Treat all warnings as errors.
- Suppress specific lints at the item level with `#[allow(clippy::...)]` and a
  comment explaining why, not at the crate level.
- Common justifiable suppressions:
  - `missing_errors_doc` on internal functions (not public API)
  - `module_name_repetitions` when it improves clarity (e.g. `OffsetParser`)
- Fix clippy before moving to the next task. No "clean up later" debt.

## Related Skills (`.agents/skills/`)

When making code decisions in `src/`, consult these skills for Rust-specific guidance:

| Area | Skill | When |
|------|-------|------|
| Ownership & borrows | m01-ownership | Borrow checker errors, ownership design |
| Smart pointers | m02-resource | Choosing Box/Rc/Arc/Cell/RefCell |
| Interior mutability | m03-mutability | RwLock/Mutex patterns, Cell/RefCell |
| Generics & traits | m04-zero-cost | Trait bounds, static vs dynamic dispatch |
| Type-driven design | m05-type-driven | Newtypes, typestate, validation at construction |
| Error handling | m06-error-handling | thiserror patterns, Result/Option, ? propagation |
| Concurrency | m07-concurrency | async/await, Send/Sync, Mutex vs RwLock |
| Web patterns | domain-web | axum handlers, extractors, middleware, state |
| Code style | coding-guidelines | Naming, formatting, idiomatic patterns |
