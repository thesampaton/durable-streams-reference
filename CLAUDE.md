# Durable Streams Rust Server

## Build & Run Commands

- **Build:** `cargo build`
- **Run:** `cargo run`
- **Test:** `cargo test`
- **Single test:** `cargo test test_name`
- **Lint:** `cargo clippy -- -D warnings`
- **Format:** `cargo fmt`
- **Format check:** `cargo fmt -- --check`

## Development Workflow

1. After every code change, run `cargo clippy -- -D warnings` to catch lint issues. Fix all warnings before moving on.
2. Run `cargo test` after changes to verify correctness.
3. Run `cargo fmt` before considering any task done.

## Code Style & Conventions

- Clippy pedantic lints are enabled project-wide via `Cargo.toml` `[lints.clippy]`. Treat all warnings as errors.
- If a specific pedantic lint is genuinely inapplicable to a particular item, suppress it with a targeted `#[allow(clippy::...)]` on that item with a comment explaining why. Do not add blanket allows at the crate level.
- Use `thiserror` for library error types, `anyhow` for application-level error propagation (add when needed).
- Prefer returning `Result` over panicking. Reserve `unwrap()`/`expect()` for cases where invariants are provably upheld, and add a comment explaining why.
- Write unit tests in a `#[cfg(test)] mod tests` block in the same file as the code under test.
- Use `snake_case` for functions/variables, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Keep functions short and focused. Extract helpers when a function exceeds ~40 lines.
- Prefer strong typing over stringly-typed APIs. Use newtypes and enums to make invalid states unrepresentable.

## Git Conventions

- Never include `Co-Authored-By` trailers in commit messages.
- Write concise commit messages explaining *why*, not *what*.
- Separate logical changes into distinct commits.