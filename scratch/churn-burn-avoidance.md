Fix clippy warnings immediately. No “we’ll clean up later” debt.

Keep the warning surface small. If you suppress a lint, do it in clippy.toml with a one-line justification, not scattered #[allow].

Prefer owned boundary types. Use Bytes, String, Arc<T> at module/API boundaries; avoid borrowing gymnastics.

Minimize async trait complexity. Keep storage traits sync where possible; wrap with tokio::sync::RwLock/Mutex at the edge. Only go async_trait when you truly need it.

Ban cleverness in the hot path. No iterator acrobatics, no lifetime tricks, no generic overengineering. Make it boring and obvious.

Make protocol behavior a thin layer. Keep handlers dumb: parse → validate → call core functions → format response.

Isolate streaming complexity. Put SSE/long-poll logic in one module with tight types; don’t let Stream<Item = ...> types leak everywhere.

Use explicit error types. One enum Error with thiserror, map to HTTP status codes in one place. No ad-hoc anyhow in core logic.

Add unit tests for invariants. Offset monotonicity under concurrency, sequencing conflict handling, cursor/offset resume boundaries.

Don’t refactor while fixing lints. Smallest change that passes clippy and tests; avoid “cleanup spirals.”