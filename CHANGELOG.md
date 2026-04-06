# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2026-04-06

### Changed

- Upgrade `axum-server` 0.7.3 → 0.8.0 (generic `Server`/`Handle`, unix socket
  support, HTTP version filtering).
- Upgrade `redb` 3 → 4.0.0 (picks up `AccessGuardMut` data-loss fix; no code
  changes required, on-disk format compatible with 3.x).
- Update all transitive dependencies (tokio 1.51, hyper 1.9, rustls-webpki
  0.103.10, arc-swap 1.9, mio 1.2, and others).

### Removed

- Remove unused `tokio-test` dev-dependency.

## [0.1.2] - 2026-03-23

### Fixed

- SSE JSON batching: batch messages into single `data` event per read.
- Spec 01 stream lifecycle: PUT accepts body, Content-Type is optional.

### Changed

- Update docs to reflect all storage backends (memory, file, acid).
- Add `DS_LOG__RUST_LOG` to README env var table.

## [0.1.1] - 2026-03-16

### Fixed

- Fix fmt check in memory storage test.
- Include invalid values in env override error messages.

### Changed

- Shorten crate name to `durable-streams-server`.
- Remove unused `tower` and `rand` dependencies, exclude `CLAUDE.md` from crate.
- Exclude non-essential files from crates.io package.
- Add keywords and categories for crates.io discoverability.
- Upgrade GitHub Actions to Node.js 24-compatible versions.

[0.1.3]: https://github.com/thesampaton/durable-streams-rust-server/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/thesampaton/durable-streams-rust-server/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/thesampaton/durable-streams-rust-server/releases/tag/v0.1.1
