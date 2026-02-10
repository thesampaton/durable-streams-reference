# durable-streams-reference

[![CI](https://github.com/thesampaton/durable-streams-reference/actions/workflows/ci.yml/badge.svg?branch=trunk)](https://github.com/thesampaton/durable-streams-reference/actions/workflows/ci.yml)
[![Conformance](https://github.com/thesampaton/durable-streams-reference/actions/workflows/conformance.yml/badge.svg?branch=trunk)](https://github.com/thesampaton/durable-streams-reference/actions/workflows/conformance.yml)

Reference implementation of the [durable streams protocol](https://github.com/durable-streams/durable-streams), built on [Electric SQL](https://electric-sql.com/).

## Quick start

```bash
cargo run
```

The server listens on `http://localhost:4437` with streams at `/v1/stream/`.

## Build and test

```bash
cargo build          # compile
cargo test           # unit + integration tests
cargo clippy -- -D warnings  # lint
cargo fmt            # format
```

## Performance builds (PGO)

Use profile-guided optimization (PGO) for production-oriented builds:

```bash
# 1) Generate and merge fresh profiles using benchmark traffic
make pgo-train

# 2) Build release binary with profile-use (fails if profile is missing/stale)
make release-pgo

# 3) Optional: compare profile-use performance vs baseline
make pgo-benchmark
```

Key PGO controls:

- `PGO_DIR` (default: `/tmp/ds-pgo`) for profile artifacts
- `PGO_REQUIRE_FRESH` (default: `true`) to enforce freshness checks
- `PGO_MAX_AGE_HOURS` (default: `168`) maximum allowed profile age
- `PGO_WARN_MISSING` (default: `false`) enables/disables LLVM missing-profile warnings

CI also has a dedicated PGO workflow at `.github/workflows/pgo-release.yml` that
trains profiles and publishes a `release-pgo` artifact.

## Conformance

This implementation targets full conformance with [`@durable-streams/server-conformance-tests@0.2.1`](https://www.npmjs.com/package/@durable-streams/server-conformance-tests) against spec commit [`a347312`](https://github.com/durable-streams/durable-streams/blob/a347312a47ae510a4a2e3ee7a121d6c8d7d74e50/PROTOCOL.md).

Run conformance tests locally:

```bash
LONG_POLL_TIMEOUT_SECS=2 SSE_RECONNECT_INTERVAL_SECS=5 cargo run &

cd /tmp/conformance-run
npm init -y
npm install @durable-streams/server-conformance-tests@0.2.1
cat > conformance.test.mjs << 'EOF'
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
runConformanceTests({ baseUrl: process.env.CONFORMANCE_TEST_URL, longPollTimeoutMs: 2000 });
EOF

CONFORMANCE_TEST_URL=http://localhost:4437 npx vitest run conformance.test.mjs
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PORT` | `4437` | Server listen port |
| `LONG_POLL_TIMEOUT_SECS` | `30` | Long-poll timeout in seconds |
| `SSE_RECONNECT_INTERVAL_SECS` | `60` | SSE reconnect interval (matches Caddy's `sse_reconnect_interval`) |
| `STORAGE_MODE` | `memory` | Storage backend: `memory`, `file-fast`, `file-durable`, `acid` (alias: `redb`) |
| `DATA_DIR` | `./data/streams` | Root directory for file/acid persistent storage |
| `ACID_SHARD_COUNT` | `16` | Number of redb shards for `STORAGE_MODE=acid` (power-of-2, `1..=256`) |
| `RUST_LOG` | `info` | Log level filter |
