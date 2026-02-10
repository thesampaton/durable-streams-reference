# durable-streams-reference

[![CI](https://github.com/thesampaton/durable-streams-reference/actions/workflows/ci.yml/badge.svg?branch=trunk)](https://github.com/thesampaton/durable-streams-reference/actions/workflows/ci.yml)
[![Conformance](https://github.com/thesampaton/durable-streams-reference/actions/workflows/conformance.yml/badge.svg?branch=trunk)](https://github.com/thesampaton/durable-streams-reference/actions/workflows/conformance.yml)

Reference implementation of the [durable streams protocol](https://github.com/durable-streams/durable-streams) in rust, with idiomatic approaches to fulfilling the protocol specification and near-parity with the caddy production implementation. This project exists to validate ai-enabled development approaches, to pressure test the durable streams approach and to document how to deliver an end to end solution with durable streams, electric sql and postgres, using the gatekeeper auth pattern.

Documents https://thesampaton.github.io/durable-streams-reference/

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

## Benchmark findings

The latest benchmark matrix findings are in
[`docs/benchmark-report.md`](docs/benchmark-report.md). It includes:

- 3 runs each for Rust (`memory`, `file-durable`, `acid`), Node (`memory`,
  `file`), and Caddy plugin (`memory`, `acid` label for plugin file-backed mode)
- fresh Rust PGO profile training before measurements
- explicit durability semantics notes for `acid` vs Caddy `acid` interpretation

## Conformance

This implementation targets full conformance with [`@durable-streams/server-conformance-tests@0.2.1`](https://www.npmjs.com/package/@durable-streams/server-conformance-tests) against spec commit [`a347312`](https://github.com/durable-streams/durable-streams/blob/a347312a47ae510a4a2e3ee7a121d6c8d7d74e50/PROTOCOL.md).

Run conformance tests locally:

```bash
DS_SERVER__LONG_POLL_TIMEOUT_SECS=2 DS_SERVER__SSE_RECONNECT_INTERVAL_SECS=5 cargo run &

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

The server now supports layered TOML config with env overrides.

Load order (later wins):
1. built-in defaults
2. `config/default.toml` (if present)
3. `config/<profile>.toml` (if present, via `--profile`)
4. `config/local.toml` (if present; gitignored)
5. extra file from `--config <path>`
6. environment variables

CLI:

```bash
# Uses config/default.toml + config/dev.toml (+ config/local.toml if present)
cargo run -- --profile dev

# Add an extra override file on top
cargo run -- --profile prod --config /etc/durable-streams/override.toml
```

Environment variables use `DS_` prefix with double-underscore section separators:

| Variable | Default | Description |
|---|---|---|
| `DS_SERVER__PORT` | `4437` | Server listen port |
| `DS_SERVER__LONG_POLL_TIMEOUT_SECS` | `30` | Long-poll timeout in seconds |
| `DS_SERVER__SSE_RECONNECT_INTERVAL_SECS` | `60` | SSE reconnect interval |
| `DS_HTTP__CORS_ORIGINS` | `*` | Allowed CORS origins |
| `DS_LIMITS__MAX_MEMORY_BYTES` | `104857600` | Global in-memory cap |
| `DS_LIMITS__MAX_STREAM_BYTES` | `10485760` | Per-stream byte cap |
| `DS_STORAGE__MODE` | `memory` | Storage backend: `memory`, `file-fast`, `file-durable`, `acid` (alias: `redb`) |
| `DS_STORAGE__DATA_DIR` | `./data/streams` | Root directory for file/acid persistent storage |
| `DS_STORAGE__ACID_SHARD_COUNT` | `16` | Number of redb shards for acid mode (power-of-2, `1..=256`) |
| `DS_TLS__CERT_PATH` | _(unset)_ | Optional PEM certificate path for direct TLS termination (requires `DS_TLS__KEY_PATH`) |
| `DS_TLS__KEY_PATH` | _(unset)_ | Optional PEM/PKCS#8 key path for direct TLS termination (requires `DS_TLS__CERT_PATH`) |
| `RUST_LOG` | `info` | Log level filter (standard tracing env var) |
