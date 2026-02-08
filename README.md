# durable-streams-rust-server

[![CI](https://github.com/thesampaton/durable-streams-rust-server/actions/workflows/ci.yml/badge.svg?branch=trunk)](https://github.com/thesampaton/durable-streams-rust-server/actions/workflows/ci.yml)
[![Conformance](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fthesampaton%2Fdurable-streams-rust-server%2Fbadges%2Fconformance.json)](https://github.com/thesampaton/durable-streams-rust-server/actions/workflows/conformance.yml)

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

## Conformance

This implementation targets full conformance with [`@durable-streams/server-conformance-tests@0.2.1`](https://www.npmjs.com/package/@durable-streams/server-conformance-tests) against spec commit [`a347312`](https://github.com/durable-streams/durable-streams/blob/a347312a47ae510a4a2e3ee7a121d6c8d7d74e50/PROTOCOL.md).

Run conformance tests locally (see `/private/tmp/conformance-run` for a working setup):

```bash
LONG_POLL_TIMEOUT_SECS=2 SSE_IDLE_CLOSE_SECS=5 cargo run &

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
| `SSE_IDLE_CLOSE_SECS` | `60` | SSE idle connection close timeout |
| `RUST_LOG` | `info` | Log level filter |
