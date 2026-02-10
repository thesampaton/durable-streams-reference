# Benchmark Findings (3x Matrix, Fresh PGO)

This report replaces the older benchmark narrative with a single findings-focused
snapshot from a full 3-run matrix.

## Run setup

- Date (UTC): 2026-02-10
- Repo: `durable-streams-rust-server`
- Branch: `feat/acid-file-storage`
- Harness: `@durable-streams/benchmarks@0.2.1`
- Matrix size: 21 runs total (7 variants x 3 runs each)
- Rust variants were run with fresh PGO profiles generated in this code state:
  - `make pgo-clean pgo-train-memory pgo-train-file pgo-train-acid pgo-merge`
  - then `pgo-benchmark-*` for Rust memory/file/acid
- Artifacts: `/tmp/benchmark-run/matrix-20260210-142910`

Variants:

- `rust-memory`
- `rust-file` (`DS_STORAGE__MODE=file-durable`)
- `rust-acid` (`DS_STORAGE__MODE=acid`)
- `node-memory`
- `node-file`
- `caddy-memory`
- `caddy-acid` (Caddy plugin file-backed store)

## Core results (mean of run means)

| Variant | RTT latency (ms) | Small throughput (msg/s) | Large throughput (msg/s) | RTT CV | Small CV | Large CV |
|---|---:|---:|---:|---:|---:|---:|
| rust-memory | 0.407 | 377,143.9 | 1,837.2 | 11.1% | 2.5% | 0.5% |
| rust-file | 5.379 | 6,441.5 | 530.7 | 0.6% | 1.1% | 2.9% |
| rust-acid | 5.908 | 6,495.9 | 454.7 | 0.4% | 1.4% | 3.9% |
| node-memory | 0.573 | 334,943.9 | 1,653.9 | 29.3% | 1.8% | 1.2% |
| node-file | 11.377 | 3,240.0 | 360.1 | 4.2% | 2.0% | 2.1% |
| caddy-memory | 0.342 | 313,490.9 | 1,144.4 | 2.1% | 1.2% | 0.8% |
| caddy-acid | 16.067 | 2,305.0 | 271.6 | 0.7% | 0.1% | 3.1% |

## Rust acid vs Caddy acid

| Metric | Rust acid | Caddy acid | Relative |
|---|---:|---:|---:|
| RTT latency (ms, lower is better) | 5.908 | 16.067 | Rust acid 2.72x lower |
| Small throughput (msg/s, higher is better) | 6,495.9 | 2,305.0 | Rust acid 2.82x higher |
| Large throughput (msg/s, higher is better) | 454.7 | 271.6 | Rust acid 1.67x higher |

Durability caveat, phrased precisely:

- Caddy plugin file-backed mode (named `caddy-acid` in this report) currently
  documents a crash-atomicity limitation: data
  append and producer metadata update are not atomically committed together.
- Rust `acid` mode commits stream state and message data in one ACID transaction.
- That means Rust `acid` is not gaining performance by weakening durability in the
  same way; if anything, it is carrying stronger commit guarantees in this
  comparison.
- This is not a criticism of Electric or the Caddy production plugin. It is only
  a precision note so semantics are interpreted correctly.

Reference:

- `.dev/durable-streams/packages/caddy-plugin/README.md` (Known Limitations:
  File Store Crash-Atomicity)

## Rust storage mode comparison

| Metric | memory | file-durable | acid |
|---|---:|---:|---:|
| RTT latency (ms) | 0.407 | 5.379 | 5.908 |
| Small throughput (msg/s) | 377,143.9 | 6,441.5 | 6,495.9 |
| Large throughput (msg/s) | 1,837.2 | 530.7 | 454.7 |

Relative to Rust `file-durable`:

- `acid` RTT latency: +9.8%
- `acid` small throughput: +0.8%
- `acid` large throughput: -14.3%

## Interpretation

- Rust `acid` lands close to Rust `file-durable` on small-message throughput while
  adding transactional crash guarantees.
- Rust `file-durable` remains better for the tested large-message throughput
  pattern in this harness.
- Against Caddy `acid`, Rust `acid` leads on all three core metrics in this local
  benchmark matrix.

## Operational and environmental notes

- These are local synthetic benchmarks, not isolated distributed load tests.
- Producer/load-generator and server processes share the same machine in this run.
- Variance is visible in some scenarios (notably Node memory RTT CV 29.3%).
- A stricter cloud benchmark should isolate clients and servers, pin hardware
  resources, and run longer steady-state windows before drawing final
  cross-implementation conclusions.
