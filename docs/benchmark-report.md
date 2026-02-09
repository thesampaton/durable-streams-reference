# Benchmark Report

- Generated (Local): 2026-02-09 21:53:39 AEDT
- Generated (UTC): 2026-02-09 10:53:39 UTC
- Branch: `feat/filestorage`
- Current commit (short): `1df0d84`
- Commit links: TODO (to be added)

## Run Setup

- Rust benchmarks: `make benchmark`
- Node benchmarks: `make benchmark-node`
- Harness: `@durable-streams/benchmarks` in `/tmp/benchmark-run`
- Result artifacts:
  - `/tmp/benchmark-run/benchmark-results-memory.json`
  - `/tmp/benchmark-run/benchmark-results-file.json`
  - `/tmp/benchmark-run/benchmark-results-node-memory.json`
  - `/tmp/benchmark-run/benchmark-results-node-file.json`

## Core Metrics (Mean / P99)

| Implementation | RTT Latency Mean (ms) | RTT Latency P99 (ms) | Small Msg Throughput Mean (msg/s) | Small Msg P99 (msg/s) | Large Msg Throughput Mean (msg/s) | Large Msg P99 (msg/s) |
|---|---:|---:|---:|---:|---:|---:|
| Rust Memory | 0.402 | 1.008 | 351,923 | 416,428 | 1,789 | 2,176 |
| Rust File | 5.666 | 7.219 | 6,507 | 6,996 | 518 | 564 |
| Node Memory | 0.357 | 0.663 | 311,644 | 365,620 | 1,567 | 1,920 |
| Node File | 10.382 | 11.985 | 3,701 | 3,875 | 366 | 390 |

## Cross-Implementation Ratios

| Comparison | Ratio |
|---|---:|
| Memory latency (Node/Rust) | 0.889x |
| Memory small throughput (Rust/Node) | 1.129x |
| Memory large throughput (Rust/Node) | 1.142x |
| File latency (Node/Rust) | 1.832x |
| File small throughput (Rust/Node) | 1.758x |
| File large throughput (Rust/Node) | 1.417x |

## Memory -> File Degradation

| Implementation | Latency Increase (File/Memory) | Small Throughput Retained | Large Throughput Retained |
|---|---:|---:|---:|
| Rust | 14.10x slower | 1.85% | 28.99% |
| Node | 29.06x slower | 1.19% | 23.36% |

## Notes

- File-mode benchmarks were run with Rust durable mode (`STORAGE_MODE=file-durable`).
- Rust benchmark limits were increased for this run so large-message scenarios fit:
  - `MAX_MEMORY_BYTES=536870912`
  - `MAX_STREAM_BYTES=268435456`
- Benchmark output mode defaults to quiet; set `BENCHMARK_VERBOSE=1` for verbose output.
