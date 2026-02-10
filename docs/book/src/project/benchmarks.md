# Benchmarks

This page summarizes the latest benchmark findings from a full 3x matrix run
(7 variants x 3 runs = 21 total), with fresh Rust PGO profiles regenerated for
this code state.

## Core results (mean of run means)

| Variant | RTT latency (ms) | Small throughput (msg/s) | Large throughput (msg/s) |
|---|---:|---:|---:|
| rust-memory | 0.407 | 377,143.9 | 1,837.2 |
| rust-file (`file-durable`) | 5.379 | 6,441.5 | 530.7 |
| rust-acid (`acid`) | 5.908 | 6,495.9 | 454.7 |
| node-memory | 0.573 | 334,943.9 | 1,653.9 |
| node-file | 11.377 | 3,240.0 | 360.1 |
| caddy-memory | 0.342 | 313,490.9 | 1,144.4 |
| caddy-acid | 16.067 | 2,305.0 | 271.6 |

## Rust acid vs Caddy acid

| Metric | Rust acid | Caddy acid | Relative |
|---|---:|---:|---:|
| RTT latency (ms, lower is better) | 5.908 | 16.067 | Rust acid 2.72x lower |
| Small throughput (msg/s, higher is better) | 6,495.9 | 2,305.0 | Rust acid 2.82x higher |
| Large throughput (msg/s, higher is better) | 454.7 | 271.6 | Rust acid 1.67x higher |

Durability note:

- Caddy plugin file-backed mode (named `caddy-acid` in this page) documents a
  crash-atomicity limitation for
  append-data + producer-metadata updates.
- Rust `acid` uses transactional commits for stream state and message data.
- So this Rust `acid` result is not achieved by relaxing durability semantics in
  the same way.

## Rust storage modes

| Metric | memory | file-durable | acid |
|---|---:|---:|---:|
| RTT latency (ms) | 0.407 | 5.379 | 5.908 |
| Small throughput (msg/s) | 377,143.9 | 6,441.5 | 6,495.9 |
| Large throughput (msg/s) | 1,837.2 | 530.7 | 454.7 |

Relative to Rust `file-durable`:

- `acid` RTT latency: +9.8%
- `acid` small throughput: +0.8%
- `acid` large throughput: -14.3%

## Benchmark quality notes

- These numbers are from local synthetic tests.
- Load generator and server processes share one machine in this run.
- For production-grade comparability, run clients and servers on separate hosts
  and keep sustained windows longer than quick local iterations.

For full methodology and caveats, see `/docs/benchmark-report.md` in the repo
root.
