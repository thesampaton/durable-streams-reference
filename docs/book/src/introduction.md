# Introduction

This is the reference implementation of the [durable streams protocol](https://github.com/durable-streams/durable-streams), built in Rust with [Electric SQL](https://electric-sql.com/) for Postgres sync.

## What are durable streams?

A durable stream is an append-only log exposed over HTTP. Clients create streams, append messages, and read them back using standard HTTP methods (PUT, POST, GET, DELETE). The protocol adds three capabilities on top of plain HTTP:

- **Offset resumption.** Every read returns an offset. Clients save it and resume later without replaying history. Close a tab, reopen it, pick up exactly where you left off.
- **Live delivery.** Clients can long-poll or open an SSE connection to receive new messages the instant they arrive.
- **Producer idempotency.** Producers tag each append with an epoch and sequence number. The server deduplicates retries automatically, giving exactly-once semantics over an at-least-once transport.

## What this project demonstrates

This repository contains two things:

1. **A protocol server** written in Rust (axum + tokio). It passes the full conformance test suite and stores streams in memory. It has no authentication, no database, and no opinions about deployment. It is the simplest correct implementation of the protocol.

2. **A full deployment stack** that shows how to compose the server with real infrastructure:
   - **Envoy** as a JWT auth proxy in front of the server
   - **Postgres** as durable storage
   - **Electric SQL** for WAL-based change replication
   - **A sync service** that bridges streams and Postgres bidirectionally

Together they implement the *durable sessions* pattern: real-time collaborative applications (like AI chat) where messages are delivered instantly via SSE and durably stored in Postgres for querying, analytics, and recovery.

## Who should read this

- **Protocol integrators** who want to write a client against the durable streams API. Start with the [Quickstart](quickstart.md) and the [Protocol](protocol/streams.md) section.
- **Operators** who want to run the full stack with auth, Postgres, and sync. Start with the [Architecture](architecture/overview.md) and [Deployment](deployment/docker-compose.md) sections.
- **Contributors** who want to extend the server or contribute to the protocol. Start with [Contributing](project/contributing.md) and the [Reference](reference/api.md) section.

## Conformance

This implementation targets full conformance with [`@durable-streams/server-conformance-tests@0.2.2`](https://www.npmjs.com/package/@durable-streams/server-conformance-tests) against spec commit [`a347312`](https://github.com/durable-streams/durable-streams/blob/a347312a47ae510a4a2e3ee7a121d6c8d7d74e50/PROTOCOL.md). All 239 conformance tests pass.
