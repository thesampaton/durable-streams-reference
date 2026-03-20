# Producer Sequencing

Version: 1.0.0
Status: stable

## Overview

Defines idempotent producer support for exactly-once write semantics. Producers
identify themselves with a stable ID, use monotonic epoch numbers for session
management, and monotonic sequence numbers for per-request deduplication. The
server validates producer state and deduplicates retried appends automatically.

## Producer Headers

Three headers control producer sequencing. All three MUST be provided together
or none at all. Partial sets are rejected.

### Request Headers

- `Producer-Id`: Non-empty string identifying the logical producer. Stable
  across restarts (the producer increments epoch instead of changing ID).
- `Producer-Epoch`: Non-negative integer (0 to 2^53-1). Starts at 0, incremented
  on producer restart to establish a new session.
- `Producer-Seq`: Non-negative integer (0 to 2^53-1). Monotonically increasing
  per epoch, starting at 0 for each new epoch. Applies per-request (per HTTP
  POST), not per-message within a batch.

### Response Headers

- `Producer-Epoch`: Echoed on success (200/204). On epoch fencing (403), returns
  the server's current epoch for the producer.
- `Producer-Seq`: On success, the highest accepted sequence number for the
  `(stream, producerId, epoch)` tuple.
- `Producer-Expected-Seq`: On 409 (sequence gap), the sequence value the server
  expected.
- `Producer-Received-Seq`: On 409 (sequence gap), the sequence value the server
  received.

## Validation Rules

The server MUST validate producer state atomically with the append operation.
No data is appended unless validation passes.

### Epoch Validation

The server MUST:
- Accept epoch >= current epoch (with sequence constraints below)
- Reject epoch < current epoch with `403 Forbidden` and `Producer-Epoch` header
  showing the server's current epoch (zombie fencing)
- Require seq = 0 when epoch > current epoch (new session must start at 0)
- Reject epoch > current epoch with seq != 0 as `400 Bad Request`
- When accepting a higher epoch, reset the producer's sequence tracking

### Sequence Validation (same epoch)

The server MUST:
- Accept seq = lastSeq + 1 (next expected) → `200 OK`, append data
- Return `204 No Content` for seq <= lastSeq (duplicate, already persisted)
- Return `409 Conflict` for seq > lastSeq + 1 (gap) with `Producer-Expected-Seq`
  and `Producer-Received-Seq` headers

### Header Validation

The server MUST return `400 Bad Request` when:
- Only some of the three producer headers are present (partial set)
- `Producer-Id` is empty
- `Producer-Epoch` is not a valid non-negative integer
- `Producer-Seq` is not a valid non-negative integer

## Response Codes

| Code | Meaning | When |
|------|---------|------|
| 200 OK | New data accepted | seq = lastSeq + 1 (or first append) |
| 204 No Content | Duplicate (idempotent success) | seq <= lastSeq |
| 400 Bad Request | Invalid headers | Partial, empty ID, non-integer, epoch bump with seq != 0 |
| 403 Forbidden | Epoch fenced | epoch < server's current epoch |
| 409 Conflict | Sequence gap | seq > lastSeq + 1 |

## Bootstrap and Restart Flows

### Initial Startup

```
POST /v1/stream/my-stream
Producer-Id: producer-1
Producer-Epoch: 0
Producer-Seq: 0
Content-Type: text/plain

hello

→ 200 OK
Producer-Epoch: 0
Producer-Seq: 0
Stream-Next-Offset: 0000000000000001_0000000000000005
```

### Producer Restart (Epoch Bump)

```
POST /v1/stream/my-stream
Producer-Id: producer-1
Producer-Epoch: 1
Producer-Seq: 0
Content-Type: text/plain

restarted

→ 200 OK
Producer-Epoch: 1
Producer-Seq: 0
```

### Duplicate Detection

```
POST /v1/stream/my-stream
Producer-Id: producer-1
Producer-Epoch: 0
Producer-Seq: 0
Content-Type: text/plain

hello

→ 204 No Content
Producer-Epoch: 0
Producer-Seq: 0
```

### Sequence Gap

```
POST /v1/stream/my-stream
Producer-Id: producer-1
Producer-Epoch: 0
Producer-Seq: 5
Content-Type: text/plain

skipped

→ 409 Conflict
Producer-Expected-Seq: 1
Producer-Received-Seq: 5
```

### Zombie Fencing

```
POST /v1/stream/my-stream
Producer-Id: producer-1
Producer-Epoch: 0
Producer-Seq: 1
Content-Type: text/plain

zombie

→ 403 Forbidden
Producer-Epoch: 1
```

## Stream Closure with Producers

### Close with Final Append

The server MUST atomically validate the producer sequence, append data, and
close the stream. If validation fails, the stream remains open.

```
POST /v1/stream/my-stream
Producer-Id: producer-1
Producer-Epoch: 0
Producer-Seq: 2
Stream-Closed: true
Content-Type: text/plain

final

→ 200 OK
Producer-Epoch: 0
Producer-Seq: 2
Stream-Closed: true
```

### Duplicate of Closing Append

If the producer retries the exact request that closed the stream
(same epoch + seq), the server MUST return `204 No Content` with
`Stream-Closed: true`.

### Append to Closed Stream (Different Sequence)

If a producer sends a new sequence to a closed stream, the server MUST
return `409 Conflict` with `Stream-Closed: true`.

## Producer State Lifecycle

### State Tracking

The server tracks per `(stream, producerId)`:
- Current epoch
- Last accepted sequence number
- Last updated timestamp

### Cleanup

The server SHOULD use a 7-day TTL on producer state, cleaning up stale
entries on stream access. After state expiry, the producer is treated as new.

## Concurrency

The server MUST serialize validation and append per `(stream, producerId)` pair.
Holding the per-stream write lock during both validation and append satisfies
this requirement across all storage backends.

## Conformance

This spec covers conformance test blocks:
- Idempotent Producers
- Producer Epoch Fencing
- Producer Sequence Validation

## Traceability

- Upstream: PROTOCOL.md#idempotent-producers
- Conformance: `describe('Idempotent Producers', ...)`

## Gaps

None identified. Spec aligns with upstream protocol and conformance tests.
