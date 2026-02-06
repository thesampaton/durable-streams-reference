# Append Semantics

Version: 1.0.0
Status: stable

## Overview

Defines how data is appended to streams via POST requests. Covers offset generation, content-type validation, empty body handling, and stream closure operations.

## Append Data (POST)

### Request

**Method:** `POST /v1/stream/{name}`

**Headers:**
- `Content-Type` (required): Must match stream's content type
- `Stream-Seq` (optional): Client-provided sequence number for ordering validation
- `Stream-Closed` (optional): If `"true"`, closes the stream (with or without data)

**Body:** Message data to append (may be empty if Stream-Closed is true)

### Response

**204 No Content** - Data appended successfully
- `Stream-Next-Offset`: The next offset that will be assigned
- No response body

**400 Bad Request** - Invalid request:
- Empty body without Stream-Closed header
- Missing Content-Type header
- Stream does not exist

**404 Not Found** - Stream does not exist

**409 Conflict** - Validation failure:
- Content-Type mismatch with stream configuration
- Stream is already closed
- Stream-Seq regression (if provided)

### Behavior

The server MUST:
- Generate a unique, monotonically increasing offset for each appended message
- Validate Content-Type matches the stream's configured content type (case-insensitive)
- Reject empty body unless Stream-Closed header is present
- Return the next offset that will be assigned in Stream-Next-Offset header
- Serialize concurrent appends to the same stream to maintain offset monotonicity
- Allow concurrent appends to different streams

The server SHOULD:
- Optimize for high-throughput sequential appends
- Minimize lock contention between streams

The server MAY:
- Impose limits on message size
- Impose limits on total stream size

## Stream Closure

### Close Without Append

**Request:**
```
POST /v1/stream/{name}
Content-Type: text/plain
Stream-Closed: true
```
(Empty body)

**Response:** 204 No Content with Stream-Next-Offset

### Close With Append

**Request:**
```
POST /v1/stream/{name}
Content-Type: text/plain
Stream-Closed: true

final message
```

**Response:** 204 No Content with Stream-Next-Offset pointing after the appended message

### Behavior After Closure

Once closed, the stream MUST:
- Reject further append attempts with 409 Conflict
- Include `Stream-Closed: true` in error response
- Include `Stream-Next-Offset` showing the final offset

## Content-Type Validation

Content-Type comparison MUST be case-insensitive and MUST ignore charset parameters.

Examples of valid matches:
- Request: `text/plain`, Stream: `text/plain` ✓
- Request: `TEXT/PLAIN`, Stream: `text/plain` ✓
- Request: `text/plain; charset=utf-8`, Stream: `text/plain` ✓

Examples of mismatches:
- Request: `application/json`, Stream: `text/plain` ✗ (409 Conflict)
- Request: `text/html`, Stream: `text/plain` ✗ (409 Conflict)

## Offset Generation

Offsets are generated server-side with the format: `{read_seq:016x}_{byte_offset:016x}`

Properties:
- Unique within a stream
- Monotonically increasing (lexicographically)
- Sequential reads can resume from any offset
- `read_seq` increments with each message
- `byte_offset` tracks cumulative byte position

Example sequence:
```
0000000000000000_0000000000000000  (message 1, 5 bytes)
0000000000000001_0000000000000005  (message 2, 10 bytes)
0000000000000002_000000000000000f  (message 3, 8 bytes)
```

## Examples

### Append Single Message

```
POST /v1/stream/my-stream
Content-Type: text/plain

Hello, world!

→ 204 No Content
Stream-Next-Offset: 0000000000000001_000000000000000d
```

### Content-Type Mismatch

```
POST /v1/stream/my-stream
Content-Type: application/json

{"data": "value"}

→ 409 Conflict
Stream-Closed: false
(if stream was created with text/plain)
```

### Append to Closed Stream

```
POST /v1/stream/closed-stream
Content-Type: text/plain

More data

→ 409 Conflict
Stream-Closed: true
Stream-Next-Offset: 0000000000000005_0000000000000032
```

### Close Stream Without Data

```
POST /v1/stream/my-stream
Content-Type: text/plain
Stream-Closed: true

→ 204 No Content
Stream-Next-Offset: 0000000000000003_000000000000001a
```

### Close Stream With Final Message

```
POST /v1/stream/my-stream
Content-Type: text/plain
Stream-Closed: true

goodbye

→ 204 No Content
Stream-Next-Offset: 0000000000000004_0000000000000021
```

## Conformance

This spec covers conformance test blocks:
- Append Operations
- HTTP Protocol (POST)
- Content-Type Validation
- Protocol Edge Cases (empty body, close operations)
- Stream Closure

## Traceability

- Upstream: PROTOCOL.md#append-data
- Conformance: `describe('Append Operations', ...)`
- Conformance: `describe('Stream Closure', ...)`

## Gaps

None identified. Spec aligns with upstream protocol and conformance tests.
