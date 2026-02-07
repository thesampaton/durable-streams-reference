# Read Modes

Version: 2.0.0
Status: stable

## Overview

Defines how clients read data from streams via GET requests. Covers three read modes: catch-up (replay historical data), long-poll (wait for new data), and SSE (streaming updates). This document covers catch-up and long-poll modes; SSE is covered in a subsequent section.

## Catch-up Mode (GET)

### Request

**Method:** `GET /v1/stream/{name}?offset={offset}`

**Query Parameters:**
- `offset` (optional): Starting offset for read
  - Sentinel `-1`: Start from beginning of stream
  - Sentinel `now`: Start from current tail (returns empty)
  - Hex format: `{read_seq:016x}_{byte_offset:016x}` - resume from specific offset
  - Default: `-1` (stream start)

**Headers:**
- `If-None-Match` (optional): ETag from previous read for 304 optimization

### Response

**200 OK** - Data returned
- `Content-Type`: Stream's configured content type
- `Stream-Next-Offset`: Next offset to read from (for resumption)
- `Stream-Up-To-Date`: `"true"` if at tail, `"false"` otherwise
- `Stream-Closed`: `"true"` if stream is closed and at tail, omitted otherwise
- `ETag`: Identifier for this read range (format: `"{start}:{end}"` or `"{start}:{end}:c"` if closed)
- `Cache-Control`: `no-store`
- **Body**: Concatenated message data (may be empty if at tail)

**304 Not Modified** - ETag matches (no new data)
- `Stream-Next-Offset`: Current offset
- `Stream-Up-To-Date`: `"true"`
- No body

**404 Not Found** - Stream does not exist or has expired

**400 Bad Request** - Invalid offset format

### Behavior

The server MUST:
- Concatenate all message data from `offset` to current tail
- Return data in the stream's configured `Content-Type`
- Include `Stream-Next-Offset` pointing after the last message returned
- Set `Stream-Up-To-Date: true` when returning all available data up to tail
- Set `Stream-Closed: true` only when both: stream is closed AND reader is at tail
- Generate ETag as `"{start_offset}:{end_offset}"` (append `:c` if closed at tail)
- Handle sentinel offsets: `-1` (start), `now` (tail)
- Support resumable reads via `Stream-Next-Offset`

The server SHOULD:
- Optimize for sequential reads
- Minimize latency for catch-up reads

The server MAY:
- Limit response size (chunk large reads)
- Impose rate limits

### Offset Sentinels

**`-1` (Stream Start):**
```
GET /v1/stream/my-stream?offset=-1

→ 200 OK
Content-Type: text/plain
Stream-Next-Offset: 0000000000000003_000000000000001a
Stream-Up-To-Date: true
ETag: "-1:0000000000000003_000000000000001a"

message1message2message3
```

**`now` (Stream Tail):**
```
GET /v1/stream/my-stream?offset=now

→ 200 OK
Content-Type: text/plain
Stream-Next-Offset: 0000000000000003_000000000000001a
Stream-Up-To-Date: true
ETag: "now:0000000000000003_000000000000001a"

(empty body - already at tail)
```

**Specific Offset (Resume):**
```
GET /v1/stream/my-stream?offset=0000000000000001_0000000000000005

→ 200 OK
Content-Type: text/plain
Stream-Next-Offset: 0000000000000003_000000000000001a
Stream-Up-To-Date: true
ETag: "0000000000000001_0000000000000005:0000000000000003_000000000000001a"

message2message3
```

### ETag and Caching

ETag format: `"{start_offset}:{end_offset}"` or `"{start_offset}:{end_offset}:c"` (closed)

The `:c` suffix indicates the stream was closed when this read occurred AND the reader is at the tail.

**304 Not Modified with If-None-Match:**
```
GET /v1/stream/my-stream?offset=-1
If-None-Match: "-1:0000000000000003_000000000000001a"

→ 304 Not Modified
Stream-Next-Offset: 0000000000000003_000000000000001a
Stream-Up-To-Date: true

(no body)
```

### Stream-Closed Semantics

`Stream-Closed: true` is included ONLY when:
1. The stream is closed, AND
2. The reader is at the tail (no more data to read)

If the stream is closed but the reader is mid-stream (catching up), omit `Stream-Closed`. This prevents confusion during catch-up reads.

**Example - Closed stream, at tail:**
```
GET /v1/stream/my-stream?offset=0000000000000002_0000000000000010

→ 200 OK
Stream-Closed: true
Stream-Up-To-Date: true
Stream-Next-Offset: 0000000000000003_000000000000001a
ETag: "0000000000000002_0000000000000010:0000000000000003_000000000000001a:c"

message3
```

**Example - Closed stream, mid-stream (catching up):**
```
GET /v1/stream/my-stream?offset=-1

→ 200 OK
Stream-Up-To-Date: true
Stream-Next-Offset: 0000000000000003_000000000000001a
ETag: "-1:0000000000000003_000000000000001a"

message1message2message3
```
(Note: No `Stream-Closed` header during catch-up, even though stream is closed)

## Read-Your-Writes Consistency

Clients MUST be able to read data immediately after appending it:

```
POST /v1/stream/my-stream (append "data1")
→ Stream-Next-Offset: 0000000000000001_0000000000000005

GET /v1/stream/my-stream?offset=-1
→ Body contains "data1"
```

No eventual consistency - reads are immediately consistent with writes.

## Resumable Reads

Clients can resume reads from any offset:

```
GET /v1/stream/my-stream?offset=-1
→ Stream-Next-Offset: 0000000000000002_000000000000000a

GET /v1/stream/my-stream?offset=0000000000000002_000000000000000a
→ Returns remaining messages
```

## Long-Poll Mode (GET with live=long-poll)

### Request

**Method:** `GET /v1/stream/{name}?offset={offset}&live=long-poll[&cursor={cursor}]`

**Query Parameters:**
- `offset` (optional): Starting offset, same format as catch-up mode. Default: `-1`
- `live` (required for long-poll): MUST be `"long-poll"`. Other values return 400
- `cursor` (optional): Echo of previous `Stream-Cursor` header value for CDN collapsing

**Headers:**
- `If-None-Match` (optional): ETag from previous read for 304 optimization

### Response

**200 OK** - Data available (immediate catch-up or arrived during wait)
- Same headers as catch-up 200 response
- `Stream-Cursor`: Opaque cursor for subsequent long-poll requests (mandatory)
- **Body**: Concatenated message data

**204 No Content** - Timeout expired or closed stream at tail
- `Stream-Next-Offset`: Current tail offset
- `Stream-Up-To-Date`: `"true"`
- `Stream-Cursor`: Opaque cursor (MUST include when stream is open; MAY omit when `Stream-Closed: true`)
- `Stream-Closed`: `"true"` if stream is closed
- No body

**304 Not Modified** - ETag matches (same as catch-up mode)

**400 Bad Request** - Invalid offset or invalid `live` parameter value

**404 Not Found** - Stream does not exist or has expired

### Behavior

The server MUST:
- If data exists at the requested offset, return it immediately as 200 (same as catch-up, plus `Stream-Cursor`)
- If at tail and stream is closed, immediately return 204 with `Stream-Closed: true` (MUST NOT wait)
- If at tail and stream is open, wait for new data up to an implementation-defined timeout
- Return 204 when the timeout expires with no new data
- Return 200 when data arrives during the wait period
- Include `Stream-Cursor` header on all long-poll responses (200 and 204)
- Reject unknown `live` parameter values with 400

The server SHOULD:
- Use a reasonable default timeout (this implementation uses 30 seconds)

The server MAY:
- Accept a `timeout` query parameter (future extension, not required)
- Impose rate limits (429)

### Timeout

The long-poll timeout is implementation-defined. This server defaults to 30 seconds,
configurable via the `LONG_POLL_TIMEOUT_SECS` environment variable.

When timeout expires:
- Return 204 No Content (not an error)
- Include `Stream-Next-Offset` so client can retry seamlessly
- Include `Stream-Up-To-Date: true`

### Closed Stream at Tail

When the stream is closed AND the client is already at the tail:
- The server MUST NOT wait for the long-poll timeout
- The server MUST immediately return 204 with `Stream-Closed: true`
- This prevents clients from hanging indefinitely on closed streams

### Stream-Cursor

`Stream-Cursor` is an opaque string that clients echo back in subsequent long-poll
requests via the `cursor` query parameter. It enables CDN request collapsing.

The cursor value is implementation-defined. Clients MUST treat it as opaque and
MUST NOT parse or rely on its format.

### Examples

**Long-poll with data already available:**
```
GET /v1/stream/my-stream?offset=-1&live=long-poll

→ 200 OK
Content-Type: text/plain
Stream-Next-Offset: 0000000000000002_000000000000000a
Stream-Up-To-Date: true
Stream-Cursor: 0000000000000002_000000000000000a
ETag: "-1:0000000000000002_000000000000000a"

message1message2
```

**Long-poll waiting, data arrives:**
```
GET /v1/stream/my-stream?offset=0000000000000002_000000000000000a&live=long-poll&cursor=0000000000000002_000000000000000a

(server waits... new data appended...)

→ 200 OK
Content-Type: text/plain
Stream-Next-Offset: 0000000000000003_000000000000001a
Stream-Up-To-Date: true
Stream-Cursor: 0000000000000003_000000000000001a
ETag: "0000000000000002_000000000000000a:0000000000000003_000000000000001a"

newdata
```

**Long-poll timeout (no new data):**
```
GET /v1/stream/my-stream?offset=0000000000000002_000000000000000a&live=long-poll

(server waits... timeout expires...)

→ 204 No Content
Stream-Next-Offset: 0000000000000002_000000000000000a
Stream-Up-To-Date: true
Stream-Cursor: 0000000000000002_000000000000000a
```

**Long-poll on closed stream at tail:**
```
GET /v1/stream/my-stream?offset=0000000000000003_000000000000001a&live=long-poll

→ 204 No Content (immediate, no waiting)
Stream-Next-Offset: 0000000000000003_000000000000001a
Stream-Up-To-Date: true
Stream-Closed: true
```

## Conformance

This spec covers conformance test blocks:
- Read Operations
- Offset Validation/Resumability
- Read-Your-Writes Consistency
- Chunking/Large Payloads
- Caching and ETag
- Long-Poll

## Traceability

- Upstream: PROTOCOL.md#read-data
- Upstream: PROTOCOL.md#read-stream-live-long-poll (§5.7)
- Conformance: `describe('Read Operations', ...)`
- Conformance: `describe('Offset Validation', ...)`

## Gaps

| Ambiguity | Interpretation | Why Conservative |
|-----------|----------------|------------------|
| Long-poll timeout value not specified | Default 30s, configurable via env var | Reasonable default; allows tuning without code changes |
