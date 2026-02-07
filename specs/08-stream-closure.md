# Stream Closure

Version: 1.0.0
Status: stable

## Overview

Defines how streams are closed, the response headers returned on successful
closure, and the behavior of read modes when a stream is closed. Closure is
an irreversible, idempotent operation.

## Closing a Stream

### Close Without Data (POST)

**Request:**
```
POST /v1/stream/{name}
Content-Type: {stream content type}
Stream-Closed: true
```
(Empty body)

**Response:** 204 No Content
- `Stream-Next-Offset`: the final offset of the stream
- `Stream-Closed: true`

### Close With Data (POST)

**Request:**
```
POST /v1/stream/{name}
Content-Type: {stream content type}
Stream-Closed: true

final message
```

**Response:** 204 No Content
- `Stream-Next-Offset`: offset after the appended message
- `Stream-Closed: true`

### Create Closed (PUT)

**Request:**
```
PUT /v1/stream/{name}
Content-Type: {content type}
Stream-Closed: true
```

**Response:** 201 Created (or 200 OK for idempotent recreate)
- `Content-Type`: normalized content type
- `Stream-Next-Offset`: the initial offset
- `Stream-Closed: true`
- `Location`: stream URL

### Close With Producer (POST)

**Request:**
```
POST /v1/stream/{name}
Content-Type: {stream content type}
Stream-Closed: true
Producer-Id: {id}
Producer-Epoch: {epoch}
Producer-Seq: {seq}
```

**Response:** 200 OK (accepted) or 204 No Content (duplicate)
- `Stream-Next-Offset`: offset after the append
- `Stream-Closed: true`
- `Producer-Epoch`: echoed epoch
- `Producer-Seq`: echoed sequence

## Requirements

The server MUST:
- Include `Stream-Closed: true` in the success response when a stream is closed
  by that request (POST close-only, POST close-with-data, PUT create-closed)
- Include `Stream-Closed: true` in the success response when the stream was
  already closed (idempotent close, idempotent PUT recreate of closed stream)
- Return 204 No Content for idempotent close of an already-closed stream
- Include `Stream-Closed: true` and `Stream-Next-Offset` in the 409 Conflict
  response when rejecting an append to a closed stream
- Treat `Stream-Closed: false` (or any non-"true" value) as if the header
  were absent (no close operation)

The server MUST NOT:
- Reopen a closed stream
- Allow appends after closure (except the atomic close-with-data operation)

## Idempotent Close

Closing an already-closed stream MUST succeed with 204 No Content. The response
MUST include `Stream-Closed: true` and `Stream-Next-Offset`.

## Read Mode Behavior

### Catch-up (GET without wait)

When reading a closed stream and the reader reaches the tail:
- Response includes `Stream-Closed: true`
- Response includes `Stream-Up-To-Date: true`

When reading a closed stream but NOT at the tail (more messages remain):
- Response MUST NOT include `Stream-Closed` header

### Long-poll (GET with `Stream-Wait: true`)

When a stream is closed at the tail position:
- Returns 204 No Content immediately (no blocking)
- Includes `Stream-Closed: true`

### SSE (GET with `Accept: text/event-stream`)

When a stream is closed at the tail position:
- Emits a final `event: control` with `streamClosed: true`
- Closes the SSE connection

## Error Precedence

When a stream is closed:
- Append attempts return 409 Conflict with `Stream-Closed: true`
- This takes precedence over content-type mismatch errors

## Examples

### Close and Verify via HEAD

```
POST /v1/stream/my-stream
Content-Type: text/plain
Stream-Closed: true

-> 204 No Content
Stream-Next-Offset: 0000000000000000_0000000000000000
Stream-Closed: true

HEAD /v1/stream/my-stream

-> 200 OK
Stream-Closed: true
Stream-Next-Offset: 0000000000000000_0000000000000000
```

### Append to Closed Stream

```
POST /v1/stream/my-stream
Content-Type: text/plain

more data

-> 409 Conflict
Stream-Closed: true
Stream-Next-Offset: 0000000000000000_0000000000000000
```

## Conformance

This spec covers conformance test blocks:
- Stream Closure
- Protocol Edge Cases (close operations)
- Long-poll closed stream behavior
- SSE closed stream behavior

## Traceability

- Upstream: PROTOCOL.md#close-stream (sections 4.1, 5.1, 5.2, 5.3)
- Conformance: `describe('Stream Closure', ...)`
- Conformance: `describe('Long-poll', ...)` (closed stream tests)
- Conformance: `describe('SSE', ...)` (closed stream tests)

## Gaps

None identified. All closure behaviors are covered by upstream spec and
conformance tests.
