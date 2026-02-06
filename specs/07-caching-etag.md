# Caching and ETag

Version: 1.0.0
Status: stable

## Overview

Defines how the server generates ETags for GET responses and how clients use
`If-None-Match` to avoid redundant data transfer. Also specifies `Cache-Control`
behaviour to prevent intermediate caches from serving stale stream data.

## ETag Generation

### Format

ETag format: `"{start_offset}:{end_offset}"` or `"{start_offset}:{end_offset}:c"`.

- `start_offset`: The offset the client requested (query parameter value, verbatim)
- `end_offset`: The `Stream-Next-Offset` from this read's snapshot
- `:c` suffix: Present only when the stream is closed AND the reader is at tail

The ETag MUST be enclosed in double quotes per HTTP semantics.

### Requirements

The server MUST:
- Include an `ETag` header on every `200 OK` GET response
- Use the exact query offset string as the start component (including sentinels
  `-1` and `now`)
- Use the snapshot's `Stream-Next-Offset` as the end component
- Append `:c` only when the stream is closed AND the reader is at the tail
- Ensure the ETag reflects the same snapshot as the response body (no TOCTOU
  between body data and ETag/offset metadata)

### Examples

**Start sentinel:**
```
GET /v1/stream/my-stream?offset=-1
ETag: "-1:0000000000000003_000000000000001a"
```

**Now sentinel:**
```
GET /v1/stream/my-stream?offset=now
ETag: "now:0000000000000003_000000000000001a"
```

**Specific offset:**
```
GET /v1/stream/my-stream?offset=0000000000000001_0000000000000005
ETag: "0000000000000001_0000000000000005:0000000000000003_000000000000001a"
```

**Closed stream at tail:**
```
GET /v1/stream/my-stream?offset=-1
ETag: "-1:0000000000000003_000000000000001a:c"
```

## If-None-Match (304 Not Modified)

### Request

The client MAY send `If-None-Match` with an ETag from a previous GET response.

### Response

**304 Not Modified** when the provided ETag matches the current ETag:
- `Stream-Next-Offset`: Current next offset
- `Stream-Up-To-Date`: `"true"`
- `Cache-Control`: `no-store`
- No response body

**200 OK** when the ETag does not match (new data available or different range):
- Normal GET response with updated ETag

### Requirements

The server MUST:
- Compare `If-None-Match` value against the generated ETag (exact string match)
- Return `304 Not Modified` with metadata headers when ETags match
- Return `200 OK` with full response when ETags differ
- Include `Cache-Control: no-store` on 304 responses
- Include `Stream-Next-Offset` and `Stream-Up-To-Date` on 304 responses
- NOT include a response body on 304 responses

### Example

```
GET /v1/stream/my-stream?offset=-1
If-None-Match: "-1:0000000000000003_000000000000001a"

→ 304 Not Modified
Stream-Next-Offset: 0000000000000003_000000000000001a
Stream-Up-To-Date: true
Cache-Control: no-store

(no body)
```

## Cache-Control

### Requirements

The server MUST include `Cache-Control: no-store` on ALL responses:
- `200 OK` (GET with data)
- `304 Not Modified` (ETag match)
- `201 Created` / `200 OK` (PUT create/idempotent)
- `204 No Content` (POST append, DELETE)
- All error responses (400, 404, 409, 413)

This prevents intermediate HTTP caches from serving stale stream data.

## Conformance

This spec covers conformance test blocks:
- Caching and ETag
- 304 Not Modified behaviour

## Traceability

- Upstream: PROTOCOL.md#read-data (ETag and caching section)
- Conformance: `describe('Read Operations', ...)` (ETag assertions)
- Related: `specs/03-read-modes.md` (GET response headers)

## Gaps

None identified. ETag format and 304 behaviour align with upstream protocol
and conformance tests.
