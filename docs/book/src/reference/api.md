# API reference

All endpoints are under the `/v1/stream/` base path. The health check is at `/healthz`.

## Health check

```
GET /healthz
```

Returns `200 OK` with body `ok`. No authentication required.

---

## Create stream

```
PUT /v1/stream/{name}
```

| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | no | Stream content type (immutable after creation; defaults to `application/octet-stream`) |
| `Stream-TTL` | no | Time-to-live in seconds |
| `Stream-Expires-At` | no | Absolute expiration (ISO 8601) |
| `Stream-Closed` | no | `"true"` to create closed |

Body is optional. If provided, it is appended as initial stream data.

| Status | Meaning |
|--------|---------|
| `201 Created` | Stream created |
| `200 OK` | Idempotent create (same config) |
| `409 Conflict` | Stream exists with different config |
| `400 Bad Request` | Invalid TTL or empty Content-Type header |

**Response headers:** `Location`, `Content-Type`, `Stream-Next-Offset`

---

## Append data

```
POST /v1/stream/{name}
```

| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | yes | Must match stream's content type |
| `Stream-Closed` | no | `"true"` to close (with or without data) |
| `Producer-ID` | no* | Producer identifier |
| `Producer-Epoch` | no* | Producer epoch |
| `Producer-Seq` | no* | Producer sequence number |

*Producer headers: all three or none.

| Status | Meaning |
|--------|---------|
| `200 OK` | New data accepted (producer mode) |
| `204 No Content` | Data appended (no producer) or duplicate (producer) |
| `400 Bad Request` | Empty body without Stream-Closed, invalid producer headers |
| `403 Forbidden` | Epoch fenced (zombie producer) |
| `404 Not Found` | Stream does not exist |
| `409 Conflict` | Content-Type mismatch, closed stream, or sequence gap |
| `413 Payload Too Large` | Memory limit exceeded |

**Response headers:** `Stream-Next-Offset`, `Stream-Closed` (if closed), `Producer-Epoch`, `Producer-Seq`

---

## Read data (catch-up)

```
GET /v1/stream/{name}?offset={offset}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `offset` | `-1` | Start offset. Sentinels: `-1` (start), `now` (tail) |

| Header | Required | Description |
|--------|----------|-------------|
| `If-None-Match` | no | ETag from previous read |

| Status | Meaning |
|--------|---------|
| `200 OK` | Data returned |
| `304 Not Modified` | ETag match (no new data) |
| `400 Bad Request` | Invalid offset format |
| `404 Not Found` | Stream does not exist or expired |

**Response headers:** `Content-Type`, `Stream-Next-Offset`, `Stream-Up-To-Date`, `ETag`, `Cache-Control`, `Stream-Closed` (if closed and at tail)

---

## Read data (long-poll)

```
GET /v1/stream/{name}?offset={offset}&live=long-poll[&cursor={cursor}]
```

Same as catch-up, plus:

| Parameter | Description |
|-----------|-------------|
| `live` | Must be `"long-poll"` |
| `cursor` | Echo of previous `Stream-Cursor` (optional) |

Additional response header: `Stream-Cursor`

Returns `204 No Content` on timeout or closed stream at tail.

---

## Read data (SSE)

```
GET /v1/stream/{name}?offset={offset}&live=sse
```

Returns `Content-Type: text/event-stream`. See [Read modes](../protocol/read-modes.md) for event format.

Additional response header: `stream-sse-data-encoding: base64` (for binary content types)

---

## Stream metadata

```
HEAD /v1/stream/{name}
```

Returns the same headers as GET with no body.

| Status | Meaning |
|--------|---------|
| `200 OK` | Stream exists |
| `404 Not Found` | Stream does not exist or expired |

**Response headers:** `Content-Type`, `Stream-Next-Offset`, `Stream-Closed`, `Stream-TTL`, `Stream-Expires-At`, `Cache-Control`

---

## Delete stream

```
DELETE /v1/stream/{name}
```

| Status | Meaning |
|--------|---------|
| `204 No Content` | Deleted |
| `404 Not Found` | Does not exist |

---

## Common response headers

These headers appear on all responses via middleware:

| Header | Value | Notes |
|--------|-------|-------|
| `Cache-Control` | `no-store` | `no-cache` for SSE |
| `X-Content-Type-Options` | `nosniff` | |
| `Cross-Origin-Resource-Policy` | `cross-origin` | |
