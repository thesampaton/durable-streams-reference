# Streams

A stream is an append-only log identified by name. Clients create, append to, read from, and delete streams using standard HTTP methods.

## Create a stream (PUT)

```bash
curl -i -X PUT -H "Content-Type: text/plain" \
  http://localhost:4437/v1/stream/my-stream
```

**Headers:**
- `Content-Type` (required): the stream's content type, fixed at creation
- `Stream-TTL` (optional): time-to-live in seconds
- `Stream-Expires-At` (optional): absolute expiration (ISO 8601)
- `Stream-Closed` (optional): `"true"` to create in closed state

**Responses:**
- `201 Created` with `Location`, `Content-Type`, `Stream-Next-Offset`
- `200 OK` if the stream already exists with the same configuration (idempotent)
- `409 Conflict` if the stream exists with different configuration
- `400 Bad Request` for invalid TTL, missing Content-Type, or non-empty body

The Content-Type is immutable after creation. Comparison is case-insensitive and ignores charset parameters.

## Append data (POST)

```bash
curl -i -X POST -H "Content-Type: text/plain" \
  -d "hello world" \
  http://localhost:4437/v1/stream/my-stream
```

**Headers:**
- `Content-Type` (required): must match the stream's content type
- `Stream-Closed` (optional): `"true"` to close the stream (with or without data)

**Responses:**
- `204 No Content` with `Stream-Next-Offset`
- `404 Not Found` if stream does not exist
- `409 Conflict` for content-type mismatch or closed stream
- `400 Bad Request` for empty body without `Stream-Closed`

Each append generates a unique, monotonically increasing offset. Concurrent appends to the same stream are serialized to maintain offset ordering.

## Read data (GET)

```bash
curl -i http://localhost:4437/v1/stream/my-stream?offset=-1
```

**Query parameters:**
- `offset`: where to start reading
  - `-1` (default): beginning of stream
  - `now`: current tail (returns empty body)
  - hex offset: resume from a specific position

**Response headers:**
- `Stream-Next-Offset`: save this for resumption
- `Stream-Up-To-Date`: `"true"` when at the tail
- `Stream-Closed`: `"true"` when closed and at tail
- `ETag`: for conditional requests with `If-None-Match`

The body contains all messages concatenated from the requested offset to the current tail.

## Stream metadata (HEAD)

```bash
curl -I http://localhost:4437/v1/stream/my-stream
```

Returns the same headers as GET (Content-Type, Stream-Next-Offset, Stream-Closed, TTL info) with no body.

## Delete a stream (DELETE)

```bash
curl -i -X DELETE http://localhost:4437/v1/stream/my-stream
```

- `204 No Content` on success
- `404 Not Found` if stream does not exist

Deleting removes all data and metadata. The stream name can be reused.

## Close a stream

Closing a stream prevents further appends. Close by sending a POST with `Stream-Closed: true`:

```bash
# Close without data
curl -X POST -H "Content-Type: text/plain" \
  -H "Stream-Closed: true" \
  http://localhost:4437/v1/stream/my-stream

# Close with a final message
curl -X POST -H "Content-Type: text/plain" \
  -H "Stream-Closed: true" \
  -d "goodbye" http://localhost:4437/v1/stream/my-stream
```

After closure:
- Appends return `409 Conflict` with `Stream-Closed: true`
- Reads still work; `Stream-Closed: true` appears when the reader is at the tail
- Closing is idempotent (re-closing returns 204)

## TTL and expiry

Streams can have a time-to-live set at creation:

```bash
# Expire in 1 hour
curl -X PUT -H "Content-Type: text/plain" \
  -H "Stream-TTL: 3600" \
  http://localhost:4437/v1/stream/temp

# Expire at a specific time
curl -X PUT -H "Content-Type: text/plain" \
  -H "Stream-Expires-At: 2026-12-31T23:59:59Z" \
  http://localhost:4437/v1/stream/temp2
```

- Only one of `Stream-TTL` and `Stream-Expires-At` may be provided
- Expired streams return 404 on all operations
- Expiration is checked lazily on access
- HEAD returns remaining `Stream-TTL` and `Stream-Expires-At`
