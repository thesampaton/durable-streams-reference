# Stream Lifecycle

Version: 1.0.0
Status: stable

## Overview

Defines stream lifecycle operations: creation (PUT), deletion (DELETE), and metadata retrieval (HEAD).

## Create Stream (PUT)

### Request

**Method:** `PUT /v1/stream/{name}`

**Headers:**
- `Content-Type` (required): Stream content type (e.g., `text/plain`, `application/json`)
- `Stream-TTL` (optional): Time-to-live in seconds (integer, no leading zeros)
- `Stream-Expires-At` (optional): Absolute expiration timestamp (ISO 8601)
- `Stream-Closed` (optional): If `"true"`, create stream in closed state

**Body:** MUST be empty. PUT with body MUST return 400.

### Response

**201 Created** - Stream created successfully
- `Location`: Absolute URL to the stream
- `Content-Type`: Echo of request Content-Type (normalized)
- `Stream-Next-Offset`: Initial offset (start of stream)

**200 OK** - Idempotent create with matching configuration
- Same headers as 201

**409 Conflict** - Stream exists with different configuration

**400 Bad Request** - Invalid request:
- Both TTL and Expires-At provided
- Invalid TTL format (leading zeros, floats, scientific notation, negative)
- Empty or missing Content-Type
- Non-empty request body

### Behavior

The server MUST:
- Normalize Content-Type to lowercase for comparison
- Support idempotent creation (200 if config matches exactly)
- Return 409 if stream exists with different Content-Type, TTL, or Expires-At
- Accept Content-Type with charset parameter but strip for storage/comparison
- Generate initial offset value for new streams

The server MAY:
- Impose limits on stream name length or character set
- Impose limits on TTL duration

### Examples

Create text stream:
```
PUT /v1/stream/my-stream
Content-Type: text/plain

→ 201 Created
Location: http://localhost:4437/v1/stream/my-stream
Content-Type: text/plain
Stream-Next-Offset: 0000000000000000_0000000000000000
```

Create with TTL:
```
PUT /v1/stream/temp-stream
Content-Type: application/json
Stream-TTL: 3600

→ 201 Created
```

Idempotent create:
```
PUT /v1/stream/my-stream
Content-Type: text/plain

→ 200 OK
Content-Type: text/plain
Stream-Next-Offset: 0000000000000000_0000000000000000
```

## Delete Stream (DELETE)

### Request

**Method:** `DELETE /v1/stream/{name}`

**Headers:** None required

**Body:** MUST be empty

### Response

**204 No Content** - Stream deleted successfully

**404 Not Found** - Stream does not exist

### Behavior

The server MUST:
- Return 204 on successful deletion of an existing stream
- Return 404 if the stream does not exist
- Remove all stream data and metadata on successful deletion
- Allow recreating stream with same name but potentially different config

## Stream Metadata (HEAD)

### Request

**Method:** `HEAD /v1/stream/{name}`

**Headers:** None required

### Response

**200 OK** - Stream exists
- `Content-Type`: Stream content type
- `Stream-Next-Offset`: Next offset to be assigned
- `Stream-Closed`: `"true"` if stream is closed, omitted otherwise
- `Cache-Control`: `no-store`

If stream has TTL:
- `Stream-TTL`: Remaining seconds until expiration
- `Stream-Expires-At`: Absolute expiration timestamp

**404 Not Found** - Stream doesn't exist or has expired

### Behavior

The server MUST:
- Return no body (HEAD request)
- Include all stream metadata in headers
- Calculate remaining TTL if applicable
- Return 404 for expired streams

## Conformance

This spec covers conformance test blocks:
- Basic Stream Operations (create, delete, idempotent)
- HTTP Protocol (PUT, DELETE, HEAD)
- Protocol Edge Cases (PUT with body, Location header format)
- TTL/Expiry Validation
- Case-Insensitivity (Content-Type comparison)

## Traceability

- Upstream: PROTOCOL.md#stream-lifecycle
- Conformance: `describe('Basic Stream Operations', ...)`
- Conformance: `describe('HTTP Protocol', ...)`
