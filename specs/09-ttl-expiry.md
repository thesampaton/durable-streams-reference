# Stream TTL and Expiration

## Overview

Streams can have a time-to-live (TTL) configured at creation time. Once a stream
expires, it behaves as if it never existed - all operations return 404 Not Found.

## TTL Configuration

### Valid TTL Format

The `Stream-TTL` header specifies the stream lifetime in seconds:

- MUST be a positive integer
- MUST NOT have leading zeros (e.g., "0123" is invalid)
- MUST NOT use floating point notation (e.g., "123.0" is invalid)
- MUST NOT use scientific notation (e.g., "1e3" is invalid)

**Valid examples:**
- `Stream-TTL: 3600` (1 hour)
- `Stream-TTL: 86400` (24 hours)
- `Stream-TTL: 1` (1 second)

**Invalid examples:**
- `Stream-TTL: 0123` (leading zeros)
- `Stream-TTL: 123.0` (floating point)
- `Stream-TTL: 1e3` (scientific notation)
- `Stream-TTL: -60` (negative)
- `Stream-TTL: abc` (non-numeric)

### Absolute Expiration

Alternatively, use `Stream-Expires-At` with RFC 3339 timestamp:

```
Stream-Expires-At: 2024-12-31T23:59:59Z
```

### Mutual Exclusion

Requests MUST NOT include both `Stream-TTL` and `Stream-Expires-At`.
If both are present, the server returns 400 Bad Request.

## Expiration Behavior

### Lazy Expiration

Expiration is checked on every stream access (lazy expiration pattern):

- HEAD requests to expired streams return 404
- GET requests to expired streams return 404
- POST requests to expired streams return 404
- DELETE requests to expired streams return 204 (idempotent)

Expired streams are treated as if they never existed. There is no grace period.

### Remaining TTL

HEAD responses for non-expired streams with TTL include:

```
Stream-TTL: 3456
Stream-Expires-At: 2024-10-15T14:30:00Z
```

The `Stream-TTL` value is the **remaining** seconds until expiration, not the
original TTL. It decreases over time.

If remaining TTL is zero or negative, the stream is expired and HEAD returns 404.

## Idempotent Create with TTL

Creating the same stream multiple times with identical TTL is allowed:

1. First PUT with TTL=3600 → 201 Created
2. Second PUT (within TTL) with same config → 200 OK
3. Third PUT (after expiry) with same config → 201 Created (new stream)

The TTL clock resets only on actual stream creation (201), not on idempotent
re-creates (200).

## Edge Cases

### TTL Without Expiration

If `expires_at` is not set in storage (no TTL or Expires-At), HEAD responses
MUST NOT include `Stream-TTL` or `Stream-Expires-At` headers.

### Zero Remaining TTL

If the remaining TTL becomes zero or negative, the stream is expired. The server
MUST NOT return `Stream-TTL: 0` - it must return 404 instead.

### Concurrent Access During Expiration

If a stream expires between a HEAD check and a subsequent operation, the
operation may fail with 404. Clients must handle this race condition.

### Deletion vs Expiration

Deleting an expired stream returns 204 (success) - expired streams still count
as "deleted successfully" for idempotency purposes.

## Testing Requirements

Implementations must validate:

1. Valid TTL formats are accepted
2. Invalid TTL formats return 400
3. Both TTL and Expires-At together return 400
4. Expired streams return 404 on HEAD, GET, POST
5. Expired streams return 204 on DELETE
6. HEAD shows decreasing remaining TTL over time
7. Idempotent PUT after expiry creates a new stream (201)
