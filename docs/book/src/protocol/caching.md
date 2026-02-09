# Caching

The protocol includes ETag-based caching to avoid redundant data transfer and `Cache-Control: no-store` to prevent stale data in intermediate caches.

## ETags

Every `200 OK` GET response includes an `ETag` header encoding the read range:

```
ETag: "{start_offset}:{end_offset}"
```

The start offset is the query parameter value (including sentinels like `-1` or `now`). The end offset is the `Stream-Next-Offset` from the same response.

### Closed stream suffix

When the stream is closed and the reader is at the tail, the ETag has a `:c` suffix:

```
ETag: "-1:0000000000000003_000000000000001a:c"
```

### Examples

```
GET ?offset=-1          → ETag: "-1:0000000000000003_000000000000001a"
GET ?offset=now         → ETag: "now:0000000000000003_000000000000001a"
GET ?offset=0000...0001 → ETag: "0000...0001:0000...0003"
```

## Conditional requests (304)

Clients can send `If-None-Match` with a previously received ETag:

```bash
curl -H 'If-None-Match: "-1:0000000000000003_000000000000001a"' \
  http://localhost:4437/v1/stream/my-stream?offset=-1
```

If the ETag matches (no new data since last read), the server returns:

```
HTTP/1.1 304 Not Modified
Stream-Next-Offset: 0000000000000003_000000000000001a
Stream-Up-To-Date: true
Cache-Control: no-store
```

No body is sent, saving bandwidth.

If the ETag does not match (new data available), the server returns a normal `200 OK` with updated data and a new ETag.

## Cache-Control

The server includes `Cache-Control: no-store` on all responses (200, 201, 204, 304, 400, 404, 409, 413). This prevents intermediate HTTP caches (CDNs, proxies) from serving stale stream data.

SSE responses use `Cache-Control: no-cache` instead, which allows caching but requires revalidation.
