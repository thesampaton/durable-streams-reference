# Read modes

The DS server supports three read modes: catch-up (default), long-poll, and SSE. All use GET requests with different query parameters.

## Catch-up (default)

Returns all data from an offset to the current tail, then closes the connection.

```bash
# Read everything
curl http://localhost:4437/v1/stream/my-stream?offset=-1

# Resume from a saved offset
curl http://localhost:4437/v1/stream/my-stream?offset=0000000000000002_000000000000000a
```

The response includes:
- Body with concatenated message data
- `Stream-Next-Offset` for resumption
- `Stream-Up-To-Date: true` when at the tail
- `ETag` for conditional requests

This is the simplest mode. Use it for one-shot reads or polling.

## Long-poll

Waits for new data if the client is already at the tail, avoiding the need for rapid polling.

```bash
curl http://localhost:4437/v1/stream/my-stream?offset=0000000000000002_000000000000000a\&live=long-poll
```

Behavior:
- If data exists at the offset, returns it immediately (same as catch-up)
- If at the tail and the stream is open, waits for new data
- If at the tail and the stream is closed, returns immediately with `Stream-Closed: true`
- Returns `204 No Content` when the timeout expires with no new data

The timeout defaults to 30 seconds (configurable via `DS_SERVER__LONG_POLL_TIMEOUT_SECS`).

The response includes a `Stream-Cursor` header. Echo it back in the `cursor` query parameter on subsequent requests to enable CDN request collapsing:

```bash
curl http://localhost:4437/v1/stream/my-stream?offset=...&live=long-poll&cursor=...
```

## SSE (Server-Sent Events)

Opens a persistent connection that delivers messages as they arrive.

```bash
curl -N http://localhost:4437/v1/stream/my-stream?offset=-1\&live=sse
```

The server returns `Content-Type: text/event-stream` and streams events:

### Event types

**`event: data`** -- one per stored message:

```
event: data
data:hello world

```

**`event: control`** -- metadata after each batch:

```
event: control
data:{"streamNextOffset":"...","streamCursor":"...","upToDate":true}

```

### Control event fields

| Field | Type | When included |
|-------|------|---------------|
| `streamNextOffset` | string | always |
| `streamCursor` | string | when stream is open |
| `upToDate` | boolean | when caught up |
| `streamClosed` | boolean | when closed and all data sent |

### Connection lifecycle

1. Historical data is sent as `event: data` events
2. A `control` event with `upToDate: true` indicates catch-up is complete
3. The connection stays open, waiting for new data
4. New appends trigger additional `data` + `control` events
5. If the stream is closed, the final `control` includes `streamClosed: true` and the connection closes
6. Idle connections close after ~60 seconds (configurable via `DS_SERVER__SSE_RECONNECT_INTERVAL_SECS`)

### Binary streams

For content types other than `text/*` and `application/json`, SSE data events carry base64-encoded payloads. The response includes `stream-sse-data-encoding: base64`.

### Multi-line data

Multi-line messages are sent as multiple `data:` lines per SSE spec:

```
event: data
data:line one
data:line two

```

### Resumption

Save the offset from control events. Reconnect with `offset={saved}` to resume:

```bash
curl -N http://localhost:4437/v1/stream/my-stream?offset=0000000000000002_000000000000000a\&live=sse
```

The `offset=now` sentinel skips historical data and receives only new messages.
