# JSON mode

Streams created with `Content-Type: application/json` operate in JSON mode. JSON mode provides array flattening on append and array wrapping on read.

## Creating a JSON stream

```bash
curl -X PUT -H "Content-Type: application/json" \
  http://localhost:4437/v1/stream/events
```

## Appending

### Single value

A single JSON object is stored as one message:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"event": "click", "x": 100}' \
  http://localhost:4437/v1/stream/events
```

### Array flattening

A JSON array is flattened: each element becomes a separate message:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '[{"event": "click"}, {"event": "scroll"}]' \
  http://localhost:4437/v1/stream/events
```

This stores **two** messages. Only the top-level array is flattened; nested arrays within objects are preserved.

### Rejected inputs

- Empty arrays (`[]`) return `400 Bad Request`
- Invalid JSON returns `400 Bad Request`

## Reading

All messages are wrapped in a JSON array regardless of how they were appended:

```bash
curl http://localhost:4437/v1/stream/events?offset=-1
```

```json
[
  {"event": "click", "x": 100},
  {"event": "click"},
  {"event": "scroll"}
]
```

An empty stream returns `[]`.

## SSE

SSE data events for JSON streams wrap each message in an array:

```
event: data
data:[{"event":"click","x":100}]
```

Consumers must unwrap the outer array:

```javascript
const parsed = JSON.parse(data);
const items = Array.isArray(parsed) ? parsed : [parsed];
```

See [ecosystem interop CI-003](../project/gaps.md) for details on this pattern.

## Non-JSON streams

JSON mode only activates for `Content-Type: application/json`. Other content types (e.g., `text/plain`) store and return raw bytes with no flattening or wrapping.
