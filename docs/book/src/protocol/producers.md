# Producers

Producer headers enable exactly-once append semantics over HTTP. A producer identifies itself with a stable ID, uses a monotonic epoch for session management, and a monotonic sequence number for per-request deduplication. The server validates state and deduplicates retries automatically.

## Producer headers

All three must be provided together (or none):

| Header | Format | Description |
|--------|--------|-------------|
| `Producer-ID` | non-empty string | Stable identifier for the producer |
| `Producer-Epoch` | integer (0 to 2^53-1) | Incremented on producer restart |
| `Producer-Seq` | integer (0 to 2^53-1) | Incremented per request within an epoch |

## Basic flow

```bash
# First message: epoch 0, seq 0
curl -X POST -H "Content-Type: text/plain" \
  -H "Producer-ID: my-producer" \
  -H "Producer-Epoch: 0" \
  -H "Producer-Seq: 0" \
  -d "message 1" \
  http://localhost:4437/v1/stream/my-stream
# → 200 OK (new data accepted)

# Second message: epoch 0, seq 1
curl -X POST -H "Content-Type: text/plain" \
  -H "Producer-ID: my-producer" \
  -H "Producer-Epoch: 0" \
  -H "Producer-Seq: 1" \
  -d "message 2" \
  http://localhost:4437/v1/stream/my-stream
# → 200 OK
```

## Response codes

| Code | Meaning | When |
|------|---------|------|
| `200 OK` | New data accepted | seq = lastSeq + 1 (or first append) |
| `204 No Content` | Duplicate (already persisted) | seq <= lastSeq |
| `400 Bad Request` | Invalid headers | Partial set, empty ID, non-integer, or epoch bump with seq != 0 |
| `403 Forbidden` | Epoch fenced (zombie producer) | epoch < server's current epoch |
| `409 Conflict` | Sequence gap | seq > lastSeq + 1 |

## Duplicate detection

Retrying the same (ID + epoch + seq) is safe and returns `204`:

```bash
# Retry of seq 0 (already succeeded above)
curl -X POST -H "Content-Type: text/plain" \
  -H "Producer-ID: my-producer" \
  -H "Producer-Epoch: 0" \
  -H "Producer-Seq: 0" \
  -d "message 1" \
  http://localhost:4437/v1/stream/my-stream
# → 204 No Content (duplicate, no data stored)
```

## Epoch fencing (zombie detection)

When a producer restarts, it bumps its epoch and starts seq at 0:

```bash
# Producer restarts with epoch 1
curl -X POST -H "Content-Type: text/plain" \
  -H "Producer-ID: my-producer" \
  -H "Producer-Epoch: 1" \
  -H "Producer-Seq: 0" \
  -d "restarted" \
  http://localhost:4437/v1/stream/my-stream
# → 200 OK
```

After this, the old epoch is fenced. A request from epoch 0 returns `403 Forbidden`:

```bash
curl -X POST -H "Content-Type: text/plain" \
  -H "Producer-ID: my-producer" \
  -H "Producer-Epoch: 0" \
  -H "Producer-Seq: 2" \
  -d "zombie" \
  http://localhost:4437/v1/stream/my-stream
# → 403 Forbidden (epoch fenced)
# Producer-Epoch: 1 (server's current epoch)
```

## Sequence gaps

If a sequence number is skipped, the server returns `409 Conflict` with diagnostic headers:

```bash
curl -X POST -H "Content-Type: text/plain" \
  -H "Producer-ID: my-producer" \
  -H "Producer-Epoch: 0" \
  -H "Producer-Seq: 5" \
  -d "skipped" \
  http://localhost:4437/v1/stream/my-stream
# → 409 Conflict
# Producer-Expected-Seq: 1
# Producer-Received-Seq: 5
```

## Multiple producers

Multiple producers can write to the same stream. Each has independent epoch and sequence tracking:

```bash
# Producer A writes
curl -X POST -H "Producer-ID: user-alice" \
  -H "Producer-Epoch: 0" -H "Producer-Seq: 0" ...

# Producer B writes
curl -X POST -H "Producer-ID: user-bob" \
  -H "Producer-Epoch: 0" -H "Producer-Seq: 0" ...
```

Messages are ordered by arrival time, not by producer.

## Closing with producer headers

A producer can atomically append and close:

```bash
curl -X POST -H "Content-Type: text/plain" \
  -H "Producer-ID: my-producer" \
  -H "Producer-Epoch: 0" \
  -H "Producer-Seq: 2" \
  -H "Stream-Closed: true" \
  -d "final message" \
  http://localhost:4437/v1/stream/my-stream
# → 200 OK, Stream-Closed: true
```

Retrying the same closing request returns `204 No Content` with `Stream-Closed: true`.
