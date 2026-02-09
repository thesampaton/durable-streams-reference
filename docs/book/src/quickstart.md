# Quickstart

This page gets you from zero to a working stream in under 5 minutes. You will start the server, create a stream, append data, read it back, and subscribe for live updates.

## Prerequisites

- Rust toolchain (stable)
- curl

## Start the server

```bash
cargo run
```

The server listens on `http://localhost:4437` with streams at `/v1/stream/`.

## Create a stream

```bash
curl -i -X PUT -H "Content-Type: text/plain" \
  http://localhost:4437/v1/stream/my-stream
```

```
HTTP/1.1 201 Created
Location: http://localhost:4437/v1/stream/my-stream
Content-Type: text/plain
Stream-Next-Offset: 0000000000000000_0000000000000000
```

The stream is created with `text/plain` content type. The `Stream-Next-Offset` header shows the initial offset.

## Append data

```bash
curl -i -X POST -H "Content-Type: text/plain" \
  -d "hello world" \
  http://localhost:4437/v1/stream/my-stream
```

```
HTTP/1.1 204 No Content
Stream-Next-Offset: 0000000000000001_000000000000000b
```

The offset advanced. Append a second message:

```bash
curl -i -X POST -H "Content-Type: text/plain" \
  -d "second message" \
  http://localhost:4437/v1/stream/my-stream
```

## Read data

Read all messages from the beginning:

```bash
curl -i http://localhost:4437/v1/stream/my-stream?offset=-1
```

```
HTTP/1.1 200 OK
Content-Type: text/plain
Stream-Next-Offset: 0000000000000002_0000000000000019
Stream-Up-To-Date: true
ETag: "-1:0000000000000002_0000000000000019"

hello worldsecond message
```

The body contains the concatenated messages. Save `Stream-Next-Offset` to resume later without replaying:

```bash
curl -i http://localhost:4437/v1/stream/my-stream?offset=0000000000000002_0000000000000019
```

This returns an empty body with `Stream-Up-To-Date: true` because there is no new data.

## Subscribe with SSE

Open a live SSE connection to receive new messages as they arrive:

```bash
curl -N http://localhost:4437/v1/stream/my-stream?offset=-1\&live=sse
```

You will see the existing messages followed by a control event:

```
event: data
data:hello world

event: data
data:second message

event: control
data:{"streamNextOffset":"0000000000000002_0000000000000019","streamCursor":"...","upToDate":true}
```

The connection stays open. In another terminal, append more data:

```bash
curl -X POST -H "Content-Type: text/plain" \
  -d "live update" \
  http://localhost:4437/v1/stream/my-stream
```

The SSE connection immediately delivers:

```
event: data
data:live update

event: control
data:{"streamNextOffset":"0000000000000003_0000000000000024","streamCursor":"...","upToDate":true}
```

## Delete the stream

```bash
curl -i -X DELETE http://localhost:4437/v1/stream/my-stream
```

```
HTTP/1.1 204 No Content
```

## Next steps

- [Architecture overview](architecture/overview.md) to understand the full stack
- [Docker Compose](deployment/docker-compose.md) to run with auth and Postgres
- [Protocol reference](protocol/streams.md) for the complete API
- [Configuration](reference/configuration.md) for all environment variables
