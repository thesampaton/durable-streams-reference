# TL;DR

## Just the server

```bash
cargo run
```

Server at `http://localhost:4437`. Streams at `/v1/stream/`.

```bash
# Create
curl -X PUT -H "Content-Type: text/plain" http://localhost:4437/v1/stream/demo

# Write
curl -X POST -H "Content-Type: text/plain" -d "hello" http://localhost:4437/v1/stream/demo

# Read
curl http://localhost:4437/v1/stream/demo?offset=-1

# SSE
curl -N http://localhost:4437/v1/stream/demo?offset=-1\&live=sse

# Delete
curl -X DELETE http://localhost:4437/v1/stream/demo
```

## Full stack (auth + Postgres + sync)

```bash
docker-compose --profile sync up -d --build
```

| Port | What |
|------|------|
| 4437 | DS server (no auth) |
| 8080 | Envoy proxy (JWT auth) |
| 54321 | Postgres |

```bash
cd e2e && npm install
TOKEN=$(node generate-token.mjs)

curl -X PUT -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  http://localhost:8080/v1/stream/my-stream

curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '[{"msg": "hello"}]' http://localhost:8080/v1/stream/my-stream

curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/stream/my-stream?offset=-1
```

## Dev mode (everything + UI)

```bash
make dev          # server, Envoy, Postgres, Electric, sync, Adminer, heartbeat producer
make dev-ui       # stream browser on :3000 (separate terminal)
make dev-down     # stop
```

Adminer at `http://localhost:8081` (server: postgres, user: postgres, pw: password).

## Key env vars

| Variable | Default | What it does |
|----------|---------|-------------|
| `PORT` | `4437` | Listen port |
| `LONG_POLL_TIMEOUT_SECS` | `30` | Long-poll wait |
| `SSE_IDLE_CLOSE_SECS` | `60` | SSE idle close (0 = off) |
| `CORS_ORIGINS` | `*` | Allowed origins |
| `MAX_MEMORY_BYTES` | `104857600` | Total memory cap |
| `MAX_STREAM_BYTES` | `10485760` | Per-stream cap |

## Tests

```bash
cargo test                      # unit + integration
make conformance                # protocol conformance suite
make integration-test           # Docker e2e (auth)
make integration-test-sessions  # Docker e2e (auth + Postgres sync)
```

## Build the docs

```bash
make docs         # build
make docs-serve   # serve with live reload
```
