# Docker Compose

The `docker-compose.yml` at the repository root defines the full stack. Services are organized into profiles so you can run as much or as little as you need.

## Profiles

| Profile | Services | Use case |
|---------|----------|----------|
| *(default)* | DS server, Envoy proxy | Authenticated protocol server |
| `sync` | + Postgres, Electric SQL, sync service | Bidirectional PG sync |
| `dev` | + Adminer, heartbeat producer | Developer observability |

## Starting the stack

### Default (server + auth proxy)

```bash
docker-compose up -d
```

This starts the DS server on port 4437 (internal) and Envoy on port 8080 (public). The health check is at `http://localhost:8080/healthz`.

### With sync

```bash
docker-compose --profile sync up -d --build
```

Adds Postgres, Electric SQL, and the sync service. Data flows bidirectionally between DS streams and Postgres.

### Full dev stack

```bash
make dev
```

Starts everything: server, Envoy, Postgres, Electric, sync service, Adminer (DB admin UI), and a heartbeat producer. See [Dev mode](dev-mode.md) for details.

## Building the Docker image

```bash
docker-compose build
# or
make docker
```

The server Dockerfile uses a multi-stage build: compile with `rust:latest`, run on `debian:bookworm-slim`.

## Stopping

```bash
docker-compose down                          # default profile
docker-compose --profile sync down           # sync profile
docker-compose --profile sync --profile dev down  # everything
make dev-down                                 # same as above
```

## Port map

See [Port map](../reference/port-map.md) for all ports and their purposes.

## Quick test

```bash
# Start the stack
docker-compose up -d

# Health check (no auth)
curl http://localhost:8080/healthz

# Generate a test JWT
cd e2e && npm install
TOKEN=$(node generate-token.mjs)

# Create a stream (authenticated)
curl -X PUT -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: text/plain" \
  http://localhost:8080/v1/stream/test-1

# Append data
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: text/plain" \
  -d "hello" http://localhost:8080/v1/stream/test-1

# Read back
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/v1/stream/test-1

# Unauthenticated requests are rejected
curl -X PUT http://localhost:8080/v1/stream/test-2  # 401
```

## Integration tests

```bash
# Full automated cycle: build, start, test, tear down
make integration-test

# With sync and sessions tests
make integration-test-sessions
```
