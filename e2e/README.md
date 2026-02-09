# End-to-End Deployment Harness

This directory contains everything needed to run and validate a complete
durable streams deployment locally: auth proxy, test fixtures, and test
utilities.

The server itself has no auth opinions — it is a pure protocol implementation.
Authentication is handled by an Envoy proxy in front of it, validating JWTs
against a local JWKS. This harness shows how to compose that stack and proves
it works end-to-end.

## Architecture

```
Client (curl / test runner)
    │  HTTP + Bearer JWT
    ▼
Envoy Proxy (:8080)
    │  validates JWT against fixtures/jwks.json
    │  forwards X-JWT-Sub header
    ▼
Durable Streams Server (:4437, internal only)
```

## Directory Layout

| Path | Purpose |
|------|---------|
| `envoy.yaml` | Envoy proxy config — JWT validation, route timeouts, health bypass |
| `fixtures/test-key.pem` | RSA 2048 private key (test-only, zero security value) |
| `fixtures/test-key.pub.pem` | RSA public key |
| `fixtures/jwks.json` | JWKS document served to Envoy for JWT verification |
| `generate-token.mjs` | CLI wrapper for minting test JWTs |
| `test-utils.mjs` | Importable `generateToken()` for programmatic JWT creation |
| `integration.test.mjs` | Vitest e2e test suite (8 scenarios) |
| `sessions.test.mjs` | Sessions + DB sync test suite (8 scenarios) |
| `package.json` | Node.js dependencies (`jose`, `@durable-streams/client`, `pg`, `vitest`) |
| `sync/` | Bidirectional sync service (PG <-> DS) |
| `sync/init.sql` | Postgres schema (`items` + `session_events` tables) |
| `sync/sync.mjs` | Sync bridge: Electric Shape API -> DS, DS SSE -> PG |
| `sync/Dockerfile` | Container image for the sync service |

## Quick Start

```bash
# From the repo root:
docker-compose up -d          # start server + Envoy proxy

# Health check (no auth required)
curl http://localhost:8080/healthz

# Generate a test token
cd e2e && npm install
TOKEN=$(node generate-token.mjs)

# Create and use a stream (auth required)
curl -X PUT -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/stream/test-1
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/octet-stream" \
  -d "hello" http://localhost:8080/v1/stream/test-1
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/stream/test-1

# Unauthenticated requests are rejected
curl -X PUT http://localhost:8080/v1/stream/test-2   # → 401
```

## Generating Test Tokens

```bash
node generate-token.mjs                   # valid token (1h expiry)
node generate-token.mjs --expired          # expired token
node generate-token.mjs --wrong-issuer     # wrong iss claim
node generate-token.mjs --no-audience      # missing aud claim
node generate-token.mjs --sub user123      # custom sub claim
```

Default claims: `iss: "durable-streams-test"`, `aud: "durable-streams"`,
`sub: "test-user"`, `exp: now + 1h`.

## Test Fixtures

The RSA keypair and JWKS in `fixtures/` are pre-generated and committed
intentionally. They are test-only keys with no security value — they exist so
the e2e stack is deterministic and works offline without any key generation
step.

To regenerate (not normally necessary):

```bash
openssl genrsa -out fixtures/test-key.pem 2048
openssl rsa -in fixtures/test-key.pem -pubout -out fixtures/test-key.pub.pem
# Then regenerate fixtures/jwks.json from the public key
```

## Integration Tests

Eight e2e test scenarios validate the full authenticated stack using
`@durable-streams/client` and plain `fetch`:

1. **Health check bypass** — `GET /healthz` without JWT returns 200
2. **Unauthenticated rejection** — `PUT` without JWT returns 401
3. **Expired token rejection** — request with expired JWT returns 401
4. **Create stream** — `DurableStream.create()` with valid JWT succeeds
5. **Append and read** — append data, read back via client, data matches
6. **Offset resumption** — read from saved offset resumes without replay
7. **SSE live subscription** — subscribe, append new data, arrives live
8. **Delete stream** — delete via client, stream returns 404

### Running the tests

```bash
# Fully automated (builds Docker, starts stack, runs tests, tears down)
make integration-test

# Manual (stack already running via docker-compose up -d)
cd e2e && npm install
E2E_BASE_URL=http://localhost:8080 npx vitest run --reporter=verbose integration.test.mjs
```

The `E2E_BASE_URL` env var defaults to `http://localhost:8080`.

## Sessions + Database Sync

The `sessions.test.mjs` suite validates the full production architecture:
durable sessions (collaborative AI chat pattern) with bidirectional Postgres
sync.

### Architecture

```
Test Runner (host)
  |
  |--- PG -> Stream direction ---------------------------------|
  |    INSERT INTO postgres -> Postgres -> Electric (Shape API) |
  |                            (wal_level    -> Sync Service    |
  |                             =logical)       -> DS Server    |
  |    verify in DS stream <- Envoy (:8080) <-----|            |
  |                                                             |
  |--- Stream -> PG direction ---------------------------------|
  |    POST session event -> Envoy -> DS Server                |
  |                                    -> Sync Service          |
  |                                       (SSE consumer)       |
  |                                       -> Postgres           |
  |    verify in postgres <------------------------------------|
```

### Services (docker-compose --profile sync)

| Service | Image | Purpose |
|---------|-------|---------|
| `postgres` | `postgres:17-alpine` | Source/sink for structured data (port 54321) |
| `electric` | `electricsql/electric:1.4.2` | PG WAL -> Shape API |
| `sync-service` | `./e2e/sync` | Bidirectional bridge (Electric->DS + DS->PG) |

### Event format

Session events are opaque JSON to the DS server. The sync service forwards them
as-is between DS streams and Postgres. A typical STATE-PROTOCOL event:

```json
{
  "key": "user:alice",
  "type": "presence",
  "operation": "set",
  "value": { "status": "online" }
}
```

### Running the tests

```bash
# Fully automated (builds all containers, starts full stack, runs tests, tears down)
make integration-test-sessions

# Manual (stack already running)
cd e2e && npm install
E2E_BASE_URL=http://localhost:8080 npx vitest run --reporter=verbose sessions.test.mjs
```

### Test scenarios

**Sessions pattern (5 tests):**

1. **Session lifecycle** — create session, write presence event, read back
2. **Multi-producer chat** — user message + AI response chunks, verify ordering
3. **SSE live subscription** — subscribe, write messages, verify real-time delivery
4. **Session recovery** — write, save offset, write more, resume from offset
5. **Producer idempotency** — write, retry same seq (204), write next seq (200)

**Database sync (3 tests):**

6. **PG -> Stream** — INSERT into `items`, poll DS stream `pg-items` until it appears
7. **Stream -> PG** — POST to `session-events`, poll Postgres until row appears
8. **Round trip** — INSERT into PG, poll DS stream until item appears

### Sync service configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `ELECTRIC_URL` | `http://electric:3000` | Electric SQL Shape API |
| `DS_SERVER_URL` | `http://server:4437` | DS server (internal, no auth) |
| `POSTGRES_URL` | `postgresql://postgres:password@postgres:5432/durable_streams` | Postgres connection |

## Envoy Configuration

Key settings in `envoy.yaml`:

- **JWT validation:** `local_jwks` from file, issuer `durable-streams-test`, audience `durable-streams`
- **Health bypass:** `/healthz` requires no auth
- **Route timeout:** `0s` (disabled) for SSE streaming support
- **Stream idle timeout:** `120s` — longer than the server's SSE idle close (60s) and long-poll timeout (30s), so the server controls connection lifecycle
- **`claim_to_headers`:** Forwards `sub` claim as `X-JWT-Sub` to the server
- **Admin:** Port 9901 for debugging
