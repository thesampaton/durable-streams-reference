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
| `generate-token.mjs` | Mints test JWTs for smoke testing |
| `package.json` | Node.js dependencies (`jose`) |

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

## Envoy Configuration

Key settings in `envoy.yaml`:

- **JWT validation:** `local_jwks` from file, issuer `durable-streams-test`, audience `durable-streams`
- **Health bypass:** `/healthz` requires no auth
- **Route timeout:** `0s` (disabled) for SSE streaming support
- **Stream idle timeout:** `120s` — longer than the server's SSE idle close (60s) and long-poll timeout (30s), so the server controls connection lifecycle
- **`claim_to_headers`:** Forwards `sub` claim as `X-JWT-Sub` to the server
- **Admin:** Port 9901 for debugging
