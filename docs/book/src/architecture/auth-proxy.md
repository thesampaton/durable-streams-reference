# Auth proxy

The DS server has no authentication. This is intentional: it keeps the protocol implementation simple and lets adopters use whatever auth system they already have.

The pattern is to put a reverse proxy in front of the server that validates credentials and forwards identity information.

## The pattern

Any reverse proxy that validates JWTs (or other credentials) and forwards the result works. The proxy needs to do three things:

1. **Validate tokens.** Check the JWT signature against a JWKS, verify `iss`, `aud`, and `exp` claims.
2. **Forward identity.** Extract a claim (typically `sub`) and pass it to the DS server as a header (e.g., `X-JWT-Sub`).
3. **Bypass health checks.** Let `/healthz` through without auth so load balancers and orchestrators can probe the server.

```
Client                    Auth Proxy                DS Server
  │                          │                          │
  │── GET /healthz ─────────►│──── pass through ───────►│
  │                          │                          │
  │── PUT /v1/stream/foo ───►│                          │
  │   Authorization: Bearer  │── validate JWT ──┐       │
  │   eyJ...                 │                  │       │
  │                          │◄─ valid ─────────┘       │
  │                          │── proxy + identity ─────►│
  │                          │                          │
  │── PUT /v1/stream/bar ───►│                          │
  │   (no token)             │── 401 Unauthorized       │
  │                          │   (proxy rejects)        │
```

### Streaming considerations

If the proxy sits in front of SSE or long-poll connections, it needs to support long-lived requests:

| Setting | Recommendation | Why |
|---------|---------------|-----|
| Route timeout | disabled or very large | SSE connections run indefinitely |
| Stream idle timeout | longer than server's SSE idle close | Let the server control connection lifecycle |

The DS server closes idle SSE connections after ~60 seconds by default. The proxy's idle timeout should exceed this so the server, not the proxy, decides when to close.

### Proxy options

- **Envoy** with `jwt_authn` HTTP filter
- **nginx** with `ngx_http_auth_jwt_module`
- **Caddy** with `jwt` middleware
- **AWS ALB** with OIDC integration
- **Traefik** with `ForwardAuth` middleware
- Custom middleware in your application server

The DS server only cares that requests arrive on its port. It does not inspect auth headers.

## In this repository

The e2e harness uses [Envoy](https://www.envoyproxy.io/) as the auth proxy, configured in `e2e/envoy.yaml`.

### Envoy configuration

| Setting | Value | Notes |
|---------|-------|-------|
| JWT validation | `local_jwks` from `e2e/fixtures/jwks.json` | Offline validation, no external JWKS endpoint |
| Issuer | `durable-streams-test` | Test-only issuer |
| Audience | `durable-streams` | Test-only audience |
| Route timeout | `0s` (disabled) | Supports SSE |
| Stream idle timeout | `120s` | Server controls lifecycle at 60s |
| `claim_to_headers` | `sub` as `X-JWT-Sub` | Forwards identity |
| Admin port | `9901` | Debugging dashboard |

### Test token generation

The `e2e/` directory includes an RSA keypair and scripts for minting test JWTs:

```bash
cd e2e && npm install

TOKEN=$(node generate-token.mjs)              # valid, 1h expiry
node generate-token.mjs --expired             # expired token
node generate-token.mjs --sub alice           # custom subject
node generate-token.mjs --wrong-issuer        # wrong iss (rejected)
```

The RSA keypair in `e2e/fixtures/` is committed intentionally. These are test-only keys with zero security value. For production, point your proxy at a real identity provider's JWKS endpoint.
