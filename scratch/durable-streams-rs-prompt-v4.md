# Durable Streams Reference Implementation — Project Brief

## What I'm building

A Rust+axum reference implementation of the Durable Streams protocol (<https://github.com/durable-streams/durable-streams>), plus deployment configuration and documentation showing how to run a complete authenticated stack with JWT auth (Clerk as the documented real-world example) and an auth proxy.

## Context on Durable Streams

Durable Streams is an emerging open protocol from the Electric SQL team for persistent, addressable, real-time HTTP streams. The protocol defines PUT (create), POST (append), GET (read with catch-up/long-poll/SSE), DELETE, and HEAD operations over standard HTTP, with offset-based resumability. There is a conformance test suite at `@durable-streams/server-conformance-tests`. The protocol is well-specified in PROTOCOL.md. The ecosystem is early; implementations exist; this repo is the Rust+axum reference implementation plus a deployment and documentation reference for the wider ecosystem.

The launch announcement is at <https://electric-sql.com/products/durable-streams>.

## Protocol governance

This project MUST comply with `docs/protocol-governance.md` (spec pinning, traceability, ambiguity policy, enforcement). Treat it as normative. The content of that document is provided alongside this brief. Create it as part of project setup.

**Prompt guardrail:** Do not invent protocol semantics. Every protocol decision MUST cite a pinned spec section or a conformance test; otherwise record it as a spec gap in `docs/gaps.md`.

## What I'm building has three parts

### Part 1 — Protocol adapter (Rust crate/binary)

The thinnest possible layer on top of axum that adds durable streams protocol semantics. This is NOT reimplementing an HTTP server — axum handles HTTP. My code handles the protocol-specific behaviour on top.

**Offset contract (hard requirement):**

Offsets are opaque strings but MUST be lexicographically sortable and monotonically increasing per stream. Implement monotonic-by-construction: either monotonic ULID or a per-stream serialised generator. Concurrency must not be able to violate this — if two concurrent appends race on the same stream, the offset ordering must still be correct. This is a correctness invariant, not a performance optimisation.

**Storage model (hard requirement):**

The in-memory storage model is a sequence of entries per stream. Each entry is: `(offset: String, data: Bytes, content_type: String, timestamp: u64)`. Reads, SSE, and long-poll behaviour must be wire-accurate per the conformance suite and protocol spec. The storage layer is trait-based so backends can be swapped (file, database, object storage) without changing the protocol layer. Start with in-memory only.

Memory bounds: configurable max total bytes and per-stream bytes via environment variables. When exceeded, return an appropriate error (507 or 413 — check what the spec allows, document the choice in `docs/decisions.md`). The reference implementation must not crash on a laptop under sustained writes.

**Route shape (hard requirement):**

Default base path MUST match conformance expectations. Prefer `/v1/stream` (singular) if the spec and other implementations converge there — verify against the pinned PROTOCOL.md and conformance test source before committing. Do not assert a path without verification. Link the exact spec section or test that defines the expected route shape. If the spec is silent, make it configurable and record the gap.

**Default port:**

Default port MUST match conformance expectations. Keep docs, docker-compose, and examples consistent with whatever port is chosen. If the conformance harness doesn't care (i.e. it accepts a configurable server URL), pick a sensible default for human docs and stick with it — but don't make it normative if tests don't require it.

**HTTP vs HTTPS:**

The server speaks plain HTTP behind the proxy by default. TLS termination happens at Envoy (or whatever proxy is in front). Do not add TLS support to the server binary — it's unnecessary complexity when the deployment model is always proxy-in-front. Document this explicitly.

**Protocol surface area:**

- PUT — create stream, set content type
- POST — append data, enforce content-type match, return offset
- GET — three modes: catch-up (immediate), long-poll (wait for data), SSE (live tail with keepalive)
- DELETE — remove stream
- HEAD — stream metadata
- Headers: at minimum Stream-Offset, Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Access-Control-Expose-Headers, plus any additional protocol/conformance-required headers. Do not assume this list is exhaustive — derive the complete set from PROTOCOL.md and the conformance test expectations.
- Special offsets: derive from spec (at minimum "-1" for stream start, "now" for current tail)
- Body streaming: support request bodies without buffering the entire payload into memory if feasible; otherwise document the limit and the decision in `docs/decisions.md`.

**Producer sequencing:**

Implement Stream-Seq and any associated epoch/producer-state headers exactly per PROTOCOL.md and conformance tests. Return the protocol-defined error codes on regression/gap. Do not invent behaviour beyond what the spec and tests require. If conformance doesn't exercise producer sequencing, still implement per spec but keep it minimal and covered by unit tests. Record in `docs/decisions.md`.

**Error semantics (derive from spec, do not invent):**

- 401 — handled by proxy, not server
- 404 — stream not found
- 409 — sequencing conflict
- 415 — content-type mismatch on append
- Derive complete status code semantics from PROTOCOL.md and conformance tests. Do not invent "reasonable" HTTP error codes that the spec doesn't define. Each status code must link to a spec section or conformance test in `docs/decisions.md`.

**CORS:**

Default CORS policy: allow GET, HEAD, and any other methods the client needs. Expose required `Stream-*` headers. Allow configurable origins via environment variable (default: `*` for development; docs should note this must be restricted in production). Document the tradeoffs. If the conformance suite doesn't test CORS, implement it anyway for the client consumption path and note the gap in `docs/gaps.md`.

**Health check:**

`GET /healthz` (or similar) — must not collide with protocol paths and must not affect conformance behaviour. Keep it out of the protocol router entirely.

The success criteria is passing the conformance test suite.

**Conformance practicalities:**

The server must run with defaults. The conformance suite may be pointed at it via its documented configuration (e.g. a server URL env var), but we must not patch conformance or require bespoke flags beyond what the suite documents. "No configuration on our side" means we don't need custom flags on the server; the test runner's own documented config (like `BASE_URL`) is fine.

### Part 2 — Auth proxy configuration and deployment

The stream server itself has no auth opinions — it is a pure protocol implementation. Auth is handled by an auth proxy in front of it.

**Default proxy: Envoy (swappable).**

Use Envoy with its `jwt_authn` HTTP filter as the reference proxy configuration. This is config-only JWT validation — no custom code. The Envoy config validates:

- JWT signature against JWKS keys
- Issuer (`iss`) claim
- Audience (`aud`) claim
- Expiry (`exp`) and not-before (`nbf`) claims
- Token extracted from `Authorization: Bearer <jwt>` header

Invalid or missing tokens receive 401. Valid requests are proxied to the stream server.

Envoy is the documented default. The guide should note that the proxy layer is swappable — Caddy, nginx, GCP API Gateway, AWS ALB with OIDC, or any reverse proxy that can validate JWTs will work. The stream server doesn't know or care what sits in front of it.

No extra processes in the hot path beyond proxy + server. The architecture is: client → Envoy → durable-streams-rs. Nothing else.

**CI auth vs documentation auth (important distinction):**

- **CI and integration tests** must be deterministic and offline. Use a local JWT issuer with static JWKS keys. Tests must not depend on any external auth service. Generate a test keypair at build time, sign test JWTs locally, configure Envoy to validate against the local JWKS.
- **Documentation** shows Clerk as the real-world example. How to get the JWKS URL, how to configure the audience, how to set up the Envoy config pointing at Clerk. This is the "deploy for real" path.

### Part 3 — Integration test harness and documentation site

**Integration test harness:**

The repository includes integration tests that validate the complete authenticated stack. These are split into two levels:

**Core integration test (`make integration-test`):**

The test stack (docker-compose, default profile):

- The durable streams server (our Rust binary)
- Envoy (auth proxy, validating JWTs against local JWKS)
- A test runner using `@durable-streams/client` for consumption and plain HTTP for production

The test scenario:

- Create a stream
- Write to it (direct POST, simulating a producer)
- Read from it with a valid JWT — should succeed, return data
- Read from it without a JWT — should get 401
- Read from it with an expired/invalid JWT — should get 401
- Reconnect from a saved offset — should resume, not replay from start
- Subscribe via SSE, append new data, confirm it arrives live
- Delete the stream

This validates: does our server implement the protocol correctly, and does the auth proxy reject unauthenticated requests? No external dependencies. Fast. Stable. This is what runs on every push and PR.

**Electric integration test (`make integration-test-electric`):**

The test stack (docker-compose, `electric` profile):

- Everything from the core test, plus:
- Postgres
- Electric SQL (syncing Postgres changes, producing to durable streams)

This validates: does Electric work as a producer into our server? This is a separate compose profile because Electric adds complexity and potential flakiness. It runs on main branch only, not on every PR.

**Test runner specifics:**

The test runner should use `@durable-streams/client` (the official TypeScript client) for the consumption side. This validates the ecosystem contract — that a real client can talk to our server — not just that curl scripts work. The production side (PUT/POST) can use plain HTTP.

**Documentation site (GitHub Pages):**

Simple docs site (mdbook or plain markdown rendered by GitHub Pages) covering:

- Architecture overview (four-layer diagram: client → proxy → server → producer)
- Two golden paths (see below)
- How to configure Clerk (or any OIDC provider) with Envoy
- How to connect Electric as a producer
- How to consume from a client application
- Full configuration reference for the server and the proxy
- How to swap the auth proxy (Caddy, nginx, managed API gateway)
- How to swap the storage backend
- Gaps and known limitations — every place you made an assumption not covered by PROTOCOL.md, every place Electric's docs were insufficient, every place you chose a default that might not suit all deployments
- Protocol governance (link to `docs/protocol-governance.md`)

The docs site should be something we'd be comfortable linking from a PR to the durable-streams repo.

## Two golden paths (document these, stop here)

### Golden path 1: Local development and testing

```
git clone <repo>
docker compose up --build
make integration-test
```

This runs: Envoy (with local JWKS) → durable-streams-rs → in-memory storage. Plus the test runner exercising the full scenario above. No external services, no Clerk account, no cloud credentials. Everything local and deterministic.

CI and the local path must run on linux/arm64 (Apple Silicon Macs via Docker) and linux/amd64.

### Golden path 2: Deploy for real

Either:

- **Envoy in front of server** — Envoy validates JWTs against Clerk's JWKS endpoint, proxies to the durable streams server on a private network. The documented default.
- **Managed edge JWT validation** — e.g. GCP API Gateway or AWS ALB validating JWTs so the stream server stays completely auth-agnostic. Noted as an alternative, not fully documented.

Both paths keep the stream server auth-free. The only difference is where JWT validation happens.

## Constraints

- Don't reinvent things that are already solved. HTTP serving is solved (axum). JWT validation in a proxy is solved (Envoy). The protocol spec is solved. This repo is the glue, the conformance, and the explanation.
- The stream server has no auth opinions. Auth is the proxy's job.
- The server speaks plain HTTP. TLS termination is at the proxy.
- No extra processes in the hot path beyond proxy + server.
- Configuration should be as simple as possible. Environment variables, sensible defaults, the fewest knobs that achieve the goal.
- The Rust code should be idiomatic, clippy-pedantic clean, well-documented.
- Offsets must be monotonic-by-construction. No "hope concurrency works out" designs.
- Default config must pass conformance without bespoke server-side flags.
- Do not enumerate protocol details as if your list is exhaustive. Derive the complete behaviour from PROTOCOL.md and the conformance tests. When in doubt, the tests are right.
- Do not invent protocol semantics. Every protocol decision must cite a pinned spec section or a conformance test; otherwise record it as a spec gap.

## Blocker policy

If you hit a blocker that prevents forward progress for more than 15 minutes (or requires human choice, secrets, or upstream clarification), stop and follow `docs/blocker-policy.md`. Do not thrash. Log it, propose a next step, and move on to unblocked work.

## Quality bar

Everything in the repository — code, tests, docs, docker-compose, CI — should be public-without-shame quality. Clean commit history, clear README, no TODO hacks left in, no credentials in config files. Someone from the Electric team or a random developer should be able to clone it, run the integration tests, read the docs, and understand what's happening.

## Guiding principle for the documentation

Approach this as someone who understands HTTP, has used an auth provider, and can read a docker-compose file — but has never heard of durable streams, Electric SQL, shapes, or offset-based resumability. Every concept should be introduced when it's first needed, not assumed. If something isn't clear from the protocol spec or Electric's docs, don't paper over it — flag it explicitly as a gap. Those gaps are the contribution: either we close them in our docs or we take them back to the Electric team as concrete "this is where a new user gets lost" feedback.

The documentation should be tested the same way we test code: if someone can't go from clone to running stack by following the guide without prior knowledge, the guide has a bug.

## Deliverables checklist

The project is done when ALL of the following are green:

### Repo structure (required from project setup)

- [ ] `docs/protocol-governance.md` — normative governance policy (content provided alongside this brief)
- [ ] `SPEC_VERSION.md` — pinned spec SHA, conformance version, date
- [ ] `docs/compatibility.md` — spec SHA → server version mapping
- [ ] `docs/decisions.md` — every protocol decision with spec/test link
- [ ] `docs/gaps.md` — every spec ambiguity with chosen interpretation
- [ ] `docs/blocker-policy.md` — blocker escalation process (content provided alongside this brief)
- [ ] `docs/blockers.md` — created as needed when blockers are logged

### Make targets (all required)

- [ ] `make build` — compiles the Rust binary (debug)
- [ ] `make release` — compiles the Rust binary (release, optimised)
- [ ] `make lint` — runs `cargo clippy --all-targets -- -D warnings` with pedantic lints
- [ ] `make fmt-check` — runs `cargo fmt --check`
- [ ] `make test` — runs Rust unit tests
- [ ] `make conformance` — starts the server with defaults, runs `@durable-streams/server-conformance-tests` (pointed at our server via suite's documented config), stops the server. Must pass.
- [ ] `make integration-test` — spins up the core docker-compose stack (server, Envoy, test runner), runs the end-to-end auth scenario, tears it down. Must pass.
- [ ] `make integration-test-electric` — spins up the full docker-compose stack (adds Postgres, Electric), runs the Electric-as-producer scenario, tears it down. Must pass.
- [ ] `make docker` — builds the Docker image for the local architecture
- [ ] `make docs` — builds the documentation site locally

### GitHub Actions jobs (all required)

- [ ] Lint + format check — on push and PR
- [ ] Rust unit tests — on push and PR
- [ ] Conformance test suite — on push and PR
- [ ] Core integration test — on push and PR
- [ ] Electric integration test — on push to main only
- [ ] Multi-arch Docker image build (linux/amd64, linux/arm64) — on main branch / tags
- [ ] Documentation site build and deploy to GitHub Pages — on push to main
- [ ] Governance check — verify `SPEC_VERSION.md` and `docs/compatibility.md` are consistent (on push and PR)

### Docker images

- [ ] Multi-arch: linux/amd64 and linux/arm64 (CI-built; local `make docker` builds native arch only)
- [ ] Minimal image size (multi-stage build, distroless or scratch base)
- [ ] Runs as non-root user
- [ ] Health check endpoint (`/healthz`, outside protocol router)

### Documentation site sections

- [ ] Architecture overview with diagram
- [ ] Quick start (golden path 1: local)
- [ ] Deployment guide (golden path 2: Envoy + Clerk)
- [ ] Configuration reference (server)
- [ ] Configuration reference (proxy / Envoy, annotated)
- [ ] Connecting Electric as a producer
- [ ] Connecting a client application
- [ ] Swapping the auth proxy
- [ ] Swapping the storage backend
- [ ] Protocol governance (link to `docs/protocol-governance.md`)
- [ ] Gaps and known limitations (every assumption, every spec ambiguity, every doc gap)

### Definition of done

All make targets pass. All CI jobs green. Multi-arch images build. Docs site deploys. Governance artifacts (`SPEC_VERSION.md`, `docs/decisions.md`, `docs/gaps.md`, `docs/compatibility.md`) exist and are populated. Any unresolved blockers are logged as GitHub Issues (or in `docs/blockers.md` with ready-to-run `gh issue create` commands). A developer with no prior durable streams knowledge can clone the repo, run `docker compose up && make integration-test`, and have a working authenticated stack.

## How to approach this

Start by reading the durable streams PROTOCOL.md and the conformance test source to understand what the server must implement. Create `SPEC_VERSION.md` and `docs/protocol-governance.md` first. Then propose a plan with clear phases:

1. Project setup — governance artifacts, spec pinning, repo structure
2. The protocol adapter passing conformance
3. The Envoy proxy configuration with local JWKS for testing
4. The core integration test harness (docker-compose + test runner with `@durable-streams/client`)
5. The Electric integration test (separate compose profile)
6. The architecture guide and docs site
7. CI, multi-arch images, and polish

Do not start building until the plan is reviewed.
