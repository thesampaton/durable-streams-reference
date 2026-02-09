# Dev mode

Dev mode starts the full stack plus observability tools for watching data flow through the system in real time.

## Starting

```bash
# Terminal 1: Start the full stack
make dev

# Terminal 2: Start the visual stream browser
make dev-ui
```

## What you get

### Heartbeat producer

A container that exercises both sync directions every 5 seconds:

- **Odd ticks (PG-to-DS):** INSERTs into the `items` Postgres table. Electric picks up the WAL change, the sync service forwards it to the `pg-items` DS stream.
- **Even ticks (DS-to-PG):** POSTs a session event to the `session-events` DS stream. The sync service consumes it via SSE and INSERTs into the `session_events` Postgres table.

Watch the logs:

```bash
docker-compose logs -f producer
```

After ~30 seconds, both `items` and `session_events` tables will have heartbeat rows.

### Adminer (database UI)

Open [http://localhost:8081](http://localhost:8081) to browse Postgres tables.

| Field | Value |
|-------|-------|
| System | PostgreSQL |
| Server | postgres |
| Username | postgres |
| Password | password |
| Database | durable_streams |

### Test UI (stream browser)

The test-ui is the official [durable-streams stream browser](https://github.com/durable-streams/durable-streams/tree/main/examples/test-ui). It is cloned into `.dev/` (gitignored) on first run via `make dev-ui`.

Connect it to the DS server at `http://localhost:4437`. The producer's streams (`pg-items`, `session-events`) can be navigated to manually.

**Requirements:** pnpm, Node 22+

**Known limitation:** The server does not implement the `__registry__` stream used for auto-discovery, so streams will not auto-populate in the sidebar.

## Port map

| Port | Service | Purpose |
|------|---------|---------|
| 4437 | server | DS server (direct, no auth) |
| 8080 | envoy | JWT auth proxy |
| 9901 | envoy | Envoy admin dashboard |
| 54321 | postgres | Postgres direct access |
| 8081 | adminer | Database admin UI |
| 3000 | test-ui | Visual stream browser (host) |

## Stopping

```bash
make dev-down          # stop all containers
rm -rf .dev/           # remove cloned test-ui monorepo (optional)
```
