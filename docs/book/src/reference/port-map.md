# Port map

All ports used by the Docker Compose stack, organized by profile.

## Default profile

| Port | Service | Purpose |
|------|---------|---------|
| 4437 | server | DS server (direct, no auth) |
| 8080 | envoy | JWT auth proxy (public endpoint) |
| 9901 | envoy | Envoy admin dashboard |

## Sync profile

Includes all default ports, plus:

| Port | Service | Purpose |
|------|---------|---------|
| 54321 | postgres | Postgres direct access (mapped from 5432) |

Internal (Docker network only, no host port):

| Service | Internal port | Purpose |
|---------|---------------|---------|
| electric | 3000 | Electric Shape API |
| sync-service | -- | No listening port (initiates connections) |

## Dev profile

Includes all default and sync ports, plus:

| Port | Service | Purpose |
|------|---------|---------|
| 8081 | adminer | Database admin UI |

The heartbeat producer has no listening port.

## Host services (not Docker)

| Port | Service | How to start |
|------|---------|-------------|
| 3000 | test-ui | `make dev-ui` |

## Quick reference

| Port | Service | Profile | Auth required |
|------|---------|---------|---------------|
| 3000 | test-ui | host | no |
| 4437 | DS server | default | no |
| 8080 | Envoy proxy | default | yes (JWT) |
| 8081 | Adminer | dev | no |
| 9901 | Envoy admin | default | no |
| 54321 | Postgres | sync | password |
