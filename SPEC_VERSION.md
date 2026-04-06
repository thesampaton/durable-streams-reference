# Protocol Specification Version

This document pins the exact version of the durable streams protocol specification
and conformance test suite that this implementation targets.

## Specification Repository

**Repository:** https://github.com/durable-streams/durable-streams

**Pinned Commit SHA:** `a347312a47ae510a4a2e3ee7a121d6c8d7d74e50`

**Specification Document:** https://github.com/durable-streams/durable-streams/blob/a347312a47ae510a4a2e3ee7a121d6c8d7d74e50/PROTOCOL.md

**Date Pinned:** 2026-02-06

## Conformance Test Suite

**Package:** `@durable-streams/server-conformance-tests`

**Version:** `0.2.3`

**NPM Registry:** https://www.npmjs.com/package/@durable-streams/server-conformance-tests

## Implementation Status

This implementation targets full conformance with the pinned specification and
conformance test suite. See `docs/compatibility.md` for detailed compatibility
information and `docs/gaps.md` for any ambiguities or unspecified behaviour.

## Updating This Pin

When updating the spec pin:
1. Update the commit SHA and date in this file
2. Update `docs/compatibility.md` with the new spec SHA and any behaviour deltas
3. Run the conformance test suite against the new version
4. Update `docs/decisions.md` and `docs/gaps.md` as needed
5. Commit all changes together with a clear message explaining what changed

Do not update the spec pin without also updating compatibility documentation.
