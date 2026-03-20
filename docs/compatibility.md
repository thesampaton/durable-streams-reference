# Protocol Compatibility Matrix

This document tracks which protocol specification versions this server implements
and any behaviour differences or breaking changes.

## Version History

| Spec SHA | Server Version | Status | Notes | Breaking Changes |
|----------|----------------|--------|-------|------------------|
| a347312a47ae510a4a2e3ee7a121d6c8d7d74e50 | 0.1.0 | Complete | Full conformance with @durable-streams/server-conformance-tests@0.2.1 | N/A |
| a347312a47ae510a4a2e3ee7a121d6c8d7d74e50 | 0.1.1 | Complete | Full conformance with @durable-streams/server-conformance-tests@0.2.2 | N/A |

## Current Implementation

**Server Version:** 0.1.1

**Spec SHA:** a347312a47ae510a4a2e3ee7a121d6c8d7d74e50

**Conformance Version:** 0.2.2

**Status:** Fully conformant. All 239 conformance tests pass.

## Conformance Coverage

As of 2026-02-09:
- Passing: 239/239 tests

## Known Limitations

- Default storage is in-memory. File-based (`file-fast`, `file-durable`) and crash-resilient (`acid`/redb) backends are available via `DS_STORAGE__MODE`.
- See `docs/gaps.md` for spec ambiguities and chosen interpretations.

## Future Compatibility

When the upstream spec or conformance tests change:
1. Pin the new spec SHA in `SPEC_VERSION.md`
2. Add a new row to the version history table above
3. Document any behaviour changes or breaking changes
4. Run the full conformance suite and update the coverage section
5. Update `docs/decisions.md` and `docs/gaps.md` as needed

## Deprecation Policy

This server follows semantic versioning:
- **Patch (0.1.x):** Bug fixes, no behaviour changes
- **Minor (0.x.0):** New features, backward-compatible spec updates
- **Major (x.0.0):** Breaking changes to protocol behaviour or API

Deprecated features will be marked in documentation and emit warnings for at
least one minor version before removal.
