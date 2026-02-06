# Protocol Compatibility Matrix

This document tracks which protocol specification versions this server implements
and any behaviour differences or breaking changes.

## Version History

| Spec SHA | Server Version | Status | Notes | Breaking Changes |
|----------|----------------|--------|-------|------------------|
| a347312a47ae510a4a2e3ee7a121d6c8d7d74e50 | 0.1.0 | In Development | Initial implementation targeting conformance v0.2.1 | N/A |

## Current Implementation

**Server Version:** 0.1.0 (in development)

**Spec SHA:** a347312a47ae510a4a2e3ee7a121d6c8d7d74e50

**Conformance Version:** 0.2.1

**Target:** Full conformance with ~195 tests in the conformance suite.

**Status:** Implementation in progress. Not yet conformant.

## Conformance Coverage

As of 2026-02-06:
- Implemented: 0/195 tests
- Passing: 0/195 tests

(Update this section as implementation progresses)

## Known Limitations

None yet (initial implementation).

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
