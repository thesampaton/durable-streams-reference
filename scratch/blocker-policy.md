# Blocker Policy

If you hit a blocker that prevents forward progress for more than 15 minutes (or requires human choice, secrets, or upstream clarification), stop thrashing and log it.

## What counts as a blocker

- Spec ambiguity not resolved by pinned spec + conformance tests
- Missing credentials, secrets, or Clerk configuration
- Conformance test failures that require interpretation, not implementation
- Dependency or tooling failures that require version pin decisions
- Unclear "which of these is intended" choices (paths, headers, status codes) not settled by tests
- CI flakiness or environment-specific failures (arm64 quirks, Docker networking, etc.)

## Where to log it

Prefer: create a GitHub Issue directly. Fallback (if you cannot create issues via API): write a structured entry to `docs/blockers.md` and print the exact `gh issue create` command for me to paste.

## Issue format (required)

Title must be action-oriented:

- `Spec ambiguity: <topic>`
- `Conformance failure: <test-name> <expected> vs <actual>`
- `CI flake: <job> on <arch>`

Body must include:

- **Context:** what you were doing
- **Observed behaviour:** logs/snippets
- **Expected behaviour:** cite spec/test link if applicable
- **Repro steps:** exact commands
- **Environment:** OS/arch, Rust version, Docker version
- **Proposed next step:** what would unblock it (e.g. "open upstream issue", "pin version", "need decision: A vs B")
- **Labels** (if supported): `blocker`, `spec-gap`, `conformance`, `ci`, `docs`

## Link it back

When a blocker is logged, also add a one-line reference in `docs/gaps.md` or `docs/decisions.md` if it's spec-related, linking the issue number.
