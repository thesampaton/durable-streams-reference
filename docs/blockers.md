# Blockers

This file is a fallback for logging blockers when GitHub issues cannot be created
via API. Prefer creating actual GitHub issues when possible.

## Active Blockers

_No blockers logged yet._

## Blocker Entry Format

When logging a blocker here, use this template:

---

### Blocker: [Action-Oriented Title]

**Type:** `spec-ambiguity` | `conformance-failure` | `ci-flake` | `tooling` | `credentials`

**Context:** What you were doing when the blocker was encountered.

**Observed Behaviour:** Logs, error messages, or concrete observations.

**Expected Behaviour:** What should happen, with spec/test link if applicable.

**Repro Steps:**
1. Exact commands to reproduce
2. ...

**Environment:**
- OS: macOS / Linux / Windows
- Arch: amd64 / arm64
- Rust Version: `rustc --version`
- Docker Version: `docker --version` (if relevant)

**Proposed Next Step:** What would unblock this (e.g., "open upstream issue", "pin dependency version X", "need decision: A vs B").

**GitHub Issue Command:**
```bash
gh issue create --title "..." --body "..." --label blocker,spec-gap
```

**Date:** YYYY-MM-DD

---

## Resolved Blockers

When a blocker is resolved, move it here with a resolution note:

---

### [RESOLVED] Blocker: [Title]

**Resolution:** Brief explanation of how it was resolved.

**Resolved By:** GitHub issue #N | Spec update SHA | Decision in docs/decisions.md

**Date Resolved:** YYYY-MM-DD

---

## Cross-References

- For spec ambiguities, see `docs/gaps.md`
- For protocol decisions, see `docs/decisions.md`
- For GitHub issues, see github.com/[org]/[repo]/issues
