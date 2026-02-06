# Review Checklist & Escalation Framework

This document defines the alarm bells escalation scale, detection heuristics,
and gated mode criteria for agent implementation work. It exists to prevent
the pattern observed in PM-001 through PM-005: incomplete reasoning about
second-order effects compounding when findings cluster together.

See `docs/postmortems.md` for the specific incidents that motivated each rule.

---

## Definitions

**Agent:** The AI assistant writing code and proposing commits.

**Reviewer:** The human approving or rejecting commits. The reviewer's
obligations: cite a failing test or spec section for bell 3+ findings; provide
a clear description of the issue class, not just the symptom.

**Commit rejection:** The reviewer requests changes before the commit can land.
This includes: explicit "do not commit" feedback, "changes requested" on a PR,
or the reviewer reverting/amending the agent's work. CI failures (red builds)
are not commit rejections but do block commits.

**Task:** One numbered item in the current implementation plan (e.g. "Task 2.11
 — ETag & Caching"). A task is complete when all its commits have landed and
`cargo test` passes.

**Protocol-facing change:** Any change that touches handlers, headers, status
codes, request parsing, response formatting, or stream semantics. Changes to
storage internals, config, or test helpers are not protocol-facing unless they
alter externally observable behaviour.

**Severity mapping:**

| Label | Bell level | Description |
|-------|-----------|-------------|
| P2 | Bell 2 | Design risk. System works but structure is fragile |
| P1 | Bell 3 | Correctness risk. Data loss, corruption, or silent misbehaviour |
| P0 | Bell 4+ | Repeated miss or systemic failure pattern |

---

## Alarm bells scale

Every issue found by the reviewer is assigned a bell level. The level
determines the required response before proceeding.

| Bells | Meaning | Trigger examples | Required action |
|-------|---------|-----------------|-----------------|
| 0 | Clean | No issues found | Proceed normally |
| 1 | Cosmetic | Style, naming, minor duplication | Fix inline, no process change. Non-escalating: cosmetic findings do not add heat unless they cluster into bell 2 (style drift reducing readability) |
| 2 | Design risk | Response-wide requirement applied inconsistently; mixed abstractions across layers; unbounded memory growth risk; incomplete audit of call sites | Stop. Re-read the code path end-to-end. Run `cargo test`; run conformance if protocol-facing. Fix, then self-review before re-submitting |
| 3 | Correctness risk | Concurrency bug, atomicity gap, data loss potential, silent misbehaviour | Stop. Write a **cause statement** (one sentence: "This is a race because X is read before Y under lock Z"). Write a failing test. Fix at the correct layer. Reviewer must approve before commit |
| 4 | Repeated miss | Same issue restated by reviewer, or heat >= 4 | Enter gated mode (see below). Write failing test, fix, full test suite before commit |
| 5 | Trust reset | Fundamental misunderstanding of the requirement; fix introduces a regression (previously passing test now fails, conformance regression, or new invariant violation) | Full stop. Re-read the spec/requirement from scratch. Explain understanding back to reviewer before writing any code |

### Bell escalation rules

- A finding starts at its natural level based on severity.
- **Restated issue** (reviewer repeats the same concern because the agent didn't
  address it): escalate by +1 bell.
- **Misunderstood issue** (agent demonstrates a wrong mental model of the
  system, not just inattention): escalate by +2 bells. Misunderstanding is
  about the model; restatement is about attention. Misunderstanding escalates
  faster.
- **Regression introduced** (previously passing test now fails, conformance
  regression, or new invariant violation): escalate to bell 5.
- Heat reaching >= 4 within the heat window automatically triggers bell 4
  (gated mode).
- Three commit rejections on the same task automatically escalate to bell 5.
- **Per-review cap:** If escalation happens twice within one review cycle
  (one commit attempt), force gated mode regardless of current heat.

### Cause statement (required for bell 3+)

Every bell 3+ finding requires a written cause statement before the fix is
attempted. One sentence that identifies the mechanism:

- "This is a race because `head()` is called after `read()` without holding
  the stream lock, allowing a concurrent write to change the next offset."
- "This is an accounting leak because `remove()` on expiration does not subtract
  `stream.total_bytes` from the global counter."

The cause statement forces concreteness and prevents hand-wavy fixes.

---

## Heat window

What matters is the *density* of findings relative to scope, not the raw count
over calendar time.

**Heat is scoped to the current task item (plan bullet), not the wall clock.**
Two findings on the same task are a pattern. Two findings separated by three
clean tasks are not.

**Heat** is a counter that accrues on findings and decays on clean work:

| Event | Heat change |
|-------|-------------|
| Bell 2 finding | +1 |
| Bell 3 finding | +2 |
| Bell 4 finding | +3 |
| Bell 5 finding | +5 |
| Restated issue | +3 |
| Regression introduced | +3 |
| Clean commit (no rejections) | -1 |
| Clean task completion (all commits clean) | -2 |
| Context switch (see below) | -1 |

**Gated mode triggers at heat >= 4.**

The decay is deliberately slower than the accrual: it takes more clean work to
cool down than it takes findings to heat up. Trust is lost fast and rebuilt
slowly, but it *does* rebuild.

**Context switch** means changing top-level module area (e.g. `src/storage/*` to
`src/handlers/*`) or changing plan item number. It is not subjective — if the
git diff touches a different module tree or the plan item number changed, it
counts.

**Heat persistence:** Heat persists across sessions (conversations, context
windows) until a clean task completion or explicit reviewer reset. A new
conversation is a UI artifact, not a trust boundary. If gated mode was active
at the end of a session, it carries forward unless the reviewer explicitly
clears it.

**Cool-down floor in gated mode:** While in gated mode, heat cannot decrease
below 3 until a full conformance run passes. This prevents premature exit from
two small clean commits on non-protocol-facing changes.

### Bell de-escalation

- Each clean commit reduces heat by 1. Each clean task completion reduces by 2.
- Gated mode exits when heat drops below 4 *and* 2 consecutive clean commits
  have landed with full test suite *and* full conformance has passed.
- Bell 5 requires explicit reviewer sign-off to resume normal pace regardless
  of heat level.

---

## Gated mode

Gated mode is a structured slowdown triggered by accumulated issues. It prevents
the compounding pattern where speed produces more bugs than it saves time.

### Entry conditions

- Heat reaches 4 or higher
- Bell level reaches 4 or higher on any single finding
- Two escalations within one review cycle (per-review cap)
- Reviewer explicitly requests gated mode

### Gated mode rules

0. **No speculative changes.** Only change what you can tie to a failing test
   or spec link. No cleanup, no "while I'm here" improvements.
1. **Test before every commit.** `cargo test` (unit + integration) must pass
   before each commit. For protocol-facing changes, also run the conformance
   subset for touched endpoints. Before exiting gated mode: full conformance +
   full integration.
2. **One failing test at a time.** Write the failing test for the next fix,
   make it pass, commit. Do not batch multiple fixes.
3. **Regression test required.** Every fix must include a test that would have
   caught the original bug. No exceptions.
4. **Fix at the correct layer with justification.** One line stating why this
   layer is correct: "Fixed in storage because invariant is atomicity of
   append; handler pre-check is insufficient." This prevents band-aids.
5. **Self-review before submission.** Before presenting code to the reviewer,
   run through the pre-commit checklist (below) and the detection heuristics.
   Document what you checked.
6. **Scope guard.** Do not refactor, clean up, or improve adjacent code. Touch
   only what the failing test requires.
7. **Blocker to issue.** Any ambiguity that can't be resolved from spec/tests
   must become a GitHub issue (or `docs/blockers.md` entry) before proceeding.

### Exit conditions (all must be met)

- Heat drops below 4
- Full conformance suite passes
- 2 consecutive clean commits (no rejections) with full test suite passing
- Reviewer explicitly approves return to normal pace (required for bell 5)

---

## Detection heuristics

These are the recurring patterns to watch for before submitting code. Each maps
to one or more postmortems in `docs/postmortems.md`.

### Snapshot boundary (PM-001)

**Trigger:** Handler makes more than one storage call.

**Ask:** Can state change between calls? Does the response mix snapshots from
different points in time?

**Rule:** All correlated metadata (offsets, closed state, message counts) must
come from a single storage call's return value. If you need data from two calls,
you need a single call that returns both.

**Verify:** `grep -n 'storage\.' src/handlers/*.rs` — count calls per handler.
Any handler with >1 call needs a snapshot justification.

### Exhaustive audit (PM-002)

**Trigger:** Adding a header, status code, or behaviour to "all responses."

**Ask:** Did I check every `into_response()` site? Every early return? Every
error path? The `Error::into_response()` impl?

**Rule:** `grep into_response src/handlers/` and verify every site. Then check
`Error::into_response()`. A response-wide requirement must cover all response
paths, not just the ones you're currently looking at.

### Removal accounting (PM-003)

**Trigger:** Calling `.remove()`, expiring, or evicting a collection entry.

**Ask:** What counters, indexes, or side-effect state are associated with this
entry? Are they all cleaned up?

**Rule:** Search for the removed entry's size/count fields (e.g. `total_bytes`,
`message_count`) and verify every one is decremented. Every removal path must
match the accounting of the insertion path.

**Verify:** `grep -n '\.remove\|saturating_sub\|total_bytes' src/storage/*.rs`
— every `.remove()` site must have a corresponding counter update nearby.

### Mutation atomicity (PM-004)

**Trigger:** A loop calling a fallible storage method, or a single HTTP request
mapping to multiple storage mutations.

**Ask:** What if the second/third mutation fails? Is partial progress acceptable
to the client? Can the client safely retry?

**Rule:** If partial progress is not acceptable (it usually isn't for append
operations), the operation needs transaction semantics in the storage layer.
Pre-checks at the handler layer are insufficient because they cannot account
for all failure modes.

**Verify:** `grep -n 'for.*storage\.\|loop.*storage\.' src/handlers/*.rs` —
any loop containing a storage call needs atomicity justification.

### Fix completeness (PM-004)

**Trigger:** Fixing "X is not atomic" or "X is missing" with a targeted patch.

**Ask:** Am I validating all failure modes, or just the one that was reported?
Does the real fix require a different layer or a different API?

**Rule:** When the fix for "operation X is not atomic" is "check some conditions
before doing X," that's usually insufficient. True atomicity requires
validate-then-commit as a single unit in the storage layer.

### Branch duplication (PM-005)

**Trigger:** `if N > 1 { ... } else { ... }` where both branches have similar
structure and error handling.

**Ask:** Can one API handle both cases? Is the branching adding value or just
complexity? Is N=1 just the degenerate case of N?

**Rule:** If both branches call similar functions with similar error handling,
unify them. A batch API that handles single items as a one-element batch is
almost always simpler than branching between single and batch code paths.

### Symptom vs class (PM-002, PM-004)

**Trigger:** A bug is reported in one specific location.

**Ask:** Is this a one-off, or a symptom of a class of bugs? Where else could
the same pattern occur?

**Rule:** Before fixing the reported instance, audit the entire class. If
Cache-Control is missing from one error path, check all error paths. If
atomicity is broken for one operation, check all operations with similar
structure.

### Middleware vs handler (PM-002)

**Trigger:** A requirement applies to "all responses" (headers, status codes,
logging, etc.).

**Ask:** Can this be enforced in middleware/layer so it's impossible to miss in
individual handlers?

**Rule:** Prefer middleware for response-wide requirements unless conformance
tests require per-handler variation. Per-handler header construction is the
root cause of PM-002: it's easy to miss one branch. Middleware makes omission
structurally impossible.

### Async cancellation (future: long-poll/SSE)

**Trigger:** Long-poll or SSE request that can be canceled mid-flight by the
client.

**Ask:** Does cancellation leak resources? Are waiters/subscribers deregistered?
Are counters or broadcast channels left in an inconsistent state?

**Rule:** Every `tokio::select!` or broadcast subscriber must have a cleanup
path on cancellation. Test by dropping the client connection mid-wait.

### Timeout semantics (future: long-poll/SSE)

**Trigger:** Any wait or long-poll with a timeout.

**Ask:** What is the contract at timeout? Which headers are returned? Is the
cursor set? Is `Stream-Up-To-Date` accurate?

**Rule:** Timeout is a valid response, not an error. The response must include
all metadata headers (next offset, up-to-date flag, cursor) so the client can
seamlessly retry.

### Backpressure and fanout (future: SSE)

**Trigger:** SSE broadcast to multiple subscribers.

**Ask:** Does one slow client block the stream for all subscribers? Is
per-subscriber buffering bounded?

**Rule:** Use bounded channels per subscriber. If a subscriber falls behind,
drop messages or disconnect rather than blocking the producer. Unbounded fanout
is a denial-of-service vector.

---

## Pre-commit checklist

Run through these items before every commit. Ordered by leverage.

1. **Regression test.** If this change fixes a bug or changes behaviour, is
   there a test that would have caught the original issue? Write it first.

2. **Diff audit.** Run `git diff --stat` and `git diff`. List externally
   observable changes (new/changed headers, status codes, response bodies).
   If the list is non-empty, this is a protocol-facing change.

3. **Invariant scan.** Check the invariants table below. Name which invariants
   this change could affect. Verify the corresponding tests still pass.

4. **Snapshot boundary.** Does any handler call storage more than once? All
   correlated metadata must come from a single call's return value.

5. **Exhaustive header audit.** After adding a header, `grep into_response
   src/handlers/` and verify every site, including error paths, early returns,
   and `Error::into_response()`.

6. **Removal accounting.** After any `.remove()` or entry replacement, search
   for the entry's size/count fields and verify all counters are updated.

7. **Mutation atomicity.** If a handler calls a fallible storage method in a
   loop, the operation likely needs transaction semantics. Move atomicity into
   the storage layer, not pre-checks in the handler.

8. **Fix the class, not the instance.** When fixing a reported bug, ask "where
   else could this same pattern occur?" Audit the whole class before committing.

9. **Avoid branch duplication.** If both branches of a conditional call similar
   functions with similar error handling, unify them. 1-vs-N becomes just N.

10. **Middleware over handlers.** If a header or behaviour must appear on every
    response, enforce it in middleware, not per-handler.

11. **Decision log.** If this change alters protocol behaviour (headers, status
    codes, mode semantics), add an entry to `docs/decisions.md` with spec/test
    link.

---

## Invariants

These are the system-wide invariants that must hold across all code paths.
Violating any of these is a bell 3+ finding. Every invariant has: where it's
enforced in code, tests that cover it today, and what failure looks like.

| Invariant | Where enforced | Failure in production | Covered by |
|-----------|---------------|----------------------|------------|
| Single-source snapshot per response | Storage return types carry all metadata; handlers never call storage twice for correlated data | Silent data loss: client resumes from wrong offset, skips messages | `test_response_headers_match_body_snapshot` |
| Offsets monotonically increase per stream | Per-stream mutex in `InMemoryStorage` | Duplicate or out-of-order reads on resume | `test_offset_monotonicity`, `test_concurrent_appends_monotonicity` |
| `Cache-Control: no-store` on every response | `SecurityHeadersLayer` middleware + `Error::into_response()` + manual 409/304 paths | Intermediary caches serve stale stream data | `test_*_security_headers` (6 tests) |
| Global byte counter = sum of stream bytes | Every insertion and removal path in `InMemoryStorage` | Spurious 413 errors or unbounded memory growth | `test_memory_limits`, `test_delete` |
| Batch operations are all-or-nothing | `batch_append()` validates all limits before committing any messages | Partial commits on failure, duplicate data on client retry | `test_json_array_append_is_atomic_on_limit_error` |
| Expired streams are invisible | Every storage access method calls `is_expired()` | Ghost streams block recreation or leak stale data | `test_*_expired_stream_*` (4 tests), `test_recreate_after_expiry` |
| Security headers on every response | `SecurityHeadersLayer` middleware | Missing `X-Content-Type-Options`, `Cross-Origin-Resource-Policy` | `test_*_security_headers` (6 tests) |
| Content-type validated on append | POST handler + `batch_append()` content-type check | Silent data corruption from mismatched formats | `test_append_content_type_mismatch_returns_409`, `test_content_type_mismatch` |
