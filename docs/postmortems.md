# Implementation Postmortems

Bugs and code smells caught by human review before they reached a commit. Each
entry records the symptom, root cause, what should have caught it earlier, and
the guardrail added to prevent recurrence.

These are not protocol gaps (see `docs/gaps.md`). These are agent implementation
mistakes that a careful reviewer intercepted.

---

## PM-001: GET handler mixed storage snapshots

**Task:** 2.6 (GET catch-up mode)
**Severity:** P1 — data correctness
**Caught by:** Human review of implementation before commit
**Detection stage:** Pre-commit
**Blast radius:** Any client doing resumable reads under concurrent writes
**User impact:** Silent data loss — messages skipped on resume, no error surfaced
**Proof:** `test_response_headers_match_body_snapshot` in `tests/read_operations.rs`

### Symptom

The GET handler called `storage.read()` to fetch message data, then called
`storage.head()` to populate `Stream-Next-Offset` and `Stream-Closed` response
headers. Under concurrency, a write landing between the two calls would cause
the response headers to reflect newer state than the response body.

A client resuming from the returned `Stream-Next-Offset` would silently skip
the message that arrived between the two calls.

### Root cause

The agent treated `read()` and `head()` as independent lookups rather than
recognising that the response body and its metadata must come from the same
point-in-time snapshot. The agent did not consider the concurrent-producer
scenario when wiring up the handler.

### What should have caught it

- **Design heuristic:** Any time a handler builds a response from two or more
  storage calls, ask: *"Can state change between these calls? If yes, does the
  response mix snapshots?"*
- **Test:** A test that appends concurrently between the first and second read
  and asserts the returned offset matches the body, not a later snapshot.

### Prevention

1. **Code comment** added to `src/handlers/get.rs` explaining the snapshot
   requirement and why `read_result` (not `head()`) must supply all offset and
   closed-state metadata.
2. **Regression test** `test_response_headers_match_body_snapshot` added in
   `tests/read_operations.rs`. It appends a second message after the first read
   and verifies that resuming from the first read's offset returns exactly the
   new message — proving the offset was consistent with the first body.
3. **Guardrail added to pre-commit checklist:** When a handler makes more than
   one storage call, all correlated metadata (offsets, closed state) must come
   from a single call's return value.

### Follow-up flags on same issue

The human also flagged two related process issues:

- **Missing "why" in code:** The initial fix lacked a comment explaining the
  race condition. Agent was asked to add context so `git blame` explains *why*
  the code is structured this way, not just *what* it does.
- **Missing regression test:** Agent did not proactively write a test to prevent
  reintroduction. Human had to request one explicitly.

### Alarm bell assessment

Started at 2 bells (multiple storage calls in handler). Should have escalated
to 3 bells (concurrency-facing). The follow-up flags (missing comment, missing
test) pushed it toward 4 bells — the human had to restate expectations on the
same issue three times.

---

## PM-002: Inconsistent Cache-Control across response paths

**Task:** 2.8 (Security headers)
**Severity:** P2 — inconsistent caching behaviour
**Caught by:** Human review on commit
**Detection stage:** Pre-commit (commit rejected)
**Blast radius:** All error responses from all endpoints
**User impact:** Intermediary caches could serve stale error responses, confusing
retry logic and producing inconsistent behaviour for the same endpoint
**Proof:** `test_error_response_security_headers` in `tests/security_headers.rs`

### Symptom

`Cache-Control: no-store` was added to success response paths (200, 201, 204)
but not to error paths. The POST handler's `Err(Error::StreamClosed)` branch
returned a 409 without the header. Intermediaries could cache the conflict
response even though other POST responses were explicitly non-cacheable.

### Root cause

The agent added the header to each handler's success path without exhaustively
auditing every `into_response()` call site. Error responses were treated as
"different" even though they carry the same caching requirements.

### What should have caught it

- **Design heuristic:** When adding a header that must appear on *every*
  response, grep for all `into_response()` sites and verify each one.
  Better: add the header in a layer that wraps all responses (middleware or
  error conversion).
- **Test:** The security header tests did check error responses
  (`test_error_response_security_headers`) but that test went through the
  `Error::into_response()` path, not the special-cased `StreamClosed` 409 in
  the POST handler.

### Prevention

1. **Added `Cache-Control: no-store` to `Error::into_response()`** so all
   automatic error responses include it without per-handler effort.
2. **Added to GET 304** (the other non-obvious path).
3. **Added to POST StreamClosed 409** (the manually constructed error).
4. **Detection rule:** When adding a response header, the checklist is:
   - Every success path in every handler
   - Every manually constructed error response (grep `into_response` in handlers)
   - The `Error::into_response()` impl (covers all automatic errors)

### Escalation

Human had to ask "can you double check that there isn't the same problem on
other response paths first" — the agent only fixed the one path flagged. The
correct instinct is to treat the reported issue as a *symptom of a class* and
audit the entire class.

### Alarm bell assessment

Started at 2 bells (manual header construction). The human's redirect to "check
other paths" was a soft escalation to 3 bells. The fact that `Error::into_response()`
was also missing the header — affecting every error in the system — made this
higher blast radius than it appeared from the initial report.

---

## PM-003: Memory accounting leak on stream expiration

**Task:** 2.9 (TTL/Expiry)
**Severity:** P1 — resource leak causes spurious 413 errors
**Caught by:** Human review on commit
**Detection stage:** Pre-commit (commit rejected)
**Blast radius:** Any server with TTL-configured streams under steady-state load
**User impact:** After enough streams expire, all appends start failing with 413
even though actual memory is well within limits. Requires server restart.
**Proof:** No dedicated regression test added (gap — should have been)

### Symptom

When `create_stream` detected an expired stream and removed it from the
`HashMap`, the stream's `total_bytes` were not subtracted from the global
`self.total_bytes` counter. Over time, memory that had been freed was still
counted against the global limit, causing legitimate appends to fail with
`Error::MemoryLimitExceeded`.

### Root cause

The agent focused on the "happy path" of removing the expired entry and
creating a new one, without considering the side-effect accounting associated
with the removed entry. The `delete()` method (which does handle accounting
correctly) was not the code path used here — expiration cleanup was a new
removal path that bypassed the existing accounting.

### What should have caught it

- **Design heuristic:** Any time you call `.remove()` on a collection entry,
  ask: *"What counters, indexes, or side-effect state are associated with this
  entry? Do they all get cleaned up?"*
- **Test:** A unit test that creates a stream with data, lets it expire, then
  verifies `storage.total_bytes()` returns 0 (not the stale value).

### Prevention

1. **Fix:** Capture `stream.total_bytes` before removing, subtract from global
   counter with `saturating_sub`.
2. **Detection rule:** Every collection removal site (`.remove()`, expiration,
   eviction) must audit all associated accounting. Search for the field name
   of the removed entry's size/count fields and verify each is decremented.

### Alarm bell assessment

2 bells (removal accounting). This was the second P1 in the session (after
PM-001). Under the alarm bell framework, two P1/P2 findings in one session
should have triggered gated mode. It did not. PM-004 followed.

---

## PM-004: Non-atomic JSON batch appends

**Task:** 2.10 (JSON mode)
**Severity:** P2 — partial commits on batch failure
**Caught by:** Human review on commit (flagged twice)
**Detection stage:** Pre-commit (commit rejected, then rejected again)
**Blast radius:** Any JSON array append where a later element exceeds limits
**User impact:** Client sees 413 error, retries, gets duplicate data for the
elements that were partially committed. Data ordering may be corrupted.
**Proof:** `test_json_array_append_is_atomic_on_limit_error` in `tests/json_mode.rs`

### Symptom

JSON arrays were flattened into N messages and appended one-by-one in a loop.
If the third message hit `MemoryLimitExceeded`, the first two were already
committed. The client saw a 413 error but some data was stored. Retries would
duplicate or reorder data.

### Root cause

The agent decomposed a batch operation into individual operations without
considering failure atomicity. The mental model was "append each element" rather
than "this is one request that must either fully succeed or fully fail."

### What should have caught it

- **Design heuristic:** When a single HTTP request maps to multiple storage
  mutations, ask: *"What if the second/third one fails? Is partial progress
  acceptable to the client?"* For append operations the answer is almost always
  no.
- **Test:** An integration test with a tight stream-size limit where the first
  array element fits but the batch total exceeds the limit.

### Prevention

1. **Added `batch_append()` to `Storage` trait** with all-or-nothing semantics:
   validates all messages (closed state, expiration, content-type, memory
   limits) *before* committing any.
2. **Regression test** `test_json_array_append_is_atomic_on_limit_error` uses
   `spawn_test_server_with_limits(1024*1024, 20)` to set a 20-byte stream
   limit and verifies the stream remains empty after a batch failure.
3. **Detection rule:** Whenever a loop calls a fallible storage method, ask
   whether the operation needs transaction semantics. If the loop represents a
   single client request, it almost certainly does.

### Escalation (flagged twice — alarm bell 4)

The agent's first fix attempt was a "pre-check" that only validated closed
state, not memory limits. The human had to flag the same issue a second time:

> *"swing and a miss — Appending JSON arrays is still non-atomic because
> messages are written one-by-one and any later Err(e) returns after earlier
> messages have already been committed."*

The correct fix required moving the atomicity boundary into the storage layer
(`batch_append`) rather than trying to pre-validate at the handler layer with
incomplete information.

**Lesson:** When the fix for "operation X is not atomic" is "check some
conditions before doing X," that's usually insufficient. True atomicity requires
the storage layer to validate-then-commit as a single unit.

### Alarm bell assessment

Started at 3 bells (atomicity). Escalated to 4 bells when the human had to
restate the same issue. The agent proposed a handler-layer pre-check instead
of a storage-layer atomic operation — classic "fix at the wrong layer" pattern.
Under the alarm bell framework, 4 bells requires: stop, write failing test,
fix at correct layer, review before commit. Should have been in gated mode
already (third P1/P2 of the session).

---

## PM-005: Duplicated error handling from conditional branching

**Task:** 2.10 (JSON mode)
**Severity:** Code smell
**Caught by:** Human review on commit
**Detection stage:** Pre-commit (commit rejected)
**Blast radius:** Maintainability — any future change to error response format
must be made in two places
**User impact:** None directly, but increases probability of future PM-002-type
inconsistencies
**Proof:** N/A (code quality, not functional)

### Symptom

The POST handler used `if messages_to_append.len() > 1` to choose between
`batch_append()` and `append()`. Both branches contained identical
`StreamClosed` error handling (15+ lines of duplicated header construction).
Brittle and error-prone: any change to the error response would need to be
made in both places.

### Root cause

The agent optimised for "single messages use the simpler API" rather than
recognising that `batch_append` with a single-element vec is semantically
identical and eliminates the branching entirely.

### What should have caught it

- **Design heuristic:** If the only difference between two branches is 1-vs-N,
  make the API accept N and let N=1 be the degenerate case. Don't branch.
- **Code smell detection:** Identical error-handling blocks in sibling branches
  are a strong signal that the branching is unnecessary.

### Prevention

1. **Fix (by human):** Always use `batch_append()` regardless of message count.
   Single messages go through as a one-element batch. Zero duplication.
2. **Detection rule:** When introducing a conditional where both branches call
   similar functions with similar error handling, check whether one function
   can handle both cases.

### Alarm bell assessment

1 bell (minor duplication). But this was the third rejection on the same task
(PM-004 first flag, PM-004 second flag, PM-005). The accumulation matters more
than the individual severity. Under the alarm bell framework, this session was
deep into 4-bell territory and should have been in gated mode.

---

## Retrospective

### What happened

Tasks 2.8 through 2.10 produced 5 postmortems across three major implementation
tasks. Context: these were complex, interleaved features (security headers, TTL
expiry, JSON mode with atomicity) covering significant surface area. The findings
clustered in PM-002 through PM-005, which all landed within the same heat window.

| PM | Bells | Commit rejections | Class | Heat delta |
|----|-------|-------------------|-------|------------|
| 001 | 2→4 | 0 (caught pre-commit, but 3 follow-up flags) | Concurrency | +1 (bell 2) |
| 002 | 2→3 | 1 (+ human redirect to audit class) | Exhaustiveness | +1 (bell 2), cumulative: 2 |
| 003 | 2 | 1 | Accounting | +1 (bell 2), cumulative: 3 |
| 004 | 3→4 | 2 (same issue restated) | Atomicity | +2 (bell 3) + 3 (restate), cumulative: 8 |
| 005 | 1 | 1 (human fixed directly) | Duplication | +0 (bell 1, no heat) |

Total commit rejections: 5. Total human escalations: 7+.

### When gated mode should have activated

Using the heat model from `docs/review-checklist.md`: PM-001 (+1), PM-002 (+1),
PM-003 (+1) brought heat to 3. No clean tasks landed between them to provide
decay. PM-004 as a bell 3 finding (+2) pushed heat to 5, triggering gated mode.
But the restate on PM-004 (+3 more) pushed heat to 8 — deep into gated
territory by the time it was caught.

The practical trigger point was after PM-003: three consecutive findings without
a clean task between them should have prompted the agent to slow down even though
heat hadn't formally hit 4 yet. PM-004's restate confirmed the pattern.

### What gated mode would have changed

- **PM-004 first fix:** Gated mode requires "fix at the correct layer." The
  agent would have recognised that handler-layer pre-checks cannot enforce
  atomicity and gone straight to `batch_append()` in the storage layer.
- **PM-005:** Gated mode's "one failing test at a time" scope would have
  forced a simpler implementation. The agent would not have introduced the
  1-vs-N branching because the unified API was the simpler path.

### Underlying pattern

The common thread across PM-001 through PM-005 is **incomplete reasoning about
second-order effects:**

- PM-001: "What happens if state changes between my two calls?"
- PM-002: "Where else does this same requirement apply?"
- PM-003: "What side effects does removing this entry have?"
- PM-004: "What if a later step in this sequence fails?"
- PM-005: "Does this branching actually add value?"

Each question is about looking one step beyond the immediate task. The agent
consistently got the first-order implementation correct but missed the
second-order consequences.

---

## Detection heuristics summary

These are the recurring patterns to watch for. Each maps to one or more
postmortems above.

| Heuristic | Trigger | Ask yourself | PMs |
|-----------|---------|-------------|-----|
| **Snapshot boundary** | Handler makes >1 storage call | Can state change between calls? Does the response mix snapshots? | PM-001 |
| **Exhaustive header audit** | Adding a header to "all responses" | Did I check every `into_response()` site, including error paths and early returns? | PM-002 |
| **Removal accounting** | Calling `.remove()` / expiring / evicting an entry | What counters or indexes are associated with this entry? Are they all updated? | PM-003 |
| **Mutation atomicity** | Loop calling fallible storage method | Does partial progress leave the system in a state the client doesn't expect? | PM-004 |
| **Fix completeness** | Fixing "X is not atomic" with pre-checks | Am I validating all failure modes, or just the one reported? Does true atomicity require a storage-layer change? | PM-004 |
| **Branch duplication** | `if N > 1 { ... } else { ... }` with similar bodies | Can one API handle both cases? Is the branching adding value or just complexity? | PM-005 |
| **Symptom vs class** | A bug is reported in one location | Is this a one-off, or a symptom of a class of bugs? Audit the whole class. | PM-002, PM-004 |

---

## Pre-commit checklist

Distilled from all postmortems. Run before every commit.

1. **Snapshot boundary:** Does any handler call storage >1 time? All correlated
   metadata (offsets, closed state) must come from a single call's return value.
2. **Exhaustive header audit:** After adding a header, `grep into_response
   src/handlers/` and verify every site, including error paths and early returns.
   Also check `Error::into_response()`.
3. **Removal accounting:** After any `.remove()` or entry replacement, search
   for the entry's size/count fields and verify all counters are updated.
4. **Mutation atomicity:** If a handler calls a fallible storage method in a
   loop, the operation likely needs transaction semantics. Move atomicity into
   the storage layer, not pre-checks in the handler.
5. **Fix the class, not the instance:** When fixing a reported bug, ask "where
   else could this same pattern occur?" Audit the whole class before committing.
6. **Write the regression test:** Don't wait to be asked. If the bug was
   catchable by a test, write that test as part of the fix.
7. **Avoid branch duplication:** If both branches of a conditional call similar
   functions with similar error handling, unify them. 1-vs-N → just use N.
