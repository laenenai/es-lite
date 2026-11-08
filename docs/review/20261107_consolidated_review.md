# es-lite — Consolidated Review (adjudication of both 2026-11-07 reviews)

- **Date:** 2026-11-07
- **Inputs:**
  - **Review A** — single-reviewer pass: `docs/review/20261107_principal_review.md`
  - **Review B** — blinded six-reviewer GLM 5.3 panel: `docs/review/20261107_independent_glm53_panel_review.md`
- **Method:** every claim that appeared in only one review, or where the two disagree, was adjudicated against the source code and — for the load-bearing ones — against PostgreSQL, SQLite and IETF documentation. Research citations are inline. The output is one findings register (§3) that the remediation plan implements task-by-task.

---

## 1. How the two reviews compare

| | Review A (principal) | Review B (blinded panel) |
|---|---|---|
| Reviewers | 1 | 6, isolated GLM 5.3 agents in a fresh worktree, barred from `docs/review/`, `docs/backlog/`, `docs/superpowers/` |
| Confirmed defects reproduced live | 6 (anchor race, duplicate `EventID`, relay-role latch, `Skip` rewind, filtered stall, sequence `SELECT`) | — (blinded; several built their own throwaway tests/`EXPLAIN` evidence) |
| Verdict | "architecture sound; fix a narrow cluster" | Same, independently, six times |

**Overlap:** six Review-A findings were independently rediscovered by blinded panel reviewers (anchor torn heads, relay-role latch, duplicate-`EventID` mapping, `Skip` rewind, filtered-stall false positive, sequence `SELECT` privilege). That convergence — plus live reproduction — is the strongest evidence in this document; those six are settled facts, not opinions.

**Divergence:** Review A produced 1 claim the panel disproved (§2.1), 2 severity calls the panel correctly downgraded (§2.2), and 4 findings the panel did not surface (§3, marked *A-only*). The panel produced 13 findings Review A missed — including the two most serious ones (head truncation, `eventfeed` swallowing terminal errors).

---

## 2. Contradictions, adjudicated

### 2.1 "SQLite has the same anchor race, and is arguably worse" — **Review A is WRONG, retracted**

Review A claimed `sqlite.Anchor` races the same way as Postgres because it runs `MAX(global_position)`, `heads(upto)` and `INSERT` without a transaction. Panel reviewer R3 argued the opposite; the adjudication confirms R3:

- SQLite admits **one write transaction at a time** (SQLite docs, *Transaction Control* §2.1: "only one simultaneous write transaction"). A row's `global_position` (rowid) is assigned at INSERT under that lock, so an *uncommitted* row's position is necessarily **above every committed position** at that instant, and commit order equals write order.
- Therefore `upto = MAX(committed)` is a stable cutoff: any row that becomes visible later has a position **above** `upto` and cannot change `heads(upto)`. The missing transaction is harmless; the second query's `global_position <= upto` filter does the work.
- Postgres has no such invariant: concurrent writers hold lower positions uncommitted while higher ones commit — hence the Postgres-only race, reproduced live in Review A's own session and independently reasoned to by R2.

**Action:** Review A's §1/SQLite half is retracted (a correction banner is added to that file). The Postgres half stands and is plan Task 4.

### 2.2 Severity disputes — **panel's lower severities accepted**

- **Duplicate `EventID` → `ErrConflict`.** Review A framed it as "retry storms" (P1/P2). The panel's adjudication is right: the transaction rolls back either way, `aggregate.Runtime` never sets `EventID` (only direct store users reach it), and both backends behave identically. It is a **diagnostics bug** (a permanent error classified as retryable, and the underlying `pgconn.PgError` discarded). Consolidated severity: **P2** (it also becomes load-bearing for the idempotency design, where deterministic event IDs make concurrent duplicate commands collide — see §4/D6).
- **`sqlite.NewWithDB` position safety.** Review A implied the multi-connection case could break the gap-free cursor; R3 showed positions stay gap-free even then (file-level single writer), and the real losses are `SQLITE_BUSY` noise, a silently broken `isConstraintViolation` (modernc-only type check), memory-DSN splitting, and the `Close`-ownership trap. Consolidated as a **contract bug, P2**, fix unchanged.

### 2.3 Claims checked against authoritative documentation

All five load-bearing claims verified:

1. **Advisory-lock leak (A + R2).** PostgreSQL docs (*System Administration Functions*): "`pg_advisory_xact_lock` works the same as `pg_advisory_lock`, except the lock is **automatically released at the end of the current transaction** and cannot be released explicitly." The migration runner's session-level lock + context-bound unlock is the leaky shape; the fix is the xact-scoped lock inside each migration transaction (plan Task 18).
2. **`pg_has_role` inside an `OR` policy arm (R6).** PostgreSQL docs (*Logical Operators* §9.1): "it is **not guaranteed that the left operand is evaluated before the right operand**." So migration 0005's policy arm `… OR (bypass = 'on' AND pg_has_role(current_user,'es_relay','member'))` can, in principle, evaluate `pg_has_role` (which **errors** on a nonexistent role) even for workspace-scoped sessions — the runbook's "workspace traffic never depends on `es_relay` existing" rests on an implementation detail. Fix: mirror the Go check's `CASE WHEN EXISTS` guard in a policy-hardening migration (Task 9).
3. **Sequence `last_value` overstates under `CACHE > 1` (R2).** PostgreSQL docs (*`pg_sequences`*): `last_value` is "the last sequence value written to disk. **If caching is used, this value can be greater than the last value handed out**." The fence's lossless proof needs the converse (every position ≤ L was drawn before L was read) — true for `CACHE 1` (the default, which nothing pins), false under an operator's `ALTER SEQUENCE … CACHE 32`. Fix: read through the documented `pg_sequences` view (which also exposes `cache_size`, and whose `last_value` reads as NULL rather than erroring when the role has only `USAGE` — killing two findings at once) and assert `cache_size = 1` on every fence round (Task 16).
4. **Sequence privilege (A + R2 + R6).** Verified live during Review A: a role with only `USAGE` gets `permission denied for sequence` on `SELECT last_value FROM events_global_position_seq`; the documented `pg_sequences` view (and `pg_sequence_last_value()`) works with `USAGE` alone and returns NULL for a never-used sequence (`COALESCE(…, 0)` handles it). The runbook currently documents **zero** grants (Task 19).
5. **Idempotency-Key semantics (A + R5).** IETF `draft-ietf-httpapi-idempotency-key-header-07` §2.6: a retried request after completion **SHOULD respond with the result of the previously completed operation**; a concurrent retry **SHOULD get 409**; key reuse with a different payload **SHOULD get 422**; §5 recommends a **composite key** and UUID-strength keys. es-lite's current header→`CommandID` mapping satisfies none of this (the example test itself appends twice on one key). Fix decided with the maintainer: implement draft-compliant dedup (Tasks 30–33, design §D6).
6. **SQLite migration locking.** SQLite docs (*Transaction Control* §2.2) define `BEGIN IMMEDIATE`; `modernc.org/sqlite` v1.55.0 supports the `_txlock` DSN parameter (driver source, `sqlite.go:333`). For es-lite the cleanest serializer is the `schema_migrations` INSERT itself (the write lock it takes serializes rivals; the loser sees the PK violation) — plan Task 22.

### 2.4 Reviewer-level disagreements settled by direct verification

- **R3 vs A on SQLite anchors** — settled for R3 (§2.1).
- **R1's head-truncation finding vs `es/verify_test.go:176-187`** — the existing test pins the *wrong* behavior as intended (mid-stream acceptance); the fix must live in `VerifyStreamWith` (which always pages from 0), not in `ChainChecker.Next` (where snapshot-seeded runtime loads legitimately start mid-stream). Both the finding and its precise fix location verified.
- **R4's "conformance suite ignores `RawHead`/`MatchingHead`"** — verified by grep; the two port methods the resume clamp, stall detection and `Status` lag depend on are untested for third-party adapters.
- **A's xid-burn claim, unconfirmed by the panel** — kept: `readLastPosition` reads the *global* sequence, so `last <= from` only short-circuits when the whole database is idle. ADR 0037's "idle streams burn no transaction ids" is false in any multi-writer deployment. Fix: per-workspace `RawHead` pre-check before drawing a fence (Task 16).

---

## 3. Consolidated findings register

The single agreed list. **Src**: A = Review A, B = panel (R# = reviewer). Severity after adjudication. **Task** = remediation plan task in `docs/superpowers/plans/2026-11-07-review-remediation.md`.

### P1 — must fix before production

| ID | Finding | Src | Task |
|---|---|---|---|
| F1 | Postgres `Anchor`/`CheckAnchor` record roots over torn heads under concurrent appends → false `ErrAnchorMismatch`. **Reproduced live.** | A, R2 | 4 |
| F2 | `VerifyStream` passes a stream whose earliest rows were deleted (head truncation) — invisible to chain *and* anchors; `--strict` unusable on pre-0002 stores. | R1 | 1 |
| F3 | `eventfeed` retries the terminal `ErrDefinitionChanged` forever, silently — no observer method fires, lease flaps; same death for persistent `ErrLeaseHeld`. **Verified with a live test.** | R4 | 14 |
| F4 | `checkRelayRole` latches transient probe failures for the Store's lifetime. **Reproduced live.** | A, R2 | 15 |
| F5 | Required privileges undocumented; the fence needs `SELECT` on the sequence, which `USAGE` does not confer → feed outage on least-privilege deployments. **Reproduced live.** | A, R2, R6 | 16, 19 |
| F6 | `Idempotency-Key` is audit metadata only; retries double-write (draft-07 noncompliant; proven by the example test). | A, R5 | 30–33 |
| F7 | `LogFeed` retries a failing handler forever, discarding the error, no backoff growth, no signal — the documented single-node live path. | R5 | 23 |
| F8 | The `anchors` table's bypass arm was never role-gated; migration 0005's comment ("Both policies that carry the arm…") is false — three carried it. | R6 | 9 |

### P2 — important, bounded

| ID | Finding | Src | Task |
|---|---|---|---|
| F9 | The fence's lossless proof silently breaks under sequence `CACHE > 1`; nothing pins or asserts `CACHE 1`. | R2 | 16 |
| F10 | Migration advisory lock can leak onto a pooled session and wedge all future migrations. | A, R2 | 18 |
| F11 | `global_position` is attested by neither chain nor anchor while the docs claim otherwise; cross-stream reordering is undetectable. | A, R6 | 6–8 |
| F12 | Append-only is convention-only at the DB level; relay over-granted (needs only `UPDATE (published_at)`). | A, R6 | 19 |
| F13 | `es-verify` prints `ok`/exit 0 for nonexistent streams and empty databases. | R6 | 3 |
| F14 | `OnStall` false-positives for caught-up filtered subscriptions (compares cursor to the **unfiltered** `RawHead`). **Reproduced live.** | A, R4 | 10 |
| F15 | The resume-clamp loop emits no signals while a fence is blocked. | R4 (A, minor) | 11 |
| F16 | `Skip` can silently rewind the checkpoint. **Reproduced live.** | A, R4 | 12 |
| F17 | Conformance suite never exercises `RawHead`/`MatchingHead` — the contract the clamp, stall and `Status` depend on. | R4 | 10 |
| F18 | SQLite lacks the feed's `type_url` index → rare-type subscriptions rescan the log tail every poll, on the single shared connection. **`EXPLAIN`-verified.** | A, R3 | 20 |
| F19 | `sqlite.NewWithDB` forfeits the documented single-connection contract, and `Close` closes the caller's handle. | A, R3 | 21 |
| F20 | Generated projections/reactions have no upcaster path; `ev` (raw legacy shape) and `state` (upcast via runtime) can disagree in one handler call. | A, R5 | 26 |
| F21 | `FETCH_MODE_STATE_AS_OF` is quadratic (full re-fold per consumed event). | A, R5 | 27 |
| F22 | Poller/Relay `Run` exits on the first transient error; a closed `Wake` channel hot-spins; zero tests for `delivery/poller.go`. | R5 | 29 |
| F23 | `BufferedMarker`'s "crash window bounded by `MaxInterval`" is false when traffic stops; no flush on `Run` exit; `Load` resurrects over out-of-band resets. | A, R5 | 24 |
| F24 | Snapshots are not lineage-checked: a snapshot newer than the log is served as current state and `Handle` wedges. Entire snapshot feature untested. | A, R1 | 25 |
| F25 | Duplicate caller-supplied `EventID` reported as `ErrConflict`; underlying DB error discarded. **Reproduced live.** (Downgraded from A's P1 emphasis.) | A, R2, R3 | 17 |
| F26 | `ErrStreamNotFound` is a dead documented contract; only a test fake returns it. | A, R1 | 2 |
| F27 | Idle subscribers burn xids whenever *any* workspace writes (global `last_value` short-circuit). | A | 16 |
| F28 | CI: release actions unpinned with `contents: write`; coverage ratchet blind to `postgres/` (measured 0.5%). | R6 | 37 |

### P3 — polish (consolidated)

| ID | Finding | Src | Task |
|---|---|---|---|
| F29 | `AnchorRoot`: corrupt-length head hash silently treated as unchained; sort-order wording ambiguous for off-box reimplementers; no golden vector. | R1 | 5 |
| F30 | `StampDecisionTime` false return ignored → hand-written `DecidedAt` commands validate expiry against client time. | R1 | 35 |
| F31 | Dead compatibility surface: `ErrDecidedAtMismatch`, `Meta.OccurredAt`, `MetaFrom` dropping non-UUID `Correlation-Id`, `MapError` doc-table omissions. | A, R1, R5 | 30, 35 |
| F32 | Generator: `commandVariants` dead, `_ = meta`, duplicated `default:` branch, `specNames` process-global; required generated deps panic at first use instead of construction; `resolveMethod` doesn't validate the WriteResult shape. | A, R5 | 28 |
| F33 | Postgres: PG14+ floor undocumented; `DBTX` accepts a pool (`ReadStreamTx` then fails *silently*); `octet_length` detoasts payloads; clock-source inconsistency (`Drain`/`Anchor` use app clock); "same snapshot" comment false under READ COMMITTED. | R2 | 17–19 |
| F34 | SQLite: `scanEnvelope` ignores malformed audit UUIDs; migration check-then-apply race ("no lock needed" comment overstates); dead columns (`stream_type`, `checkpoints.updated_at`, `claimed_at`); Postgres `checkpoints` table dead and the only table without RLS. | A, R3, R6 | 9, 22 |
| F35 | Feed: `Subscribe` can drop a lease-loss diagnosis racing a stream error; `Waiter` wakeups can never beat the `Poll` cadence (port can't distinguish wake from timeout); `Definition.Hash` NUL-join unguarded for `Consumes`. | R4 | 13, 11 |
| F36 | CLI: `es-migrate`/`es-verify` default `SQLITE_DSN` to `./eslite.db` silently; no tests for either binary. | R6 | 3 |
| F37 | Docs: ADR 0029's "ordering attested by anchors" (false until F11 lands), ADR 0037 idle/xid wording, ADR 0001 cut-table errata, runbook "startup" vs lazy role-check, shared-login trust note, TLS/DSN hygiene. | A, R2, R6 | 36 |
| F38 | Capability gaps (maintainer elected to include): `FETCH_MODE_EVENT_ONLY`; per-workspace fence high-water (F27's full fix). | R5, A | 34, 16 |

**Dropped/deferred as not-defects:** `stream_type` column removal (log-table rebuilds are not worth it pre-1.0 — comment fixed instead, F34); full `Replay` short-page contract redesign (doc note only, Task 36); `eventfeed` token-policy comment duplication (folded into Task 36 docs pass); postgres transaction-scaffolding refactor (folded opportunistically into Tasks 16–19 as those files are touched, tracked in Task 36's checklist).

---

## 4. What "solved" means (exit criteria)

1. An anchor recorded under live concurrent appends verifies forever after — pinned by a test that reproduces today's failure first (Task 4, RED-first).
2. Deleting a stream's oldest rows fails `es-verify` by default; a nonexistent stream is reported as empty, never `ok` (Tasks 1–3).
3. `es-verify order` detects repositioned events within the attested range; exported anchors carry an order root (Tasks 6–8).
4. Every bypass-carrying policy is role-gated with the `EXISTS` guard, and a migration-level test pins all of them (Task 9).
5. A definition-changed deploy stops the consumer **loudly** (Task 14); a failing projection handler is visible and backs off (Task 23); a blocked fence is observable from the clamp loop (Task 11).
6. A least-privilege role runs append + feed + drain + anchor end to end from the runbook's grant list alone (Tasks 16, 19).
7. A replayed `Idempotency-Key` returns the stored response; a concurrent duplicate gets 409; a reused-key-different-payload gets 422 (Tasks 30–33).
8. Every doc claim in README/ADR/runbook is true of the code, and Review A carries its retraction banner (Task 36).

**Companion documents:**
- Design: `docs/superpowers/specs/2026-11-07-review-remediation-design.md`
- Plan: `docs/superpowers/plans/2026-11-07-review-remediation.md`
