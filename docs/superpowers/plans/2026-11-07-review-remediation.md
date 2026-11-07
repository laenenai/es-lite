# Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix every finding F1–F38 from `docs/review/20261107_consolidated_review.md` — torn-head anchors, head-truncation blindness, swallowed terminal errors, the operational grant contract, draft-compliant idempotency, full log-order attestation — with every behavioral fix landing red-first.

**Architecture:** Seven phases: (0) the audit path becomes commit-safe and order-attesting via a per-batch `log_order` row hashed inside each append transaction (no append serialization — design D1) with anchors fenced by the existing horizon fence; (1) the feed protocol gains a raw matching head, clamp-loop signals, monotonic `Skip`, a wake-aware `Waiter`, and a fatal-error escape for the consumer adapter; (2–3) storage/ops fixes (relay-role retry, `pg_sequences` bound + cache assertion, error classification, xact-scoped migration lock, grants, indexes, SQLite contract); (4) projection liveness (retry signal, marker flush, snapshot lineage, upcaster seam, tail-bounded state loads); (5) cmdsvc idempotency (deterministic event IDs + dedup port + stored-response replay); (6) docs truth, CI hardening, dead-surface removal.

**Tech Stack:** Go 1.27.3, pgx v5, modernc.org/sqlite, protobuf-go. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-11-07-review-remediation-design.md` (decisions D1–D10) and `docs/review/20261107_consolidated_review.md` (findings register F1–F38). Section references like §D6 point at the design.

## Global Constraints

- Pre-1.0 breaking changes are allowed (design D9); **every** port/signature/behavior change adds an entry to `docs/migration/2026-11-07-breaking-changes.md` in the same task that changes it.
- Migrations are append-only and transactional (ADR 0010). Landing numbers: **postgres** `0006_log_order.sql`, `0007_policy_hardening.sql`, `0008_drop_unused_checkpoints.sql`, `0009_command_dedup.sql`; **sqlite** `0005_feed_type_index.sql`, `0006_log_order.sql`, `0007_command_dedup.sql`. No applied migration file's semantics is ever edited; corrections ship as errata in the new migration's header comment.
- New Postgres tables: `log_order` = workspace arm + role-gated bypass arm (events parity); `command_dedup` = workspace arm only, **no bypass arm** (feed-tables parity). FORCE ROW LEVEL SECURITY everywhere.
- Payloads are opaque (ADR 0025): never logged, never in errors; error text carries positions, type URLs, sizes only.
- No append-path serialization and no new append round trips beyond what exists (design D1 anti-goal; `log_order` is computed from data the append already holds).
- Appends keep their first statement `SELECT pg_current_xact_id()` — the fence's P1 (ADR 0037).
- Postgres tests run via `task test:pg` (Docker postgres:17, `PG_DSN`); plain `GOWORK=off go test ./...` must stay green without it. Postgres integration tests `t.Skip` without `PG_DSN`.
- Generated code is regenerated with `task generate` in every task that touches `cmd/protoc-gen-es-lite` or `proto/`; `task generate:check` must be green before those tasks commit.
- Conventional commits matching history (`fix(postgres): …`, `feat(feed): …`, `test(es): …`, `docs: …`, `chore(ci): …`).
- Every bug fix is **red-first**: the failing test is written, run, and observed to fail before the fix lands in the same task.
- Error style: `es:` prefix sentinels in `es/errors.go`, `feed:` prefix in `feed/errors.go`.

## Review Focus

The failure modes the findings say must not survive, and the test that pins each:

1. **An anchor recorded under live concurrent appends must verify forever after** — `TestAnchorStableUnderConcurrentAppend` (Task 4, RED first).
2. **A deleted stream head must fail the default audit; an empty stream must never read `ok`** — `TestVerifyStreamDetectsHeadTruncation` (Task 1), `TestVerifyStreamEmptyStreamIsNotIntact` (Task 3).
3. **A repositioned event must fail `es-verify order`** — `TestOrderVerifyDetectsRepositionedEvent` (Task 7).
4. **A definition-changed deploy must stop the consumer loudly** — `TestEventfeedStopsOnDefinitionChanged` (Task 14, RED first).
5. **A least-privilege role must run append + feed + drain + anchor from the runbook's grants alone** — `TestLeastPrivilegeEndToEnd` (Task 19).
6. **A replayed Idempotency-Key returns the stored response; concurrent gets `aborted`; mismatched payload gets the reuse error** — `TestIdempotencyReplay`, `TestIdempotencyConcurrentConflict`, `TestIdempotencyFingerprintMismatch` (Task 33).
7. **A rare-type SQLite subscription must not rescan the log per idle poll** — `TestMatchingHeadUsesTypeIndex` (Task 20).

---

## Phase 0 — Audit path: commit safety and order attestation

### Task 1: `VerifyStream` detects head truncation

**Files:**
- Modify: `es/verify.go` (`VerifyStreamWith`), `es/verify.go` (`ChainReport` doc)
- Test: `es/verify_test.go`

**Interfaces:**
- Produces: `VerifyStreamWith` reports a first-surviving-version > 1 as `BrokenAt = firstVersion` with an error wrapping `ErrChainBroken` ("head truncated: first version is %d"). `ChainChecker.Next` is **unchanged** (mid-stream starts stay legitimate for snapshot-seeded loads).

- [ ] **Step 1 (RED):** add `TestVerifyStreamDetectsHeadTruncation` to `es/verify_test.go` using the existing `memStore`/`chain()` harness: seed versions 1–5, delete 1–5 from the fake, verify — assert `!rep.Intact()`, `rep.BrokenAt == 6`, `errors.Is(err, ErrChainBroken)`. Run `go test ./es -run TestVerifyStreamDetectsHeadTruncation` — expect FAIL (report is intact today).
- [ ] **Step 2:** In `VerifyStreamWith`, record `firstVersion` of the first envelope fed across all pages (0 if none); after the loop, if `rep.Head > 0 && firstVersion > 1 && rep.Intact()`, set `rep.BrokenAt = firstVersion` and `rep.Err = fmt.Errorf("%w: stream %s head truncated: first version is %d", ErrChainBroken, sid, firstVersion)`; return `rep, rep.Err`. Do not touch `ChainChecker`.
- [ ] **Step 3 (doc):** one sentence on `ChainReport.ChainedFrom`: "first version carrying a hash among those fed to the checker" (F29-adjacent).
- [ ] **Step 4:** `go test ./es -count=1` — all PASS, including the existing mid-stream checker test.
- [ ] **Step 5:** Commit: `fix(es): VerifyStream reports head truncation as a broken chain`.

### Task 2: Adapters return `ErrStreamNotFound` (F26)

**Files:**
- Modify: `postgres/wsstore.go` (`readStream`), `sqlite/store.go` (`ReadStream`), `es/store.go` (`ReadStream` doc), `es/errors.go` (`ErrStreamNotFound` doc)
- Test: `postgres/postgres_test.go`, `sqlite/store_test.go` (or the append/read test files)

**Interfaces:**
- Produces: `ReadStream(ctx, sid, 0, to)` on a stream with **zero rows total** returns `fmt.Errorf("%w: %s", es.ErrStreamNotFound, sid.Canonical())`. Reads with `fromVersion > 0` on an empty range keep returning `(nil, nil)`. `VerifyStreamWith` and `aggregate.load` already tolerate the sentinel — no changes there.

- [ ] **Step 1 (RED):** `TestReadStreamStreamNotFound`: `ReadStream(ctx, sid, 0, 0)` on a never-written stream asserts `errors.Is(err, es.ErrStreamNotFound)` in both `postgres` (PG_DSN-gated) and `sqlite` suites; plus `TestReadStreamEmptyRangeStillNil`: existing stream, `ReadStream(sid, currentVersion, 0)` → `(nil, nil)`. Run — expect FAIL.
- [ ] **Step 2:** Implement: in each adapter, when `fromVersion == 0 && toVersion == 0` and the query returns zero rows, return the wrapped sentinel (Postgres: in `Workspace.readStream` only when called via `ReadStream` — pass a flag or check in the public wrappers; keep `AppendTx`-style paths unaffected). Update both doc comments to state the contract exactly. Note the breaking change in `docs/migration/2026-11-07-breaking-changes.md`.
- [ ] **Step 3:** `go test ./sqlite ./postgres ./es ./aggregate -count=1` (PG skips without DSN) + `task test:pg` — PASS.
- [ ] **Step 4:** Commit: `fix(store): ReadStream reports ErrStreamNotFound for never-written streams`.

### Task 3: `es-verify` CLI truthfulness (F13, F36)

**Files:**
- Modify: `cmd/es-verify/main.go`
- Create: `cmd/es-verify/main_test.go`
- Test: `cmd/es-verify/main_test.go`

**Interfaces:**
- Consumes: Task 2's sentinel, Task 1's truncation report.
- Produces: `verifyOne` prints `EMPTY <label>` and returns `exitUsage` when `rep.Head == 0` (unless `--allow-empty` on `stream`); `cmdAll` exits `exitUsage` with `no streams found` when `ListStreams` returns zero; `openBackend` logs loudly (`using default SQLITE_DSN file:eslite.db in the working directory`) when `SQLITE_DSN` is unset. Exit codes unchanged otherwise (0 intact, 1 broken, 2 usage/connection).

- [ ] **Step 1:** `main_test.go` with a sqlite-backed `run()` harness: subtests `stream on empty db exits 2`, `all on empty db exits 2`, `stream on ok stream exits 0`, `stream --allow-empty on empty exits 0`, `migrate+verify roundtrip sets exit 1 on a broken chain` (insert a tampered row directly via `sqlite.Store.DB()`). These double as the binary's first tests.
- [ ] **Step 2:** Run — several FAIL (today empty reads `ok`/0). Implement the three changes; run again — PASS.
- [ ] **Step 3:** Commit: `fix(cmd): es-verify reports empty streams and databases as errors`.

### Task 4: Fenced anchors on Postgres (F1) — RED-first repro

**Files:**
- Modify: `postgres/anchor.go`, `postgres/wsstore.go` (expose a fence helper)
- Test: `postgres/chain_test.go`

**Interfaces:**
- Consumes: the horizon-fence round (`readLastPosition`, `drawFence`, `fenceCleared`) — add an unexported `func (s *Store) settledBound(ctx context.Context, upto uint64) (uint64, error)` on `Store` that clamps `upto` to `last_value` (using it when `upto == 0`), draws `R`, and polls `fenceCleared` (~100 ms cadence) until true or ctx ends (error: `anchor fence still blocked`).
- Produces: `Anchor` and `CheckAnchor` call `settledBound` before computing heads. `bypassTx` unchanged.

- [ ] **Step 1 (RED):** `TestAnchorStableUnderConcurrentAppend`: open tx1 via `AppendTx` (position p1, hold open), commit a second append at p2 > p1 on another connection, call `s.Anchor(ctx, 0)`, commit tx1, then `s.CheckAnchor(ctx, a)` — assert **nil**. Run `task test:pg -run TestAnchorStableUnderConcurrentAppend` — expect FAIL with `ErrAnchorMismatch` (the live repro from the review).
- [ ] **Step 2:** Implement `settledBound` (reusing the wsstore fence statements, bypass-safe: they run on the pool, not under bypass) and call it at the top of `Anchor` and `CheckAnchor`.
- [ ] **Step 3:** Re-run — PASS. Add `TestCheckAnchorStableUnderConcurrentAppend` (same shape, anchored before, in-flight commits between Anchor and CheckAnchor).
- [ ] **Step 4:** Commit: `fix(postgres): fence anchor computation on the horizon fence`.

### Task 5: `AnchorRoot` robustness + golden vector (F29)

**Files:**
- Modify: `es/anchor.go`
- Test: `es/anchor_test.go` (new)

**Interfaces:**
- Produces: `AnchorRoot` substitutes `UnchainedHeadHash` **only** when `len(h.Hash) == 0`; any other length is a programming error — `AnchorRoot` keeps its signature (no error return) but `StreamHead` gains no validation; instead both adapters' `heads` queries already filter exact hashes, so add `es.ValidateStreamHead(h StreamHead) error` used by a new `es/anchor_test.go` golden test and available to third-party `Anchorer`s. Document the sort as **byte-wise on the canonical `type:id` string** in the `AnchorRoot` doc comment; switch `sort.Slice` → `sort.SliceStable`.

- [ ] **Step 1:** `TestAnchorRootGoldenVector` (fixed heads incl. an unchained one; hex-pinned root computed once and recorded), `TestValidateStreamHeadRejectsCorruptHash` (16-byte hash → error; empty → ok), `TestAnchorRootSortIsBytewiseOnCanonical`.
- [ ] **Step 2:** Implement; run `go test ./es -count=1` — PASS.
- [ ] **Step 3:** Commit: `fix(es): pin AnchorRoot encoding, sort order, and head-hash validation`.

### Task 6: Log-order attestation — core types and roots (F11, design D1)

**Files:**
- Create: `es/order.go`
- Modify: `es/anchor.go` (`Anchor` struct fields)
- Test: `es/order_test.go` (new)

**Interfaces:**
- Produces (used by Tasks 7–8 and `es-verify`):
  ```go
  // es/order.go
  const orderDomain = "es-lite.log-order.v1"; const orderRootDomain = "es-lite.log-order-root.v1"
  type OrderEntry struct { Position uint64; Hash []byte }
  func BatchOrderHash(entries []OrderEntry) [HashSize]byte // SHA-256(domain ‖ u32(len) ‖ per entry: u64(position) ‖ 32B hash), position order
  type OrderRow struct { From, To uint64; Count uint32; BatchHash []byte }
  func OrderRoot(rows []OrderRow) [HashSize]byte // SHA-256(domain ‖ u64(len) ‖ per row sorted by From: u64(from) ‖ u64(to) ‖ u32(count) ‖ 32B batchhash)
  ```
  `es.Anchor` gains `OrderRootHash []byte \`json:"order_root_hash,omitempty"\``, `OrderFromPosition uint64 \`json:"order_from_position"\``, `OrderCount uint64 \`json:"order_count"\``. `es.Anchorer` gains `OrderRows(ctx context.Context, upto uint64) ([]OrderRow, error)` (breaking; migration-notes entry).
- Legacy anchors (nil `OrderRootHash`) verify heads-only — the comparison skips order fields.

- [ ] **Step 1:** `TestBatchOrderHashGoldenVector` and `TestOrderRootGoldenVector` — hex-pinned vectors with a documented encoding comment (an independent Python computation recorded in the test comment, matching `es/hash_test.go`'s precedent).
- [ ] **Step 2:** Implement `es/order.go` + `Anchor` fields. `go test ./es` — PASS.
- [ ] **Step 3:** Commit: `feat(es): batch-order hash encoding and anchor order root`.

### Task 7: Log-order attestation — Postgres (F11)

**Files:**
- Create: `postgres/migrations/0006_log_order.sql`
- Modify: `postgres/wsstore.go` (append), `postgres/anchor.go`, `cmd/es-verify/main.go`
- Test: `postgres/chain_test.go`, `postgres/migrations_test.go`

**Interfaces:**
- Consumes: Task 6 (`es.OrderEntry`, `es.BatchOrderHash`, `es.OrderRow`, `es.OrderRoot`), Task 4's `settledBound`.
- Produces: migration `0006` (table `log_order(from_position bigint PRIMARY KEY, to_position bigint NOT NULL, workspace_id text NOT NULL, stream_id text NOT NULL, event_count int NOT NULL, batch_hash bytea NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp())`, index `(to_position)`, RLS workspace arm + role-gated bypass arm mirroring `events`, `FORCE ROW LEVEL SECURITY`); `Workspace.append` inserts one `log_order` row per non-empty batch (from `res.Envelopes[i].GlobalPosition/Hash` already collected — no new reads); `Anchor`/`CheckAnchor` compute and compare `OrderRootHash`/`OrderFromPosition`/`OrderCount` (first row's `From` when rows exist, else 0) inside the fenced tx; `Store.OrderRows(ctx, upto)` reads rows under bypass; `es-verify order` (and `all`, after streams) recomputes each `batch_hash` from the events in `(from, to]` via `es.Store.ReadAll`-style paging on positions — see step 3.

- [ ] **Step 1:** `postgres/migrations_test.go`: expected latest version becomes 6.
- [ ] **Step 2 (RED):** `TestOrderVerifyDetectsRepositionedEvent` (PG-gated): append two single-event batches; `UPDATE events SET global_position = <swap>` directly on the pool; run the new `verifyOrder(ctx, store, upto)` helper — expect it to report a batch-hash mismatch. Write the helper in `cmd/es-verify/main.go` as `verifyOrder(...)` returning exit code, sharing `verifyOne`'s printing style; expect FAIL (no `log_order` yet).
- [ ] **Step 3:** Migration `0006` (constraints above; header documents the encoding and the "rows before 0006 are unattested" rule). `wsstore.go`: after the events loop, if `len(p.Events) > 0` build `entries` from the loop's `(gp, env.Hash)` pairs, compute `es.BatchOrderHash`, insert the `log_order` row in the same transaction (claims order unchanged). `anchor.go`: inside the settled tx, `SELECT from_position, to_position, event_count, batch_hash FROM log_order WHERE to_position <= $1 ORDER BY from_position` → `es.OrderRoot`; set the three new `Anchor` fields; `CheckAnchor` compares all three (legacy nil → skip order fields). `Store.OrderRows` under `bypassTx`.
- [ ] **Step 4 (deep verify):** `es-verify order`: page `events` by `global_position <= upto` in batches (reuse the workspace-scoped store with a bypass-free path? — no: order verification is an admin path; use `Store` + a new `Store.ReadAllPositions(ctx, from, to, limit)` bypass read, or reuse `Drain`-style `envelopeCols` select under `bypassTx`), group by the `log_order` ranges, recompute each `batch_hash`, compare. Exit 1 on mismatch with the offending `from_position` printed.
- [ ] **Step 5:** `task test:pg` — PASS incl. the new tests; `go build ./...` green; run `es-verify all` shape check via the Task 3 harness (order wired into `all` after stream verification).
- [ ] **Step 6:** Commit: `feat(postgres): per-batch log-order attestation with fenced order roots`.

### Task 8: Log-order attestation — SQLite (F11)

**Files:**
- Create: `sqlite/migrations/0006_log_order.sql`
- Modify: `sqlite/store.go` (append, `OrderRows`), `sqlite/anchor.go`
- Test: `sqlite/chain_test.go`, `sqlite/migrate_test.go`

**Interfaces:**
- Consumes: Task 6 types.
- Produces: sqlite `0006` (same columns minus workspace/RLS, `created_at TEXT`); `Store.Append` inserts the row from its `(gp, hash)` pairs; `Anchor`/`CheckAnchor`/`OrderRows` mirror Postgres (no fence needed — adjudication §2.1); `es-verify order` works unchanged through `es.Anchorer`.

- [ ] **Step 1:** `migrate_test.go` version bump to 6.
- [ ] **Step 2 (RED):** `TestSQLiteOrderVerifyDetectsRepositionedEvent` — same shape as Task 7 (swap two positions via `db.Exec`). Expect FAIL.
- [ ] **Step 3:** Implement; `go test ./sqlite -count=1` — PASS.
- [ ] **Step 4:** Commit: `feat(sqlite): per-batch log-order attestation`.

### Task 9: Policy hardening + dead table drop (F8, F12, F34-partial)

**Files:**
- Create: `postgres/migrations/0007_policy_hardening.sql`, `postgres/migrations/0008_drop_unused_checkpoints.sql`
- Test: `postgres/migrations_test.go`, `postgres/drain_test.go` (policy assertions)

**Interfaces:**
- Produces: `0007` DROP/CREATEs all three bypass-carrying policies (`events`, `unique_claims`, `anchors`) with the arm `(current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END)`; its header carries the errata for 0005's "Both policies that carry the arm" comment. `0008` drops `checkpoints` (dead — only SQLite implements `delivery.Checkpoints`).

- [ ] **Step 1 (RED):** `TestAllBypassPoliciesRoleGated`: read `pg_policies` for `events`, `unique_claims`, `anchors`; assert each `qual` contains both `app.bypass_rls` and `pg_has_role` **and** `EXISTS`. Expect FAIL for `anchors` today.
- [ ] **Step 2:** Write both migrations; bump `migrations_test.go` to 8; run `task test:pg` — PASS.
- [ ] **Step 3:** Add `TestCheckpointsTableDropped` (query `to_regclass('checkpoints')` → NULL).
- [ ] **Step 4:** Commit: `fix(postgres): role-gate every bypass policy; drop the unused checkpoints table`.

---

## Phase 1 — Feed protocol truthfulness

### Task 10: `MatchingRawHead` + stall fix + conformance coverage (F14, F17)

**Files:**
- Modify: `feed/ports.go` (`Log` interface, doc), `feed/server.go` (`probeStall`), `postgres/wsstore.go`, `sqlite/feed.go`, `feedtest/conformance.go`
- Test: `feed/server_test.go`, `feedtest/conformance.go` cases, `postgres/feed_stores_test.go`, `sqlite/feed_test.go`

**Interfaces:**
- Produces: `Log.MatchingRawHead(ctx context.Context, types []string) (uint64, error)` — Postgres: unfenced filtered `MAX(global_position)` in its own RLS-pinned tx (the `MatchingHead` query without the fence round); SQLite: same body as `MatchingHead`. `probeStall` compares against `MatchingRawHead(req.Definition.Consumes)` instead of `RawHead` (report `OnStall(pos, matchingRaw)`). Conformance suite gains portable cases: `RawHead >= Head` after settled appends; both equal the max; `MatchingHead`/`MatchingRawHead` filtered correctly; empty types → 0; `MatchingRawHead >= MatchingHead`.

- [ ] **Step 1 (RED):** `TestObserverStallRequiresMatchingBacklog` in `feed/server_test.go`: caught-up filtered subscription + later non-consumed event → assert **zero** `OnStall` (the review's live repro as a unit test). Expect FAIL.
- [ ] **Step 2:** Port + adapters + `probeStall` change; the existing `TestObserverReportsStall` keeps passing (its events all match). Run `go test ./feed -count=1` — PASS.
- [ ] **Step 3:** Conformance cases; run `task test:pg` and `go test ./sqlite` — both adapters PASS the new cases.
- [ ] **Step 4:** Breaking-change entry (`Log` interface growth); commit: `fix(feed): stall detection compares against the matching raw head`.

### Task 11: Clamp-loop observability + `Consumes` control bytes (F15, F35-partial)

**Files:**
- Modify: `feed/server.go` (`stream`'s clamp loop, `validateDefinition`)
- Test: `feed/server_test.go`

**Interfaces:**
- Consumes: Task 10's `MatchingRawHead`.
- Produces: the clamp loop calls a rate-limited probe (reuse `stallProbeEvery` cadence) reporting `OnStall(pos, matchingRaw)` after N consecutive re-heads with `head < pos && matchingRaw >= pos`; `validateDefinition` rejects control bytes in any `Consumes` entry (`containsControl`), error wrapping `ErrInvalidRequest`.

- [ ] **Step 1 (RED):** `TestClampLoopReportsStall`: `fenceBlockedLog`-style fake (head 0, matchingRaw = pos), subscribe with a checkpoint above head → assert one `OnStall` within the window; and `TestValidateDefinitionRejectsControlBytes` (`Consumes: ["a\x00b"]` → `ErrInvalidRequest`). Expect both FAIL.
- [ ] **Step 2:** Implement; `go test ./feed -count=1` — PASS.
- [ ] **Step 3:** Commit: `fix(feed): observe blocked clamps; reject control bytes in consumed types`.

### Task 12: Monotonic `Skip` (F16)

**Files:**
- Modify: `feed/server.go` (`Skip`), `feed/ports.go` (`SkipRequest` doc)
- Test: `feed/server_test.go`

**Interfaces:**
- Produces: `Skip` loads the stored checkpoint; if `sr.Position < cp.Position && !sr.Force` → return `fmt.Errorf("%w: skip position %d is behind the checkpoint %d; pass Force to rewind", ErrInvalidRequest, sr.Position, cp.Position)` **before** recording the dead letter. `Force` keeps today's rewind semantics, now documented.

- [ ] **Step 1 (RED):** `TestSkipRefusesRewind` (checkpoint 900, `Skip(5)` → error; stored position unchanged; **no** dead letter recorded) and `TestSkipForceRewinds` (Force → 5). Expect the first FAIL.
- [ ] **Step 2:** Implement + doc; `go test ./feed -count=1` — PASS.
- [ ] **Step 3:** Commit: `fix(feed): Skip refuses to rewind the checkpoint without Force`.

### Task 13: Subscribe lost-drain restructure + wake-aware `Waiter` (F35)

**Files:**
- Modify: `feed/server.go` (`Subscribe` tail, `idleWait`), `feed/ports.go` (`Waiter`), `feed/server_test.go` (fakes)
- Test: `feed/server_test.go`

**Interfaces:**
- Produces: `Waiter.Wait(ctx context.Context, timeout time.Duration) (woke bool, err error)` — breaking (migration-notes entry); `idleWait` applies the floor only when `!woke`. `Subscribe` tail: run `cancel(); renewals.Wait()` **before** assembling causes, then drain `lost` non-blockingly (twice: after wait, a second receive after the join) so a renewal failure observed before the stream ended is never dropped.

- [ ] **Step 1 (RED):** `TestSubscribeKeepsLeaseLossRacingStreamError` (reverse of the existing join test: slow `OnLeaseLost` + concurrent send failure; assert the returned error joins `ErrLeaseLost`). Expect FAIL.
- [ ] **Step 2 (RED):** `TestWaiterWakeBypassesFloor`: a `Waiter` returning `(true, nil)` after 10 ms with `Poll` 250 ms → next page read happens within 100 ms. Expect FAIL.
- [ ] **Step 3:** Implement both; update `blockingWaiter` and every `Waiter` fake; `go test ./feed -count=1` — PASS.
- [ ] **Step 4:** Commit: `fix(feed): keep lease-loss diagnoses; let Waiter wakeups beat the poll floor`.

### Task 14: Terminal errors reach the surface (F3)

**Files:**
- Modify: `projection/reconnecting.go`, `feed/ports.go` (`Observer`, `NoopObserver`), `eventfeed/eventfeed.go`
- Test: `eventfeed/eventfeed_test.go`, `projection/reconnecting_test.go`, `feed/server_test.go` (observer fake)

**Interfaces:**
- Produces: `ReconnectingFeed.IsFatal func(error) bool` (nil = never fatal); when true, `Feed` returns the error instead of reconnecting. `Observer.OnSubscribeError(scope, projection, instance string, err error)` — called by the server's observer plumbing? No: by `eventfeed`'s attempt loop on every Subscribe error while the caller ctx is live. `eventfeed` sets `IsFatal: func(err error) bool { var dce *feed.DefinitionChangedError; return errors.As(err, &dce) }` and calls `OnSubscribeError` for every attempt-ending error (fatal or not) before backoff.

- [ ] **Step 1 (RED):** `TestEventfeedStopsOnDefinitionChanged`: real `feed.Server` + real `eventfeed` + a checkpoint whose definition hash differs — assert `Feed()` returns within a bounded window with an error matching `feed.ErrDefinitionChanged`, that `OnSubscribeError` was called, and that no lease is held afterwards. Expect FAIL (today it spins forever — bound the test's ctx so the RED failure is a timeout-with-zero-calls, not a hang).
- [ ] **Step 2:** Implement the three changes; also `TestReconnectingFeedFatalEscape` (fake inner feed returning a marked-fatal error → returned, not retried).
- [ ] **Step 3:** `go test ./eventfeed ./projection ./feed -count=1` — PASS. Breaking-change entry (`Observer` growth).
- [ ] **Step 4:** Commit: `fix(eventfeed): surface terminal Subscribe errors instead of retrying forever`.

---

## Phase 2 — Postgres storage and operations

### Task 15: `checkRelayRole` retries transient failures (F4)

**Files:**
- Modify: `postgres/drain.go`, `postgres/store.go` (fields)
- Test: `postgres/drain_test.go`

**Interfaces:**
- Produces: `Store` swaps `relayRoleOnce sync.Once`/`relayRoleErr` for `relayRoleMu sync.Mutex`, `relayRoleChecked bool`, `relayRoleMember bool`. `checkRelayRole`: under the mutex, if `relayRoleChecked` return the cached verdict; probe on `context.WithoutCancel(ctx)` with a 5 s timeout; on probe error return it **without** setting `checked`; on definitive `member=false` cache `ErrRelayRoleMissing`; on `member=true` cache success.

- [ ] **Step 1 (RED):** `TestRelayRoleTransientErrorIsNotLatched` (PG-gated): first `Drain` with a cancelled context → error; second `Drain` with a live context → succeeds (claims 0). Expect the second FAIL today.
- [ ] **Step 2:** Implement; `task test:pg` — PASS (existing member/non-member tests unchanged).
- [ ] **Step 3:** Commit: `fix(postgres): retry the es_relay probe instead of latching transient errors`.

### Task 16: `pg_sequences` bound + `CACHE 1` assertion + idle short-circuit (F5-partial, F9, F27, F38-partial)

**Files:**
- Modify: `postgres/wsstore.go` (`readLastPosition` → `readSequenceState`, `fenceBound`, `Head`), `postgres/store.go` (Open)
- Test: `postgres/wsstore_test.go` (new internal test file), `postgres/feed_test.go`

**Interfaces:**
- Produces: `readSequenceState(ctx) (last uint64, cache int, err error)` = `SELECT COALESCE(last_value, 0), cache_size FROM pg_sequences WHERE sequencename = 'events_global_position_seq'`; `fenceBound` errors with a new typed error `errSequenceCache` (exported as `ErrSequenceCacheMisconfigured`, message naming `ALTER SEQUENCE ... CACHE 1`) when `cache != 1`. Idle short-circuit: before drawing a fence (no stored fence), query the workspace's committed max (`RawHead`-shaped, no xid); if it is `<= from`, return 0 without drawing; `Head` on a workspace with zero events returns 0 without drawing. Instrumentation: unexported `draws uint64` on `Workspace` (under the existing mutex), incremented in `drawFence`, asserted by the internal test.
- Runbook note: the fence now needs only `USAGE` on the sequence (the view masks `last_value` to NULL without it) — fold the grant text into Task 19's block.

- [ ] **Step 1 (RED):** internal tests in `package postgres`: `TestIdleWorkspaceBurnsNoFence` (workspace A idle at pos N; append to workspace B; N polls of A's `ReadAllBounded` → `draws` unchanged; a fresh append to A → `draws` advances) and `TestHeadEmptyWorkspaceBurnsNoFence`; PG-gated `TestSequenceCacheAsserted`: `ALTER SEQUENCE ... CACHE 2` → `ReadAllBounded` returns `ErrSequenceCacheMisconfigured`; restore `CACHE 1`. Expect FAIL (draws today; no cache check).
- [ ] **Step 2:** Implement; `task test:pg` — PASS. Add the doc line to `readLastPosition`'s successor explaining why the view replaces the direct sequence read (privileges + cache in one statement).
- [ ] **Step 3:** Commit: `fix(postgres): read the fence bound through pg_sequences, assert CACHE 1, short-circuit idle workspaces`.

### Task 17: Duplicate-`EventID` classification + strict audit-UUID scanning (F25, F34-partial)

**Files:**
- Modify: `es/errors.go`, `postgres/wsstore.go` (`append` insert error path, `scanEnvelope`), `sqlite/store.go` (same)
- Test: `postgres/postgres_test.go`, `sqlite/append_test.go`

**Interfaces:**
- Produces: `es.ErrDuplicateEventID` sentinel ("event id already stored"). Postgres insert error: if `pgErr.ConstraintName == "events_workspace_id_event_id_key"` (or `"events_event_id_key"`) → `fmt.Errorf("%w: %s at v%d: %v", es.ErrDuplicateEventID, p.StreamID, version, err)`; `events_pkey` → today's `ErrConflict`; anything else → wrapped original. SQLite: distinguish by the modernc message (`events.stream_id, events.version` → conflict; `events.event_id` → duplicate) falling back to conflict. Both `scanEnvelope`s: `event_id`, `correlation_id`, `causation_id`, `command_id` parse failures now fail the read with the offending column and value-length, never the value's bytes.
- `cmdsvc.MapError` learns `ErrDuplicateEventID` in Task 33.

- [ ] **Step 1 (RED):** `TestDuplicateEventIDIsNotConflict` in both suites: append, then append the same `EventID` on another stream → assert `errors.Is(err, es.ErrDuplicateEventID) && !errors.Is(err, es.ErrConflict)`; the existing conflict test still passes. Also `TestScanRejectsCorruptEventID` (seed a row with a non-UUID `event_id` via raw SQL → read fails). Expect FAILs.
- [ ] **Step 2:** Implement; run both suites + `task test:pg` — PASS.
- [ ] **Step 3:** Commit: `fix(store): classify duplicate event ids and reject corrupt audit uuids`.

### Task 18: Xact-scoped migration lock + DB-clock consistency (F10, F33-partial)

**Files:**
- Modify: `postgres/store.go` (`Migrate`), `postgres/drain.go` (`published_at`), `postgres/anchor.go` (`created_at`), `postgres/wsstore.go` (comment)
- Test: `postgres/migrations_test.go`, `postgres/drain_test.go`

**Interfaces:**
- Produces: `Migrate` runs one transaction per pending migration: `BEGIN; SELECT pg_advisory_xact_lock($1); <re-check version>; <apply>; <record>; COMMIT` (auto-released — docs: "automatically released at the end of the current transaction"). The `schema_migrations` bootstrap table runs inside its own xact-locked tx. `Drain` marks `published_at = clock_timestamp()`; `Anchor` writes `created_at` as `clock_timestamp()` in SQL (app clock removed from both). The `wsstore.go` "same transaction and therefore the same snapshot" comment is corrected: safety follows from position immutability under READ COMMITTED statement snapshots.

- [ ] **Step 1 (RED):** `TestMigrateCancelDoesNotLeakAdvisoryLock`: acquire the lock path by starting `Migrate` and cancelling mid-run (a migration that sleeps is overkill — instead cancel immediately after the lock statement takes effect by racing a `pg_sleep`-free approach: call `Migrate` with a ctx cancelled after 0 delay against a fresh DB with pending migrations; then assert `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid = $1` is 0 from another connection; then run `Migrate` again with a live ctx → succeeds). Expect FAIL today (lock latched on a pooled conn).
- [ ] **Step 2:** Implement all four changes; `task test:pg` — PASS (existing migration idempotency tests still green).
- [ ] **Step 3:** Commit: `fix(postgres): xact-scoped migration lock; db clocks for published_at and anchor created_at`.

### Task 19: Grants runbook + least-privilege e2e + DBTX guard + PG14 floor (F5, F12, F33-partial)

**Files:**
- Modify: `docs/deploy/README.md` (new "Grants" section), `postgres/wsstore.go` (`AppendTx`/`ReadStreamTx` entry guard), `postgres/store.go` (`Open` version probe), `README.md` (PostgreSQL 14+)
- Test: `postgres/feedgate_test.go` or new `postgres/leastpriv_test.go`

**Interfaces:**
- Produces: runbook grant block per principal — appender: `GRANT USAGE ON SEQUENCE events_global_position_seq; GRANT SELECT, INSERT ON events, unique_claims …` (plus `DELETE ON unique_claims` for Release; and `log_order` insert from Task 7); feed process: `GRANT USAGE ON SEQUENCE …; SELECT/INSERT/UPDATE/DELETE ON feed_checkpoints, feed_leases, feed_dead_letters; SELECT ON events`; relay: `GRANT SELECT, UPDATE (published_at) ON events TO <relay>; GRANT es_relay TO <relay>; GRANT SELECT ON unique_claims, log_order TO <relay>`; anchor job: `GRANT SELECT ON events, log_order, schema_migrations; INSERT ON anchors; GRANT es_relay`. No role gets `DELETE`/`TRUNCATE` on `events`. `AppendTx`/`ReadStreamTx` reject `*pgxpool.Pool` with `AppendTx requires an active transaction, not a pool`. `Open` probes `SELECT pg_current_xact_id()` once (fails fast pre-PG14 with a clear message); README/deploy doc "PostgreSQL 14+".
- New test `TestLeastPrivilegeEndToEnd` (PG-gated): create a restricted login role with exactly the runbook grants, run append → `ReadAllBounded` (fence) → feed subscribe/ack → `Drain` → `Anchor`/`CheckAnchor` under it.

- [ ] **Step 1:** Write the runbook section + `TestLeastPrivilegeEndToEnd` — expect FAIL (sequence `SELECT` denial reproduces the finding).
- [ ] **Step 2:** Land the guard + probe + docs; the test passes against the documented grants (Task 16's `pg_sequences` switch is what makes the feed work with `USAGE` only — verify, don't re-fix).
- [ ] **Step 3:** Also update `postgres/drain_test.go`'s blanket `GRANT SELECT, UPDATE ON events` to the column-level form so the test suite itself models least privilege.
- [ ] **Step 4:** Commit: `fix(postgres): document the privilege contract and enforce it in tests`.

---

## Phase 3 — SQLite storage

### Task 20: Feed type index (F18)

**Files:**
- Create: `sqlite/migrations/0005_feed_type_index.sql`
- Test: `sqlite/migrate_test.go`, `sqlite/feed_test.go`

**Interfaces:**
- Produces: `CREATE INDEX IF NOT EXISTS events_type_pos_idx ON events (type_url, global_position);`; `migrate_test.go` version → 5.

- [ ] **Step 1 (RED):** `TestMatchingHeadUsesTypeIndex`: seed 2k events of a rare type plus 2k of another; `EXPLAIN QUERY PLAN` the `MatchingHead` query before the migration applies (construct a raw handle at schema 0004) → assert the plan names `events_type_pos_idx` — expect FAIL (today: `SEARCH events`).
- [ ] **Step 2:** Migration + version bump; `go test ./sqlite -count=1` — PASS (conformance + planner).
- [ ] **Step 3:** Commit: `fix(sqlite): add the feed's type_url index`.

### Task 21: `NewWithDB` contract + `Close` ownership (F19)

**Files:**
- Modify: `sqlite/store.go` (`Open`, `NewWithDB`, `Close`, package doc), `docs/migration/2026-11-07-breaking-changes.md`
- Test: `sqlite/store_test.go` (new cases)

**Interfaces:**
- Produces: `Store` gains `ownsDB bool`; `NewWithDB` sets `db.SetMaxOpenConns(1)` and leaves `ownsDB = false`; `Close` closes only when `ownsDB`; `Open` sets `ownsDB = true`. Package doc: the single Go connection is what buys `busy_timeout`/`BUSY_SNAPSHOT` immunity and memory-DB lifetime; **file-level single-writer** is what keeps positions gap-free; the driver must be `modernc.org/sqlite` (`isConstraintViolation` matches its error type only). Breaking-change entries for both behaviors.

- [ ] **Step 1 (RED):** `TestNewWithDBEnforcesSingleConnection` (pass a `SetMaxOpenConns(4)` handle → `Stats().MaxOpenConnections == 1` after `NewWithDB`) and `TestCloseDoesNotCloseBorrowedDB` (`NewWithDB` → `Close()` → `db.Ping()` still works). Expect both FAIL.
- [ ] **Step 2:** Implement + docs; `go test ./sqlite -count=1` — PASS.
- [ ] **Step 3:** Commit: `fix(sqlite): NewWithDB enforces the single-connection contract; Close respects ownership`.

### Task 22: SQLite migration lock via INSERT-first (F34-partial)

**Files:**
- Modify: `sqlite/store.go` (`Migrate` + its comment)
- Test: `sqlite/migrate_test.go`

**Interfaces:**
- Produces: per pending migration one transaction: `INSERT INTO schema_migrations (version) VALUES (?)` **first** — on PK violation, rollback and skip (already applied; a rival won the write lock, which the INSERT itself takes, serializing migrators); then `ExecContext(m.SQL)`; commit. The pre-check SELECT is removed. Comment states the argument (single write transaction ⇒ INSERT serializes; DDL is transactional).

- [ ] **Step 1 (RED):** `TestConcurrentMigrateSameFile`: two `Open`s on one file DSN racing `Migrate` in goroutines → both return nil, version table has each version exactly once, no `duplicate column name`. Expect flaky-FAIL today.
- [ ] **Step 2:** Implement; run the race test `-count=10` — PASS.
- [ ] **Step 3:** Commit: `fix(sqlite): serialize migrations on the schema_migrations insert`.

---

## Phase 4 — Projections, reactions, delivery liveness

### Task 23: `Runner`/`LogFeed` direct tests + retry signal (F7)

**Files:**
- Create: `projection/runner_test.go`
- Modify: `projection/runner.go` (`LogFeed`)
- Test: `projection/runner_test.go`

**Interfaces:**
- Produces: `LogFeed` gains `OnRetry func(ctx context.Context, e es.Envelope, err error, attempt int)` (nil = silent) and `MaxBackoff time.Duration` (default 5 s); retry delay = `Poll * 2^(attempt-1)` capped at `MaxBackoff`; attempts remain unbounded (at-least-once order preserved). Runner tests pin: marker skip (`<=` boundary), filtered-advance, `Filter == nil` dispatch, `Run` returning `ctx.Err()` on cancellation, `Position` loading.

- [ ] **Step 1:** `projection/runner_test.go`: the five Runner tests above (these pass immediately — they pin current behavior before the LogFeed change).
- [ ] **Step 2 (RED):** `TestLogFeedReportsRetries`: handler fails twice then succeeds → `OnRetry` called with attempts 1,2; delays observed ≤ caps. Expect FAIL (no hook).
- [ ] **Step 3:** Implement; `go test ./projection -count=1` — PASS.
- [ ] **Step 4:** Commit: `feat(projection): LogFeed retry signal with capped backoff; direct runner tests`.

### Task 24: `BufferedMarker` time-bounded flush (F23)

**Files:**
- Modify: `projection/buffered_marker.go`, `projection/runner.go` (`Run` exit flush)
- Test: `projection/buffered_marker_test.go`

**Interfaces:**
- Produces: `NewBufferedMarker` starts a flush timer (armed on first pending Save, re-armed per flush; `time.AfterFunc(MaxInterval)`, firing `Flush(context.Background())`); new `Close() error` stops the timer and flushes; new `Discard(projection string)` drops pending state for one projection (the reset-then-rebuild path). `Runner.Run`, on return, flushes when the marker implements `interface{ Flush(context.Context) error }` using `context.WithoutCancel(ctx)` + 5 s timeout. The type doc's "bounded by MaxInterval" claim becomes true; the resurrect-over-reset caveat is documented next to `Discard`.

- [ ] **Step 1 (RED):** `TestBufferedMarkerFlushesWhenIdle` (Save once, wait `MaxInterval + slack` with no further Saves → inner marker advanced; RED: pending forever today) and `TestRunFlushesOnReturn` + `TestDiscardPreventsResurrect`.
- [ ] **Step 2:** Implement; `go test ./projection -race -count=1` — PASS (the timer path is race-tested).
- [ ] **Step 3:** Commit: `fix(projection): bound BufferedMarker's flush window; flush on Run exit; Discard for resets`.

### Task 25: Snapshot lineage guard + the snapshot test suite (F24)

**Files:**
- Modify: `aggregate/runtime.go` (`load`), `snapshot/snapshot.go` (doc)
- Test: `aggregate/snapshot_test.go` (new; includes the repo's first `snapshot.Cache` implementation, test-only)

**Interfaces:**
- Produces: in `load`, when a snapshot seeded `from > 0`: read the envelope at `from` (`ReadStream(ctx, sid, from-1, from)`) — if it returns no rows, discard the seed (`state = Initial()`, `from = 0`) and re-read the full stream; if it returns rows, proceed (the snapshot version exists in this log). Test cache: `type testCache struct{...}` in `aggregate/snapshot_test.go`.

- [ ] **Step 1 (RED):** `TestLoadDiscardsSnapshotAheadOfLog` (cache at v5, log truncated to v2 → load returns the v2 state, version 2 — not the phantom). Expect FAIL. Also: `TestSnapshotsHappyPath` (seed→tail fold, write-through after Handle), `TestLoadStale`, `TestSnapshotFoldVersionMismatchIgnored`, `TestLoadAsOfVersionExcludesFutureSnapshot` (pins the existing `upToVersion==0` rule).
- [ ] **Step 2:** Implement the guard; `go test ./aggregate -count=1` — PASS.
- [ ] **Step 3:** Commit: `fix(aggregate): verify snapshot lineage against the log before folding from it`.

### Task 26: Generated upcaster seam (F20)

**Files:**
- Modify: `cmd/protoc-gen-es-lite/projection.go` (`writeDeps`, `writeDispatch`), `cmdsvc` untouched
- Test: `cmd/protoc-gen-es-lite/plugin_test.go`, `examples/projection/projection_test.go`
- Regenerate: `gen/counterview/v1/view_es.pb.go` (`task generate`)

**Interfaces:**
- Produces: for state-fetching specs, `Deps` gains `<Agg>Upcaster es.Upcaster[<Iface>]` (optional, comment: "upgrades stored event shapes on read; nil decodes as stored"); dispatch decodes, then `if <agg>Upcaster != nil { decoded, err = <agg>Upcaster.Upcast(env.TypeURL, env.SchemaVersion, decoded) … }`, then the existing variant assertion. Metadata-only specs are unchanged (their `FETCH_MODE_EVENT_ONLY` support is Task 34).

- [ ] **Step 1 (RED):** plugin test: generated projection output contains the `Upcast` call and the Deps field (assert on emitted source, matching `plugin_test.go`'s string-matching style). Expect FAIL.
- [ ] **Step 2:** Implement; regenerate; `TestProjectionAppliesUpcaster` in `examples/projection`: counter view over a v1 `Incremented` (the `examples/upcast` fixture shape) with an `upcast.Registry` wired through `CounterViewDeps.CounterUpcaster` → handler observes the upcast event while state (via the runtime's upcaster) agrees.
- [ ] **Step 3:** `go test ./cmd/protoc-gen-es-lite ./examples/... -count=1` + `task generate:check` — PASS.
- [ ] **Step 4:** Commit: `feat(codegen): optional per-aggregate upcaster in projection and reaction deps`.

### Task 27: Tail-bounded `LoadAsOfVersion` via snapshots (F21-partial)

**Files:**
- Modify: `aggregate/runtime.go` (`load`), generated Deps comments (regen), `README.md` (perf note)
- Test: `aggregate/snapshot_test.go`

**Interfaces:**
- Produces: `load` seeds from the snapshot whenever `snap.Version <= upToVersion` (not just `upToVersion == 0`), folding the `(snap.Version, upToVersion]` tail; the `asOf` path stays unseeded. README/Deps doc states the cost model: without snapshots, `FETCH_MODE_STATE_AS_OF` is O(stream length) per event; with them, O(tail).

- [ ] **Step 1 (RED):** `TestLoadAsOfVersionIsTailBoundedWithSnapshots`: counting store records `ReadStream` rows read; snapshot at v10 of a v12 stream; `LoadAsOfVersion(12)` reads 2 rows, not 12. Expect FAIL.
- [ ] **Step 2:** Implement + regenerate + doc; `go test ./aggregate -count=1` — PASS.
- [ ] **Step 3:** Commit: `perf(aggregate): seed version-bounded loads from snapshots`.

### Task 28: Generator validation + cleanup (F32)

**Files:**
- Modify: `cmd/protoc-gen-es-lite/main.go`, `service.go`, `projection.go`
- Test: `cmd/protoc-gen-es-lite/plugin_test.go`

**Interfaces:**
- Produces: `New<Name>Handler`/`New<Name>Runner` panic at construction naming each nil required dep (`Runtime`, `Identity`, `Authorize`, per-aggregate loaders/routers) — `Validate`/`Filter`/`Errors`/`Marker` keep their nil defaults. `resolveMethod` fails generation unless the `result` message has singular `stream_id` (string), `version` (uint64), `global_position` (uint64), and the state field is not a list. Cleanup: delete `commandVariants`, delete `_ = meta`, collapse `writeDispatch`'s duplicated `default:` branches, and replace package-global `specNames` with a `*nameRegistry` created in `run` and passed through `collectProjections`/`collectReactions`/`resolveConsumes` (tests stop resetting the global).

- [ ] **Step 1 (RED):** plugin tests: `TestHandlerPanicNamesMissingDeps`, `TestResultShapeValidatedAtGeneration` (a `result` message without `version` → generation error naming the field), `TestSpecNamesNotGlobal` (two `run` invocations in one process both succeed with the same spec name).
- [ ] **Step 2:** Implement; `go test ./cmd/protoc-gen-es-lite -count=1` — PASS; `task generate:check` — no drift (generated output for existing protos is unchanged).
- [ ] **Step 3:** Commit: `fix(codegen): validate required deps and result shape; remove dead code and global state`.

### Task 29: `delivery` overhaul + reaction/cmdsvc coverage (F22, coverage gaps)

**Files:**
- Modify: `delivery/poller.go`, `delivery/relay.go`
- Create: `delivery/poller_test.go`, `delivery/relay_errors_test.go`
- Test: those files plus `reaction/reaction_test.go` addition

**Interfaces:**
- Produces: shared `func runUntilCancelled(ctx context.Context, cfg loopConfig, drain func(context.Context) error) error` where `loopConfig{PollInterval time.Duration; Wake <-chan struct{}; MaxRetries int; RetryBackoff func(attempt int) time.Duration}`; `MaxRetries` semantics: `0` → default 3, negative → 0. Closed-`Wake` guard: on a wake fire, drain one buffered signal; if the receive shows the channel closed (`!ok`), nil the local copy so the select stops matching. `NewPoller`/`NewRelay` validate `src`/`cp`/`drainer` non-nil with named panics. `Poller.drain` caches the position locally after a successful `SaveCheckpoint` (reload only on the first iteration and after errors).

- [ ] **Step 1 (RED):** `delivery/poller_test.go`: `TestPollerRetriesTransientError` (store erroring once → Run continues), `TestPollerClosedWakeDoesNotSpin` (closed wake; assert drain call count stays bounded over 200 ms), `TestPollerValidatesDeps`. `delivery/relay_errors_test.go`: `TestRelayMaxRetriesThenReturns`. All FAIL today.
- [ ] **Step 2:** Implement; `go test ./delivery -count=1` — PASS; existing example test (`examples/counter` poller path) green.
- [ ] **Step 3:** `reaction/reaction_test.go`: `TestExecuteRedeliveryRecollectsIssues` (handler errors after collecting one issue → error propagates; a second Execute with the same envelope re-collects).
- [ ] **Step 4:** Commit: `fix(delivery): retry transient errors, guard closed wakes, validate deps`.

---

## Phase 5 — cmdsvc and idempotency

### Task 30: cmdsvc unit tests + `MetaFrom`/`MapError` polish (F31, F33-doc)

**Files:**
- Modify: `cmdsvc/cmdsvc.go` (`MetaFrom`, `MapError` doc), `docs/migration/2026-11-07-breaking-changes.md`
- Create: `cmdsvc/cmdsvc_test.go`
- Test: `cmdsvc/cmdsvc_test.go`

**Interfaces:**
- Produces: `MetaFrom` hashes a non-UUID `Correlation-Id` with the existing `idempotencyNamespace` (UUIDv5) instead of dropping it (documented). `MapError`'s doc table gains the `context.Canceled`/`DeadlineExceeded` rows it already implements. Tests: full `MapError` table (every sentinel → expected code, `ErrConstraintViolated` → `already_exists`, VersionConflict detail populated), `MetaFrom` edges (UUID correlation, string correlation → stable v5, Idempotency-Key UUID and string), `DefaultValidator` fail-closed (constructor error → every Validate call errors), `VersionConflictDetail` round-trip, `SetWriteResult` error shapes.

- [ ] **Step 1:** Write the tests; `MetaFrom`-string-correlation FAILs today (dropped).
- [ ] **Step 2:** Fix `MetaFrom` + doc table; `cd cmdsvc && go test ./...` — PASS.
- [ ] **Step 3:** Commit: `test(cmdsvc): full unit coverage; keep non-uuid correlation ids`.

### Task 31: Deterministic event IDs (F6-part, design D6)

**Files:**
- Modify: `aggregate/runtime.go` (+ `es` namespace const), `docs/migration/2026-11-07-breaking-changes.md`
- Test: `aggregate/runtime_test.go`

**Interfaces:**
- Produces: `Runtime.WithDeterministicEventIDs() *Runtime` — when set and `meta.CommandID != uuid.Nil`, `handleOnce` fills `EventData.EventID = uuid.NewSHA1(esLiteCommandNamespace, []byte(meta.CommandID.String() + ":" + strconv.Itoa(i)))`. Off by default. (`esLiteCommandNamespace` is a pinned const in `aggregate`, hex in a comment.)
- Consumes: Task 17's `es.ErrDuplicateEventID` (a retried committed command collides on it).

- [ ] **Step 1 (RED):** `TestDeterministicEventIDsCollideOnRetry`: sqlite store, same `es.Meta{CommandID: X}` handled twice → second returns `ErrDuplicateEventID` (with `WithDeterministicEventIDs`); without the option → second appends fine (control case).
- [ ] **Step 2:** Implement; `go test ./aggregate ./sqlite -count=1` — PASS.
- [ ] **Step 3:** Commit: `feat(aggregate): opt-in deterministic event ids derived from the command id`.

### Task 32: Dedup port, tables, adapters (F6-part, design D6)

**Files:**
- Create: `es/idempotency.go`, `postgres/migrations/0009_command_dedup.sql`, `sqlite/migrations/0007_command_dedup.sql`, `postgres/dedup.go`, `sqlite/dedup.go`
- Test: `postgres/dedup_test.go`, `sqlite/dedup_test.go`, both `migrations_test.go`

**Interfaces:**
- Produces (types live in `es` so the core adapters and the nested `cmdsvc` module share them — a refinement of design §D6's placement, noted here for the executor):
  ```go
  // es/idempotency.go
  type DedupKey struct{ StreamID, CommandID string }
  type DedupRecord struct{ Fingerprint string; Response []byte }
  type Deduplicator interface {
      Lookup(ctx context.Context, key DedupKey) (DedupRecord, bool, error)
      Save(ctx context.Context, key DedupKey, rec DedupRecord) error
  }
  ```
  Tables: `command_dedup(workspace_id, stream_id, command_id, fingerprint, response, created_at)` PK `(workspace_id, stream_id, command_id)` — postgres: RLS workspace arm only, no bypass, FORCE; sqlite: minus workspace. Adapters: `postgres.NewDedupStore(w *Workspace) *DedupStore` and `sqlite.NewDedupStore(s *Store) *DedupStore` implementing `es.Deduplicator`; `Save` uses `INSERT … ON CONFLICT DO NOTHING` (postgres) / `INSERT OR IGNORE` (sqlite); a `Save` that hits the conflict re-`Lookup`s and returns the stored record via a new `Save` return value? No — keep `Save(ctx, key, rec) error`; on conflict the caller re-Lookups (documented).

- [ ] **Step 1:** migrations + version bumps (postgres → 9, sqlite → 7).
- [ ] **Step 2:** adapters + tests: `TestDedupRoundTrip`, `TestDedupWorkspaceIsolation` (postgres: two workspaces, same key, independent), `TestDedupSaveConflictIsNotError`.
- [ ] **Step 3:** `go test ./sqlite -count=1` + `task test:pg` — PASS. Commit: `feat(store): command dedup tables and adapters`.

### Task 33: Generated handler dedup flow (F6, design D6)

**Files:**
- Modify: `cmdsvc/cmdsvc.go` (helpers), `cmd/protoc-gen-es-lite/service.go` (emit flow), `cmdsvc/example/*` (regen), `docs/migration/2026-11-07-breaking-changes.md`
- Test: `cmdsvc/example/example_test.go`, `cmd/protoc-gen-es-lite/plugin_test.go`

**Interfaces:**
- Produces:
  - `cmdsvc`: `Fingerprint(msg proto.Message) string` (SHA-256 hex of `proto.Marshal`); `IdempotencyConcurrent() *connect.Error` (code `aborted`, message naming HTTP 409 semantics); `IdempotencyKeyReused() *connect.Error` (code `aborted`, message naming HTTP 422 semantics); `MapError` maps `es.ErrDuplicateEventID` → `IdempotencyConcurrent()`.
  - Generated `Deps` gains `Dedup es.Deduplicator` (comment: draft-ietf-httpapi-idempotency-key-header-07; nil = no dedup, today's behavior). Handler flow when non-nil, after validation/stream-id/command-presence but **before** identity/authorize (dedup precedes authorization? no — after `Authorize`, so an unauthorized replay leaks nothing): fingerprint; `Lookup` → hit + matching fingerprint → unmarshal stored response, return it; hit + mismatch → `IdempotencyKeyReused()`; miss → `HandleAt` → `ErrDuplicateEventID` → `IdempotencyConcurrent()`; success → `Dedup.Save` (failure → `Internal`, documented retry path) → respond.
- Composite key note (draft §5): the adapter's workspace scoping is the client-attribute component.

- [ ] **Step 1 (RED):** plugin test asserts the emitted flow (`Dedup != nil`, `Lookup`, `IdempotencyKeyReused`, `Save` present in generated source). Expect FAIL.
- [ ] **Step 2:** Implement helpers + emit + regen; `task generate:check` green.
- [ ] **Step 3 (RED→GREEN):** example tests with a sqlite-backed dedup store + `WithDeterministicEventIDs`: `TestIdempotencyReplay` (same key twice → identical `WriteResult`, one event in the log), `TestIdempotencyConcurrentConflict` (two goroutines, same key → one success, one `aborted`), `TestIdempotencyFingerprintMismatch` (same key, different payload → reuse error; original response intact on replay of the first payload).
- [ ] **Step 4:** Commit: `feat(cmdsvc): draft-compliant Idempotency-Key dedup in generated handlers`.

### Task 34: `FETCH_MODE_EVENT_ONLY` (F38, maintainer-elected capability)

**Files:**
- Modify: `proto/es/v1/options.proto`, `cmd/protoc-gen-es-lite/projection.go` (`collectProjections` accept, `writeDeps`, `writeDispatch`, `writeHandlerIface`), `cmd/protoc-gen-es-lite/docs.go` (`humanise`)
- Test: `cmd/protoc-gen-es-lite/plugin_test.go`
- Regenerate: `gen/es/v1/options.pb.go`, docs catalogue (`task generate`)

**Interfaces:**
- Produces: enum `FETCH_MODE_EVENT_ONLY = 3`; a spec with it validates (rebuild mode required like the others); handler methods are `On<Event>(ctx, env, ev *E)`; Deps carries `<Agg>Codec` and `<Agg>Upcaster` but no loader; dispatch decodes (and upcasts when set) without any state fetch. Reactions may use it too.

- [ ] **Step 1 (RED):** plugin test: a metadata+event spec generates the decode-only dispatch and a handler interface with the `ev` parameter; `humanise` renders "typed event only (no state)". Expect FAIL (enum rejected today).
- [ ] **Step 2:** Implement + regen; `go test ./cmd/protoc-gen-es-lite -count=1`, `task generate:check` — PASS.
- [ ] **Step 3:** Commit: `feat(codegen): FETCH_MODE_EVENT_ONLY — typed events without state loads`.

---

## Phase 6 — Hygiene, documentation, CI

### Task 35: `es` dead surface + decided-at enforcement (F30, F31, F34-doc)

**Files:**
- Modify: `es/errors.go`, `es/meta.go`, `es/decided_at.go`, `aggregate/runtime.go` (`HandleAt`), `cmdsvc/cmdsvc.go` (`MapError`), affected docs
- Test: `aggregate/decided_at_test.go`

**Interfaces:**
- Produces: remove `ErrDecidedAtMismatch` (and its `MapError` case), remove `Meta.OccurredAt`; new sentinel `es.ErrDecidedAtUnstampable` — `HandleAt` returns it (wrapping the command type) when the command implements `es.DecidedAt` but `StampDecisionTime` returned false; `stream_type`'s schema comment claim is corrected in `README` (Task 36 lands the prose; here only the code references change). Breaking-change entries for all removals.

- [ ] **Step 1 (RED):** `TestUnstampableDecidedAtFails` (hand-written command type implementing `DecisionTime` only, no proto, no setter → `ErrDecidedAtUnstampable`).
- [ ] **Step 2:** Implement + removals; `go build ./... && go test ./aggregate ./es ./cmdsvc/... -count=1` — PASS; `grep -rn ErrDecidedAtMismatch` empty.
- [ ] **Step 3:** Commit: `fix(es): remove dead compatibility surface; enforce decided_at stamping`.

### Task 36: Documentation truth pass (F37 + every doc claim touched above)

**Files:**
- Modify: `README.md`, `docs/adr/0029-tamper-evident-hash-chain.md` (errata: order attestation now via `log_order` + `OrderRoot`; head truncation detection), new `docs/adr/0038-log-order-attestation.md`, `docs/adr/0037-commit-safe-feed-reads.md` (idle/xid wording), `docs/adr/0001-event-sourced-lite-architecture.md` (cut-table errata), `docs/adr/README.md` (omit-list fix), `docs/deploy/README.md` (lazy role-check wording, CACHE 1 invariant, PG14 floor, TLS/DSN hygiene, shared-login trust note, grants cross-ref), `docs/migration/2026-11-07-breaking-changes.md` (final), `docs/review/20261107_principal_review.md` (retraction banner)
- Test: none (prose)

**Interfaces:**
- Produces: every claim walks against the code per the consolidated review §6; Review A's header gains: "> **Corrected 2026-11-07:** the SQLite anchor-race claim in §1 is wrong (see `docs/review/20261107_consolidated_review.md` §2.1); Postgres half stands." New ADR 0038 records D1's design (approaches rejected: global chain = serialization; anchor-time scan = O(N)), the `CACHE 1` invariant, the `OrderFromPosition` rule, and the `es-verify order` deep-check contract.

- [ ] **Step 1:** Write ADR 0038 and the breaking-changes doc aggregating every entry added by Tasks 2–35 (grep the migration-notes file for completeness).
- [ ] **Step 2:** Apply every README/ADR/runbook correction; add the retraction banner to Review A.
- [ ] **Step 3:** Self-check: re-walk the consolidated review §6 list line by line against the edited docs; each line is either fixed or explicitly deferred with a backlog entry.
- [ ] **Step 4:** Commit: `docs: truth pass for anchors, order attestation, grants and review errata`.

### Task 37: CI hardening + fuzz + coverage (F28)

**Files:**
- Modify: `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `Taskfile.yml` (test:pg coverprofile, `fuzz` task, COVERAGE_FLOOR)
- Create: `es/fuzz_test.go`, `projection/fuzz_test.go`
- Test: the fuzz seeds + the new CI wiring

**Interfaces:**
- Produces: all third-party actions pinned to commit SHAs (`actions/checkout`, `jdx/mise-action`, `softprops/action-gh-release`, `actions/upload-artifact`) — release job especially (it holds `contents: write`); `test:pg` runs with `-coverprofile=pg.cover` and the integration job uploads it; `COVERAGE_FLOOR` recomputed from the union (`go tool cover -func` over unit+pg profiles) and ratcheted — never lowered; `task fuzz` runs 30-second smoke fuzzes of `FuzzParseCanonical` (es), `FuzzDefinitionHashConsumes` (projection: control bytes, unicode, empty parts never collide two distinct sets), `FuzzCounterCodecDecode` (examples/counter: arbitrary bytes + unknown type URLs never panic, unknown → `ErrUnknownEventType`).

- [ ] **Step 1:** Fuzz targets + seeds; `go test -fuzz=FuzzParseCanonical -fuzztime=30s ./es` (and the other two) — no crashes.
- [ ] **Step 2:** CI wiring + coverage ratchet: measure the union floor, set `COVERAGE_FLOOR` to it (rounded down to one decimal), and assert `task coverage` + the integration job both pass.
- [ ] **Step 3:** Commit: `chore(ci): pin actions, add postgres coverage to the ratchet, smoke fuzz targets`.

---

## Final verification (after Task 37)

- [ ] `GOWORK=off go build ./... && go vet ./...` — clean
- [ ] `golangci-lint run` and `cd cmdsvc && golangci-lint run -c ../.golangci.yml` — 0 issues
- [ ] `go test ./... -count=1` — green without PG_DSN
- [ ] `task test:pg` — green (race, shuffle)
- [ ] `task generate:check` — no drift
- [ ] `task coverage` — floor met; `task vuln` — clean
- [ ] Reconcile the findings register: every F1–F38 line in `docs/review/20261107_consolidated_review.md` maps to a landed task (or an explicit doc-only resolution); record the mapping in the PR description.
