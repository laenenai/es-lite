# Postgres Production-Grade Feed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Postgres feed path production-grade — commit-safe reads with zero skipped events, lease-fenced acknowledgements, a bounded and audited poison path, index-served pages, fail-closed definition drift, in-repo Postgres feed adapters plus the `eventfeed` consumer adapter, and observable stalls.

**Architecture:** Replace the naive position cursor with a *settled ceiling* (each page bounds delivery by the sequence value recorded one round earlier, delivered only once the snapshot horizon has passed that round's xmax — design §5.1). Fence acks through a caller-supplied lease owner carried in `feed.Ack`. Record skipped poison events in a dead-letter table next to the immutable log. Ship the Postgres `CheckpointStore`/`LeaseStore`/`Logs` adapters and the `eventfeed` consumer adapter behind one shared `feedtest` conformance suite.

**Tech Stack:** Go 1.27.3, pgx v5, modernc.org/sqlite. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-11-04-postgres-production-grade-design.md` — the plan argues from the spec; executors read both. Section references like §5.4 point at the spec.

## Global Constraints

- No write-path change: appends take no new locks, commits, or extra round trips (spec §3 anti-goal).
- Payloads are opaque (ADR 0025): never logged, never copied; errors carry positions, type URLs and sizes only.
- Migrations are append-only and transactional (ADR 0010) — no `CREATE INDEX CONCURRENTLY` inside migrations; heavy indexes are built runbook-first. Numbering by landing order: **postgres 0003** (indexes), **0004** (feed tables), **0005** (RLS role policies); **sqlite 0004** (definition hash column + dead letters, lands with Task 11 and 12 together).
- New Postgres feed tables get RLS policies with **no bypass arm** (spec §5.6).
- Breaking changes to `feed` ports and `Server` methods are allowed (pre-1.0). Error style: `es:` prefix sentinels in `es/errors.go`, `feed:` prefix in `feed/errors.go`.
- Postgres tests require `PG_DSN` — run via `task test:pg` (Docker `postgres:17`). Plain `GOWORK=off go test ./...` must stay green without it (PG tests skip).
- Conventional commits matching history (`feat(postgres): ...`, `test(feed): ...`).
- The settled-ceiling losslessness argument (spec §5.1.3) is reproduced as a comment on the implementation.
- `Workspace` instances are per-stream (`feed.Logs.Open` runs once per stream); ceiling state on the instance is correct without sharing.

## Review Focus

The failure modes the spec's §8 says must not be reintroduced, and the test that pins each:

1. Delivering a position while a lower position is still in flight — including the xid/position straddle that killed the first mechanism — `TestReadAllBoundedWaitsForStraddledCommit` (Task 2).
2. A stale ack that moves the cursor — `TestCheckpointRejectsStaleOwner` asserts the stored position is **unchanged**, not merely that an error returns (Task 10).
3. A two-step page that silently skips an over-cap row — `TestOversizedFirstRowIsNotMaterialized` (Task 6).
4. `Skip` without force on a held lease, or a skip that clears the definition identity — `TestSkipRefusesHeldLeaseWithoutForce`, `TestSkipPreservesDefinitionHash` (Task 12).
5. New RLS tables missing the policy or carrying a bypass arm — `TestFeedStoresRefuseCrossWorkspaceAccess` (Task 13).
6. A relay silently reading nothing after the RLS migration — `TestDrainFailsFastWithoutRelayRole` (Task 15).
7. A lockstep subscriber fleet exhausting the pool — `TestFeedGateHundredIdleSubscribers` (Task 7) plus jitter spread assertions (Task 18).

---

## Phase 0 — Spike

### Task 1: Spike the settled-ceiling assumptions (throwaway)

**Files:**
- Create: `postgres/spike_ceiling_test.go` — deleted before the task ends.

**Interfaces:**
- Produces: a go/no-go finding recorded in the PR. If any assumption fails, STOP — escalate the fallback decision (spec §5.1.5), do not start Task 2.

- [ ] **Step 1: Write the spike test** (PG_DSN-gated, `t.Skip` without it), subtests:
  (a) *Per-statement snapshots:* on connection A in a READ COMMITTED tx, `SELECT pg_snapshot_xmin(pg_current_snapshot()), pg_snapshot_xmax(pg_current_snapshot())`; with a second connection holding an open insert tx, A sees `xmin < xmax`; after it commits, A sees `xmin == xmax` on a fresh statement.
  (b) *Sequence read:* `SELECT last_value FROM events_global_position_seq` ≥ every `global_position` returned by `AppendTx` in the same test.
  (c) *Straddle proof-of-concept:* tx1 opens and appends (gp=1); tx2 opens and appends (gp=2); tx1 appends again (gp=3); commit tx1 only; from a third connection observe `MAX(global_position)=3` with a live gap at 2; commit tx2. Record observed xmin/xmax values around each step.
- [ ] **Step 2: Run** `task test:pg` — expected: all spike subtests PASS.
- [ ] **Step 3: Record findings in the PR description** (observed snapshot semantics, surprises, measured idle fast-path). If falsified: STOP and escalate.
- [ ] **Step 4: Delete the spike file** (`rm postgres/spike_ceiling_test.go`). No commit — throwaway by design; the straddle PoC returns as a red test in Task 2.

---

## Phase 1 — Gate 0

### Task 2: Red ordering tests + the settled ceiling in `ReadAllBounded`

**Files:**
- Modify: `postgres/wsstore.go` (Workspace state, `ReadAllBounded`)
- Test: `postgres/feed_test.go` (new, PG_DSN-gated); `sqlite/feed_test.go` (one new test)

**Interfaces:**
- Consumes: `Workspace.AppendTx(ctx, db DBTX, p es.AppendParams)` (existing, `wsstore.go:83`), `Store.Pool()`.
- Produces: `ReadAllBounded` with the same signature, now ceiling-bounded; unexported `roundMeta{xmin, xmax, lastPos uint64}` and `readRoundMeta` on `Workspace`; used by Task 3 (Head/RawHead) and Task 16.

- [ ] **Step 1: Write the failing test** `TestReadAllBoundedWaitsForOutOfOrderCommit`:
  tx1 Begin + AppendTx batch A (gp=1); tx2 Begin + AppendTx batch B (gp=2); commit tx2 **first**. `ws.ReadAllBounded(ctx, 0, 100, 0, allTypes)` must return an empty page (ceiling establishment — never `[2]` alone). Commit tx1; poll until non-empty; assert the page is `[1, 2]` in order.
- [ ] **Step 2: Write the failing test** `TestReadAllBoundedWaitsForStraddledCommit` (Review Focus 1):
  tx1 appends gp=1; tx2 appends gp=2; tx1 appends gp=3 (same tx as its first append — positions 1 and 3 belong to tx1, 2 to tx2); commit tx1 only. `ReadAllBounded(ctx, 0, ...)` must NOT deliver `[1, 3]` — it returns empty. Commit tx2; poll; assert `[1, 2, 3]`.
- [ ] **Step 3: Write the failing test** `TestReadAllBoundedPassesAbortedHole`:
  tx1 gp=1 committed; tx2 gp=2 rolled back; tx3 gp=3 committed. `ReadAllBounded(ctx, 0, ...)` returns `[1, 3]`; a read from 3 returns empty (no stall).
- [ ] **Step 4: Run** `task test:pg` — expected: the three tests **FAIL** against current code (red proof of the P0; `[2]`-alone, `[1,3]`, and the hole stall are the failure signatures).
- [ ] **Step 5: Implement the ceiling** in `postgres/wsstore.go`:
  - Add state to `Workspace`: `mu sync.Mutex; ceiling uint64; ceilingXmax uint64` (0 = none), with the §5.1.3 losslessness argument as the doc comment.
  - `readRoundMeta(ctx, db DBTX) (roundMeta, error)` — one statement: `SELECT pg_snapshot_xmin(pg_current_snapshot()), pg_snapshot_xmax(pg_current_snapshot()), last_value FROM events_global_position_seq`.
  - In `ReadAllBounded`'s existing tx, after `setWorkspace`: read meta; under `mu` compute the bound —
    fast path `meta.xmin >= meta.xmax` → bound = `meta.lastPos`; store `(ceiling=meta.lastPos, ceilingXmax=meta.xmax)`;
    slow path `ceiling > 0 && meta.xmin >= ceilingXmax` → bound = `ceiling`; then roll the stored ceiling to `(meta.lastPos, meta.xmax)`;
    otherwise → store `(meta.lastPos, meta.xmax)` and return `nil, false, nil` (empty page; the feed's existing wait cycle retries).
  - Add `AND global_position <= $5` (the bound) to the page query; when `bound <= from`, return an empty page without querying rows.
  - Budget/type/truncated logic unchanged.
- [ ] **Step 6: Run** `task test:pg` — expected: the three tests PASS, full PG suite green.
- [ ] **Step 7: Add the SQLite regression guard** (spec §5.10.1.4): `TestConcurrentAppendsKeepPositionsGapFree` in `sqlite/feed_test.go` — 8 goroutines × 50 single-event appends to distinct streams; assert positions are exactly `1..400` (contiguous) and an interleaved `ReadAll` reader never observes gp N+1 before gp N. (Guards the `SetMaxOpenConns(1)` invariant the whole SQLite safety argument rests on.)
- [ ] **Step 8: Run** `GOWORK=off go test -race ./...` — expected: green (PG skipped).
- [ ] **Step 9: Commit** — `feat(postgres): settle feed pages behind the in-flight horizon`

### Task 3: Settled `Head` + `RawHead`

**Files:**
- Modify: `postgres/wsstore.go`
- Test: `postgres/wsstore_test.go` (extend; the existing head test keeps passing once idle)

**Interfaces:**
- Produces: `Workspace.RawHead(ctx) (uint64, error)` (unbounded — for stall detection only); `Workspace.Head(ctx)` now ceiling-bounded. Task 16's `MatchingHead` reuses the same bound logic.

- [ ] **Step 1: Write the failing test** `TestHeadIsSettledWhileAppendInFlight`: tx1 appends gp=1 and commits; tx2 appends gp=2 uncommitted → `RawHead()==2`, and `Head()` returns ≤ 1 on some poll while tx2 is in flight (eventually-consistent; document the transient in the method doc). Commit tx2 → `Head()==2`.
- [ ] **Step 2: Implement**: move the current `Head` body to `RawHead`; new `Head` = the ceiling logic from Task 2 + `SELECT COALESCE(MAX(global_position), 0) ... AND global_position <= $2`.
- [ ] **Step 3: Run** `task test:pg` — green.
- [ ] **Step 4: Commit** — `feat(postgres): settled Head plus RawHead for stall detection`

### Task 4: Composite indexes + build-order net

**Files:**
- Create: `postgres/migrations/0003_feed_indexes.sql`
- Modify: `docs/deploy/README.md`
- Test: `postgres/migrations_test.go` or `postgres/store_test.go` (PG_DSN-gated)

**Interfaces:**
- Produces: `events_ws_pos_idx (workspace_id, global_position)`, `events_ws_type_pos_idx (workspace_id, type_url, global_position)`. EXPLAIN assertions live in Task 7's gate.

- [ ] **Step 1: Write the failing test** `TestFeedIndexesExist`: after `Migrate`, assert via `pg_indexes` that ≥8 per-partition indexes exist matching each column set (parent `CREATE INDEX` creates one child index per partition with suffixed names).
- [ ] **Step 2: Migration 0003**: two `CREATE INDEX IF NOT EXISTS` statements; header comment states the runbook-first `CONCURRENTLY` build order for ≥10M-row databases (spec §5.2).
- [ ] **Step 3: Runbook**: in `docs/deploy/README.md`, add the pre-migration manual build steps for large databases and the "migration is a no-op afterwards" guarantee.
- [ ] **Step 4: Run** `task test:pg` — green (migration idempotency included).
- [ ] **Step 5: Commit** — `feat(postgres): workspace+position indexes for feed pages`

### Task 5: Append-time payload cap

**Files:**
- Modify: `es/errors.go`, `postgres/store.go` + `postgres/wsstore.go` (append path), `sqlite/store.go` (append path)
- Test: `postgres/postgres_test.go`, `sqlite/append_test.go` (or the backend's existing append test file)

**Interfaces:**
- Produces: `es.ErrPayloadTooLarge` sentinel; `postgres.WithMaxPayloadBytes(n int) Option`; `sqlite.WithMaxPayloadBytes(n int) Option` — default `8 << 20`; `0` = default; `<0` = unlimited. Consumed by Task 6 (read side) and Task 7.

- [ ] **Step 1: Add the sentinel** in `es/errors.go` with the `es-lite:` prefix and a doc comment that errors wrap it with stream, version, type URL and size — never the payload.
- [ ] **Step 2: Write failing tests** `TestAppendRejectsOversizedPayload` (both backends): open with `WithMaxPayloadBytes(64)`, append a 100-byte payload → `errors.Is(err, es.ErrPayloadTooLarge)`; the stream version is unchanged; `Head`/`RawHead` unchanged.
- [ ] **Step 3: Implement**: each backend's `openConfig` gains `maxPayloadBytes` (functional options, existing pattern — see `WithoutAutoMigrate`); the append path rejects over-cap events before any insert.
- [ ] **Step 4: Run** `task test:pg` and `GOWORK=off go test ./sqlite/...` — green.
- [ ] **Step 5: Commit** — `feat(store): append-time payload cap (WithMaxPayloadBytes)`

### Task 6: Two-step pages — never materialize an over-cap payload

**Files:**
- Modify: `postgres/wsstore.go`, `sqlite/feed.go`
- Test: `postgres/feed_test.go`, `sqlite/feed_test.go`

**Interfaces:**
- Consumes: Task 5's store cap.
- Produces: adapters return `*feed.EventTooLargeError` (position, type URL, size, limit — no payload) when the **first** row of a page is over the store cap. The server's own check remains as backstop for adapters without SQL-side checks. No port signature changes.

- [ ] **Step 1: Write the failing test** `TestOversizedFirstRowIsNotMaterialized` (Review Focus 3, both backends): store opened with cap 64; insert one 4096-byte payload row directly (raw SQL — the cap now prevents appending one) followed by a normal row. `ReadAllBounded(ctx, 0, ...)` returns `*feed.EventTooLargeError` with `Position`, `Size`, `TypeURL`, `Limit` set and no envelopes. A later row over cap but not first → page stops before it with `truncated=true`, and the next page surfaces the error as first row.
- [ ] **Step 2: Implement the two-step page** in both adapters: step 1 selects `global_position, octet_length(payload)` (Postgres) / `length(payload)` (SQLite) under the existing filter and bound, applies count/budget/cap selection (first row always included); step 2 fetches full rows only for the chosen positions (`global_position = ANY($n)` / `IN (...)`, ordered). Over-cap first row → the typed error.
- [ ] **Step 3: Run both suites** — green, including the existing budget/`truncated` tests (semantics unchanged).
- [ ] **Step 4: Commit** — `feat(feed): two-step pages never materialize over-cap payloads`

### Task 7: `task test:feedgate` — chaos, plan gate, fleet, benchmark

**Files:**
- Create: `postgres/feedgate_test.go` (`//go:build feedgate`)
- Modify: `Taskfile.yml`, `.github/workflows/ci.yml`
- Test: the gate file itself.

**Interfaces:**
- Consumes: current `Checkpoint` API (updated to `Ack` in Task 10 — note in the gate file), Tasks 4–6.
- Produces: `task test:feedgate` (chaos + EXPLAIN + fleet, `-race -shuffle=on`); nightly CI job; `BenchmarkWorkspaceAppend` baseline; harness helpers reused nightly.

- [ ] **Step 1: Add the Taskfile target**: same Docker pattern as `test:pg`, then `PG_DSN=... go test -race -shuffle=on -tags feedgate -count=1 -timeout 45m ./postgres/ -run TestFeedGate`.
- [ ] **Step 2: Write** `TestFeedGateNoLoss` (spec §5.10.2): 8 appenders × ≥12.5k events each (batches of 20), every batch `Begin → AppendTx → sleep rand(0–500ms) → Commit` (decoupled insert/commit); 3 `feed.Server` consumers with disjoint type filters, `LeaseTTL=100ms`; kill windows per consumer: cancel mid-page after send/before checkpoint, and full store close/reopen. Oracle: **set equality** — each consumer's delivered-position set equals the appended positions filtered by its type set (duplicates allowed, deduplicated for comparison; invention or omission both fail). Hash-chain oracle: per stream, delivered envelopes are version-contiguous and `PrevHash` links (use the existing `es` chain helpers).
- [ ] **Step 3: Write** `TestFeedGatePageQueryPlan`: seed ≥1M rows via direct batched inserts (512-byte payloads); `EXPLAIN (FORMAT JSON)` the exact page query; assert `events_ws_type_pos_idx` appears and no `Seq Scan` on events partitions.
- [ ] **Step 4: Write** `TestFeedGateHundredIdleSubscribers` (Review Focus 7): pool `MaxConns=50`; 100 subscriptions (separate instances); after a quiet start, append one matching event; all 100 deliver within 1s; pool stats healthy. Task 18 adds jitter-spread assertions here.
- [ ] **Step 5: Write** `BenchmarkWorkspaceAppend` (batch of 20) and record the baseline numbers in the PR — the write-path-unchanged evidence (Task 2's mechanism must not move it).
- [ ] **Step 6: Run** `task test:feedgate` — expected: green. Record the output in the PR (this run is Phase 1's exit evidence).
- [ ] **Step 7: Wire the nightly CI job** (cron) running `task test:feedgate`.
- [ ] **Step 8: Commit** — `test(postgres): feedgate chaos, plan and fleet scenarios`

---

## Phase 2 — Protocol (feed core)

### Task 8: Validation hardening

**Files:**
- Modify: `feed/server.go`, `feed/server_internal_test.go`, `feed/server_test.go`
- Test: same files.

**Interfaces:** Produces: `validateKey` rejects NUL/control bytes and validates `instance`; `validateRequest` enforces `Request.Projection == Definition.Name`; `newPrefix` returns an error (no constant fallback); documented negative-config semantics; deadline dedup in `Subscribe`'s error join.

- [ ] **Step 1: Failing test** `TestValidateKeyRejectsControlBytes`: scope/projection/instance containing `"\x00"`, `"\x01"`, `"\x7f"` → `ErrInvalidRequest`; empty instance remains allowed (documented default).
- [ ] **Step 2: Failing test** `TestSubscribeRejectsProjectionNameMismatch`: `Request{Projection: "counter-view", Definition: {Name: "counterview", ...}}` → `ErrInvalidRequest`.
- [ ] **Step 3: Failing test** `TestNewPrefixPropagatesEntropyError`: swap a package-level `randRead` var in the internal test; `NewServer` returns the error instead of minting `"feed"`.
- [ ] **Step 4: Implement** the three changes plus: document `MaxPayloadBytes < 0` = disabled vs `MaxBatchBytes < 0` = default; in `Subscribe`'s error join, drop `streamErr` when it matches `context.DeadlineExceeded` and the caller ctx expired (dedup — test with `strings.Count(err.Error(), "deadline") == 1`).
- [ ] **Step 5: Run** `GOWORK=off go test -race ./...` — green.
- [ ] **Step 6: Commit** — `fix(feed): fail closed on ambiguous keys and config drift`

### Task 9: Port reshaping — the key triple

**Files:**
- Modify: `feed/ports.go`, `feed/server.go`, `feed/server_test.go`, `feed/server_internal_test.go`, `sqlite/feed.go`, `sqlite/feed_test.go` (+ any test using `LeaseKey`)

**Interfaces:**
- Produces (consumed by Tasks 10–17):

```go
type CheckpointStore interface {
    Load(ctx context.Context, scope, projection, instance string) (uint64, error)
    Save(ctx context.Context, scope, projection, instance string, position uint64) error
    Reset(ctx context.Context, scope, projection, instance string, position uint64) error
    Delete(ctx context.Context, scope, projection, instance string) error
    Checkpoint(ctx context.Context, scope, projection, instance string) (Checkpoint, bool, error)
}
type LeaseStore interface {
    Acquire(ctx context.Context, scope, projection, instance, owner, consumer string, ttl time.Duration) (bool, error)
    Renew(ctx context.Context, scope, projection, instance, owner string, ttl time.Duration) (bool, error)
    Release(ctx context.Context, scope, projection, instance, owner string) error
    Holder(ctx context.Context, scope, projection, instance string) (Lease, bool, error)
}
```

(Task 11 adds the `definition string` parameter to `Save`/`Reset` — deliberately not here.)

- [ ] **Step 1: Refactor** ports, core call sites, and the sqlite adapter (it composes its own TEXT key internally from the triple; behavior identical).
- [ ] **Step 2: Delete `feed.LeaseKey`** from the public API; fix all references.
- [ ] **Step 3: Run** `GOWORK=off go test -race ./...` and `task test:pg` — green (pure refactor, all existing tests pass unchanged in intent).
- [ ] **Step 4: Commit** — `refactor(feed): lease and checkpoint ports take the key triple`

### Task 10: Fenced acknowledgements

**Files:**
- Modify: `feed/server.go`, `feed/server_test.go`
- Interfaces below.

**Interfaces:**
- Consumes: Task 9's ports.
- Produces (consumed by Task 17 and the feedgate gate):

```go
type Ack struct {
    Scope, Projection, Instance string
    Owner     string // Request.Owner token of the acking subscription
    Position  uint64
    Definition string // Definition.Hash(); empty = preserve the stored hash
}
func (s *Server) Checkpoint(ctx context.Context, ack Ack) error
```

`Request` gains `Owner string` (caller-supplied token; empty → server mints, and such a stream is not ackable — documented). The old `Checkpoint` signature is removed.

- [ ] **Step 1: Failing test** `TestCheckpointRejectsStaleOwner` (Review Focus 2): subscriber A (`Owner:"a"`) delivers; expire A's lease and let B acquire; A's `Checkpoint(Ack{Owner:"a", ...})` fails matching `ErrLeaseLost`; assert the fake store's position is **unchanged**.
- [ ] **Step 2: Failing test** `TestCheckpointRejectsForeignOwner`: holder is B, ack claims owner "a" → `*LeaseHeldError`; `TestCheckpointRejectsWithoutLease`: nothing held → `ErrLeaseLost`.
- [ ] **Step 3: Failing test** `TestSubscribeUsesRequestOwnerAsLeaseOwner`: acquire succeeds; a second Subscribe with the same key and a different owner gets `ErrLeaseHeld`; re-subscribing with the **same** owner re-acquires (self-steal) and the first stream's next renewal fails.
- [ ] **Step 4: Implement**: `Request.Owner`, holder check in `Checkpoint` (not held → `ErrLeaseLost`; another owner → `*LeaseHeldError`), method doc states the check-then-save window and its safe failure mode (rejected ack, never an advanced cursor).
- [ ] **Step 5: Update the feedgate gate consumers** (Task 7) to the `Ack` API.
- [ ] **Step 6: Run** `GOWORK=off go test -race ./...` and `task test:feedgate` — green.
- [ ] **Step 7: Commit** — `feat(feed): lease-fenced acknowledgements via Ack`

### Task 11: Definition fingerprint

**Files:**
- Modify: `projection/definition.go`, `feed/ports.go`, `feed/errors.go`, `feed/server.go`, `sqlite/feed.go`, `feed/server_test.go`
- Create: `sqlite/migrations/0004_feed_definition.sql`

**Interfaces:**
- Produces (consumed by Tasks 12, 13, 14, 17): `func (d Definition) Hash() string` (SHA-256 over `"es-lite/definition\x00" + Name + "\x00" + strings.Join(sorted(Consumes), "\x00")`); **BREAKING** — the port signatures become, exactly:

```go
Save(ctx context.Context, scope, projection, instance, definition string, position uint64) error
Reset(ctx context.Context, scope, projection, instance, definition string, position uint64) error
```

`definition` is the checkpoint's new identity: `Server.ResetCheckpoint` passes the request definition's hash, `Skip` passes the loaded hash to preserve it. `feed.Checkpoint` gains `DefinitionHash string`; sentinel `feed.ErrDefinitionChanged`; sqlite column `definition_hash text NOT NULL DEFAULT ''`.

- [ ] **Step 1: Failing test** `TestDefinitionHashIsStable` (projection): identical definitions hash equal; reordered `Consumes` hash equal; name or type-set change hash different.
- [ ] **Step 2: Implement `Hash()`** in `projection/definition.go`.
- [ ] **Step 3: SQLite migration 0004** — both changes land here (this task + Task 12): `ALTER TABLE feed_checkpoints ADD COLUMN definition_hash text NOT NULL DEFAULT ''` and the `feed_dead_letters` table from spec §5.3 (SQLite spells the tenant column `scope` for parity with `feed_checkpoints`).
- [ ] **Step 4: Failing core test** `TestSubscribeRejectsChangedDefinition`: subscribe `{A}`, ack at pos 10 (hash recorded); subscribe `{A,B}` → `ErrDefinitionChanged` naming the projection and pointing at `ResetCheckpoint`; after `ResetCheckpoint` with the new definition, subscribe `{A,B}` succeeds and replays from the reset point.
- [ ] **Step 5: Implement**: adapter Save/Reset write the hash (sqlite upsert: empty definition preserves the stored hash); `Subscribe` loads `CheckpointStore.Checkpoint` and compares non-empty hashes; `Checkpoint` (the struct method) returns `DefinitionHash`.
- [ ] **Step 6: Run** `GOWORK=off go test -race ./...` — green.
- [ ] **Step 7: Commit** — `feat(feed): definition fingerprint fails closed on drift`

### Task 12: `Skip` + dead letters

**Files:**
- Modify: `feed/ports.go`, `feed/server.go`, `feed/server_test.go`, `sqlite/feed.go`
- Test: `sqlite/feed_test.go` (end-to-end), `feed/server_test.go` (core)

**Interfaces:**
- Consumes: Task 11 (loaded hash; migration 0004's dead-letter table).
- Produces:

```go
type DeadLetter struct {
    Scope, Projection, Instance, TypeURL, Reason, Actor string
    Position uint64; SizeBytes int; CreatedAt time.Time
}
type DeadLetterStore interface { Record(ctx context.Context, dl DeadLetter) error }
type SkipRequest struct {
    Scope, Projection, Instance string
    Position uint64; Actor, Reason string; Force bool
}
func (s *Server) Skip(ctx context.Context, sr SkipRequest) error
```

`Config.DeadLetters DeadLetterStore` (optional; nil = advance without recording — documented). Note: the spec's §5.3 signature omitted `force`; its own held-lease rule requires it — this plan adds it.

- [ ] **Step 1: Failing test** `TestSkipRefusesHeldLeaseWithoutForce` (Review Focus 4): live lease → `Skip{Force:false}` → `*LeaseHeldError`, checkpoint unchanged; `Force:true` advances to `sr.Position`.
- [ ] **Step 2: Failing test** `TestSkipRecordsDeadLetter`: `Skip` with `DeadLetters` set → one `Record` with position/type/size/reason/actor; checkpoint == position; stored definition hash **preserved** (assert via the fake exposing the row).
- [ ] **Step 3: Failing e2e test** `TestSkipResumesPastPoison` (sqlite): over-cap row → subscribe dies with `*EventTooLargeError`; `Skip`; resubscribe delivers the events after the poison position.
- [ ] **Step 4: Implement**: `Skip` = refuse-if-held unless force → `CheckpointStore.Reset(scope, proj, inst, loadedHash, position)` → `DeadLetters.Record`. Method doc carries the runbook path (spec §5.3 lifecycle: pointer, not copy; replay via `ResetCheckpoint`).
- [ ] **Step 5: Implement the sqlite `DeadLetterStore`** (INSERT OR REPLACE).
- [ ] **Step 6: Run** `GOWORK=off go test -race ./...` — green.
- [ ] **Step 7: Commit** — `feat(feed): Skip with audited dead letters`

---

## Phase 3 — Adapters, ops, docs

### Task 13: Postgres feed adapters + migration 0004

**Files:**
- Create: `postgres/feed.go`, `postgres/feed_test.go`, `postgres/migrations/0004_feed_tables.sql`

**Interfaces:**
- Consumes: Task 9/11 ports (final signatures), Task 12 `DeadLetterStore`.
- Produces: `postgres.NewLogs(s *Store) *Logs` (`Open` → `WorkspaceStore(scope)`), `postgres.NewCheckpointStore(s *Store) *CheckpointStore`, `postgres.NewLeaseStore(s *Store) *LeaseStore`, `postgres.NewDeadLetterStore(s *Store) *DeadLetterStore` — all RLS-pinned via the existing `setWorkspace` pattern, DB clocks (`clock_timestamp()`).

- [ ] **Step 1: Migration 0004**: `feed_checkpoints(workspace_id, projection, instance, position, definition_hash, updated_at)`, `feed_leases(workspace_id, projection, instance, owner, consumer, expires_at)`, `feed_dead_letters` (spec §5.3, with `workspace_id`); PKs on the natural keys; `ENABLE ROW LEVEL SECURITY` + `FORCE` + policy `workspace_id = current_setting('app.workspace_id', true)` — **no bypass arm**.
- [ ] **Step 2: Failing test** `TestFeedStoresRefuseCrossWorkspaceAccess` (Review Focus 5): rows written under workspace A are invisible under B's GUC and vice versa; `pg_policies` for the three tables contains no `app.bypass_rls` reference.
- [ ] **Step 3: Implement the adapters** mirroring sqlite semantics exactly: owner-guarded upsert acquire (`expires_at <= clock_timestamp()` takeover), renew-only-by-owner, monotonic `GREATEST` save, expiry boundary `<=` acquire / `>` holder, `clock_timestamp()` for `updated_at`/`expires_at`.
- [ ] **Step 4: Run** `task test:pg` — green.
- [ ] **Step 5: Commit** — `feat(postgres): workspace-scoped feed adapters`

### Task 14: `feedtest` conformance suite

**Files:**
- Create: `feedtest/conformance.go`
- Modify: `sqlite/feed_test.go`, `postgres/feed_test.go`

**Interfaces:**
- Produces: `feedtest.RunLog(t, newLog)`, `feedtest.RunCheckpoints(t, newStore)`, `feedtest.RunLeases(t, newStore)`, `feedtest.RunAll(t, fixture)` where the constructors are `func(ctx context.Context, t testing.TB) (T, func())` (the `func()` is the cleanup) and `fixture` carries optional backend capabilities (e.g. `MultiWorkspace bool` for the RLS test). Platform adapters must pass the same suite.

- [ ] **Step 1: Extract** the contract behaviors (spec §5.6 list) from `sqlite/feed_test.go` into `feedtest`: monotonic-vs-rewinding Save, expiry boundaries, owner-guarded renew/release, empty types, first-row-over-budget, `truncated` at exact budget, `*EventTooLargeError` without payload, definition-hash round-trip + preserve-on-empty, and the cross-workspace refusal when `MultiWorkspace`.
- [ ] **Step 2: Rewire** the sqlite and postgres adapter tests to the suite, keeping only backend-specific tests local.
- [ ] **Step 3: Run both suites** — green.
- [ ] **Step 4: Commit** — `test: shared feedtest conformance suite`

### Task 15: RLS role hardening + Drain fail-fast

**Files:**
- Create: `postgres/migrations/0005_relay_role_policies.sql`
- Modify: `postgres/drain.go`, `postgres/drain_test.go`, `docs/deploy/README.md`, the `test:pg` CI setup
- Interfaces below.

**Interfaces:**
- Produces: `postgres.ErrRelayRoleMissing` sentinel; the `es_relay` role requirement in both the `events` and `unique_claims` policies.

- [ ] **Step 1: Failing test** `TestDrainFailsFastWithoutRelayRole` (Review Focus 6): Drain as a session that is not an `es_relay` member → the typed error (wrapping the sentinel, pointing at the runbook), zero rows read — never a silent empty success.
- [ ] **Step 2: Migration 0005**: alter both policies' bypass arm to `... AND pg_has_role(current_user, 'es_relay', 'member')` (spec §5.7).
- [ ] **Step 3: Implement the startup check** in `Drain` (once per process, `sync.Once`): verify membership before setting the bypass; error loudly.
- [ ] **Step 4: CI + runbook**: the `test:pg` container setup creates and grants `es_relay` to the test user; the runbook documents create-role-then-migrate ordering.
- [ ] **Step 5: Run** `task test:pg` — green.
- [ ] **Step 6: Commit** — `fix(postgres): role-gate the RLS bypass, fail fast in Drain`

### Task 16: Observability — Observer, MatchingHead, filter-aware Status

**Files:**
- Modify: `feed/ports.go`, `feed/server.go`, `feed/server_test.go`, `postgres/wsstore.go`, `sqlite/feed.go`, gate + fakes
- Interfaces below.

**Interfaces:**
- Produces:

```go
type Observer interface {
    OnPage(events, bytes int)
    OnCheckpoint(position uint64)
    OnLeaseLost(scope, projection, instance, owner string)
    OnPoison(err *EventTooLargeError)
    OnStall(settledHead, rawHead uint64)
}
```

`Config.Observer Observer` + `feed.NoopObserver`; `feed.Log` gains `MatchingHead(ctx context.Context, types []string) (uint64, error)` (ceiling-bounded on Postgres); `func (s *Server) Status(ctx context.Context, req Request) (Status, error)` replaces the type-blind `Status` (old signature removed; the duplicate checkpoint read dies with it). `Status.Lag` becomes `matchingHead − position`.

- [ ] **Step 1: Failing test** `TestObserverReportsStall`: an in-flight append holds the ceiling; two consecutive empty pages → exactly one `OnStall` (rate limit: ≥1 probe per stream per few seconds) with `raw > settled`; after commit, no further stalls.
- [ ] **Step 2: Failing test** `TestStatusLagIsFilterAware`: matching head 100, all-type head 500, checkpoint 100 → `Lag == 0` (the old code reported 400).
- [ ] **Step 3: Implement**: `Observer`/`NoopObserver`; wire `OnPage`, `OnCheckpoint` (fenced acks), `OnLeaseLost` (renewal failure), `OnPoison` (oversized first row, both server check and adapter path), `OnStall` (rate-limited `RawHead` probe in the empty-page branch); `MatchingHead` on both backends; `Status(ctx, req)`.
- [ ] **Step 4: Run** `GOWORK=off go test -race ./...` and `task test:pg` — green.
- [ ] **Step 5: Commit** — `feat(feed): observer, matching head, filter-aware lag`

### Task 17: `eventfeed` consumer adapter

**Files:**
- Create: `eventfeed/eventfeed.go`, `eventfeed/eventfeed_test.go`

**Interfaces:**
- Consumes: Task 10 `Ack`, Task 11 `Hash()`, Task 16 `Observer`; `projection.Feed`, `projection.ReconnectingFeed`.
- Produces:

```go
type Config struct {
    Server *feed.Server
    Definition projection.Definition
    Scope, Instance, Consumer string
    BatchSize int             // default: server batch
    AckInterval time.Duration // default: 250ms
    Observer feed.Observer
}
func New(cfg Config) *Feed // implements projection.Feed
```

- [ ] **Step 1: Failing e2e test** `TestEventfeedDeliversAtLeastOnceAcrossReconnect` (sqlite): feed.Server + `eventfeed.New`; run, cancel mid-stream, reconnect ×N with appends between; assert set equality (marker dedups) and that each reconnect's first delivery is within one ack window of the last acked position.
- [ ] **Step 2: Failing test** `TestEventfeedAcksAtPageBoundary`: deliver N=2×BatchSize events → exactly 2 `Checkpoint` calls (not 2×BatchSize); trailing sub-page events ack on the `AckInterval` tick and on stream end.
- [ ] **Step 3: Implement**: `Sink` adapter calls `handle` per envelope, tracks the pending position/count; acks when `count >= BatchSize`, on the tick, or on stream end — always `Ack{Scope: cfg.Scope, Projection: cfg.Definition.Name, Instance: cfg.Instance, Owner: token, Position: pending, Definition: cfg.Definition.Hash()}`; wraps itself in `ReconnectingFeed` (defaults 250ms/30s); reconnect token policy is the builder's call (spec §7) — stable token for self-steal is suggested; poison surfaces via `Observer.OnPoison`.
- [ ] **Step 4: Run** `GOWORK=off go test -race ./...` — green.
- [ ] **Step 5: Commit** — `feat(eventfeed): fenced consumer adapter with page-coalesced acks`

### Task 18: Idle poll jitter + Waiter floor

**Files:**
- Modify: `feed/server.go`, `feed/server_internal_test.go`, the Task 7 gate

**Interfaces:**
- Produces: ±20% jitter on idle waits (unexported `jitter func(time.Duration) time.Duration` field, swappable in internal tests); a wait floor so a `Waiter` returning nil immediately cannot spin the loop.

- [ ] **Step 1: Failing internal test** `TestWaitJittersIdlePoll`: fixed jitter factors; assert every idle wait ∈ `[0.8·Poll, 1.2·Poll]`.
- [ ] **Step 2: Failing test** `TestWaiterFloorPreventsSpin`: a `Waiter` returning nil instantly; assert per-iteration elapsed ≥ `Poll` (the server sleeps the remainder).
- [ ] **Step 3: Implement** both.
- [ ] **Step 4: Extend** `TestFeedGateHundredIdleSubscribers`: with jitter on, assert the fleet's poll timestamps spread across ≥10 distinct 100ms buckets in a 1s window; run `task test:feedgate` — green.
- [ ] **Step 5: Commit** — `feat(feed): idle jitter and waiter floor`

### Task 19: CI gates, ADR 0037, README, runbook

**Files:**
- Modify: `Taskfile.yml`, `.github/workflows/ci.yml`, `README.md`, `docs/adr/0001-*`, `docs/adr/0010-*`, `docs/adr/0036-*`, `docs/deploy/README.md`
- Create: `.golangci.yml`, `docs/adr/0037-commit-safe-feed-reads.md`

- [ ] **Step 1: Taskfile/CI**: `test:pg` gains `-race -shuffle=on`; add `lint:go` (golangci-lint) with `.golangci.yml` (errcheck, govet, staticcheck) as a CI job; add `coverage` writing `coverage.out` with a `COVERAGE_FLOOR` Taskfile var set to the first green measurement (ratchet).
- [ ] **Step 2: ADR 0037** per spec §5.11: the settled-ceiling mechanism and its losslessness argument, the rejected per-row `xmin` filter and pre-reserved watermark **with the killer interleavings**, stall semantics, the fallback gate, index build order, RLS rollout.
- [ ] **Step 3: Update** ADR 0001 §5 (cross-ref 0037), ADR 0010 (index-build caveat), ADR 0036 + README delivery section (settled-head semantics + a guarantees block).
- [ ] **Step 4: Runbook** (`docs/deploy/README.md`): `es_relay` creation/grant ordering, index build ordering, poison/Skip triage (spec §5.3 lifecycle), stall alert + analytics-on-replica, pool sizing guidance (spec §5.12), lease RTO ≤ `LeaseTTL`.
- [ ] **Step 5: Full local gates**: `GOWORK=off go test ./...`, `task test:pg`, `task lint:go`, `task test:feedgate` — all green.
- [ ] **Step 6: Commit** — `docs: ADR 0037, feed guarantees, runbook; chore: CI gates`

---

## Definition of done

- All seven Review Focus tests exist, are deterministic, and are green in per-PR CI (or the gate for #1/#3/#7 where specified).
- `task test:feedgate` green once, end-to-end, output recorded in the final PR.
- `BenchmarkWorkspaceAppend` within noise of the pre-Task-2 baseline.
- `feedtest.RunAll` green for sqlite and postgres adapters.
- ADR 0037 merged; README and runbook updated; the spec's §8 list walked item by item in the closing PR description.
