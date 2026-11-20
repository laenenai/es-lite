# Snapshot Write Policy and In-Tree Caches Implementation Plan (v0.19.0)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `snapshot.Cache` a write policy that only marshals when a load would otherwise fold too much, plus working Postgres and SQLite adapters so the interface is usable as shipped.

**Architecture:** The threshold lives in `aggregate.Runtime`, evaluated after the append and before `stateCodec.Encode`, keyed on the gap between the append's `ToVersion` and the snapshot version the command's load actually used (`seed`). Two adapters implement the unchanged `snapshot.Cache` (`Load`/`Save`) over a `snapshots` table in the events database, with a workspace-scoped `Purge` for erasure and no monotonic guard.

**Tech Stack:** Go 1.27, `pgx`/Postgres 17, `modernc.org/sqlite`.

**Spec:** `docs/adr/0042-snapshot-write-policy.md` (the ADR is the binding design; read it before starting).

## Global Constraints

- No new module dependencies.
- Go 1.27; every command runs with `GOWORK=off`.
- Migrations run in one transaction (ADR 0010): no `CONCURRENTLY`, no index build needed for these tables.
- Migrations issue **no grants**; role creation and grants are deployment's (`docs/deploy/README.md`).
- Postgres RLS: `ENABLE` + `FORCE`, workspace-only policy, **no** `es_relay` bypass arm on `snapshots`.
- `snapshot.Cache` stays exactly `Load`/`Save` — no method added to the interface.
- `Handle` must never fail because of the cache: a `Load` error falls through to a full fold, a `Save` error is discarded. `WarmSnapshot` is the one path that surfaces cache errors.
- The write policy is evaluated **before** `stateCodec.Encode`; a not-due append must not encode.
- Conventional commits; `gofmt` clean; `golangci-lint run ./...` clean.
- Postgres tests: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable'` (port 5434; 5432/5433 belong to other projects).

## Review Focus

1. **A cache `Load` error must never fail a read.** Missing table, revoked grant, or a dead connection has to produce a full fold, not an error from `Load`/`Handle`. Pinned in Task 4 and Task 6 (missing-table tests) and Task 7 (full-fold fallback).
2. **A snapshot may name a version the log does not yet contain** (a caller-owned `aggregate.WithAppend` transaction commits later). The lineage guard must discard it, never seed from it. Pinned in Task 1 (`TestSnapshotEveryHealsADiscardedSeed`).
3. **`Save` errors are silent by design in `Handle` and loud in `WarmSnapshot`.** Pinned in Task 1 (lost-write retry) and Task 2 (error surfaced).
4. **A stale row after a log reset must be replaceable**, not wedge the stream: last-writer-wins, no monotonic guard. Pinned in Task 1 and Task 4 (overwrite test).
5. **Short and read-only streams must not pay the encode**: no-op commands and `WithSnapshotEvery(0)` write nothing and encode nothing. Pinned in Task 1.

---

## File Structure

- `aggregate/runtime.go` — policy fields, `WithSnapshotEvery`, `snapshotDue`, `loadSeeded`, the gated write-through, `WarmSnapshot`.
- `aggregate/snapshot_policy_test.go` (new) — behavioural policy tests.
- `aggregate/snapshot_policy_internal_test.go` (new, `package aggregate`) — the predicate table.
- `aggregate/snapshot_adapter_test.go` (new) — runtime over a real adapter.
- `snapshot/snapshot.go` — doc comments only (policy, warm-up, trust).
- `postgres/migrations/0013_snapshot_cache.sql` (new) — table, shape refusal, RLS.
- `postgres/snapshot.go` (new) — `SnapshotCache`.
- `postgres/snapshot_test.go` (new) — adapter contract + RLS.
- `postgres/migrations_test.go`, `postgres/postgres_test.go` — counts, policy shape, refusal test, truncate list.
- `sqlite/migrations/0011_snapshot_cache.sql` (new) — table.
- `sqlite/snapshot.go` (new) — `SnapshotCache`.
- `sqlite/snapshot_test.go` (new) — adapter contract.
- `sqlite/migrate_test.go`, `sqlite/populated_test.go` — version assertions.
- `README.md`, `docs/deploy/README.md`, `docs/adr/0007-snapshots-deferred.md`, `docs/adr/0042-snapshot-write-policy.md`, `docs/adr/README.md` — docs.

---

### Task 1: The write policy in `Runtime`

**Files:**
- Modify: `aggregate/runtime.go` (struct at ~38-66, `WithSnapshots` at ~92, `HandleAt` load at ~355, write-through at ~426-436, `load` at ~498)
- Test: `aggregate/snapshot_policy_test.go` (new), `aggregate/snapshot_policy_internal_test.go` (new)

**Interfaces:**
- Consumes: `snapshot.Cache`, `es.StateCodec[S]`, `es.AppendResult.FromVersion/ToVersion`, the existing test helpers in `aggregate/snapshot_test.go` (`testCache`, `counterStateCodec`, `newSnapshotRuntime`, `mustHandle`, `openStore`, `countingStore`).
- Produces:
  - `func (r *Runtime[S, C, E]) WithSnapshotEvery(n uint64) *Runtime[S, C, E]`
  - `func (r *Runtime[S, C, E]) snapshotDue(seed, to uint64) bool` (unexported)
  - `func (r *Runtime[S, C, E]) loadSeeded(ctx, sid, upToVersion, asOf) (S, uint64, uint64, error)` returning state, version, seed, error; `load` keeps its signature and delegates.

- [ ] **Step 1: Write the failing predicate test**

`aggregate/snapshot_policy_internal_test.go`, `package aggregate`:

```go
func TestSnapshotDue(t *testing.T) {
	cases := []struct {
		name      string
		set       bool
		n, seed   uint64
		to        uint64
		want      bool
	}{
		{"unset writes every append", false, 0, 5, 6, true},
		{"zero disables", true, 0, 5, 6, false},
		{"below the gap", true, 3, 3, 5, false},
		{"at the gap", true, 3, 3, 6, true},
		{"beyond the gap", true, 3, 3, 7, true},
		{"batch jumps the gap", true, 3, 0, 4, true},
		{"full fold seeds from zero", true, 3, 0, 2, false},
		{"zero version never writes", true, 3, 0, 0, false},
	}
	for _, c := range cases {
		r := &Runtime[struct{}, struct{}, struct{}]{snapshotEverySet: c.set, snapshotEvery: c.n}
		if got := r.snapshotDue(c.seed, c.to); got != c.want {
			t.Errorf("%s: snapshotDue(%d,%d)=%v, want %v", c.name, c.seed, c.to, got, c.want)
		}
	}
}
```

- [ ] **Step 2: Write the failing behavioural tests**

`aggregate/snapshot_policy_test.go` (`package aggregate_test`). Use the existing `newSnapshotRuntime`, `mustHandle`, `openStore`, and `counterv1` commands. Assertions:

- `TestSnapshotEveryDefaultWriteThrough`: `cache, rt := newSnapshotRuntime(t, store)`; one `Init`; `cache.Load(ctx, sid.Canonical())` returns `ok == true`, `snap.Version == 1`.
- `TestSnapshotEveryZeroDisablesWrites`: `rt.WithSnapshotEvery(0)`; `Init` plus three `Increment`s; `cache.Load` returns `ok == false`.
- `TestSnapshotEveryWritesAtTheGap`: `rt.WithSnapshotEvery(3)`; `Init` (v1) and `Increment` (v2) leave the cache absent; the next `Increment` (v3) yields a snapshot at 3; `Increment` (v4, v5) keeps 3; the next (v6) yields 6.
- `TestSnapshotEveryEncodesOnlyWhenDue`: build the runtime directly with a counting codec:

  ```go
  type countingCodec struct{ encodes int }
  func (c *countingCodec) Encode(s *counterv1.Counter) ([]byte, error) { c.encodes++; return proto.Marshal(s) }
  func (c *countingCodec) Decode(b []byte) (*counterv1.Counter, error) { /* proto.Unmarshal */ }
  ```

  With `WithSnapshots(cache, codec, 1)` and no policy, one `Init` leaves `codec.encodes == 1`. With `WithSnapshotEvery(3)`, one `Init` leaves `encodes == 0`, and the third command leaves `encodes == 1`.
- `TestSnapshotEveryRetriesALostWrite`: a wrapper whose `Save` fails once (`failNext bool` over `*testCache`), `WithSnapshotEvery(3)`; the v3 `Save` fails; the v4 command succeeds and leaves the cache at 4.
- `TestSnapshotEveryHealsADiscardedSeed`: as in `TestLoadDiscardsSnapshotAheadOfLog`, append to v5, `DELETE FROM events ... version > 2`, then `WithSnapshotEvery(3)` and one `Increment` (v3); `cache.Load` returns `ok == true`, `Version == 3`.
- `TestSnapshotEveryWithNoCacheIsInert`: `aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).WithSnapshotEvery(3)` handles `Init` without panic and with no cache.
- `TestNoOpCommandWritesNoSnapshot`: a `struct{}{}`-typed runtime whose decider returns no events, with a `countingCodec`-style counter; `Handle` returns equal `FromVersion`/`ToVersion`, `encodes == 0`, cache empty. (Use `es.Decider[struct{}, struct{}, struct{}]` with a `es.StateCodec[struct{}]`; the handler needs only a valid `es.StreamID`.)

- [ ] **Step 3: Run the tests to verify they fail**

Run: `GOWORK=off go test ./aggregate/ -run 'TestSnapshot|TestNoOpCommandWritesNoSnapshot' -v`

Expected: FAIL — `undefined: WithSnapshotEvery`, `undefined: snapshotDue`, `r.snapshotEverySet undefined`.

- [ ] **Step 4: Implement the policy**

In `aggregate/runtime.go`:

- Add fields beside `foldVersion`:
  ```go
  snapshotEvery    uint64
  snapshotEverySet bool
  ```
- Add `WithSnapshotEvery`:
  ```go
  // WithSnapshotEvery writes a snapshot only when an append carries the
  // stream far enough past the snapshot the command loaded (the seed):
  // to-seed >= n. Not calling it keeps the per-append write-through.
  // n == 0 disables automatic writes (a read-only cache).
  func (r *Runtime[S, C, E]) WithSnapshotEvery(n uint64) *Runtime[S, C, E] {
      r.snapshotEvery, r.snapshotEverySet = n, true
      return r
  }
  ```
- Add `snapshotDue` exactly as the ADR's §3.1 (unset → true; zero → false; else `to > seed && to-seed >= n`).
- Split the load. Rename the current `load` body to `loadSeeded` and return the seed (`from` after the lineage guard; `0` for an `asOf` load). Re-add `load` as a three-value wrapper so all existing call sites are unchanged. Record the seed from the *last* attempt, so the retry loop's successful append uses its own load.
- In `HandleAt`, capture the seed: `state, version, seed, err := r.loadSeeded(ctx, sid, 0, time.Time{})`.
- Gate the write-through at the existing site: `if r.cache != nil && r.snapshotDue(seed, res.ToVersion) { ... }`.
- Update `WithSnapshots`'s doc comment: it still writes through unless `WithSnapshotEvery` is set, the cache is trusted infrastructure (a less-trusted store needs a MAC'ing `stateCodec`, ADR 0042 §3.3), and `LoadStale`'s `maxAge` is measured against `Snapshot.RecordedAt`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `GOWORK=off go test ./aggregate/ -run 'TestSnapshot|TestNoOpCommandWritesNoSnapshot' -v`

Expected: PASS. Then `GOWORK=off go test ./aggregate/` (whole package), `GOWORK=off go test ./...`, `golangci-lint run ./aggregate/`, `gofmt -l aggregate`.

- [ ] **Step 6: Commit**

```bash
git add aggregate/runtime.go aggregate/snapshot_policy_test.go aggregate/snapshot_policy_internal_test.go
git commit -m "feat(aggregate): write snapshots on a stream-version gap"
```

---

### Task 2: Explicit warm-up

**Files:**
- Modify: `aggregate/runtime.go`
- Test: `aggregate/snapshot_policy_test.go`

**Interfaces:**
- Consumes: `loadSeeded` from Task 1.
- Produces: `func (r *Runtime[S, C, E]) WarmSnapshot(ctx context.Context, sid es.StreamID) (uint64, error)`.

- [ ] **Step 1: Write the failing tests**

- `TestWarmSnapshotWritesAtHead`: `WithSnapshotEvery(100)` (so no automatic write) and five commands; `version, err := rt.WarmSnapshot(ctx, sid)`; `version == 5`, cache holds 5.
- `TestWarmSnapshotWithZeroPolicyStillWrites`: `WithSnapshotEvery(0)`, three commands, `WarmSnapshot` writes 3.
- `TestWarmSnapshotWithoutCacheErrors`: a runtime with no `WithSnapshots`; `WarmSnapshot` returns a non-nil error and does not panic.
- `TestWarmSnapshotSurfacesSaveError`: the failing cache from Task 1; `WarmSnapshot` returns the error.
- `TestWarmSnapshotThenLoadSeeds`: after a warm, a load over a `countingStore` reads at most one envelope (the lineage probe) and returns the full state.

- [ ] **Step 2: Run to verify they fail**

Run: `GOWORK=off go test ./aggregate/ -run TestWarmSnapshot -v`
Expected: FAIL — `rt.WarmSnapshot undefined`.

- [ ] **Step 3: Implement**

```go
// WarmSnapshot folds a stream once and writes its snapshot at the head,
// bypassing the write policy. It is the explicit path for streams the
// append path cannot reach: enabling a cache on an existing long stream,
// an archived aggregate that no longer appends, and a post-restore or
// fold-version warm-up. It never runs from a read path. It errors when no
// cache (or state codec) is configured, and it returns Save errors —
// unlike Handle, which treats the cache as best-effort.
func (r *Runtime[S, C, E]) WarmSnapshot(ctx context.Context, sid es.StreamID) (uint64, error)
```

Body: validate `sid`; error if `r.cache == nil || r.stateCodec == nil`; `state, version, _, err := r.loadSeeded(ctx, sid, 0, time.Time{})`; `Encode` (return the error); `Save` (return the error); return `version`.

- [ ] **Step 4: Run to verify they pass**

Run: `GOWORK=off go test ./aggregate/ -run 'TestWarmSnapshot|TestSnapshot' -v`, then the full `./aggregate/` and `./...`, lint, gofmt.

- [ ] **Step 5: Commit**

```bash
git add aggregate/runtime.go aggregate/snapshot_policy_test.go
git commit -m "feat(aggregate): add an explicit snapshot warm-up"
```

---

### Task 3: Postgres table, policy, and migration

**Files:**
- Create: `postgres/migrations/0013_snapshot_cache.sql`
- Modify: `postgres/postgres_test.go:62` (TRUNCATE list), `postgres/postgres_test.go:400-403` (12 → 13), `postgres/migrations_test.go:305-306` (12 → 13)
- Test: `postgres/migrations_test.go` (two new tests)

**Interfaces:**
- Produces: table `snapshots (workspace_id, stream_id, version, fold_version, recorded_at, state)`; RLS policy `snapshots_workspace_isolation`; ledger version 13.

- [ ] **Step 1: Write the failing migration tests**

Add to `postgres/migrations_test.go` (`package postgres_test`, uses `openStore`, `openScratchStore`):

- `TestSnapshotsRLSShape`: query `pg_class` for `relrowsecurity` and `relforcerowsecurity` on `snapshots` (both true); query `pg_policies` for the table — exactly one policy, its `qual` contains `app.workspace_id`, and it does **not** contain `app.bypass_rls`.
- `TestSnapshotMigrationRefusesForeignTable`: `s := openScratchStore(t, postgres.WithoutAutoMigrate())`; `CREATE TABLE snapshots (x int)`; `s.Migrate(ctx)` must return an error whose text contains `snapshots`, and the ledger must not record 13. (The scratch database is disposable; no cleanup of the table needed.)

Update the two version assertions to 13 and the comment on 12.

- [ ] **Step 2: Run to verify they fail**

Run: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test ./postgres/ -run 'TestSnapshotsRLSShape|TestSnapshotMigrationRefusesForeignTable|TestMigrateIdempotentAndConcurrent|TestMigrateWritesTheSharedLedger' -v`

Expected: FAIL — no `snapshots` relation, version 12.

- [ ] **Step 3: Write the migration**

`postgres/migrations/0013_snapshot_cache.sql`:

```sql
-- Snapshot cache (ADR 0042). Derived, droppable state: one row per
-- (workspace, stream), written by aggregate.Runtime outside the append
-- transaction. Not part of the audit log; the hash chain attests events,
-- not this table. The name is reserved: an adopter's own "snapshots"
-- table with a different shape must fail this migration loudly, because
-- the runtime ignores Load/Save errors and would otherwise full-fold
-- forever with no signal.
--
-- No monotonic guard and no updated_at: last writer wins. Per-stream
-- appends are serialized by expected-version conflicts, and a stale write
-- costs one load at most n extra folded events before the next gap
-- boundary rewrites it. A guard would permanently wedge a stream after a
-- log reset (ADR 0042 §3.2).

CREATE TABLE IF NOT EXISTS snapshots (
    workspace_id text        NOT NULL,
    stream_id    text        NOT NULL,   -- canonical "type:id"
    version      bigint      NOT NULL,
    fold_version bigint      NOT NULL,
    recorded_at  timestamptz NOT NULL,   -- the runtime's Snapshot.RecordedAt
    state        bytea       NOT NULL,
    PRIMARY KEY (workspace_id, stream_id)
);

-- Refuse a pre-existing table of the same name with an incompatible shape.
DO $$
DECLARE missing text;
BEGIN
    SELECT string_agg(c, ', ') INTO missing
    FROM unnest(ARRAY['workspace_id','stream_id','version','fold_version','recorded_at','state']) AS c
    WHERE NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'snapshots'
          AND column_name = c);
    IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'snapshots exists with an incompatible shape (missing columns: %) — rename it (the name is reserved by ADR 0042) and re-run the migration', missing;
    END IF;
END $$;

ALTER TABLE snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE snapshots FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS snapshots_workspace_isolation ON snapshots;
CREATE POLICY snapshots_workspace_isolation ON snapshots
    USING (workspace_id = current_setting('app.workspace_id', true))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
```

Add `snapshots` to the `TRUNCATE` list in `postgres/postgres_test.go:62`.

- [ ] **Step 4: Run to verify they pass**

Run: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test ./postgres/ -run 'TestSnapshotsRLSShape|TestSnapshotMigrationRefusesForeignTable|TestMigrateIdempotentAndConcurrent|TestMigrateWritesTheSharedLedger' -v`

Expected: PASS. Confirm a fresh database migrates to 13 (drop/recreate `eslite` once if needed).

- [ ] **Step 5: Commit**

```bash
git add postgres/migrations/0013_snapshot_cache.sql postgres/migrations_test.go postgres/postgres_test.go
git commit -m "feat(postgres): add the snapshot cache table and policy"
```

---

### Task 4: Postgres adapter

**Files:**
- Create: `postgres/snapshot.go`, `postgres/snapshot_test.go`

**Interfaces:**
- Consumes: `snapshot.Cache` (`Load(ctx, streamID) (Snapshot, bool, error)`, `Save(ctx, streamID, Snapshot) error`), `snapshot.Snapshot{Version, FoldVersion, RecordedAt, State}`, `Store.runWorkspace`, `DBTX`, `pgx.ErrNoRows`.
- Produces: `func NewSnapshotCache(s *Store, workspace string) *SnapshotCache`; methods `Load`, `Save`, `Purge(ctx) error`.

- [ ] **Step 1: Write the failing tests**

`postgres/snapshot_test.go`, `package postgres_test`, `openStore(t)`:

- `TestSnapshotCacheRoundTrip`: save `{Version: 7, FoldVersion: 2, RecordedAt: time.Date(2026, 11, 20, 10, 0, 0, 123456789, time.UTC), State: []byte{1,2,3}}`; load returns `ok`, and every field equals; compare the time with `.Equal(fixed.Truncate(time.Microsecond))` (Postgres keeps microseconds).
- `TestSnapshotCacheAbsent`: load on an empty table returns `ok == false`, no error.
- `TestSnapshotCacheOverwrite`: save v7 then v5; load returns 5 and the v5 bytes (last-writer-wins, pinned deliberately).
- `TestSnapshotCacheWorkspaceIsolation`: `a := NewSnapshotCache(s, "ws_a")`, `b := NewSnapshotCache(s, "ws_b")`; save via `a`; `b.Load` is absent; `b.Purge` succeeds and `a.Load` still finds the row. Also a direct `SELECT count(*)` under `b`'s workspace sees 0.
- `TestSnapshotCachePurge`: save two streams, purge, both absent.
- `TestSnapshotCacheMissingTableErrors`: `openScratchStore(t, postgres.WithoutAutoMigrate())`; `Load` and `Save` return errors (not `ok=false`, not nil).

- [ ] **Step 2: Run to verify they fail**

Run: `PG_DSN=... GOWORK=off go test ./postgres/ -run TestSnapshotCache -v`
Expected: FAIL — `undefined: postgres.NewSnapshotCache`.

- [ ] **Step 3: Implement `postgres/snapshot.go`**

- `type SnapshotCache struct { store *Store; workspace string }`, `NewSnapshotCache`, `var _ snapshot.Cache = (*SnapshotCache)(nil)`.
- `Load`: `store.runWorkspace(ctx, c.workspace, ...)`; `SELECT version, fold_version, recorded_at, state FROM snapshots WHERE workspace_id = $1 AND stream_id = $2`; `pgx.ErrNoRows` → `snapshot.Snapshot{}, false, nil`; wrap other errors `postgres: snapshots: load: %w`; scan `version`/`fold_version` as `int64` and cast to `uint64`/`uint32`.
- `Save`: `INSERT ... VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (workspace_id, stream_id) DO UPDATE SET version = excluded.version, fold_version = excluded.fold_version, recorded_at = excluded.recorded_at, state = excluded.state`; pass `snap.RecordedAt.UTC()`; wrap `postgres: snapshots: save: %w`.
- `Purge`: `DELETE FROM snapshots WHERE workspace_id = $1` inside `runWorkspace`; wrap `postgres: snapshots: purge: %w`.
- Doc comments: the workspace is bound at construction (the `Cache` interface carries none); state is opaque; `Save` is last-writer-wins by design; `Purge` is best-effort erasure and needs a quiesced workspace.

- [ ] **Step 4: Run to verify they pass**

Run: `PG_DSN=... GOWORK=off go test ./postgres/ -run TestSnapshotCache -v`, then the full `./postgres/`, `./...`, lint, gofmt.

- [ ] **Step 5: Commit**

```bash
git add postgres/snapshot.go postgres/snapshot_test.go
git commit -m "feat(postgres): add a snapshot cache adapter"
```

---

### Task 5: SQLite table and migration

**Files:**
- Create: `sqlite/migrations/0011_snapshot_cache.sql`
- Modify: `sqlite/migrate_test.go:25-26` and `:139`, `sqlite/populated_test.go:19`, `:64-71`
- Test: `sqlite/migrate_test.go`

**Interfaces:**
- Produces: table `snapshots (stream_id, version, fold_version, recorded_at, state)`; schema version 11.

- [ ] **Step 1: Update the failing assertions**

- `sqlite/migrate_test.go`: version 10 → 11; the explanatory string gains `+ 0011 snapshot cache`; the `want := []int{...}` list becomes `{1, 2, 3, 4, 6, 7, 8, 9, 10, 11}`.
- `sqlite/populated_test.go`: both 10 → 11 and their comments.
- Add `TestSnapshotsTableColumns`: `PRAGMA table_info(snapshots)` names equal `stream_id, version, fold_version, recorded_at, state` in order, and `stream_id` is the primary key (`PRAGMA table_info` `pk` column = 1).

- [ ] **Step 2: Run to verify they fail**

Run: `GOWORK=off go test ./sqlite/ -run 'TestMigrateIdempotent|TestPopulatedStoreChainsFromMigration|TestSnapshotsTableColumns' -v`
Expected: FAIL — version 10, and `no such table: snapshots`.

- [ ] **Step 3: Write the migration**

`sqlite/migrations/0011_snapshot_cache.sql`:

```sql
-- Snapshot cache (ADR 0042). Derived, droppable state: one row per
-- stream, written by aggregate.Runtime outside the append transaction.
-- One SQLite database is one workspace, so there is no workspace column.
--
-- recorded_at uses the same fixed-width RFC3339 UTC layout as events
-- (tsLayout), so lexical order matches chronological order. No
-- monotonic guard: last writer wins (ADR 0042 §3.2).

CREATE TABLE IF NOT EXISTS snapshots (
    stream_id    TEXT    PRIMARY KEY,
    version      INTEGER NOT NULL,
    fold_version INTEGER NOT NULL,
    recorded_at  TEXT    NOT NULL,
    state        BLOB    NOT NULL
);
```

- [ ] **Step 4: Run to verify they pass**

Run: `GOWORK=off go test ./sqlite/ -run 'TestMigrateIdempotent|TestPopulatedStoreChainsFromMigration|TestSnapshotsTableColumns' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sqlite/migrations/0011_snapshot_cache.sql sqlite/migrate_test.go sqlite/populated_test.go
git commit -m "feat(sqlite): add the snapshot cache table"
```

---

### Task 6: SQLite adapter

**Files:**
- Create: `sqlite/snapshot.go`, `sqlite/snapshot_test.go`

**Interfaces:**
- Consumes: `snapshot.Cache`, `snapshot.Snapshot`, `Store.db`, `formatTS`/`parseTS` (`sqlite/store.go:93-94`).
- Produces: `func NewSnapshotCache(s *Store) *SnapshotCache`; `Load`, `Save`, `Purge(ctx) error`.

- [ ] **Step 1: Write the failing tests**

`sqlite/snapshot_test.go`, `package sqlite_test`, `openStore(t)` (in-memory, unique per test):

- `TestSQLiteSnapshotCacheRoundTrip`: `{Version: 7, FoldVersion: 2, RecordedAt: time.Date(2026, 11, 20, 10, 0, 0, 123456789, time.UTC), State: []byte{1,2,3}}` round-trips with **exact** time equality (the fixed-width layout preserves nanoseconds) and exact bytes.
- `TestSQLiteSnapshotCacheAbsent`, `TestSQLiteSnapshotCacheOverwrite` (v7 then v5 → 5), `TestSQLiteSnapshotCachePurge`.
- `TestSQLiteSnapshotCacheMissingTableErrors`: `sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared", sqlite.WithoutAutoMigrate())`; `Load` and `Save` return errors.

- [ ] **Step 2: Run to verify they fail**

Run: `GOWORK=off go test ./sqlite/ -run TestSQLiteSnapshotCache -v`
Expected: FAIL — `undefined: sqlite.NewSnapshotCache`.

- [ ] **Step 3: Implement `sqlite/snapshot.go`**

- `type SnapshotCache struct { store *Store }`, `NewSnapshotCache`, `var _ snapshot.Cache = (*SnapshotCache)(nil)`.
- `Load`: `SELECT version, fold_version, recorded_at, state FROM snapshots WHERE stream_id = ?`; `sql.ErrNoRows` → absent; parse the timestamp with `parseTS`; cast `int64` → `uint64`/`uint32`.
- `Save`: `INSERT ... ON CONFLICT(stream_id) DO UPDATE SET version=excluded.version, fold_version=excluded.fold_version, recorded_at=excluded.recorded_at, state=excluded.state`; write `formatTS(snap.RecordedAt)`.
- `Purge`: `DELETE FROM snapshots`.
- Error wrapping `sqlite: snapshots: load|save|purge: %w`; doc comments mirroring Task 4 (single workspace, opaque state, last writer wins, best-effort purge).

- [ ] **Step 4: Run to verify they pass**

Run: `GOWORK=off go test ./sqlite/ -run TestSQLiteSnapshotCache -v`, then the full `./sqlite/`, `./...`, lint, gofmt.

- [ ] **Step 5: Commit**

```bash
git add sqlite/snapshot.go sqlite/snapshot_test.go
git commit -m "feat(sqlite): add a snapshot cache adapter"
```

---

### Task 7: A runtime over a real adapter

**Files:**
- Test: `aggregate/snapshot_adapter_test.go` (new, `package aggregate_test`)

**Interfaces:**
- Consumes: `sqlite.NewSnapshotCache`, the `countingStore` from `aggregate/snapshot_test.go`, `newSnapshotRuntime`'s pattern.

- [ ] **Step 1: Write the failing tests**

- `TestSnapshotAdapterSeedsALoad`: `store := openStore(t)`; `cache := sqlite.NewSnapshotCache(store)`; `rt := NewRuntime(store, counter.Decider, counterv1.EventCodec{}).WithSnapshots(cache, counterStateCodec{}, 1).WithSnapshotEvery(3)`; six commands; assert the cached version is 6. Then build a second runtime over `&countingStore{Store: store}` with the same cache and `Load`: the returned state matches the six commands' result and `rows == 1` (the lineage probe only — no tail).
- `TestSnapshotAdapterFoldVersionMismatch`: same setup, then a third runtime with `foldVersion: 2`; `Load` returns the correct state and `rows == 6` (full fold, no seed); one command then rewrites the cache (seed 0, `to >= 3`), assert the cached `FoldVersion` is 2.
- `TestWarmSnapshotArchivedStream`: `WithSnapshotEvery(100)`, five commands, cache absent; `WarmSnapshot` returns 5 and the cache holds it; a fresh runtime's `Load` over `countingStore` reads `rows == 1`.

- [ ] **Step 2: Run to verify they fail**

Run: `GOWORK=off go test ./aggregate/ -run 'TestSnapshotAdapter|TestWarmSnapshotArchived' -v`
Expected: FAIL — `undefined: sqlite.NewSnapshotCache` (until Task 6 lands) or a wrong `rows` count.

- [ ] **Step 3: Implement the wiring the tests need**

No production code: if a test needs a helper (e.g., a `fixedRecorder` or a second-runtime constructor), add it to `aggregate/snapshot_adapter_test.go`. If the `countingStore` counted the lineage probe as a row and the assertion is wrong, fix the assertion to `rows == 1` (probe) — do not change `countingStore`.

- [ ] **Step 4: Run to verify they pass**

Run: `GOWORK=off go test ./aggregate/ -run 'TestSnapshotAdapter|TestWarmSnapshotArchived' -v`, then `GOWORK=off go test ./...`, lint, gofmt.

- [ ] **Step 5: Commit**

```bash
git add aggregate/snapshot_adapter_test.go
git commit -m "test(aggregate): prove the snapshot adapters seed a real load"
```

---

### Task 8: Docs, runbook, status

**Files:**
- Modify: `README.md:292-304`, `docs/adr/0007-snapshots-deferred.md:6`, `docs/adr/0042-snapshot-write-policy.md` (status), `docs/adr/README.md:42`, `docs/deploy/README.md` (appender grants + backup/purge notes)

**Interfaces:** none.

- [ ] **Step 1: README**

Replace the "The library ships the interface, not a storage implementation" sentence and the code block with:

- the two adapters (`postgres.NewSnapshotCache(store, workspace)`, `sqlite.NewSnapshotCache(store)`);
- `WithSnapshotEvery(n)` semantics — not called = every append, `0` = never, `n` = gap since the seed — and why the threshold is safe;
- `WarmSnapshot` for archived streams and post-restore warm-up;
- the trust boundary: the cache is as protected as the log; with `WithChainVerification` a seeded load verifies only the tail, so a less-trusted store needs a MAC'ing `stateCodec` (ADR 0025);
- erasure: `Purge` on the adapter, matching a quiesced workspace or key destruction.

- [ ] **Step 2: ADR 0007 errata**

Replace the false `snapshot/natskv` errata with a dated correction: snapshots are implemented by `postgres.NewSnapshotCache` and `sqlite.NewSnapshotCache` (ADR 0042), sealing/erasure remain client-owned. State that the trigger, the four invalidation surfaces and the non-goals are unchanged; note that ADR 0042 supersedes the "no code until measured" clause for the implementation.

- [ ] **Step 3: ADR 0042 status and index**

`- **Status:** Proposed` → `- **Status:** Accepted (implemented)`; the index row `Proposed` → `Accepted (implemented)`.

- [ ] **Step 4: Deploy runbook**

- Appender grants: `GRANT SELECT, INSERT, UPDATE, DELETE ON snapshots TO <appender>;` with a line saying the command process both reads and writes it (the same role as `events`).
- Name reservation: `snapshots` is reserved; rename a pre-existing table before upgrading (0013 refuses an incompatible shape).
- Backups: exclude `snapshots` from logical backups if size matters (`--exclude-table=snapshots`); after a physical restore, missing snapshots rebuild lazily through the next append or `WarmSnapshot`.
- Erasure: `Purge` is workspace-scoped and best-effort; quiesce the workspace (or destroy the key first) before running it.

- [ ] **Step 5: Verify and commit**

Run: `GOWORK=off go test ./...`; `golangci-lint run ./...`; `git diff --check`.

```bash
git add README.md docs/adr/0007-snapshots-deferred.md docs/adr/0042-snapshot-write-policy.md docs/adr/README.md docs/deploy/README.md
git commit -m "docs: document the snapshot write policy, adapters and trust boundary"
```

---

## Release notes (for the tag)

- **New API**: `Runtime.WithSnapshotEvery(n)` (default write-through; `0` disables; `n` writes on the gap since the load's seed) and `Runtime.WarmSnapshot`.
- **New adapters**: `postgres.NewSnapshotCache(store, workspace)` and `sqlite.NewSnapshotCache(store)`, both with a workspace-scoped `Purge`.
- **Migrations**: Postgres `0013` (table + workspace RLS), SQLite `0011`. Additive.
- **Operational**: the `snapshots` name is reserved; the appender role needs `SELECT, INSERT, UPDATE, DELETE` on it; exclude it from logical backups if size matters.
- **Not breaking**: `snapshot.Cache` is unchanged; adopters' own caches keep working unchanged.
- Ships as `v0.19.0`.

## Out of scope

- The async writer, a separate database or schema, attestation of cached state, a proto knob, per-stream thresholds, and a public conformance package (ADR 0042 §9).
- Fixing the pre-existing `TestFeedWakeLatency` flake (issue #27).
