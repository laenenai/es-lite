# Append Decisions and Log-Only Restore Implementation Plan (v0.21.0)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record every append's non-event decision (its `ConstraintOp`s) in the append transaction, make `unique_claims` a rebuildable index, and ship the rebuild/verify/cutover tooling plus the restore and clone contract.

**Architecture:** A new `append_decisions` table gets one row per constraint-bearing append, written after `applyClaims` inside the existing append transaction (covering claim-only appends that write no events). `unique_claims` remains the live, enforcing index and becomes derivable by replaying those rows in `id` order. A shared binary codec lives in `es`; both adapters implement `es.ClaimsMaintainer` (rebuild, verify, adopt); `es-verify claims` is the operator surface.

**Tech Stack:** Go 1.27, `pgx`/Postgres 17, `modernc.org/sqlite`.

**Spec:** `docs/adr/0043-append-decisions-and-restore.md` (binding design; read it before starting). The operational contract is the "Backup, restore and clones" section of `docs/deploy/README.md`.

## Global Constraints

- No new module dependencies. Go 1.27; every command runs with `GOWORK=off`.
- Migrations are append-only (ADR 0010), run in one transaction, and issue **no grants**: next numbers are Postgres `0014` and SQLite `0012`.
- No change to `es.Store`, `es.Decide`, `es.AppendParams`, the chain, `log_order`, anchors, the feed, or snapshots.
- The decision record is unconditional: there is no option to disable it. It is written only when `len(p.Constraints) > 0`, **after** `applyClaims` and inside the same transaction; a rollback leaves no row.
- `es.Envelope` and `CanonicalEnvelope` are untouched; the hash chain does not cover the decision row.
- Postgres RLS mirrors `unique_claims`: workspace arm plus the role-gated `es_relay` bypass arm in the form hardened by `0007_policy_hardening.sql`, `ENABLE` + `FORCE ROW LEVEL SECURITY`. SQLite is single-workspace: no `workspace_id`, no RLS.
- `ConstraintOp.Value` is an opaque byte string. The ops encoding is binary and versioned; no JSON, no proto.
- `command_id` on a live append is `p.CommandID.String()` (the zero UUID when unset), matching what `events` stores. `NULL` is reserved for the one-time cutover backfill.
- Conventional commits; `gofmt` clean; `golangci-lint run ./...` clean.
- Postgres tests: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable'` (port 5434; 5432/5433 belong to other projects).

## Review Focus

1. **A missing grant fails every constraint-bearing append.** The decision insert is in the append transaction, so a missing table/grant/sequence privilege rolls the whole append back. That is loud but total. Pinned in Task 8 (least-privilege, including a claim-only append) and in the migration note (Task 9): grant before deploying the binary.
2. **The claim-only append must be recorded, and a rollback must not be.** Zero events, `from_version == to_version`, exactly one row; a colliding claim must leave the count unchanged. Pinned in Tasks 3 and 4.
3. **Rebuilding on a pre-cutover database must refuse, not wipe.** Live claims with an empty decision log means "the cutover has not run"; `RebuildClaims` returns `es.ErrClaimsNotAdopted` and deletes nothing. Pinned in Tasks 5, 6, 7.
4. **The cutover backfill must be once-only.** `AdoptClaims` refuses when `append_decisions` is non-empty, because a duplicate Claim row would make every later rebuild fail on the unique index. Pinned in Tasks 5, 6, and the CLI.
5. **Replay order is load-bearing.** Rows in `id` order, releases before claims within a row, `id` gaps allowed (a rolled-back append consumes an id). A release that matched nothing replays as a no-op. Pinned in Tasks 5 and 7.

---

## File Structure

- `es/decisions.go` (new) — codec, report types, `ClaimsMaintainer`, sentinels.
- `es/decisions_test.go` (new) — codec round-trip and rejection table.
- `postgres/migrations/0014_append_decisions.sql` (new) — table, RLS, replay index.
- `postgres/wsstore.go` — `applyOps` extraction, `recordDecision`, call site.
- `postgres/decisions.go` (new) — `RebuildClaims`, `VerifyClaims`, `AdoptClaims`.
- `postgres/decisions_integration_test.go` (new) — PG-gated behaviour.
- `postgres/postgres_test.go` — truncate list, ledger version.
- `postgres/migrations_test.go` — ledger version assertions.
- `sqlite/migrations/0012_append_decisions.sql` (new).
- `sqlite/store.go` — `applyOps` extraction, `recordDecision`, call site.
- `sqlite/decisions.go` (new).
- `sqlite/decisions_test.go` (new).
- `sqlite/migrate_test.go`, `sqlite/populated_test.go` — ledger version.
- `cmd/es-verify/main.go`, `cmd/es-verify/main_test.go` — the `claims` subcommand.
- `postgres/leastpriv_test.go` — grants and the claim-only flow.
- `docs/migration/2026-11-21-append-decisions.md` (new), `README.md`, `docs/deploy/README.md` — docs.

---

### Task 1: The decision-record codec

**Files:**
- Create: `es/decisions.go`
- Test: `es/decisions_test.go`

**Interfaces:**
- Produces:
  - `func EncodeDecisionOps(ops []ConstraintOp) []byte`
  - `func DecodeDecisionOps(b []byte) ([]ConstraintOp, error)`
  - `type ClaimsRebuildReport struct { Decisions, Claims int64 }`
  - `type ClaimsVerifyReport struct { OK bool; Decisions, Claims int64; FirstDivergence string }`
  - `type ClaimsMaintainer interface { RebuildClaims(context.Context) (ClaimsRebuildReport, error); VerifyClaims(context.Context) (ClaimsVerifyReport, error); AdoptClaims(context.Context) (int64, error) }`
  - `var ErrClaimsAlreadyAdopted = errors.New("es-lite: append decisions already exist")`
  - `var ErrClaimsNotAdopted = errors.New("es-lite: live claims have no decision history")`

- [ ] **Step 1: Write the failing codec tests**

In `es/decisions_test.go` (`package es`):

```go
func TestDecisionOpsRoundTrip(t *testing.T) {
	ops := []ConstraintOp{
		Claim("email", "a@b"),
		Release("email", "c\nd"),
		Claim("slug", string([]byte{0xff, 0x00, 0x80})),
	}
	got, err := DecodeDecisionOps(EncodeDecisionOps(ops))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, ops) {
		t.Fatalf("round trip = %#v, want %#v", got, ops)
	}
}

func TestDecisionOpsEmpty(t *testing.T) {
	b := EncodeDecisionOps(nil)
	got, err := DecodeDecisionOps(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("empty ops decode to %d ops, want 0", len(got))
	}
}

func TestDecisionOpsLayout(t *testing.T) {
	b := EncodeDecisionOps([]ConstraintOp{Claim("email", "a@b")})
	want := append([]byte{0, 0, 0, 27}, []byte("es-lite.append-decisions.v1")...)
	want = append(want, 0, 0, 0, 1, 0, 0, 0, 0, 5)
	want = append(want, []byte("email")...)
	want = append(want, 0, 0, 0, 3)
	want = append(want, []byte("a@b")...)
	if !bytes.Equal(b, want) {
		t.Fatalf("encoding =\n%x\nwant\n%x", b, want)
	}
}

func TestDecisionOpsRejects(t *testing.T) {
	good := EncodeDecisionOps([]ConstraintOp{Claim("email", "a@b")})
	cases := map[string][]byte{
		"nil":            nil,
		"short":          {0, 0},
		"wrong domain":   append(append([]byte{0, 0, 0, 27}, bytes.Repeat([]byte{'x'}, 27)...), good[31:]...),
		"trailing bytes": append(append([]byte{}, good...), 0),
		"unknown kind":   func() []byte { b := append([]byte{}, good...); b[35] = 9; return b }(),
		"count too high": func() []byte { b := append([]byte{}, good...); b[31] = 0; b[34] = 2; return b }(),
		"huge length":    func() []byte { b := append([]byte{}, good...); b[36], b[37], b[38], b[39] = 0xff, 0xff, 0xff, 0xff; return b }(),
	}
	for name, b := range cases {
		if _, err := DecodeDecisionOps(b); err == nil {
			t.Errorf("%s: DecodeDecisionOps accepted malformed input", name)
		}
	}
	for i := 0; i < len(good); i++ {
		if _, err := DecodeDecisionOps(good[:i]); err == nil {
			t.Errorf("truncated at %d: accepted", i)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `GOWORK=off go test ./es -run 'TestDecisionOps' -v`
Expected: FAIL — `undefined: EncodeDecisionOps`, `undefined: DecodeDecisionOps`.

- [ ] **Step 3: Implement the codec and types in `es/decisions.go`**

Layout: `appendString("es-lite.append-decisions.v1")` — the helper already lives in `es/hash.go` — (u32 big-endian length, then bytes), `u32 count`, then per op `u8 kind` (0 Claim, 1 Release), `u32 len(scope) ‖ scope`, `u32 len(value) ‖ value`. Decode must reject a wrong domain, an unknown kind, a count larger than the remaining byte count, any length that exceeds the remaining buffer, and trailing bytes; never preallocate from `count`. Wrap errors with `fmt.Errorf("es-lite: decision ops: …")` and return `ErrInvalidConstraint` only where a kind is unknown.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `GOWORK=off go test ./es -run 'TestDecisionOps' -v`
Expected: PASS. Then `gofmt -l es` (empty) and `golangci-lint run ./es/` (0 issues).

- [ ] **Step 5: Commit**

```bash
git add es/decisions.go es/decisions_test.go
git commit -m "feat(es): add the append-decision ops codec"
```

---

### Task 2: Postgres schema and the append-side write

**Files:**
- Create: `postgres/migrations/0014_append_decisions.sql`
- Modify: `postgres/wsstore.go` (the `append` method, after the `applyClaims` call)
- Test: `postgres/decisions_integration_test.go` (new)
- Modify: `postgres/postgres_test.go` (truncate list at ~61, ledger version at ~402)
- Modify: `postgres/migrations_test.go` (ledger version assertions at ~364, ~444, ~539, ~597, ~621)

**Interfaces:**
- Consumes: `es.EncodeDecisionOps`, `es.Claim`, `DBTX` (`postgres/wsstore.go`).
- Produces: `func (w *Workspace) recordDecision(ctx context.Context, tx DBTX, streamID string, p es.AppendParams) error` (unexported).

- [ ] **Step 1: Write the migration**

`postgres/migrations/0014_append_decisions.sql`, opening comment explaining ADR 0043, the append-transaction ordering, the claim-only case, the backfill's `NULL` command id, and that no grants are issued here:

```sql
CREATE TABLE IF NOT EXISTS append_decisions (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workspace_id text        NOT NULL,
    stream_id    text        NOT NULL,
    from_version bigint      NOT NULL,
    to_version   bigint      NOT NULL,
    command_id   text        NULL,
    ops          bytea       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (from_version <= to_version)
);

CREATE INDEX IF NOT EXISTS append_decisions_workspace_id_idx
    ON append_decisions (workspace_id, id);

ALTER TABLE append_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE append_decisions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS append_decisions_workspace_isolation ON append_decisions;
CREATE POLICY append_decisions_workspace_isolation ON append_decisions
    USING ( /* copy the unique_claims policy expression verbatim from 0007_policy_hardening.sql:42-45 */ )
    WITH CHECK ( /* the same expression */ );
```

Copy the two expressions byte-for-byte from the `unique_claims_workspace_isolation` policy; only the table and policy names change.

- [ ] **Step 2: Write the failing integration tests**

`postgres/decisions_integration_test.go`, `package postgres_test`, using `openStore(t)` from `postgres/postgres_test.go` and `streamID(t)`. Use `s.Pool()` for the raw queries.

- `TestAppendWritesDecisionRecord`: append one event with `Constraints: []es.ConstraintOp{es.Claim("email","a@b")}`; query `SELECT from_version, to_version, command_id, ops FROM append_decisions`; assert exactly one row, `from=0`, `to=1`, `command_id == params.CommandID.String()`, and `es.DecodeDecisionOps(ops)` equals the claim.
- `TestAppendRollbackWritesNoDecision`: append with the same claim from another stream; expect `es.ErrConstraintViolated`; assert the row count is still 1.
- `TestClaimOnlyAppendWritesDecision`: append with no events and `Constraints: []es.ConstraintOp{es.Claim("email","only@b")}`; assert one row with `from_version == to_version == 0`, one claim in `unique_claims`, and zero events.
- `TestAppendDecisionsWorkspaceIsolation`: write through workspace A, then through workspace B; in B, `SELECT count(*) FROM append_decisions` is 1 and the B row is not visible. Follow the RLS test pattern in `postgres/snapshot_test.go`.

Add `append_decisions` to the `TRUNCATE` list in `postgres/postgres_test.go`, and raise every applied-version assertion from 13 to 14.

Extend `TestAllBypassPoliciesRoleGated` in `postgres/migrations_test.go`: add `append_decisions` to its table list and change the "All four policies" wording to five. The test is a structural pin, not a red-first behaviour test — it must fail if the new policy is workspace-only or drops the `EXISTS` guard.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test ./postgres -run 'TestAppend.*Decision|TestClaimOnly.*Decision|TestAppendDecisionsWorkspace' -v`
Expected: FAIL — the migration exists and applies, but the append writes no decision row (zero rows where the test wants one), because `recordDecision` is not implemented.

- [ ] **Step 4: Implement `recordDecision` and call it**

In `postgres/wsstore.go`, extract the existing claim application into:

```go
func applyOps(ctx context.Context, tx DBTX, table, streamID string, ops []es.ConstraintOp) error
```

`table` is a closed set (`unique_claims` is the only caller in this task; Task 5 adds the temp table). Keep the release-then-claim order and the same error wrapping. Replace the `applyClaims` call with `applyOps(ctx, db, "unique_claims", canonical, p.Constraints)`.

Immediately after it, before `notifyAppend`:

```go
if len(p.Constraints) > 0 {
    if err := w.recordDecision(ctx, db, canonical, p); err != nil {
        return zero, err
    }
}
```

`recordDecision` inserts `(workspace_id, stream_id, from_version, to_version, command_id, ops)` with `w.ws`, the canonical id, `p.ExpectedVersion`, `p.ExpectedVersion + uint64(len(p.Events))`, `p.CommandID.String()`, and `es.EncodeDecisionOps(p.Constraints)`, wrapping errors as `postgres: append decision: %w`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test -count=1 ./postgres -run 'TestAppend.*Decision|TestClaimOnly.*Decision|TestAppendDecisionsWorkspace|TestAllBypassPoliciesRoleGated' -v`, then the full `./postgres/` suite. Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add postgres/migrations/0014_append_decisions.sql postgres/wsstore.go postgres/decisions_integration_test.go postgres/postgres_test.go postgres/migrations_test.go
git commit -m "feat(postgres): record append decisions in the append transaction"
```

---

### Task 3: SQLite schema and the append-side write

**Files:**
- Create: `sqlite/migrations/0012_append_decisions.sql`
- Modify: `sqlite/store.go` (`applyClaims` and its call site)
- Test: `sqlite/decisions_test.go` (new)
- Modify: `sqlite/migrate_test.go` (~28 and the 0011 comments), `sqlite/populated_test.go` (~19, ~64)

**Interfaces:**
- Consumes: `es.EncodeDecisionOps`.
- Produces: `func (s *Store) recordDecision(ctx context.Context, tx *sql.Tx, streamID string, p es.AppendParams) error` (unexported) and `applyClaims(ctx, tx, table, streamID, ops)` with the extra table argument.

- [ ] **Step 1: Write the migration**

`sqlite/migrations/0012_append_decisions.sql`:

```sql
CREATE TABLE IF NOT EXISTS append_decisions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id    TEXT    NOT NULL,
    from_version INTEGER NOT NULL,
    to_version   INTEGER NOT NULL,
    command_id   TEXT    NULL,
    ops          BLOB    NOT NULL,
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (from_version <= to_version)
);
```

`AUTOINCREMENT` is deliberate: ids must not be reused after a `DELETE`, because `id` is the replay order.

- [ ] **Step 2: Write the failing tests**

`sqlite/decisions_test.go`, `package sqlite_test`, using `openStore(t)` from `sqlite/store_test.go`; `s.DB()` gives the raw handle for direct row manipulation. Mirror Task 2 without RLS:
`TestAppendWritesDecisionRecord`, `TestAppendRollbackWritesNoDecision`, `TestClaimOnlyAppendWritesDecision`. Use the existing `openStore` helper in the package.

Also add `TestAppendDecisionsTableColumns` next to `TestSnapshotsTableColumns` in `sqlite/migrate_test.go`: assert the seven columns and the `INTEGER` primary key via `PRAGMA table_info(append_decisions)`.

Raise the applied version from 11 to 12 in `sqlite/migrate_test.go:28` (and its comment) and `sqlite/populated_test.go` (the two 11s).

- [ ] **Step 3: Run the tests to verify they fail**

Run: `GOWORK=off go test ./sqlite -run 'TestAppend.*Decision|TestClaimOnly.*Decision|TestAppendDecisionsTableColumns' -v`
Expected: FAIL — `no such table: append_decisions`.

- [ ] **Step 4: Implement the write**

Change `applyClaims` to `applyClaims(ctx, tx, table, streamID, ops)`, passing `"unique_claims"` at the call site, and add the same `if len(p.Constraints) > 0 { s.recordDecision(...) }` immediately after it. Insert with `p.CommandID.String()` and `es.EncodeDecisionOps(p.Constraints)`; wrap as `sqlite: append decision: %w`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `GOWORK=off go test -count=1 ./sqlite -run 'TestAppend.*Decision|TestClaimOnly.*Decision|TestAppendDecisionsTableColumns' -v`, then the full `./sqlite/` suite.

- [ ] **Step 6: Commit**

```bash
git add sqlite/migrations/0012_append_decisions.sql sqlite/store.go sqlite/decisions_test.go sqlite/migrate_test.go sqlite/populated_test.go
git commit -m "feat(sqlite): record append decisions in the append transaction"
```

---

### Task 4: Rebuild, verify and adopt — Postgres

**Files:**
- Create: `postgres/decisions.go`
- Test: extend `postgres/decisions_integration_test.go`

**Interfaces:**
- Consumes: `es.ClaimsMaintainer`, `es.ClaimsRebuildReport`, `es.ClaimsVerifyReport`, `es.ErrClaimsAlreadyAdopted`, `es.ErrClaimsNotAdopted`, `es.DecodeDecisionOps`, `applyOps`.
- Produces: methods on `*postgres.Workspace`:
  - `func (w *Workspace) RebuildClaims(ctx context.Context) (es.ClaimsRebuildReport, error)`
  - `func (w *Workspace) VerifyClaims(ctx context.Context) (es.ClaimsVerifyReport, error)`
  - `func (w *Workspace) AdoptClaims(ctx context.Context) (int64, error)`
  - `var _ es.ClaimsMaintainer = (*Workspace)(nil)`

- [ ] **Step 1: Write the failing tests**

In `postgres/decisions_integration_test.go`:

- `TestRebuildClaimsFromDecisions`: claim A, claim B, release A; delete every `unique_claims` row directly; `RebuildClaims` returns `Decisions == 3, Claims == 1`; `LookupClaim(B)` finds it and `LookupClaim(A)` does not; `VerifyClaims` reports `OK`.
- `TestRebuildRefusesPreCutoverState`: claim A; delete the `append_decisions` rows for the workspace; `RebuildClaims` returns `es.ErrClaimsNotAdopted` and the claim row is still there.
- `TestVerifyClaimsReportsDivergence`: claim A; insert a bogus `unique_claims` row directly; `VerifyClaims` returns `OK == false` and a non-empty `FirstDivergence` naming the bogus scope/stream.
- `TestAdoptExistingClaims`: claim A and B; delete the decision rows; `AdoptClaims` returns 2; delete `unique_claims`; `RebuildClaims` reproduces A and B; `VerifyClaims` is OK.
- `TestAdoptRefusesWhenDecisionsExist`: after a live append, `AdoptClaims` returns `es.ErrClaimsAlreadyAdopted`.

Use `s.Pool()` for the raw deletes/inserts and `set_config`/`s.Workspace(ws)` as the neighbouring tests do.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test ./postgres -run 'TestRebuild|TestVerifyClaims|TestAdopt' -v`
Expected: FAIL — the methods are undefined.

- [ ] **Step 3: Implement the three methods in `postgres/decisions.go`**

All three open one transaction, call `setWorkspace(ctx, tx, w.ws)`, and commit at the end.

- `RebuildClaims`: `SELECT count(*) FROM unique_claims`; `SELECT count(*) FROM append_decisions`; if decisions == 0 && claims > 0, return `es.ErrClaimsNotAdopted`. Then `DELETE FROM unique_claims`, stream `SELECT id, stream_id, ops FROM append_decisions ORDER BY id`, `DecodeDecisionOps`, `applyOps(ctx, tx, "unique_claims", streamID, ops)`, count rows processed. Return `{Decisions, Claims}` where `Claims` is `SELECT count(*) FROM unique_claims` after replay.
- `VerifyClaims`: `CREATE TEMP TABLE claims_rebuild (LIKE unique_claims INCLUDING ALL) ON COMMIT DROP`; replay the same stream into `"claims_rebuild"`; run

  ```sql
  (SELECT scope, value_key, stream_id FROM unique_claims
   EXCEPT
   SELECT scope, value_key, stream_id FROM claims_rebuild)
  UNION ALL
  (SELECT scope, value_key, stream_id FROM claims_rebuild
   EXCEPT
   SELECT scope, value_key, stream_id FROM unique_claims)
  LIMIT 1
  ```

  and format a row as `scope=%s value=%x stream=%s`. An empty result is `OK`. Never return OK when the decision read failed.
- `AdoptClaims`: `SELECT count(*) FROM append_decisions`; if > 0 return `es.ErrClaimsAlreadyAdopted`. Stream `SELECT scope, value_key, stream_id FROM unique_claims ORDER BY scope, value_key`, and insert one row per claim with `from_version = to_version = 0`, `command_id = NULL`, `ops = es.EncodeDecisionOps([]es.ConstraintOp{es.Claim(scope, string(valueKey))})`, the claim's stream id, and `w.ws`. Return the count.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test -count=1 ./postgres -run 'TestRebuild|TestVerifyClaims|TestAdopt' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add postgres/decisions.go postgres/decisions_integration_test.go
git commit -m "feat(postgres): rebuild, verify and adopt the claims index"
```

---

### Task 5: Rebuild, verify and adopt — SQLite

**Files:**
- Create: `sqlite/decisions.go`
- Test: `sqlite/decisions_test.go`

**Interfaces:**
- Same as Task 4, on `*sqlite.Store`; `var _ es.ClaimsMaintainer = (*Store)(nil)`.

- [ ] **Step 1: Write the failing tests**

Mirror Task 4's five tests without workspaces; use `s.DB()` for the raw deletes/inserts. For the temp table use:

```sql
CREATE TEMP TABLE claims_rebuild (
    scope      TEXT NOT NULL,
    value_key  BLOB NOT NULL,
    stream_id  TEXT NOT NULL,
    claimed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (scope, value_key)
)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `GOWORK=off go test ./sqlite -run 'TestRebuild|TestVerifyClaims|TestAdopt' -v`
Expected: FAIL — undefined methods.

- [ ] **Step 3: Implement the three methods in `sqlite/decisions.go`**

One `sql.Tx` each; `applyClaims(ctx, tx, "claims_rebuild", ...)` for the replay. Same guards and the same EXCEPT/UNION ALL diff as Task 4. SQLite has no RLS, so no workspace plumbing.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `GOWORK=off go test -count=1 ./sqlite -run 'TestRebuild|TestVerifyClaims|TestAdopt' -v`, then `GOWORK=off go test -count=1 ./sqlite/`.

- [ ] **Step 5: Commit**

```bash
git add sqlite/decisions.go sqlite/decisions_test.go
git commit -m "feat(sqlite): rebuild, verify and adopt the claims index"
```

---

### Task 6: `es-verify claims`

**Files:**
- Modify: `cmd/es-verify/main.go` (dispatch at ~80-95, usage, `backend` and `openBackend` at ~312)
- Test: `cmd/es-verify/main_test.go`

**Interfaces:**
- Consumes: `es.ClaimsMaintainer` on `postgres.Workspace` and `sqlite.Store`.
- Produces: `es-verify claims [--rebuild | --adopt-existing] [<workspace>]`, exit 0 OK/rebuilt, 1 divergence, 2 usage/connection/refusal.

- [ ] **Step 1: Write the failing CLI tests**

In `main_test.go`, using `newTestDB` and public store calls:

- `TestClaimsCommandEmpty`: `run([]string{"claims"})` on a fresh DB returns 0.
- `TestClaimsCommandRebuildAndVerify`: seed a claim through `s.Append`; run `claims --rebuild` (expect 0, output contains `rebuilt`), then `claims` (expect 0); delete all `unique_claims` rows via `s.RebuildClaims`? No — instead assert rebuild is idempotent by running it twice and verifying OK after each.
- `TestClaimsCommandAdoptRefuses`: seed a claim (which now writes a decision row); `run([]string{"claims","--adopt-existing"})` returns 2 and stderr mentions the cutover; assert the exit code is 2 and no row was added.
- `TestClaimsCommandFlagsConflict`: `--rebuild --adopt-existing` returns 2.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `GOWORK=off go test ./cmd/es-verify -run TestClaimsCommand -v`
Expected: FAIL — the `claims` command is not dispatched.

- [ ] **Step 3: Implement the subcommand**

- Add to `backend`: `claims func(workspace string) es.ClaimsMaintainer`.
- Postgres wiring: `claims: func(ws string) es.ClaimsMaintainer { return store.Workspace(ws) }`. SQLite: `claims: func(string) es.ClaimsMaintainer { return store }`.
- `cmdClaims`: flags `--rebuild` and `--adopt-existing` (mutually exclusive). Resolve workspaces exactly like `stream`/`all`: an explicit argument wins; on Postgres with no argument, collect the distinct `ref.Workspace` from `b.ListStreams(ctx)`; SQLite uses one empty workspace.
- `--adopt-existing` prints `es-verify: adopt the claims cutover with writers stopped` to stderr, calls `AdoptClaims`, prints the count, and returns 0.
- `--rebuild` calls `RebuildClaims` per workspace, prints `claims rebuilt: N decisions, M claims`, and returns 0; map `es.ErrClaimsNotAdopted` and `es.ErrClaimsAlreadyAdopted` to exit 2 with their message.
- Default calls `VerifyClaims` per workspace; print `claims OK: N decisions` or `DIVERGED: <FirstDivergence>` and return 1 on any divergence.
- Add the usage lines.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `GOWORK=off go test ./cmd/es-verify -v`, then `GOWORK=off go test ./...`.

- [ ] **Step 5: Commit**

```bash
git add cmd/es-verify/main.go cmd/es-verify/main_test.go
git commit -m "feat(es-verify): add the claims rebuild and verify command"
```

---

### Task 7: Pin the appender grant and the claim-only flow

**Files:**
- Modify: `postgres/leastpriv_test.go`

**Interfaces:**
- Consumes: the runbook's appender block, `Workspace.recordDecision`.

- [ ] **Step 1: Extend the failing test**

In `grantLeastPrivilege`, add to the appender entry, keeping the statements byte-consistent with the "Backup, restore and clones" grant snippet and the runbook's appender block:

```go
`GRANT SELECT, INSERT, DELETE ON append_decisions TO ` + lpAppender + `;`,
`GRANT USAGE ON SEQUENCE append_decisions_id_seq TO ` + lpAppender + `;`,
```

In `TestLeastPrivilegeEndToEnd` Facet 1, after the existing claim/release, read `before := SELECT count(*) FROM append_decisions` through the appender probe's own pool, add a claim-only append under `lpAppender` (`Constraints: []es.ConstraintOp{es.Claim("email","lp-only@example.test")}`, no events), then assert the count is `before+1`, that `from_version = to_version`, and that the row with the greatest `id` decodes to that claim. Do not hard-code an absolute count: the earlier facets' row count is not this test's contract.

- [ ] **Step 2: Run it to verify it fails**

Run: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test ./postgres -run TestLeastPrivilegeEndToEnd -v`
Expected: FAIL — `permission denied for table append_decisions` until the grant and pool call are added.

- [ ] **Step 3: Make it pass**

Add the grants and the exercise; no production code should be needed.

- [ ] **Step 4: Full verification**

```bash
PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test -count=1 ./postgres/
GOWORK=off go test -race -count=1 ./aggregate/ ./sqlite/ ./feed/ ./postgres/
GOWORK=off go test ./...
gofmt -l es postgres sqlite cmd
golangci-lint run ./...
```

- [ ] **Step 5: Commit**

```bash
git add postgres/leastpriv_test.go
git commit -m "test(postgres): pin the append_decisions appender grant"
```

---

### Task 8: Docs and the adopter migration note

**Files:**
- Create: `docs/migration/2026-11-21-append-decisions.md`
- Modify: `docs/deploy/README.md` (the "Backup, restore and clones" section and the appender grant block)
- Modify: `README.md`

- [ ] **Step 1: Write the migration note**

Cover: what changed (new table, new grant, new row per constraint-bearing append; no API break), the required order — **migrate, then grant, then deploy** (the grant needs the table; the deploy needs the grant, or every constraint-bearing append fails) — the once-only cutover, how to verify, and the rollback story (dropping the table loses only rebuildability; the live index is unchanged).

- [ ] **Step 2: Reconcile the runbook**

- Add the two grant lines to the **Appender** block (the block is the current contract at v0.21.0), and keep the same two statements in the restore section's step 2 so the operator sees them in both places.
- Make every command name and flag in the section match the implementation (`es-verify claims`, `--rebuild`, `--adopt-existing`) and every exit code match.
- Confirm the grant statements are byte-identical to `leastpriv_test.go`.

- [ ] **Step 3: README**

Add one paragraph near the existing log/backup material: the authoritative set is three append-written tables; `unique_claims` is derived and rebuildable; the runbook link; and the limits — the stream restores to a consistent cut, not arbitrary PITR, and does not carry `command_dedup`.

- [ ] **Step 4: Verify**

```bash
git diff --check
GOWORK=off go test ./...
grep -n "append_decisions" docs/deploy/README.md docs/migration/2026-11-21-append-decisions.md README.md
```

- [ ] **Step 5: Commit**

```bash
git add README.md docs/deploy/README.md docs/migration/2026-11-21-append-decisions.md
git commit -m "docs: record the append-decisions upgrade and restore contract"
```

---

## Self-Review Notes

- **Spec coverage:** §3.1 table → Tasks 2/3; §3.2 rebuild + oracle → Tasks 4/5/6; §3.3 restore set → Task 8; §3.4 erasure (`DELETE` grant) → Tasks 2/7; §3.5 cutover (`--adopt-existing`, refuse twice) → Tasks 4/5/6; §3.6 dedup stays out → Task 8; §3.7 feed contract → Task 8 (documentation; the applier is deliberately out of tree).
- **Deliberately out of scope:** an applier/replication engine, retention for `append_decisions`, proto-typed ops, and any change to `es.Store`.
- **Naming is fixed across tasks:** `recordDecision`, `applyOps`, `RebuildClaims`, `VerifyClaims`, `AdoptClaims`, `es.ClaimsMaintainer`, `es.ErrClaimsNotAdopted`, `es.ErrClaimsAlreadyAdopted`.
