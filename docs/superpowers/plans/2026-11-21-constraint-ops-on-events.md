# Constraint Ops on the Event Log — Implementation Plan (v0.21.0)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `events` table the functional source of truth by recording each append's `ConstraintOp`s on the append's first event, so an events-only restore rebuilds the claims index; remove the `append_decisions` sidecar, its adopt path, and the ordering lock.

**Architecture:** A nullable `constraint_ops` column on `events` (Postgres `bytea`, SQLite `BLOB`) holds a binary, versioned encoding of the ops, written on the first event of the batch and decoded into `Envelope.ConstraintOps` on read. The field is excluded from `CanonicalEnvelope`. A constraint-bearing append with no events is rejected (`ErrConstraintWithoutEvent`), which also removes the claim-only ordering race. `unique_claims` stays the live index and becomes a projection of the events; rebuild/verify/`ClaimWorkspaces` read events.

**Tech Stack:** Go 1.27, `pgx`/Postgres 17, `modernc.org/sqlite`.

**Spec:** `docs/adr/0043-constraint-ops-on-events.md` (binding; read it before starting).

## Global Constraints

- No new module dependencies. Go 1.27; every command runs with `GOWORK=off`.
- Migrations are append-only (ADR 0010), one transaction, no grants. Next numbers: Postgres `0014`, SQLite `0012` (replace the sidecar migrations; the applied version stays 14/12).
- The ops are written **only** on the first event of a batch; every other event row has `NULL`.
- `ConstraintOps` is excluded from `CanonicalEnvelope`; the chain does not change. Do not alter `es/hash.go`'s canonical field list.
- `len(events) == 0 && len(constraints) > 0` is `es.ErrConstraintWithoutEvent`, enforced by `aggregate.Runtime` and by both stores' `validateAppend`.
- `ConstraintOp.Value` is opaque bytes. The codec is binary and versioned (`es-lite.constraint-ops.v1`), u32 count, per op u8 kind + length-prefixed scope/value; decode rejects wrong domain, unknown kind, over-length, trailing bytes.
- No `append_decisions`, no `AdoptClaims`, no `ErrClaimsAlreadyAdopted`, no advisory lock, no decision ids.
- The live `unique_claims` index stays authoritative on the append path; rebuild/verify replace or compare it.
- An inconsistent store (live claims with no op-bearing events) is refused with `es.ErrClaimsWithoutDecisions`; never delete the index it cannot reconstruct.
- Conventional commits; `gofmt` clean; `golangci-lint run ./...` clean; pristine test output.
- Postgres tests: `PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable'` (port 5434).

## Review Focus

1. **The hash must not move.** An event with ops and an identical event without them must both verify; `CanonicalEnvelope` must be untouched; tampering with `constraint_ops` must not break the chain (that is the documented trust boundary). Pinned in Task 1.
2. **First-event-only.** A multi-event batch writes the column on the first row and `NULL` on the rest; a claim/append round-trips through `ReadStream`/`ReadAll` with the ops on the first envelope. Pinned in Tasks 2 and 3.
3. **No invisible state.** A claim-only append is refused at the runtime *and* at both stores (direct callers), and no claim row is left behind. Pinned in Task 1 (runtime) and Tasks 2/3 (stores).
4. **Rebuild order is global position order**, and a release that matched nothing replays as a no-op. Pinned in Task 4.
5. **No compatibility residue.** No `append_decisions` table/column/code/test, no adopt flag, no `decision` vocabulary in the event model; the restore set is events (+ `log_order` for audit).

---

## File Structure

- `es/envelope.go` — `ConstraintOps []ConstraintOp`.
- `es/constraints_ops.go` (rename/replace `es/decisions.go`) — `EncodeConstraintOps`, `DecodeConstraintOps`, report types, `ClaimsMaintainer`, sentinels.
- `es/runtime` (`aggregate/runtime.go`) — the claim-only rejection after `Decide`.
- `postgres/migrations/0014_event_constraint_ops.sql` (replaces `0014_append_decisions.sql`).
- `postgres/store.go` — `envelopeCols`, `scanEnvelope`.
- `postgres/wsstore.go` — insert the column; drop `recordDecision` and the lock.
- `postgres/decisions.go` (rename to `claims.go`) — rebuild/verify from events.
- `postgres/anchor.go` — `ClaimWorkspaces` from `events`.
- `sqlite/migrations/0012_event_constraint_ops.sql` (replaces `0012_append_decisions.sql`).
- `sqlite/store.go` — `envelopeCols`, `scanEnvelope`, insert the column; drop `recordDecision`.
- `sqlite/decisions.go` (rename to `claims.go`) — rebuild/verify from events.
- `cmd/es-verify/main.go` — labels only (`ops`, not `decisions`); no sidecar references.
- Tests: `es/*`, `aggregate/*`, `postgres/*`, `sqlite/*`, `cmd/es-verify/*` — replace sidecar tests with event-ops tests; every claim-only test becomes a rejection or gains an event.
- `docs/deploy/README.md`, `docs/migration/2026-11-07-breaking-changes.md`, `README.md`, `docs/adr/README.md`.

---

### Task 1: The code pivot (all packages)

**Files:** the Go and migration files above, plus every test that currently assumes `append_decisions` or a claim-only append.

**Interfaces:**
- `es.Envelope` gains `ConstraintOps []ConstraintOp`.
- `func EncodeConstraintOps(ops []ConstraintOp) []byte`, `func DecodeConstraintOps(b []byte) ([]ConstraintOp, error)`.
- `var ErrConstraintWithoutEvent = errors.New("es-lite: constraints require at least one event")`.
- `var ErrClaimsWithoutDecisions = errors.New("es-lite: live claims have no decision history")`.
- `type ClaimsRebuildReport struct { Ops, Claims int64 }`; `type ClaimsVerifyReport struct { OK bool; Ops, Claims int64; FirstDivergence string }`.
- `type ClaimsMaintainer interface { RebuildClaims(context.Context) (ClaimsRebuildReport, error); VerifyClaims(context.Context) (ClaimsVerifyReport, error) }`.
- `func (s *Store) ClaimWorkspaces(ctx context.Context) ([]string, error)` (Postgres; unchanged signature, new query).

- [ ] **Step 1: Reset the local test database.** Replacing migration `0014`/`0012` means a local database that already recorded the old version will not re-apply it. Drop and recreate the Postgres test database (or `DROP SCHEMA public CASCADE; CREATE SCHEMA public;` and re-run `ensureRelayRole` via the tests). CI is fresh; this is local hygiene.

- [ ] **Step 2: Write the failing model tests.**

`es/envelope` / codec tests:
- `EncodeConstraintOps`/`DecodeConstraintOps` round-trip (including a non-UTF-8 value), empty list, exact layout, and the rejection table (nil, short, wrong domain, unknown kind, count too large, over-length, trailing bytes, every truncation).
- `CanonicalEnvelope` and `EnvelopeHash` produce the **same bytes/hash** for two envelopes that differ only in `ConstraintOps`; `VerifyStream` accepts a stream whose rows carry ops; a tampered `ConstraintOps` blob does not break the chain (assert the documented behavior, do not "fix" it).

`aggregate`:
- A `Decide` returning events + constraints still appends, and the first returned envelope carries the ops.
- A `Decide` returning zero events + constraints returns `es.ErrConstraintWithoutEvent` and writes nothing (no event, no claim).
- The existing no-op test (zero events, zero constraints) still succeeds.

`postgres`/`sqlite` integration:
- An append with 2 events and a claim writes `constraint_ops` on the first row only; `ReadStream` returns the ops on the first envelope and nil on the second; `ReadAll` (and the feed) return them.
- A direct `Append` with zero events + constraints returns `ErrConstraintWithoutEvent` and leaves `unique_claims` and `events` untouched.
- A release round-trips through the column and replays as a no-op when it matches nothing.

- [ ] **Step 3: Run the new tests to confirm they fail** for the expected reasons (undefined fields/functions, no column).

- [ ] **Step 4: Implement.**

- Migration: replace the sidecar files with `0014_event_constraint_ops.sql` (`ALTER TABLE events ADD COLUMN constraint_ops bytea;`) and `0012_event_constraint_ops.sql` (`ALTER TABLE events ADD COLUMN constraint_ops BLOB;`). No new table, no grants.
- `es`: the field, the codec, the sentinels, the reports/interface; update the doc comments to the events-only model. Delete the sidecar vocabulary (`decision` ids/tables, `AdoptClaims`, `ErrClaimsAlreadyAdopted`, the old `Encode/DecodeDecisionOps`).
- `aggregate/runtime.go`: after `Decide`, reject zero events + non-empty constraints.
- Postgres: add `constraint_ops` to `envelopeCols`; scan into `[]byte`/`*[]byte`, decode into `e.ConstraintOps`, fail the read on a corrupt blob; in the insert, encode once and pass it for `i == 0` (NULL otherwise); set `env.ConstraintOps` on the first envelope. Remove `recordDecision` and the advisory-lock key path; `applyOps` stays (release-then-claim).
- SQLite: the same, plus remove `recordDecision` and drop the table-name parameter if it is no longer needed (the rebuild still uses a temp table, so keep it if so).
- `ClaimWorkspaces`: `SELECT DISTINCT workspace_id FROM events ORDER BY 1` under `bypassTx`.
- Rebuild/verify: stream `SELECT global_position, constraint_ops FROM events WHERE workspace_id=$1 AND constraint_ops IS NOT NULL AND global_position > $2 ORDER BY global_position LIMIT $3` (keyset paging in Postgres; bounded pages in SQLite), decode, apply release-then-claim. `VerifyClaims` builds the temp table and diffs both directions. Keep the `ErrClaimsWithoutDecisions` guard.
- CLI: the output labels ("N op records" / "N decisions" — pick one and use it everywhere); no sidecar/adopt references.
- Remove every `append_decisions` test, table, grant, and comment; update every claim-only test to either include an event or assert `ErrConstraintWithoutEvent`. Use `grep -rn "Constraints:" --include='*_test.go'` and check each zero-event case.

- [ ] **Step 5: Run and fix.**

```bash
GOWORK=off go test -count=1 ./...
PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test -count=1 ./postgres/
GOWORK=off go test -race -count=1 ./aggregate/ ./sqlite/ ./feed/ ./postgres/
gofmt -l es postgres sqlite cmd aggregate feed snapshot
golangci-lint run ./...
```

- [ ] **Step 6: Commit.**

```bash
git add -A
git commit -m "feat(es): record constraint ops on the event log"
```

---

### Task 2: Least privilege, feed coverage, docs, final gate

**Files:**
- Modify: `postgres/leastpriv_test.go`
- Modify: `docs/deploy/README.md`, `docs/migration/2026-11-07-breaking-changes.md`, `README.md`, `docs/adr/README.md`, `docs/adr/0043-constraint-ops-on-events.md` (status)
- Delete: `docs/superpowers/plans/2026-11-21-append-decisions.md` (superseded by this plan)

- [ ] **Step 1: Least privilege.**

Remove the `append_decisions` grant lines and the sequence grant from the appender block (the table is gone). Keep the event `SELECT, INSERT`. Drive a claim-bearing append with an event through the appender role, assert `ReadStream` returns the ops, then run `RebuildClaims`/`VerifyClaims` through the appender's `*Workspace` and assert `OK` and a non-zero `Claims`. Keep the `docs/deploy/README.md` appender block byte-identical.
- Add a feed/replica test proving the ops survive `ReadAll`/the feed (if not already in Task 1).

- [ ] **Step 2: Docs.**

- Runbook "Backup, restore and clones": the authoritative set is **events** (functional) and optionally `log_order` (order verification); remove `append_decisions`, the decision sequence, the adopt/cutover section, and the two-table restore. The restore steps: migrations, roles, load events (+ `log_order` if auditing), `setval('events_global_position_seq', max(global_position))`, rebuild claims, verify. Keep the clone follow/fork rules, with `log_order` optional and `command_dedup` excluded. Rewrite "Erasing claims": the op is in the event log, so a plaintext claim value is permanent; MAC + key destruction; deleting `unique_claims` is a cache wipe, not erasure.
- Migration note: new column, no new table, no new grants, `ErrConstraintWithoutEvent`, no legacy path, rollback (drop the column or redeploy; old binaries ignore it).
- README: the "Backups and replay" bullet becomes "events are the functional source; `log_order` is the order witness; claims replay from the ops on the first event; a plaintext claim value is permanent (MAC it)"; remove the cutover/append-decision text and fix the ADR links.
- ADR: status → `Accepted (implemented)`; index row matches; no stale sidecar/cutover text anywhere (`grep -rni "append_decisions\|adopt-existing\|cutover" docs README.md`).
- Delete the superseded plan file.

- [ ] **Step 3: Final gate.**

```bash
GOWORK=off go test -count=1 ./...
PG_DSN='postgres://postgres:pw@127.0.0.1:5434/eslite?sslmode=disable' GOWORK=off go test -count=1 ./postgres/
GOWORK=off go test -race -count=1 ./aggregate/ ./sqlite/ ./feed/ ./postgres/
gofmt -l es postgres sqlite cmd aggregate feed snapshot
golangci-lint run ./...
grep -rn "append_decisions\|AdoptClaims\|adopt-existing" --include='*.go' --include='*.md' . || echo clean
```

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -m "docs: state the events-only restore contract"
```

---

## Self-Review Notes

- **Spec coverage:** §2.1 field/column/codec/error → Task 1; §2.2 projection → Task 1; §2.3 restore set → Task 2; §2.4 erasure → Task 2; §2.5 no legacy → Task 1 code + Task 2 docs; §2.6 dedup → Task 2; §2.7 clones → Task 2.
- **Deliberately out of scope:** a v2 envelope that chain-attests the ops; a replica applier; per-value erasure of plaintext claims; `command_dedup` retention.
- **Naming is fixed:** `ConstraintOps`, `EncodeConstraintOps`, `DecodeConstraintOps`, `ErrConstraintWithoutEvent`, `ErrClaimsWithoutDecisions`, `ClaimsRebuildReport{Ops, Claims}`, `ClaimsMaintainer`.
