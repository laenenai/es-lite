# ADR 0010: Schema Migrations

- **Status:** Accepted
- **Date:** 2026-08-02

## Context

Through v0.1.0 both backends applied the whole schema on `Open` via idempotent
`CREATE ... IF NOT EXISTS`. That is fine for SQLite, dev, and a fresh single
node, but it has two production gaps for Postgres (surfaced in
`docs/backlog/2026-08-02_migrations.md`):

1. **No evolution.** `CREATE TABLE IF NOT EXISTS` cannot alter an existing
   table. Once a deployment is live, changing `events` (a column, an index)
   does nothing — there is no history, no `ALTER`, no ordered forward steps.
2. **Multi-replica boot races.** With many replicas starting together,
   concurrent `CREATE`/`CREATE POLICY` DDL can race (Postgres can error on a
   duplicate `pg_type` insert under concurrent `CREATE TABLE IF NOT EXISTS`).

## Decision

### Versioned migrations with a history table

Schema is a sequence of numbered, immutable SQL migrations
(`migrations/0001_*.sql`, `0002_*.sql`, …), embedded per backend. A
`schema_migrations(version, applied_at)` table records what has run. `Migrate`
applies pending migrations in order, each recorded on success. Migration 0001
is the current v0.1.0 schema; evolution is a new file, never an edit to an
applied one.

### Concurrency: a lock, so many migrators are safe

- **Postgres:** a session-level **advisory lock** (`pg_advisory_lock`) is held
  for the run. Concurrent migrators serialize on it; the losers wait, then see
  every migration already applied and no-op. Each migration runs in its own
  transaction (Postgres DDL is transactional), so a failure leaves earlier
  migrations applied and recorded.
- **SQLite:** single-writer by construction (the adapter pins one connection),
  so no lock is needed; each migration runs in a transaction.

### `Open` no longer applies DDL by default in production

`Open` gains a `WithoutAutoMigrate()` option. The default **keeps** auto-migrate
(SQLite, dev, tests, single-node stay zero-config), but a production Postgres
deployment sets it so **application replicas never touch DDL on boot** — they
assume the schema is present.

### A dedicated migrate entrypoint

`Store.Migrate(ctx)` is callable directly, and `cmd/es-migrate` runs it once
and exits — for a Kubernetes **init container** (or a leader / a deploy step).
The rollout is: init container runs `es-migrate`; app replicas run with
auto-migrate off. This removes the boot race entirely (one migrator, then N
DDL-free replicas).

## Consequences

### Positive

- **Schema can evolve** — an ordered, recorded, forward-only history; a new
  migration is a new file.
- **Multi-replica boot is race-free** — one init-container migrator, replicas
  assume-migrated. The advisory lock is a backstop if two migrators ever run.
- **Zero-config stays for dev/SQLite** — auto-migrate remains the default; only
  production opts out.
- **No new dependency** — a small hand-rolled runner over embedded files, not
  goose/tern (keeps es-lite lite).

### Negative

- **Forward-only, no down-migrations.** Rollback is a new compensating
  migration, not an automatic revert — deliberate (down-migrations are rarely
  safe on real data). 
- **Migrations must be append-only and idempotent-safe.** An applied file is
  immutable; editing it is a mistake the version check will not catch. Team
  discipline, documented here.
- **RLS/partition setup lives in migrations now** — 0001 carries the RLS
  policies and hash partitions (ADR 0004) and the `unique_claims` objects
  (ADR 0008); later migrations must preserve them.
- **Migrations run in one transaction, so they cannot build indexes
  concurrently** (this ADR's own rule). This stopped being hypothetical with
  migration 0003's feed indexes: `CREATE INDEX CONCURRENTLY` is forbidden
  inside a migration's transaction, and Postgres (through at least 18)
  rejects it on a partitioned parent outright — while a plain build on a
  ≥10M-row `events` table blocks appends for the whole build, and
  auto-migrate would take that lock at boot. The operational answer is
  runbook-first: build the partition child indexes `CONCURRENTLY` before
  the migration runs, attach them to `ON ONLY` parents, and let the
  migration's `CREATE INDEX IF NOT EXISTS` be the instant no-op that
  enforces it. ADR 0037 records the full pattern; the runbook
  (`docs/deploy/README.md`) carries the commands.

## Alternatives Considered

### Keep auto-apply on Open (v0.1.0 behavior)

Rejected for production Postgres: no evolution and a real boot race. Retained
only as the *default* for SQLite/dev via the auto-migrate flag.

### goose / tern / golang-migrate

Capable, but each is a dependency (and a CLI/format) heavier than es-lite
needs. A handful of embedded files + a `schema_migrations` table + an advisory
lock is the whole requirement. Revisit if migrations grow complex (data
backfills, concurrent-index builds).

### One big transaction for all pending migrations

Rejected. Simpler, but a failure rolls back everything including successful
earlier steps, and it forbids statements that cannot run in a transaction
(e.g. a future `CREATE INDEX CONCURRENTLY`). Per-migration transactions under a
session lock compose better.

## Errata (2026-11-07)

Both locking claims in this ADR were replaced by the 2026-11-07 review
remediation; the design's shape (recorded, forward-only, per-migration
transactions) is unchanged:

1. **Postgres: the lock is transaction-scoped, not session-level.** The
   "session-level advisory lock (`pg_advisory_lock`) held for the run"
   described above — and the "session lock" of the One-big-transaction
   alternative — leak: when a migration run was cancelled or failed, the
   deferred session unlock never ran on a dead context, and the lock stayed
   latched on a pooled connection, wedging every later migration that landed
   on another connection (review F10, found by two reviewers independently).
   Since 2026-11-07
   each pending migration runs in exactly one transaction that *begins* with
   `SELECT pg_advisory_xact_lock($1)` and then re-checks the recorded version
   before applying — "automatically released at the end of the current
   transaction and cannot be released explicitly", so commit, rollback,
   cancellation and connection loss all release it
   (`postgres/store.go` `Migrate`).
2. **SQLite: "no lock is needed" overstated.** Single-writer keeps a
   *running migration* safe, but the old check-then-apply shape let two
   migrators both see a version pending and both apply it — the loser
   failing on the DDL instead of skipping. Since 2026-11-07 each pending
   migration inserts its `schema_migrations` row **first**, inside the
   transaction: the SQLite write lock the INSERT takes is what serializes
   rival migrators, and the loser sees the primary-key violation and skips
   (`sqlite/store.go` `Migrate`) — the lock was always there; the code just
   wasn't the one taking it.
