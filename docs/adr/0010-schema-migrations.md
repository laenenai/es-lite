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
