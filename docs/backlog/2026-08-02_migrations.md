# Backlog: Schema migration strategy

- **Filed:** 2026-08-02
- **Area:** storage / operations
- **Status:** Open

## Problem

Both backends apply the embedded `schema.sql` on `Open` (idempotent
`CREATE TABLE/INDEX IF NOT EXISTS`, and on Postgres `ENABLE/FORCE RLS` +
`DROP/CREATE POLICY`). Convenient for dev / SQLite / single-node, but two
real gaps for production Postgres:

1. **No schema evolution.** `CREATE TABLE IF NOT EXISTS` cannot alter an
   existing table. Adding a column to `events` on a live install does
   nothing — there is no migration history, no `ALTER`, no down-migrations.
   (The parent `eventstore` used numbered goose migrations for this.)

2. **Multi-replica boot races (Postgres).** At 100k-workspace scale with
   many replicas starting together, concurrent `CREATE ... IF NOT EXISTS` /
   `CREATE POLICY` can race — Postgres can error on a duplicate `pg_type`
   insert under concurrent `CREATE TABLE IF NOT EXISTS`. Schema application
   should happen once, not on every replica boot.

## Proposed direction

- Add a one-shot `Migrate(ctx)` entrypoint (and/or `cmd/es-migrate`) run once
  in CI/deploy or an init container / leader.
- Make `Open` able to **skip** auto-apply (e.g. an `AssumeMigrated` option),
  so app replicas don't touch DDL on boot; keep auto-apply as the default for
  SQLite / dev.
- Introduce a versioned migration mechanism (numbered SQL files + a
  `schema_migrations` history table) so schema can evolve, not just be
  created. Decide: hand-rolled runner vs. goose / tern.
- Write an ADR (migration strategy / discipline) — a load-bearing operational
  decision, not just code.

## Acceptance criteria

- [ ] Versioned migrations with a history table; forward migrations apply once.
- [ ] `Migrate(ctx)` callable independently; `cmd/es-migrate` for deploy.
- [ ] `Open` option to skip DDL (multi-replica-safe boot).
- [ ] SQLite + Postgres both covered; integration-tested.
- [ ] ADR documenting the strategy.

## Notes

Surfaced while reviewing startup behavior. Migrations must preserve the
RLS + hash-partition setup from [ADR 0004](../adr/0004-workspaces-and-tenant-isolation.md)
and the `unique_claims` / policy objects from
[ADR 0008](../adr/0008-uniqueness-constraints.md). No SQL-injection concern in
current code — all queries are parameterized; this is purely a schema-lifecycle
gap.
