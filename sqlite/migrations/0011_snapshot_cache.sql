-- Snapshot cache (ADR 0042). Derived, droppable state: one row per
-- stream, written by aggregate.Runtime outside the append transaction.
-- One SQLite database is one workspace, so there is no workspace column.
--
-- recorded_at uses the same fixed-width RFC3339 UTC layout as events
-- (tsLayout), so lexical order matches chronological order. No
-- monotonic guard: last writer wins (ADR 0042 §3.2).
--
-- Deliberately a bare CREATE TABLE, not IF NOT EXISTS: the Cache
-- interface has been public while unimplemented, so an adopter may
-- already own a foreign "snapshots" table of a different shape. IF NOT
-- EXISTS would silently no-op over it and record 0011 as applied, and
-- aggregate.Runtime swallows Load/Save errors — the cache would die
-- permanently with no signal. The migration is the only loud place, so
-- a collision must fail it (ADR 0042 §3.2), as Postgres 0013's shape
-- assertion does. Nothing idempotent is lost: Migrate records each
-- version in schema_migrations and skips recorded ones, so a
-- ledger-following re-run never re-executes this DDL — the same reason
-- 0009's CREATE TABLE events_new is already non-idempotent.
--
-- stream_id carries an explicit NOT NULL because SQLite's rowid tables
-- do not apply the standard's "PRIMARY KEY implies NOT NULL" — a
-- documented compatibility quirk — so a bare PRIMARY KEY column would
-- accept NULL keys and PRAGMA table_info would report notnull = 0 for
-- it.

CREATE TABLE snapshots (
    stream_id    TEXT    NOT NULL PRIMARY KEY,
    version      INTEGER NOT NULL,
    fold_version INTEGER NOT NULL,
    recorded_at  TEXT    NOT NULL,
    state        BLOB    NOT NULL
);
