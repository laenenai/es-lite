-- Projection checkpoints (ADR 0045, read-your-writes). One row per
-- (scope, projection) recording the highest global position the projection
-- has applied, the projector's state and when the row last changed.
-- This is derived progress, not audit state: a restore resets and replays
-- it, and it is never a backup set.
--
-- SQLite is one database per workspace, so there is no workspace_id column
-- and no RLS: the scope is the feed ports' parity argument, usually the
-- empty string, and the database file's own boundary is the tenant
-- boundary Postgres's policy enforces.
--
-- position advances monotonically within a state; a state change may
-- replace it (a rebuild restarts the position). updated_at is written by
-- the adapter in the store's fixed-width RFC3339 UTC layout (tsLayout),
-- like events.recorded_at and snapshots.recorded_at, so lexical order
-- matches chronological order and a reader never compares a writer's clock
-- to its own.
--
-- The state CHECK mirrors Postgres's 0017 and admits only the statuses the
-- stores persist; StatusUnknown means "no row" and can never be written.

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    scope      TEXT    NOT NULL,
    projection TEXT    NOT NULL,
    position   INTEGER NOT NULL DEFAULT 0,
    state      TEXT    NOT NULL DEFAULT 'live'
        CHECK (state IN ('live','rebuilding','stalled')),
    updated_at TEXT    NOT NULL,
    PRIMARY KEY (scope, projection)
);
