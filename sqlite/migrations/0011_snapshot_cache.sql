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
