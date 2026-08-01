-- es-lite SQLite schema. Applied idempotently on Open.
--
-- SQLite serializes writers at the file level, so global_position
-- (INTEGER PRIMARY KEY AUTOINCREMENT) is gap-free and assigned in commit
-- order by construction. That is what lets the delivery poller tail the
-- log with a plain `global_position > :cursor` cursor and never miss a
-- row (docs/adr/0001 §5). Postgres will need extra care here; SQLite
-- does not.
--
-- The events table is append-only: es-lite never issues UPDATE or DELETE
-- against it. History, audit, and time-travel rest on that invariant.

CREATE TABLE IF NOT EXISTS events (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,

    event_id        TEXT    NOT NULL,
    stream_type     TEXT    NOT NULL,   -- aggregate type, for "all streams of type X"
    stream_id       TEXT    NOT NULL,   -- canonical "type:id"
    version         INTEGER NOT NULL,   -- per-stream, 1-based, contiguous

    type_url        TEXT    NOT NULL,
    schema_version  INTEGER NOT NULL,

    occurred_at     TEXT    NOT NULL,   -- domain time  (RFC3339, fixed-width, UTC)
    recorded_at     TEXT    NOT NULL,   -- commit time  (RFC3339, fixed-width, UTC)

    correlation_id  TEXT    NOT NULL,
    causation_id    TEXT    NOT NULL,
    command_id      TEXT    NOT NULL,
    actor_type      TEXT    NOT NULL,
    actor_id        TEXT    NOT NULL,
    actor_principal TEXT    NOT NULL,   -- "type:id", indexed for audit queries

    payload         BLOB    NOT NULL,

    -- Optimistic concurrency: two writers racing on the same stream
    -- version collide here; the loser's INSERT fails and maps to
    -- es.ErrConflict.
    UNIQUE (stream_id, version),
    UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS events_correlation_idx ON events (correlation_id);
CREATE INDEX IF NOT EXISTS events_command_idx     ON events (command_id);
CREATE INDEX IF NOT EXISTS events_actor_idx       ON events (actor_principal);
CREATE INDEX IF NOT EXISTS events_stream_time_idx ON events (stream_id, recorded_at);

-- checkpoints: one row per delivery subscriber, holding the highest
-- global_position it has durably processed. The poller reads events
-- past this and advances it after publishing (at-least-once).
CREATE TABLE IF NOT EXISTS checkpoints (
    subscriber TEXT PRIMARY KEY,
    position   INTEGER NOT NULL,
    updated_at TEXT    NOT NULL
);
