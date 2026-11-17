-- Producer span context (ADR 0040): the producer's W3C trace context,
-- persisted alongside each event so consumers can join the events back
-- to the producing trace.
--
-- The three columns are nullable and carried as a set: a context is
-- either wholly absent (all three NULL — the shape every pre-0009 row
-- has) or wholly present. SQLite cannot ALTER TABLE ADD CONSTRAINT, so
-- parity with the Postgres CHECK (postgres 0011) means a rebuild:
-- create events_new with the full column set (0001's sixteen plus
-- 0002's prev_hash/hash plus the three new ones), both UNIQUE
-- constraints and the CHECK, copy every row, drop the old table, rename,
-- and recreate the five indexes (0001's four plus 0007's
-- events_type_pos_idx) — they die with the table.
--
-- The CHECK enforces the same all-or-nothing shape and wire sizes as the
-- Postgres one: both ids present or both absent, the trace id 16 bytes,
-- the span id 8 bytes (length() is SQLite's byte length for BLOBs), and
-- the flags byte in range. INTEGER holds the unsigned flags byte
-- (0-255); the store maps it to uint8 on read.
--
-- global_position is copied EXPLICITLY: it is the log cursor and the
-- basis of log_order's attestation, and renumbering it would rewrite
-- history. The explicit values advance sqlite_sequence to the old
-- maximum, which DROP/RENAME carries over, so the next append draws
-- max+1 and the cursor keeps its meaning.
--
-- Do NOT re-run this script against a database already migrated to
-- 0009: the copy selects only the 18 base columns (the 0008 source
-- table it was written for has no others), so a re-run would NULL any
-- stored span contexts. Migrate's exactly-once schema_migrations
-- protocol makes that unreachable in production; only a hand re-run
-- can hit it (the rebuild test does, deliberately, on rows whose
-- contexts are all absent).
--
-- Telemetry, not attestation: the columns are excluded from the
-- envelope hash (es.EnvelopeHash), so a context is never part of the
-- chain and a legacy reader ignores them safely.

CREATE TABLE events_new (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT    NOT NULL,
    stream_type     TEXT    NOT NULL,
    stream_id       TEXT    NOT NULL,
    version         INTEGER NOT NULL,
    type_url        TEXT    NOT NULL,
    schema_version  INTEGER NOT NULL,
    occurred_at     TEXT    NOT NULL,
    recorded_at     TEXT    NOT NULL,
    correlation_id  TEXT    NOT NULL,
    causation_id    TEXT    NOT NULL,
    command_id      TEXT    NOT NULL,
    actor_type      TEXT    NOT NULL,
    actor_id        TEXT    NOT NULL,
    actor_principal TEXT    NOT NULL,
    payload         BLOB    NOT NULL,
    prev_hash       BLOB,
    hash            BLOB,
    trace_id        BLOB,
    span_id         BLOB,
    trace_flags     INTEGER,
    UNIQUE (stream_id, version),
    UNIQUE (event_id),
    CHECK ((trace_id IS NULL) = (span_id IS NULL)
       AND (span_id IS NULL) = (trace_flags IS NULL)
       AND (trace_id IS NULL OR length(trace_id) = 16)
       AND (span_id  IS NULL OR length(span_id)  = 8)
       AND (trace_flags IS NULL OR trace_flags BETWEEN 0 AND 255))
);

INSERT INTO events_new (
    global_position, event_id, stream_type, stream_id, version,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor_type, actor_id, actor_principal, payload, prev_hash, hash
)
SELECT
    global_position, event_id, stream_type, stream_id, version,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor_type, actor_id, actor_principal, payload, prev_hash, hash
FROM events;

DROP TABLE events;
ALTER TABLE events_new RENAME TO events;

CREATE INDEX IF NOT EXISTS events_correlation_idx ON events (correlation_id);
CREATE INDEX IF NOT EXISTS events_command_idx     ON events (command_id);
CREATE INDEX IF NOT EXISTS events_actor_idx       ON events (actor_principal);
CREATE INDEX IF NOT EXISTS events_stream_time_idx ON events (stream_id, recorded_at);
CREATE INDEX IF NOT EXISTS events_type_pos_idx    ON events (type_url, global_position);
