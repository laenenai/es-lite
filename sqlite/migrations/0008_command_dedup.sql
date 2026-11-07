-- Command dedup storage (design D6, finding F6) — the SQLite mirror of
-- Postgres's 0010: the table behind the draft-compliant
-- Idempotency-Key flow (draft-ietf-httpapi-idempotency-key-header-07).
-- One row per completed command, keyed by (stream, command): the
-- fingerprint of the request whose response was stored (draft §2.4's
-- checksum-of-payload option) and the marshaled response itself, so a
-- retrying request replays the completed response instead of
-- double-appending (draft §2.6), and a reused key carrying a different
-- payload is detected by fingerprint comparison (draft §2.7's 422 case —
-- the caller compares; the table only stores).
--
-- No workspace_id and no RLS: SQLite is single-workspace with no
-- row-level security (0001's rule), so the (stream_id, command_id) pair
-- alone keys a record and the database file's own boundary is the
-- workspace boundary Postgres's policy enforces. There is no relay and
-- no bypass arm to refuse.
--
-- The primary key IS the idempotency contract: (stream_id, command_id)
-- admits exactly one record per command, written once by the first
-- completed response. The adapter Saves with INSERT OR IGNORE, so a
-- Save racing or retrying a completed command is silently a no-op — the
-- conflict is NOT an error (the es.Deduplicator contract); the first
-- response wins forever, and a caller that needs to know what is stored
-- re-Lookups.
--
-- created_at is table bookkeeping in the schema's TEXT house style
-- (ms-precision strftime, like schema_migrations and log_order's
-- created_at), supplied by the DEFAULT.
--
-- Numbered 0008, mirroring Postgres's 0010: SQLite has no 0005
-- (Postgres's is RLS-role-specific), and none of Postgres's 0007–0009
-- apply here (RLS policy hardening, dropping the checkpoints table only
-- Postgres grew tired of — SQLite still implements
-- delivery.Checkpoints — and completing the bypass-arm role guard), so
-- the backends' numbering diverges.

CREATE TABLE IF NOT EXISTS command_dedup (
    stream_id   TEXT NOT NULL,   -- canonical "type:id", as events stores it
    command_id  TEXT NOT NULL,   -- the Idempotency-Key's UUID, as the edge maps it onto CommandID
    fingerprint TEXT NOT NULL,   -- SHA-256 hex of the marshaled request (draft §2.4)
    response    BLOB NOT NULL,   -- the marshaled response, replayed verbatim
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (stream_id, command_id)
);
