-- Tamper-evident hash chain (ADR 0029). Adds the per-stream prev_hash/hash
-- pair the store computes inside every append transaction from here on,
-- and the anchors table that fixes stream heads for off-box export.
--
-- Both columns are NULLABLE: rows written before this migration are never
-- back-filled (that would UPDATE the append-only log and certify history
-- the store did not observe). The chain starts at the first append after
-- the migration; es.VerifyStream reports ChainedFrom accordingly. ALTER on
-- the partitioned parent propagates to every partition.

ALTER TABLE events ADD COLUMN IF NOT EXISTS prev_hash bytea;
ALTER TABLE events ADD COLUMN IF NOT EXISTS hash      bytea;

-- Unchained rows are the pre-0002 population; the partial index lets
-- es-verify find them (and confirm the set never grows) without a scan.
CREATE INDEX IF NOT EXISTS events_unchained_idx ON events (global_position) WHERE hash IS NULL;

-- anchors: periodic fixings of every stream's (version, hash) under one
-- root hash, keyed by the global_position they cover. Store-wide (no
-- workspace_id): the root spans all workspaces, so the table is readable
-- and writable only under app.bypass_rls, like Drain.
CREATE TABLE IF NOT EXISTS anchors (
    anchor_id            text        PRIMARY KEY,
    created_at           timestamptz NOT NULL DEFAULT now(),
    upto_global_position bigint      NOT NULL,
    stream_count         bigint      NOT NULL,
    root_hash            bytea       NOT NULL
);

ALTER TABLE anchors ENABLE ROW LEVEL SECURITY;
ALTER TABLE anchors FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anchors_admin_only ON anchors;
CREATE POLICY anchors_admin_only ON anchors
    USING (current_setting('app.bypass_rls', true) = 'on')
    WITH CHECK (current_setting('app.bypass_rls', true) = 'on');
