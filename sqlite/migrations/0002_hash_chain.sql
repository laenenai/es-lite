-- Tamper-evident hash chain (ADR 0029). Adds the per-stream prev_hash/hash
-- pair the store computes inside every append transaction from here on,
-- and the anchors table that fixes stream heads for off-box export.
--
-- Both columns are NULLABLE: rows written before this migration are never
-- back-filled (that would UPDATE the append-only log and certify history
-- the store did not observe). The chain starts at the first append after
-- the migration; es.VerifyStream reports ChainedFrom accordingly.

ALTER TABLE events ADD COLUMN prev_hash BLOB;
ALTER TABLE events ADD COLUMN hash      BLOB;

-- anchors: periodic fixings of every stream's (version, hash) under one
-- root hash, keyed by the global_position they cover. The row is what the
-- operator exports (signed commit, transparency log, TSA); es-lite is
-- keyless and does not sign it (ADR 0025).
CREATE TABLE IF NOT EXISTS anchors (
    anchor_id            TEXT    PRIMARY KEY,
    created_at           TEXT    NOT NULL,
    upto_global_position INTEGER NOT NULL,
    stream_count         INTEGER NOT NULL,
    root_hash            BLOB    NOT NULL
);
