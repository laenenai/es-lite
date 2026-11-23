-- Event tags (design 2026-11-23). events.tags is the flat JSON object source;
-- event_tags is its synchronous, rebuildable projection. SQLite is one
-- database per workspace, so the projection has no workspace_id column and
-- the primary key is tag-first: (tag_key, tag_value, global_position).
--
-- The column is excluded from CanonicalEnvelope, so existing event hashes are
-- unchanged. log_order.tags_hash is the per-batch attestation; an anchor adds
-- a sibling tags root in Task 13.

ALTER TABLE events ADD COLUMN tags TEXT;

CREATE TABLE IF NOT EXISTS event_tags (
    tag_key         TEXT    NOT NULL,
    tag_value       TEXT    NOT NULL,
    global_position INTEGER NOT NULL,
    PRIMARY KEY (tag_key, tag_value, global_position)
);

ALTER TABLE log_order ADD COLUMN tags_hash BLOB;
