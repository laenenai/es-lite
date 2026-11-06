-- Definition fingerprint (spec §5.5) and the poison-path dead-letter
-- table (spec §5.3) land as one migration: the dead letters have no
-- code in this release yet (Task 12's Skip records them), but a
-- deployed database must never see the table arrive separately from the
-- checkpoint identity it is keyed beside.

-- The fingerprint of the projection definition (projection.Definition's
-- Hash()) that the checkpoint's position was last acknowledged under.
-- Empty means "never recorded": the feed's drift check fires only on a
-- non-empty mismatch, so checkpoints written before this migration (and
-- acks that carry no definition, which preserve the stored value) keep
-- streaming under any definition until an ack records one.
ALTER TABLE feed_checkpoints ADD COLUMN definition_hash text NOT NULL DEFAULT '';

-- A dead letter is a pointer, not a copy: the event itself stays in the
-- immutable log and the row only names the position, so nothing is
-- duplicated into a second retention surface. SQLite is
-- single-workspace; scope is kept in the key for parity with
-- feed_checkpoints and named tenants, not for isolation. created_at is
-- supplied by the adapter, like feed_checkpoints.updated_at.
CREATE TABLE IF NOT EXISTS feed_dead_letters (
    scope text NOT NULL,
    projection text NOT NULL,
    instance text NOT NULL,
    position integer NOT NULL,
    type_url text NOT NULL,
    size_bytes integer NOT NULL,
    reason text NOT NULL,
    actor text NOT NULL,
    created_at text NOT NULL,
    PRIMARY KEY (scope, projection, instance, position)
);
