-- Event tags (design 2026-11-23). Tags are first-class on the event:
-- events.tags is a flat JSON object of string keys to string values, and
-- event_tags is a synchronous, rebuildable projection of it. Storing the tag
-- set on the event is what keeps an events-only restore complete; the side
-- table exists only to make "events carrying this tag, in position order" a
-- tag-first index scan rather than a bitmap over a GIN index.
--
-- The column is deliberately excluded from CanonicalEnvelope, like
-- span_context and constraint_ops, so no existing event hash changes. Tags
-- are attested separately: each append writes log_order.tags_hash over its
-- own tag rows, and an anchor aggregates those into a sibling tags root.
--
-- The primary key is tag-first: (workspace_id, tag_key, tag_value,
-- global_position). That is the read path — one range scan per tag, ordered
-- by global position. There is deliberately no secondary position index:
-- "tags of this event" is events.tags, already on the event row.
--
-- Eight hash partitions match events (ADR 0004 errata). Partitioning is by
-- workspace_id, so a workspace's tag rows are pruned like its events; the
-- tables need not be physically co-located.
--
-- RLS mirrors events exactly: the workspace arm plus the role-gated
-- app.bypass_rls arm from 0007_policy_hardening.sql. No grants are issued
-- here — deployment grants the appender SELECT, INSERT, the rebuild
-- principal SELECT, INSERT, DELETE, and readers SELECT (runbook).

ALTER TABLE events ADD COLUMN tags jsonb;
ALTER TABLE events ADD CONSTRAINT events_tags_object
    CHECK (tags IS NULL OR jsonb_typeof(tags) = 'object') NOT VALID;

CREATE TABLE IF NOT EXISTS event_tags (
    workspace_id    text   NOT NULL,
    tag_key         text   NOT NULL,
    tag_value       text   NOT NULL,
    global_position bigint NOT NULL,
    PRIMARY KEY (workspace_id, tag_key, tag_value, global_position)
) PARTITION BY HASH (workspace_id);

CREATE TABLE IF NOT EXISTS event_tags_p0 PARTITION OF event_tags FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS event_tags_p1 PARTITION OF event_tags FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS event_tags_p2 PARTITION OF event_tags FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS event_tags_p3 PARTITION OF event_tags FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS event_tags_p4 PARTITION OF event_tags FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS event_tags_p5 PARTITION OF event_tags FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS event_tags_p6 PARTITION OF event_tags FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS event_tags_p7 PARTITION OF event_tags FOR VALUES WITH (MODULUS 8, REMAINDER 7);

-- Every existing row is NULL, so validation is instant.
ALTER TABLE events VALIDATE CONSTRAINT events_tags_object;

ALTER TABLE event_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_tags FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS event_tags_workspace_isolation ON event_tags;
CREATE POLICY event_tags_workspace_isolation ON event_tags
    USING (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay')
                        THEN pg_has_role(current_user, 'es_relay', 'member')
                        ELSE false END))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay')
                        THEN pg_has_role(current_user, 'es_relay', 'member')
                        ELSE false END));

-- Batches before this migration have no tag rows and stay NULL. A tagged
-- batch always carries es.TagsHash over its own (position, key, value) rows.
ALTER TABLE log_order ADD COLUMN tags_hash bytea;
