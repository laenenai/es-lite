-- Projection checkpoints (ADR 0045, read-your-writes). One row per
-- (workspace, projection) recording the highest global position the
-- projection has applied, the projector's state and when the row last
-- changed. This is derived progress, not audit state: a restore resets and
-- replays it, and it is never a backup set.
--
-- position advances monotonically within a state; a state change may
-- replace it (a rebuild restarts the position). updated_at is the
-- database's own clock (clock_timestamp()), matching the feed tables, so
-- readers never compare a writer's clock to their own.
--
-- The workspace arm plus the role-gated app.bypass_rls arm mirror 0016's
-- event_tags policy, including 0007's EXISTS guard around the role test.
-- No grants are issued here — deployment grants the projection runner
-- SELECT, INSERT, UPDATE and the waiter/query role SELECT (runbook).

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    workspace_id text        NOT NULL,
    projection   text        NOT NULL,
    position     bigint      NOT NULL DEFAULT 0,
    state        text        NOT NULL DEFAULT 'live'
        CHECK (state IN ('live','rebuilding','stalled')),
    updated_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, projection)
);

ALTER TABLE projection_checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_checkpoints FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS projection_checkpoints_workspace_isolation ON projection_checkpoints;
CREATE POLICY projection_checkpoints_workspace_isolation ON projection_checkpoints
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
