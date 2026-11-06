-- Feed delivery state (spec §5.3, §5.4, §5.6): per-subscription
-- checkpoints with their definition fingerprint, the leases that fence
-- one active subscriber per key, and the poison path's dead-letter audit
-- rows. The feed's key is the three text columns — no NUL-joined storage
-- keys (§5.4) — and every table carries workspace_id: the feed is
-- per-workspace.
--
-- Unlike events and unique_claims (0001), these policies carry NO bypass
-- arm: the feed never reads across tenants, so no session — not even one
-- holding app.bypass_rls — may reach another workspace's subscription
-- state (spec §5.6). FORCE keeps the table owner subject as well.
--
-- Timestamps come from the database clock (clock_timestamp() in SQL),
-- not the application's: updated_at, expires_at and created_at are all
-- written by the adapters in SQL, so feed replicas never trust each
-- other's clocks (spec §5.6).

CREATE TABLE IF NOT EXISTS feed_checkpoints (
    workspace_id    text        NOT NULL,
    projection      text        NOT NULL,
    instance        text        NOT NULL,
    position        bigint      NOT NULL,
    definition_hash text        NOT NULL DEFAULT '',
    updated_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, projection, instance)
);

-- One live lease per (workspace, projection, instance); expires_at is
-- absolute wall time the adapter writes as clock_timestamp() + ttl.
CREATE TABLE IF NOT EXISTS feed_leases (
    workspace_id text        NOT NULL,
    projection   text        NOT NULL,
    instance     text        NOT NULL,
    owner        text        NOT NULL,
    consumer     text        NOT NULL,
    expires_at   timestamptz NOT NULL,
    PRIMARY KEY (workspace_id, projection, instance)
);

-- A dead letter is a pointer, not a copy (spec §5.3): the event itself
-- stays in the immutable log and the row only names it, so nothing is
-- duplicated into a second retention surface. type_url/size_bytes are
-- the triage pointer the store enriches from the events row by position
-- (metadata only, never the payload); created_at is the database's
-- clock at Record.
CREATE TABLE IF NOT EXISTS feed_dead_letters (
    workspace_id text        NOT NULL,
    projection   text        NOT NULL,
    instance     text        NOT NULL,
    position     bigint      NOT NULL,
    type_url     text        NOT NULL,
    size_bytes   int         NOT NULL,
    reason       text        NOT NULL,
    actor        text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, projection, instance, position)
);

ALTER TABLE feed_checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE feed_checkpoints FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS feed_checkpoints_workspace_isolation ON feed_checkpoints;
CREATE POLICY feed_checkpoints_workspace_isolation ON feed_checkpoints
    USING (workspace_id = current_setting('app.workspace_id', true))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true));

ALTER TABLE feed_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE feed_leases FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS feed_leases_workspace_isolation ON feed_leases;
CREATE POLICY feed_leases_workspace_isolation ON feed_leases
    USING (workspace_id = current_setting('app.workspace_id', true))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true));

ALTER TABLE feed_dead_letters ENABLE ROW LEVEL SECURITY;
ALTER TABLE feed_dead_letters FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS feed_dead_letters_workspace_isolation ON feed_dead_letters;
CREATE POLICY feed_dead_letters_workspace_isolation ON feed_dead_letters
    USING (workspace_id = current_setting('app.workspace_id', true))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
