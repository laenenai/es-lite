-- Command dedup storage (design D6, finding F6): the table behind the
-- draft-compliant Idempotency-Key flow
-- (draft-ietf-httpapi-idempotency-key-header-07). One row per
-- completed command, keyed by (workspace, stream, command): the
-- fingerprint of the request whose response was stored (draft §2.4's
-- checksum-of-payload option) and the marshaled response itself, so a
-- retrying request replays the completed response instead of
-- double-appending (draft §2.6), and a reused key carrying a different
-- payload is detected by fingerprint comparison (draft §2.7's 422 case —
-- the caller compares; the table only stores).
--
-- WORKSPACE-PRIVATE, WITH NO BYPASS ARM. Unlike events, unique_claims,
-- anchors and log_order, this policy carries ONLY the workspace arm —
-- the feed tables' shape (0004), not the log tables' (0001/0005/0007).
-- A stored response is the workspace's own data — it carries the
-- workspace's domain content — and no cross-workspace job ever needs
-- these rows: Drain reads events, the anchor path reads events and
-- log_order. So even es_relay under app.bypass_rls must be refused; a
-- relay reading a foreign workspace's dedup responses would be pure
-- disclosure. FORCE ROW LEVEL SECURITY keeps the table owner subject to
-- the same single arm.
--
-- The primary key IS the idempotency contract: (workspace_id, stream_id,
-- command_id) admits exactly one record per command, written once by
-- the first completed response. The adapter Saves with
-- INSERT ... ON CONFLICT DO NOTHING, so a Save racing or retrying a
-- completed command is silently a no-op — the conflict is NOT an error
-- (the es.Deduplicator contract); the first response wins forever, and
-- a caller that needs to know what is stored re-Lookups.
--
-- created_at is table bookkeeping (clock_timestamp, like log_order),
-- not part of any contract: nothing reads it yet.
--
-- No grants are issued here, like every migration before this one:
-- role creation and per-principal grants belong to deployment
-- (docs/deploy/README.md). A deployment running the dedup flow grants
-- its command-handling principal SELECT and INSERT on command_dedup;
-- nothing else needs any access.

CREATE TABLE IF NOT EXISTS command_dedup (
    workspace_id text        NOT NULL,
    stream_id    text        NOT NULL,   -- canonical "type:id", as events stores it
    command_id   text        NOT NULL,   -- the Idempotency-Key's UUID, as the edge maps it onto CommandID
    fingerprint  text        NOT NULL,   -- SHA-256 hex of the marshaled request (draft §2.4)
    response     bytea       NOT NULL,   -- the marshaled response, replayed verbatim
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, stream_id, command_id)
);

ALTER TABLE command_dedup ENABLE ROW LEVEL SECURITY;
ALTER TABLE command_dedup FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS command_dedup_workspace_isolation ON command_dedup;
CREATE POLICY command_dedup_workspace_isolation ON command_dedup
    USING (workspace_id = current_setting('app.workspace_id', true))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
