-- Append decisions (ADR 0043): the durable record of the constraint ops
-- an append applied, so the unique_claims index can be reconstructed from
-- the log alone after a restore, a clone, or a verification rebuild.
--
-- One row records one append that carried at least one constraint op,
-- not one op: the encoded ops (es.EncodeDecisionOps) are the append's
-- original Claim/Release sequence, in order, so a rebuild replays the
-- decisions rather than guessing at their effects. An append with no
-- constraints writes no row. The row is written inside the append
-- transaction, AFTER the claim index has been updated in that same
-- transaction: a constraint violation aborts the append and rolls the
-- decision row back with the claims and events, and a committed append
-- can never have a decision row without its claim effects (or the
-- reverse). A claim-only append — zero events — still writes a row,
-- with from_version == to_version == ExpectedVersion: the decision is
-- the append's only durable effect, and dropping it would lose the
-- claim's provenance.
--
-- from_version/to_version are the appended span:
-- [from_version, to_version), to_version = ExpectedVersion + len(events).
-- command_id is the append's CommandID as events stores it, including
-- the zero UUID when the caller left it unset; live appends always write
-- a string.
--
-- created_at is table bookkeeping (clock_timestamp, like log_order), not
-- part of any contract: replay order is decided by id, the identity
-- order in which the appends committed.
--
-- RLS mirrors unique_claims, the table this log reconstructs: the
-- workspace arm plus the role-gated es_relay bypass arm in the shape
-- hardened by 0007 (the arm is guarded by CASE WHEN EXISTS so a missing
-- es_relay role refuses the bypass instead of erroring). ENABLE + FORCE
-- keeps the table owner subject to the policy. No grants are issued
-- here, like every migration before this one: role creation and
-- per-principal grants belong to deployment (docs/deploy/README.md).
-- The append principal needs INSERT; rebuild, verify and adopt need
-- SELECT (and the adopt path writes once).

CREATE TABLE IF NOT EXISTS append_decisions (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workspace_id text        NOT NULL,
    stream_id    text        NOT NULL,   -- canonical "type:id", as events stores it
    from_version bigint      NOT NULL,
    to_version   bigint      NOT NULL,
    command_id   text        NULL,       -- NULL only for the adopt backfill
    ops          bytea       NOT NULL,   -- es.EncodeDecisionOps, the original op sequence
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (from_version <= to_version)
);

CREATE INDEX IF NOT EXISTS append_decisions_workspace_id_idx
    ON append_decisions (workspace_id, id);

ALTER TABLE append_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE append_decisions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS append_decisions_workspace_isolation ON append_decisions;
CREATE POLICY append_decisions_workspace_isolation ON append_decisions
    USING (workspace_id = current_setting('app.workspace_id', true) OR (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true) OR (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END));
