-- Log-order attestation (design D1, finding F11). The per-stream hash
-- chain and the anchors over stream heads bind WHAT each stream
-- contains; nothing bound the order in which events landed in the
-- global log — a batch's positions were the only witness, and they live
-- in the same rows a database operator can rewrite. This migration adds
-- the per-batch witness table; anchors aggregate its rows into an order
-- root (the order analog of the heads root), and `es-verify order`
-- recomputes each row's hash from the events — the audit that actually
-- catches a repositioned event, which leaves every chain and every head
-- intact. It mirrors the chain/anchor trust split of ADR 0029: the
-- per-batch hash is the cheap in-transaction witness, the exported
-- order root the off-box aggregate.
--
-- One row per non-empty append batch, written in the SAME transaction
-- as the batch's events — no new reads, no new locks, no
-- cross-transaction dependency, so the append path gains no
-- serialization (ADR 0029's global-chain rejection stands). The row's
-- batch_hash is es.BatchOrderHash: SHA-256 over the
-- "es-lite.log-order.v1" domain separator (u32 length-prefixed), a
-- big-endian u32 event count, then per event, in ascending position
-- order, a big-endian u64 global position followed by the event's
-- 32-byte stored chain hash. Positions are drawn from the shared
-- sequence in event order inside one transaction, so the batch's rows
-- span [from_position, to_position] with from_position its first
-- event's position and to_position its last.
--
-- ROWS BEFORE 0006 ARE UNATTESTED. Events appended before this
-- migration have no log_order row and are never covered by one; an
-- anchor's order fields attest only batches appended from here on (the
-- order analog of the chain's unhashed pre-0002 rows and
-- ChainedFrom). An anchor covering no rows reports OrderCount 0 and
-- OrderFromPosition 0 while still carrying a real order root — the
-- hash of the empty row set.
--
-- from_position is the PRIMARY KEY: a batch's first position is drawn
-- from the sequence by exactly one transaction, so no two committed
-- batches share it. to_position gets the index: the anchor path and the
-- deep verify both select rows with to_position <= the settled bound.
--
-- workspace_id and stream_id scope a row to the stream its batch wrote
-- (one append writes one workspace and one stream). Concurrent
-- transactions interleave their sequence draws, so different batches'
-- position ranges can overlap — the verifier recomputes a row's hash
-- over exactly the events of its own (workspace, stream) inside its
-- range, which no other batch can share.
--
-- RLS mirrors events exactly (0001's workspace arm, 0005's role-gated
-- bypass arm, FORCE ROW LEVEL SECURITY): a workspace session sees only
-- its own rows, and only es_relay members may read across workspaces
-- under app.bypass_rls. As with events, no grants are issued here —
-- role creation and per-principal grants belong to deployment
-- (docs/deploy/README.md): the relay/anchor principals need SELECT on
-- log_order from the moment this migration lands.

CREATE TABLE IF NOT EXISTS log_order (
    from_position bigint      PRIMARY KEY,
    to_position   bigint      NOT NULL,
    workspace_id  text        NOT NULL,
    stream_id     text        NOT NULL,   -- canonical "type:id", as events stores it
    event_count   int         NOT NULL,
    batch_hash    bytea       NOT NULL,   -- es.BatchOrderHash over the batch's own events
    created_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS log_order_to_position_idx ON log_order (to_position);

ALTER TABLE log_order ENABLE ROW LEVEL SECURITY;
ALTER TABLE log_order FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS log_order_workspace_isolation ON log_order;
CREATE POLICY log_order_workspace_isolation ON log_order
    USING (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND pg_has_role(current_user, 'es_relay', 'member')))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND pg_has_role(current_user, 'es_relay', 'member')));
