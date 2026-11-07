-- Complete the bypass-policy role guard on log_order (fix round 1,
-- F12 completion).
--
-- ERRATA for 0007: its header claimed the guarded bypass arm "is
-- rebuilt on every policy that carries it, in one consistent shape" —
-- that undercounted. FOUR policies carried the arm: 0006's
-- log_order_workspace_isolation (landed in Task 7, mirroring 0005's
-- then-current unguarded shape) also carried it, and 0007 rebuilt only
-- the three 0005 knew about. F8 is not at issue here — 0006's arm was
-- already role-gated — but F12 stayed open on log_order: on a database
-- where es_relay has not been created yet, pg_has_role's missing-role
-- error could hard-fail even a plain workspace-scoped log_order query,
-- because Postgres does not guarantee AND short-circuit evaluation in
-- policy expressions. This migration completes the set: every
-- arm-carrying policy (events, unique_claims, anchors, log_order) now
-- carries the same CASE/EXISTS-guarded arm, so no policy can error on
-- a missing role — it merely refuses the bypass.
--
-- Everything else is preserved from 0006: the policy name, the
-- workspace arm, and log_order's ENABLE + FORCE ROW LEVEL SECURITY
-- state (no ALTER TABLE here). es_relay is still NOT created here —
-- role creation belongs to deployment (docs/deploy/README.md), which
-- must create the role and grant it to the relay and anchor principals.

DROP POLICY IF EXISTS log_order_workspace_isolation ON log_order;
CREATE POLICY log_order_workspace_isolation ON log_order
    USING (workspace_id = current_setting('app.workspace_id', true) OR (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true) OR (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END));
