-- Policy hardening (review F8, F12). The bypass arm — the OR branch
-- that lets a session holding app.bypass_rls read across workspaces —
-- is rebuilt on every policy that carries it, in one consistent shape.
--
-- ERRATA for 0005: its header said "Both policies that carry the arm
-- (events and unique_claims, the pair 0001 created; the feed tables of
-- 0004 never had one)". That undercounted. THREE policies carried the
-- arm: 0002's anchors_admin_only is bypass-only (anchors is store-wide,
-- so the 0001 workspace arm never applied to it), and 0005 never
-- extended the role gate to it. The Go-side gate (checkRelayRole, which
-- Drain and the anchor path's bypassTx share) did cover the anchor path
-- — but the anchors POLICY itself still trusted app.bypass_rls alone,
-- so any non-member session that set the GUC could read and write
-- anchors directly, bypassing the gate the other two policies carried.
-- This migration corrects the count and the asymmetry: all three bypass
-- arms now require es_relay membership.
--
-- F12: 0005's arm evaluated pg_has_role(current_user, 'es_relay',
-- 'member') unguarded. pg_has_role errors when the role does not
-- exist, and AND does not guarantee short-circuit evaluation, so on a
-- database where es_relay has not been created yet every
-- events/unique_claims query — even a plain workspace-scoped read —
-- could hard-fail on the missing role instead of merely refusing the
-- bypass. The arm now wraps the membership test in CASE WHEN EXISTS
-- (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay'): no role, no
-- bypass, no error — mirroring the same guard postgres.Store's
-- checkRelayRole has used since it was written.
--
-- Everything else is preserved: policy names, the workspace arm of
-- events and unique_claims, and the tables' ENABLE + FORCE ROW LEVEL
-- SECURITY state. anchors keeps no workspace arm (0002's shape: the
-- root spans all workspaces), so the role-gated bypass arm is its
-- whole policy. es_relay is still NOT created here — role creation
-- belongs to deployment (docs/deploy/README.md), which must create the
-- role and grant it to the relay and anchor principals.

DROP POLICY IF EXISTS events_workspace_isolation ON events;
CREATE POLICY events_workspace_isolation ON events
    USING (workspace_id = current_setting('app.workspace_id', true) OR (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true) OR (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END));

DROP POLICY IF EXISTS unique_claims_workspace_isolation ON unique_claims;
CREATE POLICY unique_claims_workspace_isolation ON unique_claims
    USING (workspace_id = current_setting('app.workspace_id', true) OR (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true) OR (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END));

DROP POLICY IF EXISTS anchors_admin_only ON anchors;
CREATE POLICY anchors_admin_only ON anchors
    USING (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END)
    WITH CHECK (current_setting('app.bypass_rls', true) = 'on' AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN pg_has_role(current_user, 'es_relay', 'member') ELSE false END);
