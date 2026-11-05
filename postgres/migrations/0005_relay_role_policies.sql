-- Role-gate the RLS bypass (spec §5.7, Review Focus 6). 0001's bypass arm
-- trusts any session that sets app.bypass_rls='on' — a GUC any session can
-- set — so it cannot be the only thing standing between a workspace's rows
-- and the rest of the database. Both policies that carry the arm (events
-- and unique_claims, the pair 0001 created; the feed tables of 0004 never
-- had one) now additionally require es_relay membership, and
-- postgres.Drain refuses to run without it (ErrRelayRoleMissing) instead of
-- silently claiming zero rows.
--
-- es_relay is NOT created here: migrator privileges vary, so role creation
-- belongs to deployment (docs/deploy/README.md) — which must create the
-- role and grant it to the relay principal BEFORE this migration applies.
-- From the moment 0005 lands, the bypass no longer works for non-members:
-- with the grant in place the relay keeps draining; without it, Drain fails
-- fast at startup. Superusers are implicitly members of every role, so
-- psql/admin tooling is unaffected.

DROP POLICY IF EXISTS events_workspace_isolation ON events;
CREATE POLICY events_workspace_isolation ON events
    USING (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND pg_has_role(current_user, 'es_relay', 'member')))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND pg_has_role(current_user, 'es_relay', 'member')));

DROP POLICY IF EXISTS unique_claims_workspace_isolation ON unique_claims;
CREATE POLICY unique_claims_workspace_isolation ON unique_claims
    USING (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND pg_has_role(current_user, 'es_relay', 'member')))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND pg_has_role(current_user, 'es_relay', 'member')));
