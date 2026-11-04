-- Feed page indexes (spec §5.2). The feed pages events by global_position
-- within one workspace; by-type reads add type_url ahead of the position.
--
-- Build order is operational, not incidental: migrations run in one
-- transaction (ADR 0010), which forbids CREATE INDEX CONCURRENTLY, and a
-- plain build on a >=10M-row partitioned table blocks appends for the whole
-- build — with auto-migrate-on-Open, the first upgraded replica would take
-- that lock at boot. For databases of that size, build the indexes without
-- blocking appends BEFORE this migration is allowed to run (per-partition
-- CONCURRENTLY builds, then ON ONLY + ATTACH PARTITION — Postgres cannot
-- build a partitioned parent index concurrently at all; see
-- docs/deploy/README.md, "Index build order"). These IF NOT EXISTS
-- statements are the enforcement net — an instant no-op when the runbook
-- was followed, a blocking build only on small databases that skipped it.
-- A CREATE INDEX on the partitioned parent creates one child index per
-- partition under the named parent index.

CREATE INDEX IF NOT EXISTS events_ws_pos_idx      ON events (workspace_id, global_position);
CREATE INDEX IF NOT EXISTS events_ws_type_pos_idx ON events (workspace_id, type_url, global_position);
