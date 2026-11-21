-- The claims replay scans op-bearing events in global position order (ADR
-- 0043 §2.2). Without an index whose predicate matches
-- `constraint_ops IS NOT NULL`, the planner walks the workspace's events and
-- fetches every row to test the null; the partial index makes the scan
-- proportional to op-bearing batches, not events.
--
-- `(workspace_id, global_position)` serves the workspace predicate, the
-- position range and the ORDER BY. `INCLUDE (stream_id, constraint_ops)`
-- covers the replay's SELECT, so it is an index-only scan: without the
-- INCLUDE the default cost model prefers the global position index and pays a
-- heap fetch per candidate row. The predicate excludes non-op rows from the
-- index entirely. The appender's existing SELECT, INSERT on events covers it;
-- no new grants.
--
-- On a store with >=10M rows, build per partition with CONCURRENTLY and
-- ATTACH before upgrading — see the deployment runbook (migration 0015).
-- The plain build here runs inside the migration transaction and blocks
-- appends for its duration.
CREATE INDEX IF NOT EXISTS events_ops_idx
    ON events (workspace_id, global_position)
    INCLUDE (stream_id, constraint_ops)
    WHERE constraint_ops IS NOT NULL;
