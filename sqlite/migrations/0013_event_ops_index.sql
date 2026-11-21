-- The claims replay scans op-bearing events in global position order (ADR
-- 0043 §2.2). SQLite is single-workspace, so the range and ORDER BY are on
-- global_position alone; the predicate keeps the index to op-bearing rows.
-- The plain build runs inside the migration transaction. For a large events
-- table, build the index out of band before upgrading — see the deployment
-- runbook (migration 0015).
CREATE INDEX IF NOT EXISTS events_ops_idx
    ON events (global_position)
    WHERE constraint_ops IS NOT NULL;
