-- Snapshot cache (ADR 0042). Derived, droppable state: one row per
-- (workspace, stream), written by aggregate.Runtime outside the append
-- transaction. Not part of the audit log; the hash chain attests events,
-- not this table. The name is reserved: an adopter's own "snapshots"
-- table with a different shape must fail this migration loudly, because
-- the runtime ignores Load/Save errors and would otherwise full-fold
-- forever with no signal.
--
-- No monotonic guard and no updated_at: last writer wins. Per-stream
-- appends are serialized by expected-version conflicts, and a stale write
-- costs one load at most n extra folded events before the next gap
-- boundary rewrites it. A guard would permanently wedge a stream after a
-- log reset (ADR 0042 §3.2).

CREATE TABLE IF NOT EXISTS snapshots (
    workspace_id text        NOT NULL,
    stream_id    text        NOT NULL,   -- canonical "type:id"
    version      bigint      NOT NULL,
    fold_version bigint      NOT NULL,
    recorded_at  timestamptz NOT NULL,   -- the runtime's Snapshot.RecordedAt
    state        bytea       NOT NULL,
    PRIMARY KEY (workspace_id, stream_id)
);

-- Refuse a pre-existing table of the same name with an incompatible
-- shape. The columns are the shape, not just their names: a required
-- column of the wrong type breaks every Save (and Load), and an extra
-- NOT NULL column with no default breaks the adapter's INSERT, which
-- names only its own six columns — the runtime ignores those errors,
-- which is exactly the silent degradation this refusal exists to
-- prevent. An extra nullable column, one with a default, or an
-- identity/generated one, breaks neither and is tolerated.
DO $$
DECLARE problems text;
BEGIN
    WITH required(name, type) AS (
        VALUES ('workspace_id', 'text'),
               ('stream_id',    'text'),
               ('version',      'bigint'),
               ('fold_version', 'bigint'),
               ('recorded_at',  'timestamp with time zone'),
               ('state',        'bytea')
    ),
    existing AS (
        SELECT column_name, data_type, is_nullable, column_default,
               is_identity, is_generated
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'snapshots'
    )
    SELECT string_agg(problem, '; ') INTO problems
    FROM (
        SELECT CASE
                   WHEN e.column_name IS NULL THEN r.name || ' missing'
                   WHEN r.name IS NULL THEN
                       CASE WHEN e.is_nullable = 'NO'
                                 AND e.column_default IS NULL
                                 AND e.is_identity <> 'YES'
                                 AND e.is_generated <> 'ALWAYS'
                               THEN e.column_name || ' is NOT NULL with no default'
                       END
                   WHEN e.data_type <> r.type THEN
                       r.name || ' is ' || e.data_type || ', want ' || r.type
               END AS problem
        FROM required r
        FULL JOIN existing e ON e.column_name = r.name
    ) v
    WHERE problem IS NOT NULL;
    IF problems IS NOT NULL THEN
        RAISE EXCEPTION 'snapshots exists with an incompatible shape (%) — rename it (the name is reserved by ADR 0042) and re-run the migration', problems;
    END IF;
END $$;

-- Workspace-private, with no bypass arm — the feed tables' shape (0004,
-- 0010), not the log tables' (0001/0005/0007): the relay never reads
-- snapshots, and the state they carry is the workspace's own derived
-- data, so even es_relay under app.bypass_rls must be refused. FORCE
-- keeps the table owner subject to the same single arm.
ALTER TABLE snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE snapshots FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS snapshots_workspace_isolation ON snapshots;
CREATE POLICY snapshots_workspace_isolation ON snapshots
    USING (workspace_id = current_setting('app.workspace_id', true))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
