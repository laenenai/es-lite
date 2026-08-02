-- es-lite Postgres schema (ADR 0004). Applied idempotently on Open.
--
-- Shared tables for 100k+ workspaces: every row carries workspace_id,
-- isolation is enforced by RLS (not a WHERE clause callers must remember),
-- and the events table is HASH-partitioned by workspace_id to keep indexes
-- and autovacuum tractable at scale. The events table is append-only:
-- es-lite issues no UPDATE/DELETE against payloads. published_at is the one
-- mutable column, flipped by the delivery relay's claim-drain (ADR 0001 §5).

-- A shared sequence gives every event a global, monotonic position across
-- all partitions and workspaces — the delivery ordering key.
CREATE SEQUENCE IF NOT EXISTS events_global_position_seq;

CREATE TABLE IF NOT EXISTS events (
    workspace_id    text        NOT NULL,
    global_position bigint      NOT NULL DEFAULT nextval('events_global_position_seq'),
    event_id        text        NOT NULL,
    stream_type     text        NOT NULL,
    stream_id       text        NOT NULL,   -- canonical "type:id"
    version         bigint      NOT NULL,   -- per-stream, 1-based, contiguous
    type_url        text        NOT NULL,
    schema_version  int         NOT NULL,
    occurred_at     timestamptz NOT NULL,
    recorded_at     timestamptz NOT NULL DEFAULT now(),
    correlation_id  text        NOT NULL,
    causation_id    text        NOT NULL,
    command_id      text        NOT NULL,
    actor_type      text        NOT NULL,
    actor_id        text        NOT NULL,
    actor_principal text        NOT NULL,
    payload         bytea       NOT NULL,   -- AEAD ciphertext (per-workspace DEK)
    published_at    timestamptz,            -- NULL until the relay publishes it

    -- Optimistic concurrency + partition key lead. UNIQUE constraints on a
    -- partitioned table must include the partition key (workspace_id).
    PRIMARY KEY (workspace_id, stream_id, version),
    UNIQUE (workspace_id, event_id)
) PARTITION BY HASH (workspace_id);

-- Fixed partition count (8 here; size for your workspace count/throughput).
-- NOT one-per-workspace: 100k partitions would cripple planning (ADR 0004).
CREATE TABLE IF NOT EXISTS events_p0 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS events_p1 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS events_p2 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS events_p3 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS events_p4 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS events_p5 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS events_p6 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS events_p7 PARTITION OF events FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX IF NOT EXISTS events_global_pos_idx  ON events (global_position);
CREATE INDEX IF NOT EXISTS events_unpublished_idx ON events (global_position) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS events_correlation_idx ON events (workspace_id, correlation_id);
CREATE INDEX IF NOT EXISTS events_actor_idx       ON events (workspace_id, actor_principal);
CREATE INDEX IF NOT EXISTS events_stream_time_idx ON events (workspace_id, stream_id, recorded_at);

-- RLS: isolation is structural. current_setting(..., true) returns NULL when
-- unset, so an unscoped query matches no rows. The relay/admin path sets
-- app.bypass_rls='on' to read across workspaces. FORCE makes even the table
-- owner subject to the policy.
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS events_workspace_isolation ON events;
CREATE POLICY events_workspace_isolation ON events
    USING (workspace_id = current_setting('app.workspace_id', true)
           OR current_setting('app.bypass_rls', true) = 'on')
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true)
           OR current_setting('app.bypass_rls', true) = 'on');

-- workspace_keys: one wrapped DEK per workspace (shred.WrappedDEKStore).
-- No RLS: the wrapped bytes are cryptographically inert without the KEK in
-- OpenBao, and the DEK-provisioning path queries them by explicit id outside
-- a workspace-scoped transaction.
CREATE TABLE IF NOT EXISTS workspace_keys (
    workspace_id text        PRIMARY KEY,
    wrapped_dek  bytea       NOT NULL,
    kek_version  int         NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- checkpoints: per-subscriber delivery progress (SQLite-style poller path).
CREATE TABLE IF NOT EXISTS checkpoints (
    subscriber text        PRIMARY KEY,
    position   bigint      NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- unique_claims: cross-stream uniqueness, per-workspace (ADR 0008). value_key
-- is the plaintext value for non-PII scopes, or an HMAC under the workspace
-- key for PII scopes (so shredding the workspace key unlinks it). The PK is
-- the uniqueness constraint; a colliding Claim rolls back the whole append
-- with es.ErrConstraintViolated. RLS scopes it like events.
CREATE TABLE IF NOT EXISTS unique_claims (
    workspace_id text        NOT NULL,
    scope        text        NOT NULL,
    value_key    bytea       NOT NULL,
    stream_id    text        NOT NULL,
    claimed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, scope, value_key)
);

ALTER TABLE unique_claims ENABLE ROW LEVEL SECURITY;
ALTER TABLE unique_claims FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS unique_claims_workspace_isolation ON unique_claims;
CREATE POLICY unique_claims_workspace_isolation ON unique_claims
    USING (workspace_id = current_setting('app.workspace_id', true)
           OR current_setting('app.bypass_rls', true) = 'on')
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true)
           OR current_setting('app.bypass_rls', true) = 'on');
