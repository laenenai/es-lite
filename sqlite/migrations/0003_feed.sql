-- Feed delivery state (ADR 0002): per-subscription checkpoints and the
-- leases that fence one active subscriber per key. SQLite is
-- single-workspace, so neither table carries a workspace id; scope is kept
-- in the checkpoint key for parity with multi-tenant backends and named
-- tenants, not for isolation.

CREATE TABLE IF NOT EXISTS feed_checkpoints (
    scope text NOT NULL,
    projection text NOT NULL,
    instance text NOT NULL,
    position integer NOT NULL,
    updated_at text NOT NULL,
    PRIMARY KEY (scope, projection, instance)
);

CREATE TABLE IF NOT EXISTS feed_leases (
    key text PRIMARY KEY,
    owner text NOT NULL,
    consumer text NOT NULL,
    expires_at_ns integer NOT NULL
);
