-- Log-order attestation (design D1, finding F11) — the SQLite mirror
-- of Postgres's 0006. The per-stream hash chain and the anchors over
-- stream heads bind WHAT each stream contains; nothing bound the order
-- in which events landed in the global log — a batch's positions were
-- the only witness, and they live in the same rows a database operator
-- can rewrite. This migration adds the per-batch witness table;
-- anchors aggregate its rows into an order root (the order analog of
-- the heads root), and `es-verify order` recomputes each row's hash
-- from the events — the audit that actually catches a repositioned
-- event, which leaves every chain and every head intact. It mirrors
-- the chain/anchor trust split of ADR 0029: the per-batch hash is the
-- cheap in-transaction witness, the exported order root the off-box
-- aggregate.
--
-- One row per non-empty append batch, written in the SAME transaction
-- as the batch's events — no new reads, no new locks, no
-- cross-transaction dependency. The row's batch_hash is
-- es.BatchOrderHash: SHA-256 over the "es-lite.log-order.v1" domain
-- separator (u32 length-prefixed), a big-endian u32 event count, then
-- per event, in ascending position order, a big-endian u64 global
-- position followed by the event's 32-byte stored chain hash. The
-- single writer assigns global_position (the events table's rowid) in
-- insert order inside the one append transaction, so a batch's rows
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
-- from_position is the PRIMARY KEY: the single-writer connection
-- serializes appends, so a batch's first position is drawn by exactly
-- one transaction and no two committed batches share it. to_position
-- gets the index: the anchor path and the deep verify both select rows
-- with to_position <= the bound.
--
-- stream_id scopes a row to the stream its batch wrote (one append
-- writes one stream): the verifier recomputes a row's hash over
-- exactly the events of its own stream inside its range, which no
-- other batch can share — under the single writer every batch's range
-- is disjoint anyway, but the scope keeps the recompute honest
-- whatever a database operator has rearranged behind the store's back.
--
-- No workspace_id and no RLS: SQLite is single-workspace with no
-- row-level security (0001's rule), so a row needs no further scope or
-- isolation. created_at is table bookkeeping in the schema's TEXT
-- house style (ms-precision strftime, like schema_migrations and
-- unique_claims.claimed_at), supplied by the DEFAULT — the append does
-- not track it.
--
-- Numbered 0006 for cross-backend version parity with Postgres's
-- log_order migration: SQLite has no 0005 — Postgres's is
-- RLS-role-specific.

CREATE TABLE IF NOT EXISTS log_order (
    from_position INTEGER PRIMARY KEY,
    to_position   INTEGER NOT NULL,
    stream_id     TEXT    NOT NULL,   -- canonical "type:id", as events stores it
    event_count   INTEGER NOT NULL,
    batch_hash    BLOB    NOT NULL,   -- es.BatchOrderHash over the batch's own events
    created_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS log_order_to_position_idx ON log_order (to_position);
