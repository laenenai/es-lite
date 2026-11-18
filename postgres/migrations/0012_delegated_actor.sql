-- Delegated actor (ADR 0041): the service an actor acted through,
-- persisted alongside the accountable actor so the audit trail records
-- the whole delegation chain — who acted (actor_type, actor_id) and
-- through what (via_type, via_id).
--
-- The three columns are nullable and carried as a set, like the span
-- context before them (0011): a Via is either wholly absent (all three
-- NULL — every pre-0012 row, and the shape of an actor acting for
-- itself) or wholly present. The CHECK below enforces exactly that
-- all-or-nothing shape; the store refuses a partial Via at append time
-- (es.Actor.Validate, ErrInvalidDelegation), so the constraint is the
-- second line of defence against a bypassed write path.
--
-- Attestation, not telemetry — the opposite tolerance of 0011: Via is
-- part of the envelope hash (es.EnvelopeHash's delegation block), so a
-- read that finds a half-set triple is an audit-fact defect and fails
-- the read instead of silently zeroing it. The CHECK makes that shape
-- unreachable from the store; strict reconstruction keeps a restored
-- dump or a constraint-bypassed migration honest too.
--
-- via_principal mirrors actor_principal: the canonical "type:id" form,
-- indexed with the workspace ahead of it so the reverse audit query
-- ("everything service S did, across users") is cheap. Unlike the two
-- id columns it is a derived value, which is why the index keys on it.
--
-- NOT VALID is kept; VALIDATE CONSTRAINT is deliberately omitted, and
-- the one-transaction rule (ADR 0010) is why. The ADD CONSTRAINT takes
-- its ACCESS EXCLUSIVE lock only to register the check — no scan — but
-- the migration's transaction holds that lock to commit, so a
-- following VALIDATE CONSTRAINT, the one long scanning step, would run
-- while the exclusive lock is still held and block appends for its
-- whole duration — it never gets the weaker lock a standalone
-- session's VALIDATE would take. The scan would also be pure cost: the
-- three columns are added in this same transaction, so every
-- pre-existing row is NULL and the CHECK cannot fail on any of them.
-- NOT VALID alone already enforces the all-or-nothing shape on every
-- new write — the store's 23514 test proves it — so the constraint is
-- left unvalidated: convalidated=false in the catalog is expected, and
-- an operator who cares about that state may run VALIDATE CONSTRAINT
-- events_via_complete later, in a maintenance window, where the
-- scan's lock lives and dies in that session's own transaction. The
-- events table is HASH-partitioned (ADR 0004), so both ALTERs target
-- the parent and Postgres propagates the columns and the constraint
-- to every partition.
--
-- No grants are issued here, like every migration before this one: the
-- columns inherit the table's existing grants, so no new privilege is
-- needed to read or write them.
--
-- Build order is operational, not incidental — the same constraint 0003
-- documents: migrations run in one transaction (ADR 0010), which forbids
-- CREATE INDEX CONCURRENTLY, and a plain build on a >=10M-row
-- partitioned table blocks appends for the whole build — with
-- auto-migrate-on-Open, the first upgraded replica would take that lock
-- at boot. For databases of that size, build the index without blocking
-- appends BEFORE this migration is allowed to run (per-partition
-- CONCURRENTLY builds, then ON ONLY + ATTACH PARTITION — Postgres cannot
-- build a partitioned parent index concurrently at all; see
-- docs/deploy/README.md, "Index build order (migration 0012)"). The IF
-- NOT EXISTS statement is the enforcement net — an instant no-op when
-- the runbook was followed, a blocking build only on small databases
-- that skipped it. A CREATE INDEX on the partitioned parent creates
-- one child index per partition under the named parent index.

ALTER TABLE events
    ADD COLUMN via_type      text,
    ADD COLUMN via_id        text,
    ADD COLUMN via_principal text;

ALTER TABLE events
    ADD CONSTRAINT events_via_complete CHECK (
        (via_type IS NULL) = (via_id IS NULL)
        AND (via_id IS NULL) = (via_principal IS NULL)
    ) NOT VALID;

CREATE INDEX IF NOT EXISTS events_via_idx
    ON events (workspace_id, via_principal)
    WHERE via_principal IS NOT NULL;
