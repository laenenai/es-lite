-- Delegated actor (ADR 0041): the service an actor acted through,
-- persisted alongside the accountable actor so the audit trail records
-- the whole delegation chain — who acted (actor_type, actor_id) and
-- through what (via_type, via_id).
--
-- The three columns are nullable and carried as a set, like the span
-- context before them (0009): a Via is either wholly absent (all three
-- NULL — every pre-0010 row, and the shape of an actor acting for
-- itself) or wholly present.
--
-- Deliberate divergence from Postgres 0012 (ADR 0041 §3.3): NO
-- all-or-nothing CHECK. Postgres registers its events_via_complete
-- CHECK on the live table; SQLite cannot ALTER TABLE ADD CONSTRAINT,
-- and parity would mean a third full events rebuild — 0009 rewrote the
-- table one release ago — to add a constraint the writer already
-- enforces. All-or-nothing is carried by the writer instead:
-- es.Actor.Validate refuses a partial Via in Append before anything is
-- written, and the strict read (decodeVia) fails a half-set row rather
-- than silently zeroing it into a forged envelope. A future migration
-- that touches events for another reason can add the constraint then.
--
-- via_principal mirrors actor_principal: the canonical "type:id" form,
-- indexed so the reverse audit query ("everything service S did,
-- across users") is cheap. Unlike the two id columns it is a derived
-- value, which is why the index keys on it. The index is partial —
-- WHERE via_principal IS NOT NULL — so it stays small while delegation
-- is rare. SQLite keys on via_principal alone: there is no workspace_id
-- to carry into the key the way Postgres's partitioned table does.
--
-- No table rebuild: ALTER TABLE ADD COLUMN is metadata-only, so every
-- existing row reads the three new columns as NULL — exactly its
-- recorded delegation, absent (ADR 0041 §3.3: no backfill; historical
-- events read as Via zero, which is true). SQLite DDL is transactional,
-- so the columns and the index commit atomically with the
-- schema_migrations row Migrate inserts first (ADR 0010).

ALTER TABLE events ADD COLUMN via_type      TEXT;
ALTER TABLE events ADD COLUMN via_id        TEXT;
ALTER TABLE events ADD COLUMN via_principal TEXT;

CREATE INDEX IF NOT EXISTS events_via_idx
    ON events (via_principal)
    WHERE via_principal IS NOT NULL;
