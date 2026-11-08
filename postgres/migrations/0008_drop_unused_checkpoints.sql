-- Drop the unused checkpoints table (review F34-partial). 0001 created
-- it as "per-subscriber delivery progress (SQLite-style poller path)",
-- but the Postgres store never grew that poller: delivery on Postgres
-- is the gap-safe relay Drain (ADR 0001 §5), and per-subscriber feed
-- progress lives in feed_checkpoints (0004). Only SQLite implements
-- delivery.Checkpoints; the Postgres table has no reader, no writer,
-- and — alone among es-lite's tables — no RLS policy, so a stray
-- session could read it without any workspace gate.
--
-- Dead weight: it goes. DROP TABLE IF EXISTS is transactional DDL, so
-- it is safe inside this migration's transaction (ADR 0010:
-- migrations are append-only, each runs in one transaction). Nothing
-- that reads the table is lost: no es-lite code path ever consulted
-- it, and the forward-only history records the drop as 0008.

DROP TABLE IF EXISTS checkpoints;
