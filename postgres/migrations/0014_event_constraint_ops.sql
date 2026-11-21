-- Constraint ops on the event log (ADR 0043): the append's Claim/Release
-- sequence is stored on the FIRST event row of the batch, so an events-only
-- restore reconstructs the unique_claims index by replaying the log.
--
-- Nullable: an append with no constraints writes NULL on every row, and every
-- row after the first in an op-bearing batch is NULL too — the ops are a
-- batch-level record anchored at the append's lowest version/position. The
-- encoding is es.EncodeConstraintOps ("es-lite.constraint-ops.v1"). The
-- column is deliberately excluded from the hash chain (CanonicalEnvelope, like
-- span_context), so adding it changes no existing event's hash.
--
-- No new table, no new grants: the appender's existing SELECT, INSERT on
-- events covers the column. The parent is partitioned; partitions inherit the
-- column.

ALTER TABLE events ADD COLUMN constraint_ops bytea;
