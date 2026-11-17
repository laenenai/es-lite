-- Producer span context (ADR 0040): the producer's W3C trace context,
-- persisted alongside each event so consumers can join the events back
-- to the producing trace.
--
-- The three columns are nullable and carried as a set: a context is
-- either wholly absent (all three NULL — the overwhelming majority of
-- rows, and the shape every pre-0011 row has) or wholly present. The
-- CHECK below enforces exactly that all-or-nothing shape and the W3C
-- Trace Context wire sizes: both ids present or both absent, the trace
-- id 16 bytes, the span id 8 bytes, and the flags byte in range.
-- smallint holds the unsigned flags byte (0-255) with headroom; the
-- store maps it to uint8 on read.
--
-- Telemetry, not attestation: the columns are excluded from the
-- envelope hash (es.EnvelopeHash), so a context is never part of the
-- chain and a legacy reader ignores them safely.
--
-- NOT VALID + VALIDATE CONSTRAINT keeps adding the check short-lock on
-- a live table: the ALTER takes only the brief ACCESS EXCLUSIVE lock to
-- register the constraint without scanning, and the validation scan —
-- the only long step — runs afterwards under the weaker lock. New rows
-- are checked immediately; existing rows are verified by the scan. The
-- events table is HASH-partitioned (ADR 0004), so both ALTERs target
-- the parent and Postgres propagates the columns and the constraint to
-- every partition.
--
-- No grants are issued here, like every migration before this one: the
-- columns inherit the table's existing grants, so no new privilege is
-- needed to read or write them.

ALTER TABLE events
    ADD COLUMN trace_id    bytea,
    ADD COLUMN span_id     bytea,
    ADD COLUMN trace_flags smallint;

ALTER TABLE events
    ADD CONSTRAINT events_span_context_complete CHECK (
        (trace_id IS NULL) = (span_id IS NULL)
        AND (span_id IS NULL) = (trace_flags IS NULL)
        AND (trace_id IS NULL OR octet_length(trace_id) = 16)
        AND (span_id  IS NULL OR octet_length(span_id)  = 8)
        AND (trace_flags IS NULL OR trace_flags BETWEEN 0 AND 255)
    ) NOT VALID;

ALTER TABLE events VALIDATE CONSTRAINT events_span_context_complete;
