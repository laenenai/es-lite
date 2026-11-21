# S3 (object storage) events sink — possible backup and clone landing zone

- **Filed:** 2026-11-21
- **Area:** deploy / backup / streaming
- **Status:** Open — proposal only; nothing is implemented

## Idea

The events table is a logical stream ([ADR 0043 §2.7](../adr/0043-constraint-ops-on-events.md)).
An object-storage sink could land that stream in S3 (or any S3-compatible
store) as the durable backup and clone source, instead of requiring a CDC tool
or a second Postgres. The restore path already consumes events plus optional
`log_order`; the sink would produce exactly those.

This is recorded so the option is not lost. It is **not** a shipped feature and
the runbook must not present it as one.

## Contract the sink must honor

- **Per source transaction.** The ops ride on the first event of a batch, so an
  event and its claim must never be split across objects.
- **Order by `global_position`**, with gaps allowed: concurrent appends commit
  out of draw order.
- **Idempotent consumption.** At-least-once is acceptable if the loader
  dedupes on `(workspace, stream, version)` or `event_id` and a duplicate
  Claim is a no-op.
- **`log_order` optional.** Stream it only for order re-verification; the
  exported anchor JSON is the fallback evidence.
- **`command_dedup` excluded.**
- **Consistent cuts.** A manifest marks each cut at one source-transaction
  boundary; a restore loads one cut, never a mixture.
- **Restore side effects:** `setval('events_global_position_seq',
  max(global_position))`, `es-verify claims --rebuild`, and
  `es-verify all --chains-only` when there is no `log_order`.
- **Erasure:** objects are immutable, and the claim-erasure contract is the
  same as the log (keyed MAC plus key destruction), not object deletion.
  Object-lock/retention policy is a deployment choice.

## Open questions (an ADR is needed before code)

- **Shape:** an in-process `Drain`-like sink, a Postgres logical-decoding
  consumer, or a standalone tool (Debezium/pgoutput → S3)? What owns the
  position cursor and the cut manifest?
- **Format and layout:** raw `COPY`/NDJSON/Avro/Parquet; one object per batch,
  per position range, or per cut? How is the manifest versioned?
- **Delivery:** exactly-once versus at-least-once. Is dedupe on load enough,
  or must the sink guarantee one object per batch?
- **Compaction and cost:** small objects per batch are expensive. Is a
  compactor in scope, or does the sink write larger position-range objects?
- **Encryption and PII:** SSE-S3/SSE-KMS versus client-side. Does the sink see
  plaintext claim values, and does that change the MAC obligation?
- **Permissions:** which role reads `events` (and `log_order`) through the RLS
  bypass, and how is that principal scoped?
- **Verification:** can the clone run `es-verify claims` directly against the
  loaded store, and how is a cut's completeness proven before promotion?

## Acceptance criteria

- An ADR decides the shape, format, cut/manifest and dedupe contract.
- A reference implementation, or a documented and tested external-tool
  configuration, plus an end-to-end restore drill from S3 on a non-trivial
  dataset.
- The deployment runbook gains a concrete S3 procedure and loses the "possible"
  label here.

## Until then

Use the logical restore path in the
[deployment runbook](../deploy/README.md#backup-restore-and-clones-adr-0043)
(`pg_dump`/CDC of events plus optional `log_order`), or PITR.
