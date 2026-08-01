# ADR 0005: Projection Rebuild

- **Status:** Accepted (design; primitive deferred)
- **Date:** 2026-08-01

## Context

Projections (read models) are asynchronous and disposable (ADR 0001,
ADR 0003): they are derived from the event log, never the source of
truth. That means they can — and periodically must — be rebuilt: when a
read model's schema changes, when a projection bug is fixed, or when a
new projection is added long after the events it needs were written.

This ADR settles how rebuild works, what primitive es-lite provides, and
how rebuild interacts with the two things that could make it subtly
wrong: at-least-once delivery and per-workspace crypto-shredding.

## Decision

### Rebuild replays the DB log, not JetStream

The authoritative rebuild source is the **event log in the database**,
read via `Store.ReadAll` from position 0. JetStream retention is a
fast-replay convenience, not an archive (ADR 0003): a rebuild that must
reach further back than retention holds would come up short. The DB is
truth, so the DB is what a full rebuild reads.

### The primitive: `Replay` over `ReadAll`

es-lite provides a small `projection.Replay`:

```go
func Replay(ctx, store es.Store, from uint64, apply func(context.Context, []es.Envelope) error) (uint64, error)
```

It pages `Store.ReadAll(from, batch)` in global order, calls `apply` on
each batch, and returns the last position applied. It is the same shape
as a `delivery.Handler`, so the *same* projection code serves both live
delivery and rebuild — there is no separate "rebuild handler" to keep in
sync with the live one.

### Idempotency + a last-applied-position marker

Every read model records the **highest `global_position` it has
applied** — a single marker (a column on a metadata row, or on each
row). This one value does double duty:

- **At-least-once safety.** On live delivery, an event whose
  `global_position` ≤ the marker is skipped, so a redelivered batch after
  a crash is a no-op. This is what lets handlers be idempotent without
  every handler hand-rolling dedup.
- **Exact cutover.** After a rebuild replays up to position P, live
  delivery resumes at exactly P with no gap and no overlap.

Read-model writes should be deterministic upserts keyed by the
projection's natural key, so re-applying an event already reflected in
the model changes nothing.

### Default strategy: blue/green replay

Two rebuild strategies; **blue/green is the default**:

1. **Blue/green (zero-downtime, default).** Build a *new* versioned read
   model (`orders_v2`) in the background: `Replay(store, 0, applyV2)`
   while the old model keeps serving reads and consuming live events.
   When v2's marker catches the live tail, atomically flip readers to v2
   (a view swap, a table rename, or a config pointer) and retire v1.
   The overlap window — live events arriving while the backfill runs — is
   handled by v2 also consuming live from the start; the position marker
   makes the overlap idempotent.

2. **Stop-the-world (documented, for small/offline models).** Pause the
   consumer, `Replay(store, 0, apply)` into the truncated model, resume
   live from the returned position. Trivial, but the projection is stale
   until it finishes. Fine for small or non-user-facing read models.

The `Replay` primitive is identical for both; the difference is purely in
how the application manages the model version and the reader swap, which
es-lite does not prescribe (it is application-owned, like the read-model
schema itself).

### Interaction with crypto-shredding

Rebuild decrypts each event via its workspace DEK (ADR 0004). A workspace
that has been shredded has no DEK, so its events cannot be decrypted —
they are, correctly, **excluded from any rebuild**. Rebuild therefore
naturally reflects erasure: a rebuilt projection contains nothing for a
forgotten workspace, with no special-casing. The decrypt step returns a
"shredded" sentinel the `Replay` caller skips; it is not an error.

This is also why erasure must purge the *existing* projection rows for a
workspace at shred time (ADR 0004 §4): until the next rebuild, the live
model still holds the decrypted copies a rebuild would omit.

## Consequences

### Positive

- **One code path for live and rebuild.** `apply` is a
  `delivery.Handler`; `Replay` feeds it from the log. No divergence.
- **Correctness from one number.** The last-applied-position marker gives
  both at-least-once idempotency and exact rebuild cutover.
- **Zero-downtime by default.** Blue/green keeps reads live throughout;
  stop-the-world stays available for the simple cases.
- **Erasure is respected for free.** Shredded workspaces drop out of
  rebuilds because their events won't decrypt.

### Negative

- **Full replay cost grows with log size.** A rebuild reads the whole
  log. Hash partitioning (ADR 0004) and batched `ReadAll` keep it
  tractable, and a rebuild can be scoped to one workspace
  (`ReadAllForWorkspace`) when only one read model is affected — but a
  global rebuild is inherently O(events).
- **Blue/green needs double storage transiently.** v1 and v2 coexist
  until the swap. Acceptable and temporary.
- **Read-model versioning is the application's job.** es-lite provides
  `Replay` and the marker convention; the table-versioning and reader
  swap are not framework-owned, by design.

## Alternatives Considered

### Rebuild from JetStream by resetting the consumer

Convenient for recent history and fine as an optimization, but incomplete
whenever retention is shorter than the rebuild horizon. Kept as a fast
path for "replay the last N days", not as the authoritative rebuild.

### A dedicated rebuild handler distinct from the live handler

Rejected. Two handlers for one projection drift apart; the bug you fixed
in live delivery reappears in rebuild. Reusing the one `apply` via
`Replay` keeps them identical by construction.
