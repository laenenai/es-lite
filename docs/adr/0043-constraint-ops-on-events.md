# ADR 0043: Constraint Ops on the Event Log (Log-Only Restore)

- **Status:** Accepted (implementation in progress)
- **Date:** 2026-11-21
- **Related:** ADR 0008 (uniqueness as a first-class constraint), ADR 0025
  (client-owned crypto and erasure), ADR 0029 (the chain attests the log),
  ADR 0037 (commit-safe feed reads), ADR 0038 (`log_order`), ADR 0042
  (derived state is re-derivable)

## 1. Context

The question is operational: can a deployment back up es-lite by streaming the
**events** table to object storage and rebuild everything else by replay?

For aggregate state the answer was always yes. For uniqueness claims it was no.
`Decide` returns constraints beside the events (`aggregate/runtime.go`), the
envelope had no field for them, and the store applied them to `unique_claims` and
discarded the operations. An append that carried a claim and **zero events**
(`aggregate/runtime.go`'s no-op check) changed committed state with no record
anywhere. An events-only restore therefore lost the claims index silently.

A first design recorded the ops in an `append_decisions` sidecar table, in the
append transaction. It restored the index and kept the claim-only append, but it
left two tables where the event-sourcing promise says one, needed a one-time
backfill for older stores, and needed an advisory lock to keep sidecar id order
equal to apply order — because a claim-only append does not bump the stream
version.

This ADR decides the stronger model. **The append's constraint ops are part of
the event log.** A claim is a state change; a state change without an event is
invisible state, and invisible state is the bug this feature exists to remove.
The product decision is explicit: there is no backward-compatibility requirement
for stores written before this model, and no compatibility shims are carried.

## 2. Decision

### 2.1 The ops ride on the first event of the append

- `AppendParams.Constraints` stays the input. The store encodes the ops and
  writes them to a new nullable `constraint_ops` column on the **first** event
  row of the batch; every other row in the batch gets `NULL`. One append writes
  one stream, and the first event is that append's lowest version and position —
  a stable anchor for a batch-level record.
- `es.Envelope` gains `ConstraintOps []ConstraintOp`, decoded on read; the first
  envelope in an `AppendResult` carries them. A corrupt blob fails the read
  loudly, like a corrupt audit UUID.
- The encoding is binary and versioned: domain `es-lite.constraint-ops.v1`, u32
  op count, then per op `u8 kind` (Claim/Release) and length-prefixed `scope`
  and `value`. `ConstraintOp.Value` is an opaque byte string and is never
  normalized.
- **The field is excluded from `CanonicalEnvelope`**, exactly like
  `SpanContext`: the chain attests the event, not the ops. Including it would
  either change every event's hash and break historical verification, or require
  a conditional canonical format with its own version. The ops are a restore
  record on the log row; the index they feed is trusted infrastructure, the same
  boundary as `unique_claims` before this change. A future envelope version can
  attest them; this one does not.
- **A constraint-bearing append must carry at least one event.** `len(events) ==
  0 && len(constraints) > 0` is a new typed error, `es.ErrConstraintWithoutEvent`,
  checked by `aggregate.Runtime` after `Decide` and by both stores in
  `validateAppend`. A `Release` with no domain event must name an event too. This
  is the semantic core, and it also removes the ordering race: every
  constraint-bearing append bumps the stream version, so same-stream appends
  serialize on optimistic concurrency. No sidecar, no advisory lock, no id
  ordering rule.

### 2.2 Claims are a projection of the events

`unique_claims` remains the live, enforcing index, maintained by `applyOps` in
the append transaction. It becomes indistinguishable from any other projection
of the log:

- `RebuildClaims` reads the workspace's events in **global position order**,
  decodes `constraint_ops` on each row, applies release-then-claim, and replaces
  the index in one transaction.
- `VerifyClaims` rebuilds into a temporary table and diffs both directions.
- `ClaimWorkspaces` is `SELECT DISTINCT workspace_id FROM events`; a workspace
  whose only appends were claim-only cannot exist.
- An inconsistent store — live claims with no ops in the event log — is refused
  with `es.ErrClaimsWithoutDecisions` rather than guessed at or deleted. That is
  a partial restore or out-of-band SQL, not a migration state.
- There is no `append_decisions` table, no decision id, and no adopt/backfill.

### 2.3 What a restore needs

**`events` is the functional source.** Loading the events reconstructs the
aggregates, the chain, the positions, and the claims index (by replaying the
ops). Schema and roles come from migrations and deployment; the position
sequence needs `setval`.

`log_order` is the **order witness**, not state. It is not needed to restore
working behavior; it is needed to re-verify the global append order on the
restored database. Stream it alongside the events if you want `es-verify order`
and historical anchor checks on the clone; otherwise the exported anchor JSON
is the evidence of the original order. It is small and predates this ADR
(ADR 0038).

`command_dedup` is excluded (a post-commit response cache; ADR 0043 §Idempotency
below). `snapshots` rebuild lazily, `feed_checkpoints` and `feed_dead_letters`
are optional, `feed_leases` is ephemeral, and `anchors` derive from events and
`log_order`.

On restore, advance `events_global_position_seq` to `max(global_position)` and
keep `CACHE 1`. There is no decision sequence.

### 2.4 Erasure

The ops are in the immutable log, so a **plaintext claim value is permanent**.
The ADR 0025 contract is now load-bearing rather than advisory: for personal
data the caller passes a keyed MAC and destroys the key; the stored value is
inert without it. Deleting `unique_claims` rows is a rebuildable cache wipe, not
erasure — the next rebuild resurrects the claim from the event. The erasure
model for claims is now the same as for event payloads: client-owned
crypto-shredding, not row deletion. There is no `PurgeDecisions`.

Because the chain does not attest the ops, a database writer can alter them —
as they can alter `unique_claims`. The trust boundary is the database, exactly
as before; nothing in this ADR claims otherwise.

### 2.5 No legacy path

A pre-0.21 store has claims with no ops on events. There is no backfill: the
information does not exist. `RebuildClaims` and `VerifyClaims` refuse such a
store (`ErrClaimsWithoutDecisions`); the upgrade path is a fresh database, or an
explicit export of the claim values and a re-claim through the new append path.
No dual-write, no sidecar, no source branching. This is the accepted cost of the
events-only model.

### 2.6 Idempotency stays out

`command_dedup` is written after the append commits, because the response does
not exist before then; it cannot be part of the append transaction. It is a
cache of a completed command, carries domain content (often PII), and is
workspace-private with no relay bypass arm. The default is to exclude it from
backups. A retry after a restore can return the documented 409 duplicate
outcome with `WithDeterministicEventIDs`; without it, optimistic concurrency is
the defence. Its missing erasure surface (ADR 0025 §5 does not name it, and
`DedupStore` has no purge) is a separate gap, not something to fix by copying
responses into the log.

### 2.7 Streaming and clones

The events table is a logical replication feed. The consumer contract:

- **Apply per source transaction, not per table.** The ops are part of the
  event row, so an event and its claim are never split. Deduplicate events on
  `(workspace, stream, version)` or `event_id`; a duplicate event skips its ops
  with it. A consumer that applies ops separately must treat a duplicate Claim
  as a no-op, not `ErrConstraintViolated` — the source already validated it.
- **Order by `global_position`.** Per-workspace clones filter by
  `workspace_id`; claims are per-workspace, so such a clone is self-consistent.
- **Refuse an unknown ops format.** The encoding carries a versioned domain; a
  consumer on an older schema must reject a newer record, not mis-decode it.
- **Follow vs fork.** A read-only clone keeps applying and can run
  `es-verify all|order|claims` continuously. A writable fork seeds at a cut,
  `setval`s the position sequence, and then stops applying; it never merges back,
  and promotion is a cutover, not a merge.
- **`log_order` is optional for a functional clone** and required to re-verify
  order. `command_dedup` does not travel.

## 3. Consequences

Positive:

- The events table is the functional source of truth in the literal sense:
  aggregates, chain, positions, and claims. One table to stream.
- The ops travel on the feed, so a consumer that already reads envelopes gets
  them with no second stream.
- The sidecar, the adopt/backfill path, the decision codec's id ordering, and
  the advisory lock are gone. A claim-only append cannot hide state.
- Same-stream ordering is ordinary optimistic concurrency again: every
  constraint-bearing append bumps the version.

Costs:

- A claim or release must be accompanied by a domain event. A command that
  claims without an event must name one. That is a real API/domain restriction.
- Claim values live in the immutable log: erasure is MAC + key destruction, and
  a plaintext claim value cannot be erased. This is a stronger obligation than
  the sidecar's deletable rows.
- The ops are not chain-attested. The DB is the trust boundary for them.
- `log_order` is still a second table if you want order re-verification.
- Pre-0.21 stores are unsupported for rebuild/verify.

## 4. Alternatives considered

- **An `append_decisions` sidecar** (the first design). It kept claim-only
  appends and deletable decisions, at the cost of a second table, an adopt path,
  and an ordering lock. Rejected by the product owner: the property required is
  an events-only restore, and a claim without an event is invisible state.
- **A control stream of synthetic events.** It keeps claim-only appends by
  inventing an event, but pollutes versions, the chain, the feed, and every
  consumer's type filter. Rejected for the same reason the runtime now rejects
  the invisible append.
- **Ops on every event of the batch.** Duplicates a batch-level record N times
  and leaves readers to pick one. Rejected.
- **Derive ops from domain event payloads.** Requires every domain's schema and
  a per-domain reindex; not generic, and impossible for a release with no event.
- **Include the ops in the chain.** Would change every event's hash (breaking
  historical verification) or require a conditional canonical format. Deferred
  to a future envelope version, if ever needed.

## 5. Rollout

- Migration `postgres 0014`: `ALTER TABLE events ADD COLUMN constraint_ops
  bytea` (partitioned parent; partitions inherit). `sqlite 0012`: the same as
  `BLOB`. No new table, no new grants.
- Store insert/scan: write the encoded ops on the first event of the batch, read
  and decode on every envelope scan; `Envelope.ConstraintOps` is excluded from
  the hash.
- Runtime and both stores reject `ErrConstraintWithoutEvent`.
- Rebuild/verify/`ClaimWorkspaces` read events; delete the sidecar code, its
  migrations, and its tests.
- Least privilege: the appender already has `SELECT, INSERT` on `events`; the
  new column is covered. Remove the `append_decisions` grants.
- Docs: runbook (backup/restore/clone/erasure), migration note (new column, no
  new table, claim-only rejection), README (the events-only rule and the MAC
  obligation).
- Version: v0.21.0. No Go API break beyond `ErrConstraintWithoutEvent` and the
  new envelope field; a claim-only command that used to succeed now fails.

## 6. Resolved

1. **Where the ops live:** on the first event of the append, in a nullable
   `constraint_ops` column, decoded into `Envelope.ConstraintOps`.
2. **Chain:** excluded, like `SpanContext`; the chain attests the event.
3. **Claim-only appends:** rejected (`ErrConstraintWithoutEvent`).
4. **Restore set:** events for function; `log_order` for order verification;
   `command_dedup` excluded.
5. **Legacy:** no backfill; an inconsistent store is refused, not guessed.
6. **Erasure:** MAC + key destruction; claims follow the event-payload model.
