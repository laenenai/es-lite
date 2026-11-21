# ADR 0043: Append Decisions and Log-Only Restore

- **Status:** Accepted (implementation pending)
- **Date:** 2026-11-21
- **Related:** ADR 0008 (uniqueness as a first-class constraint), ADR 0025
  (client-owned crypto and erasure), ADR 0029 (the chain attests the log, not
  derived state), ADR 0037 (commit-safe reads), ADR 0038 (`log_order`),
  ADR 0042 (derived state is re-derivable), ADR 0010 (migrations)

## 1. Context

The question that prompted this ADR is operational: can a deployment back up
es-lite by streaming the event log to object storage, and rebuild everything
else by replay, instead of taking database backups?

For aggregate state the answer is yes. Events are the source of truth, versions
are contiguous, and the chain plus `log_order` make both the contents and the
append order of the log verifiable. An events-only replay reproduces every
stream.

For the *append* the answer is no. Three pieces of committed state are not on
the events:

1. **Uniqueness claims.** `Decide` returns them beside the events
   (`aggregate/runtime.go:443`), and the envelope has no field for them
   (`es/envelope.go`). The only durable record is the `unique_claims` row
   applied in the append transaction (`postgres/wsstore.go:420,473`). The
   runtime also permits an append that carries a claim and **zero events**
   (`aggregate/runtime.go:447` short-circuits only when both are empty;
   `postgres/wsstore.go:404` writes `log_order` only for a non-empty batch), so
   such a claim has no representation anywhere in the log — not even a version
   bump.
2. **Idempotency responses.** `command_dedup.response` is the only copy of a
   completed response (`postgres/migrations/0010_command_dedup.sql`). The
   handler writes it *after* the append commits (`postgres/dedup.go:60`), and
   its own comment states the case: "The command committed but its response was
   not stored." It therefore cannot be part of the append's transaction.
3. **Batch identity.** `log_order` is one row per append, and concurrent
   batches' position ranges interleave (`0006_log_order.sql`), so batch
   membership is not recoverable from the events alone. Without the row, an
   anchor's order root cannot be rechecked (ADR 0038).

The claim values themselves are opaque by contract. For personal data the
caller supplies a keyed MAC and es-lite never holds the key (`es/constraint.go`,
ADR 0025 §3). Even a domain whose events happen to carry the plaintext cannot
rebuild the index without that key and a per-domain pass that reproduces the
ops — and the ops, unlike the events, were never recorded, while a claim-only
append left no event to reindex from at all.

The consequence is the current backup rule: the authoritative set is `events`
(+ partitions), `log_order`, `unique_claims`, `command_dedup`, the migration
ledger, and the position sequence. Everything else derives or is ephemeral.
That rule is correct, and it is exactly what an adopter choosing event sourcing
expects not to need.

This ADR separates the two things the word "log" conflates. **Events are the
record of what happened to a stream.** Claims are decisions about values,
idempotency responses are post-commit artifacts, and batch boundaries are
properties of the append. The fix is not to force them onto envelopes — a claim
that moves no version has no envelope to sit on — but to record an append's
non-event decision next to it, in the same transaction, in a form a restore can
replay.

## 2. What the log is, and what it is not

| State | Where it lives | Rebuildable from events? |
| --- | --- | --- |
| stream contents, versions, hashes | `events` | — (it is the log) |
| aggregate state | fold of `events` | yes |
| chain and heads anchors | `events` | yes (roots recompute; exported JSON is the evidence) |
| batch identity and order root | `log_order` | no — ranges interleave across batches |
| uniqueness claims | `unique_claims` | no — op not recorded, value may be client-MAC'd, claim-only appends invisible |
| idempotency responses | `command_dedup` | no — written after commit, response bytes |
| snapshots | `snapshots` | yes (ADR 0042) |
| feed checkpoints, dead letters | `feed_*` | no, but optional: replay from zero |
| feed leases | `feed_leases` | ephemeral, ignore |

Why the missing pieces cannot simply ride on an envelope:

- An event is a fact about a stream: it has a version, extends the stream, enters
  the chain, and is delivered. A constraint op has none of those properties.
  Attaching it to an event that did not cause it is a lie about causality;
  synthesizing an event pollutes versions, the chain, the feed, and every
  domain's `Evolve`.
- `ConstraintOp.Value` is a Go string used as an opaque byte string; under the
  ADR 0025 contract it is often a non-UTF-8 MAC. Any encoding must be binary and
  length-prefixed; JSON is not an option.
- `applyClaims` applies all releases first, then all claims, regardless of the
  order the caller supplied (`postgres/wsstore.go:473`). A replay must reproduce
  that rule rather than the literal slice.

## 3. Decision

### 3.1 Record the append's decisions in `append_decisions`

Add a table written in the same transaction as the append, one row per append
that carries at least one `ConstraintOp`:

```sql
append_decisions (
    id           bigint generated always as identity primary key,
    workspace_id text        not null,
    stream_id    text        not null,
    from_version bigint      not null,
    to_version   bigint      not null,  -- equal to from_version for a claim-only append
    command_id   text        null,      -- the append's CommandID, as events stores it
    ops          bytea       not null,  -- canonical encoding of the applied ops
    created_at   timestamptz not null default clock_timestamp()
)
```

- The row is written **after** `applyClaims` succeeds and before commit. A
  rolled-back append leaves no row, so the table never records an op the index
  did not apply.
- `ops` records the ops as passed to `applyClaims` (already validated), in
  caller order. The encoding is `u32 count` followed, per op, by `u8 kind`,
  `u32 len(scope) ‖ scope`, `u32 len(value) ‖ value`, under a versioned domain
  separator. The replay applies the same two-pass release-then-claim rule.
- Every append with at least one op writes a row, including a `Release` that
  matched nothing. The row answers "what did this append decide", not "what
  changed"; the replay is identical either way, and skipping no-ops would need
  a rows-affected check that makes the table's row set depend on prior state.
- `command_id` is kept. A claim-only append has no envelope to carry it, so for
  that case the column is the only link from the decision back to its cause; on
  an event-bearing append it duplicates the events' value and saves a join
  during an audit.
- SQLite mirrors the table without `workspace_id`, like `unique_claims`
  (single-workspace backend).
- `id` is the replay order. It is deliberately not a global position and does
  not draw from `events_global_position_seq`: a claim-only append must not burn
  a position, because SQLite's feed contract and regression guard assert that
  event positions are gap-free (`sqlite/store.go:7`, `sqlite/feed_test.go:362`).
- Two appends that touch the same `(workspace, scope, value)` serialize on the
  unique index or the row lock: the second one's `applyClaims` cannot finish
  until the first commits or rolls back, so it inserts its row afterwards.
  `id` order is therefore an apply order for every pair whose relative order
  can change the index; appends with no shared key commute. Writing the row
  after `applyClaims` is what makes that true — a row drawn before the claims
  could block and be overtaken.
- RLS mirrors `unique_claims`: the workspace arm plus the role-gated relay
  bypass arm in its hardened form (`0007_policy_hardening.sql`), with `FORCE ROW
  LEVEL SECURITY`. Grants mirror the index the table derives: the appender gets
  `SELECT, INSERT, DELETE`, `USAGE` on the identity sequence (`id` is
  `GENERATED ... AS IDENTITY`, so the insert draws, not supplies), and no
  `UPDATE`. The `DELETE` is the erasure arm
  (§3.4). The least-privilege test must pin the grant and drive a decision row
  through the appender role; v0.20.0 showed what an unpinned grant costs.

The name says what the row is: the part of `Decide`'s output that is not an
event. Alternatives are in §5.

### 3.2 `unique_claims` becomes a derived index

The rebuild is mechanical: start from an empty `unique_claims`, read
`append_decisions` in `id` order (filtered by workspace, or across workspaces
through the relay arm), apply each row's ops with the same release-then-claim
rule, in one transaction, against a database with no writers. `LookupClaim` and
the release/claim arms of `append` then see exactly the pre-restore index
(`postgres/wsstore.go:479,490,1206`).

Ship the oracle with it: `es-verify claims` rebuilds into a temporary table and
diffs it against the live one, reporting the first divergent
`(scope, value_key, stream_id)`. That is the same shape as the chain and order
checks: a cheap, keyless verification of state that is otherwise trusted. It is
part of the same release as the migration, because a rebuild path without an
oracle is untested by construction and a restore is exactly when that would
matter.

The hot path does not change. `applyClaims` keeps maintaining the live index,
because the unique index is the serialization point; the decision row is a
record, not a replacement. The rebuild refuses a pre-cutover state — live claims
with an empty decision log means the one-time backfill (§3.5) has not run — and
returns an error instead of deleting an index it cannot reconstruct.

### 3.3 The backup contract

After a one-time cutover (§3.5), the complete mutable truth is three tables
written by the append path, plus the schema:

- `events` (all partitions) — stream history, chain, positions;
- `log_order` — batch identity and the order root's input;
- `append_decisions` — the claims index's input.

A restore into an empty database is: run the migrations to head; create the
roles and grants (they are deployment, not data); load the three tables;
`setval('events_global_position_seq', max(global_position))` and keep `CACHE 1`;
rebuild `unique_claims` from `append_decisions`; then run the chain check
(`es-verify all`), the order check (`es-verify order`), and the index check
(`es-verify claims`). `anchors` may be restored or left to recompute; the
exported anchor JSON is the durable evidence. `feed_checkpoints` and
`feed_dead_letters` are optional; `snapshots` and `feed_leases` are not
restored.

A logical dump or a CDC stream of those three tables is sufficient because the
only consistency boundary that matters is the append transaction: claims commit
with their events. Three independent per-table tailers would not preserve that
boundary, which is the same constraint an events-only stream already has.

### 3.4 Erasure, and why the row is deletable

An append-only record of every claim value would make the erasure story worse
than the index it derives: releasing a value would leave the value in the log
forever. The table is therefore **not append-only by grant**. The appender holds
`DELETE`, mirroring `unique_claims`, and the concrete adapters can expose a
workspace-scoped `PurgeDecisions` next to `Purge` on the snapshot caches.

Erasure keeps the shape ADR 0025 prescribes. A workspace purge deletes its
decision rows and its index rows; a later rebuild yields "nothing claimed" for
that workspace. Erasing a single value is the key-destruction case: a MAC'd
value is inert without its key, and scanning opaque `ops` blobs for a plaintext
value is neither possible nor necessary. A deployment that stores plaintext
claim values and needs per-value erasure would need indexable copies of those
values — an argument for keeping the ADR 0025 contract, not a reason to weaken
the record.

This also fixes a hole that exists today, independent of backups: a
constraint-only append changes committed state (a value becomes unclaimable)
with no record anywhere. After this ADR, at least the decision is recorded and
purging the workspace removes it.

The table is a **restore record, not an attestation**. The chain and anchors
still attest only the events. Nothing here claims `append_decisions` is
tamper-evident; an operator who can delete a claim can already delete the
`unique_claims` row today, so the grant adds no new capability.

The MAC key remains client-owned and becomes backup-critical: a restored index
still enforces uniqueness on the stored bytes, but without the key no one can
recompute a claim from a plaintext value. Key custody is the adopter's, per
ADR 0025.

### 3.5 Cutover

Existing `unique_claims` rows have no decision history, and the migration cannot
invent it — `ops` is binary, so the backfill cannot be a portable SQL
`INSERT … SELECT`. The cutover is a one-time command, run with writers quiesced:

1. migrate;
2. grant the appender `SELECT, INSERT, DELETE` on `append_decisions` and
   `USAGE` on its identity sequence, then deploy the binary — the grant needs
   the table, and the binary needs the grant;
3. run `es-verify claims --adopt-existing`, which writes one synthetic `Claim`
   decision row per live `unique_claims` row (`from_version = to_version = 0`,
   `command_id = NULL`, the claim's own `stream_id`);
4. resume writers.

The current index is exactly "these values are claimed", so replaying those
Claims into an empty index reproduces it; released values are absent because
their rows are absent. The command refuses to run when `append_decisions` holds
any row, so it cannot run twice — a duplicate Claim row would make the rebuild
fail. From that point the log is self-sufficient and no pre-cutover copy has to
be kept. "Log-only" is true forward from the cutover, not retroactively: a
restore from a stream older than it is a restore to a pre-0043 state and must
use the old four-table rule.

### 3.6 Idempotency stays out

`command_dedup` is not a decision, and cannot be made one without changing when
the response exists. The row is written after the append commits because the
response is built from the post-append state (`cmdsvc`'s generated handler).
It cannot ride in the append transaction — it does not exist until that
transaction returns — so a second write is unavoidable; and putting responses
in an immutable log would be worse: they carry the workspace's domain content,
often PII, and the table is deliberately workspace-private with no relay bypass
arm (`0010_command_dedup.sql`).

Losing `command_dedup` on restore is the existing crash window between commit
and `Save`: a retry re-handles, and with `WithDeterministicEventIDs` it collides
on the event id (`es.ErrDuplicateEventID`); without them, the command's own
optimistic concurrency is the defence. The default is therefore to **exclude**
it from the backup set; include it only if replaying responses across a restore
matters more than its retention and PII obligations. It is a cache of a
completed command, not part of the state transition.

Separately: ADR 0025 §5 lists the erasure surfaces and does not name
`command_dedup`, and `DedupStore` has no purge. If responses can carry PII, that
is a gap to fix on its own — retention or a purge — not something this ADR
should paper over by copying them into the log.

### 3.7 The record set is a replication feed

The same three tables are not only a backup format; they are a logical
replication feed. A consistent cut of `events`, `log_order` and
`append_decisions` can seed a read-only clone that stays current by applying
later cuts, or a writable staging environment that forks at a cut and diverges.
That is a stronger product than "backup", and it is the reason the decision
record has to be append-ordered and self-describing rather than a copy of the
index: `unique_claims` is mutable (a release is a `DELETE` with no history),
while `append_decisions` replays deterministically into an empty index and ties
each claim back to its append.

What a feed consumer must do:

- **Apply per source transaction, not per table.** A replica that applies an
  event batch without its decision row has a wrong index for a window. Cut all
  three tables at one LSN/commit boundary; never mix export epochs.
- **Apply idempotently.** CDC is at-least-once. Events dedupe on
  `(workspace, stream, version)` or `event_id`, `log_order` on `from_position`,
  decisions on `id`. A duplicate `Claim` is a **no-op on a replica**, not
  `ErrConstraintViolated`: the source already validated it.
- **Order by `id`, and allow gaps.** Identity sequences are non-transactional;
  a rolled-back append consumes an `id` and leaves no row. Never assume `+1`.
- **Refuse an unknown format.** The ops encoding carries a versioned domain
  separator; a consumer on an older schema must reject a newer record, not
  mis-decode it.
- **Treat a per-workspace clone as an export, not replication.** Replication
  sees the whole database; filter by `workspace_id` (or use the relay path) for
  a tenant-scoped clone. Claims are per-workspace, so such a clone is
  self-consistent.

Two clone shapes, with different rules:

- **Follow (read-only).** The feed keeps flowing; the clone never appends, so
  the sequence is irrelevant and `es-verify all` / `order` / `claims` can run
  continuously. This is the useful shape for DR verification: the copy proves
  itself every day instead of at incident time.
- **Fork (writable).** Seed at a cut, `setval` the sequence, then stop applying
  and let it diverge. It is a fork: never merge it back, and never point it at
  the backup prefix. Promotion is a cutover — quiesce, take a final cut,
  repoint — not a merge.

Data governance is part of the design, not an afterthought. A clone moves
production payloads and claim identifiers into a lower-trust environment. Sealed
payloads must not share the production key. Claim values are MACs: the same key
lets the clone match them (and correlate environments); a different key keeps
uniqueness working but makes them opaque. Choose deliberately. `command_dedup`
does not travel, so a clone cannot replay production's Idempotency-Key
responses; that is correct, since it should not answer production's clients.

## 4. Consequences

Positive:

- The log's restore path becomes complete: three append-written tables, with
  only the dedup response cache outside them.
- A constraint-only append stops being invisible. That is a correctness fix
  independent of backups.
- The claims index gains a rebuild path and a verification oracle, the same
  shape projections already have (ADR 0005).
- The same record set doubles as a replication feed: a consistent cut seeds a
  read-only clone that verifies itself continuously, or a writable fork that
  diverges deliberately (§3.7).
- No change to `es.Store`, `Decide`, `AppendParams`, the chain, `log_order`,
  anchors, the feed, or snapshots.

Costs:

- One row and one `INSERT` per constraint-bearing append. The row is small, and
  usually far smaller than the events it accompanies; a claim-only append is the
  one case where the row is the whole write.
- The append transaction grows for a capability many deployments do not use.
  Accepted: the alternative is a backup that silently misses claims.
- Claim values now appear twice. Under ADR 0025 they are opaque MACs; a
  deployment that stores plaintext claims widens its erasure surface (§3.4).
- A new table, migration, grant, rebuild command, and least-privilege coverage.
- A feed consumer is a new operational surface: lag, at-least-once apply, and a
  schema/format version it must refuse rather than mis-read (§3.7).
- The cutover caveat makes "log-only" a forward guarantee, not a retroactive
  one.

## 5. Alternatives considered, and why not

- **Status quo: back up four tables.** Correct and already documented. Rejected
  as the destination because it prevents the log from being the backup and still
  leaves claim-only appends invisible. It remains the interim rule until this
  ADR is implemented.
- **A control stream (`es.control`, per workspace).** It would put the op in the
  chain and the feed, and `AppendTx` could write it in the same transaction.
  Rejected: it burns positions, pollutes every consumer's feed with internal
  events, and makes a claim's durability depend on a second stream.
- **Constraint ops on the envelope, like `SpanContext`.** Excluded from the hash
  the same way, and a close fit for event-bearing appends. Rejected because a
  claim-only append has no envelope — exactly the case that needs recording.
- **Forbid claim-only appends.** A cleaner log, but it is a tested, legitimate
  capability (`TestConstraintOnlyAppendWritesNoStaleSnapshot`): releasing a value
  need not move a stream.
- **Re-derive the index from domain events.** Needs the plaintext in events, the
  MAC key, and per-domain reindex code; still misses claim-only appends and
  releases. Correctness would depend on every domain's event schema.
- **Record only a hash of the op.** Smaller and PII-free, but cannot rebuild
  `unique_claims`, which needs the exact `value_key`.
- **Put the ops in `log_order`.** One table, and claims would share the position
  order. Rejected because a claim-only append has no position, and burning one
  breaks the gap-free event-position contract the SQLite feed relies on.

## 6. Rollout

This ADR is accepted; the implementation is the next work item.

- Migration `postgres/migrations/0014_append_decisions.sql` and
  `sqlite/migrations/0012_append_decisions.sql` (numbers may shift if another
  migration lands first): table, RLS, and an index on `(workspace_id, id)` for
  the replay scan.
- `Workspace.append` writes the row when `len(p.Constraints) > 0`, inside the
  existing transaction, after `applyClaims`.
- `docs/deploy/README.md`: the appender grant line, the three-table backup rule,
  the restore and clone procedures, the cutover, and `command_dedup` named as an
  erasure surface.
- `postgres/leastpriv_test.go`: pin the grant and drive a claim through the
  appender role, including a claim-only append.
- `es-verify claims`: rebuild into a temporary table, diff, report the first
  divergence; `--adopt-existing` is the one-time cutover backfill. Same release
  as the migration (§3.2, §3.5).
- A migration note for adopters: grant before deploying the binary, and the
  cutover order.
- README: the backup rule, the clone mode, and the limits of replay.

Not in scope: an asynchronous decision writer (the row must be transactional),
per-workspace retention for `append_decisions`, and proto-typed ops (the binary
encoding keeps the store opaque and the table backend-neutral).

## 7. Resolved before acceptance

1. **Name:** `append_decisions`. It says what the row is, and it does not need a
   new name if append decisions later carry more than constraints.
2. **`es-verify claims`:** ships with the migration. The oracle is the proof
   that the restore set is complete; a rebuild path without one is untested by
   construction.
3. **`command_id`:** kept. It is the only link from a claim-only append's
   decision back to its cause, and the identifier is not claim data.
4. **No-op releases:** every append with ops writes a row, even when a `Release`
   matched nothing. The row records the decision, and the replay is identical
   either way.
5. **Restore shape:** the shared logical stream (three tables at one commit
   boundary), used for restore, for a read-only clone, and for a writable fork.
   WAL/PITR remains the only arbitrary-time-travel option; the stream restores
   to a consistent cut.
