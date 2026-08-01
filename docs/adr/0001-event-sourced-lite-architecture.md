# ADR 0001: Event-Sourced Log as Source of Truth

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

We want a small event-sourcing library. Aggregates, commands, and
events are defined in `.proto` files. Storage must run on either SQLite
or Postgres. Three properties are **non-negotiable**: full **history**,
**auditability**, and **time-travel** (reconstruct any aggregate's state
as it stood at an arbitrary past point).

The sibling `eventstore` framework already solves all of this — and a
great deal more: a durable workflow-orchestrated command bus (Restate /
DBOS), synchronous projections, a synchronous `state_cache`, per-subject
crypto-shredding with a KMS, Cedar authorization, mandatory
multi-tenancy with Postgres RLS, and a tamper-evident hash chain. That
machinery is the right call for a regulated, multi-tenant SaaS platform.
It is too much for a service that just needs a durable, replayable log
with reliable downstream delivery. The cost is not only lines of code —
it is conceptual surface: every one of those features is a thing a
newcomer must understand before they can reason about a write.

So the question this ADR settles is twofold:

1. **Storage model** — is the event log the source of truth, or do we
   store current state in normal tables and emit events as a side effect
   (the "state + outbox" pattern)?
2. **Delivery** — how do downstream consumers (projections, integrations)
   receive events reliably, given we explicitly do **not** want
   synchronous projections or a durable workflow runner?

## Decision

### 1. The event log is the source of truth (event sourcing, not state+outbox)

Aggregate state is **never** stored as the authoritative record. The
authoritative record is an append-only `events` table. Current state is
*derived* by folding a stream's events through `Decider.Evolve`.

This is forced by the non-negotiables. "State + outbox" keeps only the
*current* row; the moment you `UPDATE` it, the previous value is gone.
That destroys time-travel by construction and leaves auditability
dependent on whatever the outbox happens to have shipped. History that
exists only in a message broker is not history — it is a cache with
retention limits. Event sourcing is the only model under which
"what did aggregate X look like on 3 March, and why?" is answerable from
the database alone.

The trade the parent framework makes — and that we inherit — is that
**events are immutable**. There is no `UPDATE` or `DELETE` on the
`events` table, ever. This is what makes the three non-negotiables hold;
it is also the source of the one hard tension (see *PII*, below).

### 2. Storage is neutral; the payload is opaque bytes + a type URL

The `Store` interface knows nothing about proto, JSON, or aggregate
shapes. It persists an `Envelope`: identity, ordering, audit metadata,
and an opaque `Payload []byte` tagged with a `TypeURL`. Serialization
is the codec's job, not the store's. This keeps the two backends
(SQLite, Postgres) trivially parallel — they differ only in SQL dialect
and the global-ordering mechanism — and lets proto be the *recommended*
codec without the core depending on it.

Aggregates/commands/events are defined in `.proto`. The codegen plugin
`protoc-gen-es-lite` reads the `(es.v1.sum_type)` option on each oneof
container and emits the sealed sum-type interfaces, the variant marker
methods, and the `Codec` — into the message's own Go package. What stays
hand-written per aggregate is exactly the parent's split: the `Decider`
and the error sentinels. The plugin is proven by regenerating the worked
counter example and passing the same end-to-end suite the hand-written
codec passed.

### 3. Delivery: the event log *is* the outbox

The recurring question — "should I add an outbox table?" — has a clean
answer under event sourcing: **no separate outbox**. A separate outbox
means a dual write (state row + outbox row) and a reconciliation story.
But an append-only event log is *already* a durable, totally-ordered
record of everything that happened. That is precisely what an outbox is.
So the log doubles as the outbox: one append per command, no second
table.

Delivery is a **poller** (a "relay") that tails the log by a monotonic
global position and advances a per-subscriber checkpoint:

```
SELECT ... FROM events WHERE global_position > :checkpoint
ORDER BY global_position LIMIT :batch
```

Publish the batch, persist the new checkpoint, repeat. A projection is
just a subscriber whose handler updates a read model. Because delivery
is decoupled from the write and driven by a persisted checkpoint, it is
**at-least-once** and crash-safe without a workflow runner: a crash
mid-batch simply re-delivers from the last committed checkpoint.
Handlers must therefore be **idempotent** — the standard event-driven
contract, and far less machinery than DBOS.

This also means projections are **asynchronous only**. We deliberately
drop the parent's synchronous, in-transaction projections. Read models
are eventually consistent. Read-your-writes, when needed, is served from
the aggregate runtime's own post-append folded state, not from a
projection.

### 4. Optimistic concurrency via expected version

`Append` takes an `ExpectedVersion`. The `events` table has a
`UNIQUE (stream_id, version)` constraint. Two concurrent writers to the
same stream race on that constraint; the loser gets `ErrConflict` and
reloads. No locks, no advisory-lock dance. Cross-aggregate uniqueness
(the parent's `unique_claims`) is **out of scope** for lite.

### 5. Backend-specific: global ordering and the delivery gap

The poller's correctness depends on `global_position` being a
gap-free, commit-ordered cursor. The two backends differ here, and this
is the single most important implementation subtlety:

- **SQLite** serializes writers at the file level. An
  `INTEGER PRIMARY KEY AUTOINCREMENT` column is gap-free and assigned in
  commit order by construction. The simple `WHERE global_position > :cursor`
  poller is **correct as-is**. SQLite is therefore the first backend.

- **Postgres** assigns a `BIGSERIAL` at insert time, but rows become
  *visible* at commit time, and commit order ≠ assignment order. A
  cursor poller can read past a row whose id was assigned in an
  as-yet-uncommitted transaction and never return for it — the classic
  outbox gap. The Postgres adapter must therefore not rely on a naive
  monotonic cursor. It will use one of: a `published_at IS NULL` claim
  column drained with `FOR UPDATE SKIP LOCKED` (true outbox-style), or a
  visibility low-watermark (only read rows older than the oldest
  in-flight transaction). This is deferred with the Postgres adapter,
  but the `Store` interface is designed so it can be added without
  touching SQLite or the poller's public contract.

### What we cut from the parent (and why it's safe here)

| Parent feature | Cut in lite | Rationale |
| --- | --- | --- |
| Durable command bus (Restate/DBOS) | ✂ | At-least-once poller + idempotent handlers covers reliable delivery without a workflow runtime. |
| Synchronous projections / `state_cache` | ✂ | Async projections off the log. Read-your-writes served from the runtime's folded state. |
| Crypto-shredding + KMS | ✂ | Only needed if PII lands in events. Keep PII out of the log, or add it back (see tension). |
| Cedar authz | ✂ | Authorization is an application/edge concern, not a store concern. |
| Mandatory multi-tenancy + RLS | ✂ | `StreamID` is `Type:ID`; tenancy, if needed, is one DB-per-tenant or an application-level prefix. |
| Tamper-evident hash chain | ✂ | Immutability + DB backups covers audit for non-adversarial threat models. Re-add per parent ADR 0028 if required. |
| Upcasters / schema-version dispatch | Deferred | `schema_version` is recorded on every event so upcasting can be added later without a migration. |

### What we keep

Proto-defined aggregates; the `Decider{Initial, Decide, Evolve, IsTerminal}`
model (parent ADR 0003); a storage-neutral `Store`; an `Envelope` with
first-class audit metadata (actor, causation, correlation, occurred-at
vs recorded-at).

## Consequences

### Positive

- **The three non-negotiables hold by construction.** History = the
  append-only table. Audit = immutable events + metadata. Time-travel =
  fold `WHERE version <= N` or `WHERE recorded_at <= T`.
- **One write path.** A command is one transaction that appends events.
  No dual write, no outbox reconciliation, no saga to keep two tables in
  step.
- **Small conceptual surface.** A newcomer needs to understand three
  things: the Decider, the append/fold cycle, and the poller. That is
  the whole system.
- **Backends are near-identical.** The `Store` is ~5 methods; SQLite and
  Postgres differ only in dialect and the ordering mechanism.

### Negative

- **Immutability vs. erasure.** An append-only log fights GDPR "right to
  erasure." Lite has no crypto-shredding, so the rule is operational:
  **do not put erasable PII in events.** If that becomes impossible,
  crypto-shredding must be re-introduced (parent ADR 0010) — it is the
  one cut that is genuinely hard to add late, so it is called out loudly
  here.
- **Eventual consistency everywhere downstream.** Projections lag the
  log. Anything needing strong read-your-writes must read the aggregate,
  not a projection.
- **At-least-once, not exactly-once.** Handlers must be idempotent.
  This is a contract the parent's durable bus could partially hide; lite
  makes it explicit.
- **Replay cost grows with stream length.** With no `state_cache`,
  loading a long stream folds every event. Mitigation — an *optional*
  snapshot cache — is deferred, and when added it must remain a pure
  optimization: always re-derivable from the log, never the source of
  truth, so it cannot corrupt time-travel.

## Alternatives Considered

### State + outbox (normal DB schema, events as a side effect)

Rejected as the *storage* model. It is simpler for pure current-state
CRUD and its reads are trivial, but it cannot satisfy history or
time-travel: the authoritative row is overwritten in place, and the
outbox is a delivery buffer with retention, not a system of record.
Auditability would rest on whatever the broker retained. The moment the
requirements list "time-travel," this option is out.

Note that the outbox *pattern* is not rejected — it reappears as the
Postgres *delivery* mechanism (§5). The insight is that storage model
and delivery mechanism are orthogonal: we chose event sourcing for
storage and a log-as-outbox relay for delivery.

### Full parent framework (`eventstore`)

Rejected as too heavy for the target. It satisfies every non-negotiable
and many more, but at a conceptual and operational cost (workflow
runtime, KMS, RLS, codegen for six concerns) that the target service
does not need. Lite is the subset; the parent remains the upgrade path
when a cut feature is genuinely required.

### Separate outbox table alongside the event log

Rejected as redundant. Under event sourcing the log already *is* the
ordered durable record a relay tails. Adding an outbox table would
reintroduce the dual-write it was invented to avoid, plus a cleanup job.
The only reason to add a claim column is the Postgres visibility gap
(§5) — and there it lives *on the events table*, not as a second table.
