# ADR 0007: Snapshots (Deferred)

- **Status:** Deferred
- **Date:** 2026-08-01

## Context

`aggregate.Runtime.Load` reconstructs state by folding a stream's entire
history through `Decider.Evolve` (`ReadStream(0,0)` → fold). The cost is
O(stream length). For the overwhelming majority of aggregates — a
counter, an order, a user — a stream is a handful to a few hundred
events, so a load is microseconds and folding is irrelevant.

A snapshot cache — the folded state at version N, stored so that `Load`
can start from it and fold only the tail — would cut that cost for
**long-lived, high-event-count streams** (thousands of events on one
aggregate). The parent framework built exactly this and then some (its
`state_cache`, ADR 0023), mirroring state synchronously in the append
transaction. es-lite deliberately cut that (docs/adr/0001): a
synchronous, in-transaction, read-your-writes state mirror is
load-bearing infrastructure, not a cache.

The question is whether es-lite should add a (leaner, async) snapshot
cache now, or treat it as premature optimization.

## Decision

**Defer building snapshots.** Record the trigger, the discipline, and the
design here so the decision is made and the invalidation rules are
pre-thought — but write no code until a measured need appears.

### Why defer

- **Snapshots add zero capability.** Unlike the Postgres backend,
  NATS delivery, upcasters, or crypto-shredding — each of which lets an
  adopter *do something new* — a snapshot only makes an existing
  operation faster. It is pure optimization.
- **There is no measured latency problem.** Building it now optimizes a
  cost nothing has yet paid. es-lite's premise is cutting what is not
  needed; speculative caching is the opposite.
- **Deferring is free.** The insertion point is a single method
  (`Runtime.Load`), and adding a snapshot read there is transparent — it
  changes no public API and breaks no caller. There is no
  "design-it-in-now-or-regret-it" pressure, unlike crypto-shredding
  (ADR 0004), which genuinely was expensive to retrofit and so was built.

### The trigger to build it

A specific hot aggregate's p99 `Load` latency crosses the service's
budget **because its streams have grown to thousands of events**. Measure
first; most aggregates never reach it. "It might get slow" is not the
trigger — a number is.

### The discipline, if built

Non-negotiable, and the exact line the parent's `state_cache` crossed:

- **A snapshot is a pure cache.** It is always re-derivable from the log
  and is **never** the source of truth. Dropping the entire snapshot
  table must lose nothing but speed.
- **Written asynchronously**, never inside the append transaction. A
  background snapshotter (or "every N events") writes them off the hot
  path. Appends never depend on snapshot writes.

### Design sketch (for whoever builds it)

- A `snapshots` table keyed by `(workspace_id, stream_id)` holding
  `{version, state_schema_version, state_bytes}`. On Postgres it carries
  `workspace_id` and lives under the same RLS as events; its payload is
  encrypted under the workspace DEK like event payloads (it is derived
  PII).
- `Runtime.Load`: read the snapshot, seed state from it, then
  `ReadStream(snapshot.version, 0)` and fold only the tail. Absent
  snapshot → today's full fold. Insertion is localized to `load`.

### Invalidation rules (the reason this ADR exists)

A folded-state cache has four invalidation surfaces, all of which must be
handled or the cache silently corrupts reads:

1. **State shape / Decider change** — bump a `state_schema_version`; a
   snapshot whose version predates the current one is ignored and the
   stream refolded.
2. **Upcaster change** — a snapshot bakes in the interpretation the
   upcasters gave old events (ADR 0001 schema_version, upcast registry).
   Changing upcaster logic must invalidate snapshots, or old meaning
   persists.
3. **Crypto-shredding** — a shredded workspace's snapshots hold decrypted
   derived state and must be purged at shred time, exactly like
   projections (ADR 0004 §4).
4. **Time-travel** — `LoadAsOfVersion` / `LoadAsOfTime` must never seed
   from a snapshot newer than the target point; either pick a snapshot
   with `version <= target` or skip the cache for as-of reads. Getting
   this wrong returns a *future* state for a past query — the one thing
   time-travel exists to prevent (ADR 0001).

## Consequences

- **No complexity added now.** The four invalidation surfaces stay off
  the board until a real number justifies them.
- **Long streams remain O(n) to load** until the trigger fires. This is
  acceptable and, crucially, *measurable* — you will know when it
  matters.
- **The decision and its rules are recorded**, so building it later is a
  scoped, well-specified change rather than a rediscovery of every
  invalidation edge.
- **This ADR supersedes nothing.** It documents an intentional absence.

## Alternatives Considered

### Build snapshots now

Rejected: no measured need, and it imports four invalidation surfaces for
a pure speed win. Optimize when a profile says to.

### Parent-style synchronous `state_cache` (ADR 0023)

Rejected — it was cut in docs/adr/0001 and nothing here reverses that. A
synchronous, in-transaction state mirror is load-bearing infrastructure
that couples the write path to a cache; if snapshots are ever built, they
are the async, droppable kind, not this.

### Periodic async snapshotter (every N events / background job)

The **likely implementation when the trigger fires** — noted here as the
default shape, not adopted now. It keeps snapshots off the hot path and
inherently treats them as a rebuildable cache.
