# ADR 0002: Deployment Topology and Delivery

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

es-lite is a **library**, not a service — you `import` it into whatever
process owns an aggregate's streams. There is no "es-lite server". That
raises three questions this ADR answers: how do multiple instances of a
service share one database; how is the write side kept correct without a
coordinator; and how is delivery driven with low latency without a
polling lag or a durable workflow runner.

## Decision

### Write side: horizontal, coordinator-free

Every service replica embeds the same library against the same database.
Any replica may serve any command. The only contention is two replicas
handling commands for the **same stream** concurrently: both compute
`version = current+1` and try to append it. `UNIQUE (stream_id, version)`
lets exactly one commit; the loser gets `es.ErrConflict`, reloads,
re-Decides, and retries. Different streams never contend.

So N replicas need **no leader and no distributed lock** for writes.
Optimistic concurrency on the version column is the entire coordination
mechanism. This is a direct consequence of the append-only design in
ADR 0001 — there is no mutable current-state row to lock.

### Backends map to two topologies

- **SQLite** — the database is a file local to the process, so the store
  is **single-writer** and co-located. Deployment is one pod (or
  one-DB-per-tenant / one-DB-per-pod). Ideal for development, edge, and
  single-node services. Multiple K8s replicas cannot share one SQLite
  file without networked-storage machinery (LiteFS, rqlite) that is out
  of scope. `Store` keeps `MaxOpenConns=1` to guarantee the single-writer
  invariant that makes `global_position` gap-free.

- **Postgres** — a shared server, so **many stateless replicas** embed
  the library and point at the same database. This is the horizontally
  scalable production shape. (Adapter deferred; ADR 0001 §5 covers the
  ordering caveat it must handle.)

### Delivery: one logical consumer per subscriber

A subscriber is defined by its checkpoint row. If every replica ran a
poller with the **same** subscriber, they would each read the same
events and publish duplicates while racing on the checkpoint. Two
sanctioned patterns:

1. **Singleton relay (default).** Run the poller in exactly one place —
   a `replicas: 1` deployment, or leader-elected via a Kubernetes
   `Lease`. One publisher, one checkpoint, simplest to reason about.
2. **Competing consumers.** Multiple pollers claim disjoint batches with
   `SELECT … FOR UPDATE SKIP LOCKED`, sharded by stream hash so
   per-stream order is preserved. Higher throughput, more moving parts.

Because delivery is at-least-once and handlers are idempotent (ADR 0001),
an accidental double-run is *safe*, merely wasteful. The rule of thumb:
**fan the write side out across all pods; keep the read/relay side as one
logical consumer per subscriber.**

### Push wake-up: the `Signal` seam (no push source ships)

Polling alone bounds delivery latency to the poll interval. To let a
deployment remove that lag later without changing the poller, it drains on
either a fallback tick **or** a `Wake` channel (`delivery.Signal`):

- **Postgres** — a deployment that wants push runs its own listener
  goroutine (`LISTEN es_events`) and calls `Signal.Notify()` on every
  notification. es-lite ships no listener and, deliberately, no
  append-time `NOTIFY`: it would be a write-path cost ADR 0037's
  anti-goals forbid, so a LISTEN/NOTIFY source stays the operator's to
  wire (or a future ADR's to add).
- **SQLite** — the application calls `Signal.Notify()` itself after a
  successful append (same process, so no transport needed).

The same poller code works in both modes. Crucially, **correctness never
depends on a wake arriving**: `NOTIFY` is best-effort and non-durable (a
notification fired while no session is listening is simply lost), so the
fallback poll remains as a safety net and the durable checkpoint
guarantees no event is ever skipped. The wake only reduces latency; it is
never load-bearing for completeness. The `feed` package exposes the same
seam one level up (`feed.Waiter`, consumed by the subscribe loop's idle
wait) — see ADR 0037 for the deferred LISTEN/NOTIFY question there.

## Consequences

- **Writes scale by adding replicas.** No leader election on the hot
  path; the database arbitrates via a unique constraint.
- **Delivery is a deliberate singleton (or a deliberate shard).** The one
  operational decision an adopter must make consciously, called out here
  so it is not discovered in production as duplicate publishes.
- **Latency is tunable independently of correctness.** Wire a LISTEN/NOTIFY
  listener into the `Signal`/`Waiter` seam for near-instant delivery
  (es-lite ships no push source — no append-time `NOTIFY`, by ADR 0037's
  write-path rule); rely on the poll interval alone for simplicity. Either
  way, the checkpoint is the source of truth for progress.
- **SQLite and Postgres are genuinely different deployment shapes**, not
  just different drivers — single-writer-local vs. shared-multi-writer.
  Choosing a backend is choosing a topology.
