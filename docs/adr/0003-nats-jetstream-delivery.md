# ADR 0003: NATS/JetStream Delivery and Projections

- **Status:** Accepted (design; adapter deferred)
- **Date:** 2026-08-01

## Context

ADR 0001 makes the append-only event log the source of truth and treats
the log as its own outbox, tailed by a `delivery.Poller`. ADR 0002 notes
the poller must run as a singleton (or SKIP-LOCKED shards) to avoid
duplicate publishing, and that a `Wake` seam exists for push delivery.

Two questions remained:

1. The singleton relay is a single point of failure — can we do better?
2. Should NATS carry the event stream and drive projections, given the
   surrounding stack already runs on NATS (`natskit`, `nats-contracts`)?

The answer to both is the same design, and it is the idiomatic one for
this ecosystem. This ADR records it. It changes no core code — the core
already exposes the exact seam this design plugs into.

## Decision

### The database log stays the source of truth; NATS is transport

We do **not** publish to NATS from the command path. A "append to DB,
then publish to NATS" sequence is a dual write: a crash between the two
loses or duplicates the event, which is precisely the failure the
log-as-outbox design (ADR 0001) exists to prevent.

Instead, exactly one **relay** bridges the DB log to JetStream. The relay
is the `delivery.Poller` from ADR 0001 with a Handler that publishes each
event to a JetStream stream and then advances the DB checkpoint. The DB
remains the audit record; JetStream becomes the fan-out transport and the
projection substrate — never the system of record.

```
 write path ─▶ DB events log (truth, append-only)
                     │
            delivery.Poller  ── the ONE bridge (core, unchanged)
                     │  publish, Nats-Msg-Id = global_position
                     ▼
              JetStream stream (durable, ordered, replayable)
                 ├─▶ projection A   (durable consumer, own ack cursor)
                 ├─▶ projection B
                 └─▶ cross-service consumers
```

### Idempotent publish removes the singleton as a *correctness* SPOF

Each event is published with `Nats-Msg-Id = global_position` (globally
unique and monotonic). JetStream deduplicates by message id within its
dedup window, so two relays publishing the same event still yield one
stream message. Correctness therefore no longer depends on there being
exactly one relay — only on stable, unique publish ids.

### Leader election removes the singleton as an *availability* SPOF

Run the relay 2–3× with leader election (Kubernetes `Lease` or a NATS KV
lock). One instance is active; the others are hot standbys; failover is
automatic. Combined with dedup, a double-publish during a failover race
is harmless. This is the "protect against the singleton" answer from
ADR 0002: not a fragile `replicas: 1` pod, but an elected leader with
standbys **and** a dedup backstop.

The relay's DB checkpoint dedup window must exceed the maximum relay lag
plus failover time, so a newly-elected leader's replays fall inside the
JetStream dedup window. This is the one tunable to get right.

**Implemented (NATS KV lease).** The `leader` package does exactly this:
candidates race to `Create` a key in a TTL'd KV bucket and the winner
refreshes it with an `Update` revision-CAS; on death the key expires and a
standby takes over. `es-lited` with `RELAY=true` runs the relay inside every
serving replica under this election (election key `relay.<subject-prefix>`, so
each region elects independently) — folding the relay into the normal process,
no dedicated Deployment. The dedicated `es-relayd` singleton remains for
deployments that want the relay isolated from the serving path. The lease is
time-fenced (not session-fenced like a Postgres advisory lock), so a brief
two-leader overlap is possible during failover — harmless here because drain
claims rows `SKIP LOCKED` and publish dedups on `global_position`.

### Projections are JetStream durable consumers

Each projection is a durable consumer that acks after updating its read
model. JetStream provides redelivery, backpressure, and per-consumer
cursors, so projections no longer need the DB-checkpoint poller — the
consumer's ack position *is* the checkpoint. Delivery stays at-least-once,
so projection handlers remain idempotent (unchanged from ADR 0001).

Rebuild a projection by resetting its consumer to sequence 1 and
replaying. JetStream retention is a fast-replay convenience, not the
archive: if a rebuild must reach further back than retention holds, the
projection replays from the DB log (`Store.ReadAll` from position 0) — the
reason the DB stays truth.

### Subjects follow the nats-contracts taxonomy

Publish per the established taxonomy
(`nats-contracts/subjects`): lifecycle events use
`<prefix>.<tenant>.<domain>.<stage>`. For es-lite the mapping is:

```
evt.<tenant>.<aggregate>.<event>
      │         │           └─ event type, e.g. "incremented"
      │         └───────────── StreamID.Type, e.g. "counter"
      └─────────────────────── tenant (see reconciliation below)
```

Per-stream ordering is preserved within a subject; global ordering is the
JetStream stream sequence. The subject scheme is hard to change once
consumers exist, so it is fixed here, not left to each service.

**Tenant reconciliation.** es-lite's `StreamID` is `Type:ID` with no
tenant (ADR 0001). The subject taxonomy wants a tenant segment, so the
relay config supplies a tenant resolver (static for single-tenant
deployments, or derived from an ID prefix / deployment identity). The
core stays tenant-free; tenancy re-enters only at the publishing edge.

### Private events vs. published language

Raw es-lite event protos are a service's **private** representation. Per
`nats-contracts`, only messages consumed by *another* service belong in
the shared catalogue. So the relay has two modes:

- **Internal fan-out** (projections in the same service): publish the raw
  event proto to an internal subject.
- **Published language** (cross-service): the relay maps the internal
  event to a `nats-contracts` message type before publishing. The
  mapping is explicit application code, not automatic — it is a
  contract boundary and should break at compile time when it drifts.

### Built on natskit

The adapter uses `natskit` for plumbing — `Publish[T]` in the relay
Handler, `Consume[T]` for projection consumers, `natstest`'s embedded
broker for tests. `natskit` prescribes no subjects or payloads, which is
why the taxonomy above and the contract mapping are es-lite/application
concerns layered on top.

## Consequences

### Positive

- **No SPOF.** Dedup gives correctness under N relays; leader election
  gives availability. Neither alone, both together.
- **Projections get replay and backpressure for free** from JetStream,
  and the DB remains the deep-rebuild source of last resort.
- **The core does not change.** The relay is a `delivery.Handler`; the
  `delivery.Signal` seam already lets a publish/NOTIFY wake it. This ADR
  is a deployment/adapter decision, not a core one.
- **Fits the mesh.** Subjects, contracts, and plumbing reuse existing
  `nats-contracts` / `natskit` conventions rather than inventing parallel
  ones.

### Negative

- **A second cursor to reason about.** The DB checkpoint (relay progress)
  and the JetStream consumer cursors (projection progress) are distinct.
  Clear ownership — relay owns the DB checkpoint, each projection owns its
  consumer — keeps this manageable, but it is more than the single-DB
  poller of ADR 0001.
- **Dedup window is a real tunable.** Set it too short and a failover
  replay re-emits events. It must exceed max relay lag + failover time.
- **Contract mapping is manual.** Cross-service events need explicit
  translation to `nats-contracts` types. This is deliberate (a contract
  boundary), but it is work per published event.
- **NATS retention is not an archive.** Operators must not treat the
  JetStream stream as the audit log; that remains the DB.

## Alternatives Considered

### Publish to NATS directly from the command path

Rejected: dual write. Loses the single-write-path property and the
crash-safety that motivates event sourcing here.

### Keep the DB-poller for projections; use NATS only for integration

Viable and simpler (no JetStream retention/ordering concerns), but
forfeits NATS's free fan-out, replay, and backpressure for in-service
projections and splits the delivery model in two. Given the stack is
already NATS-native, JetStream consumers are the better default. The
DB-poller path remains available for a projection that deliberately wants
DB-local checkpointing.

### Strict singleton relay, no dedup

Rejected as the primary design: a failover race can double-publish with
nothing to catch it. Dedup by `global_position` is cheap insurance and is
what lets the relay be highly-available instead of a single pod.
