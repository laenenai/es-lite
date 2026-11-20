# ADR 0042: Snapshot Write Policy and In-Tree Snapshot Caches

- **Status:** Proposed
- **Date:** 2026-11-20
- **Related:** ADR 0007 (snapshots: the trigger, the discipline, the design
  sketch, and an errata this ADR corrects), ADR 0025 (crypto and erasure are
  client-owned), ADR 0004 (workspaces, RLS, erasure surfaces), ADR 0010
  (migrations and the shared ledger), ADR 0029 (the chain attests the log, not
  derived state).

## 1. Context

ADR 0007 deferred snapshots and specified two things for whoever built them:
the trigger (a measured p99 `Load` on a hot aggregate) and the discipline (a
pure cache, written off the append path). It also sketched the shape and left
the work unbuilt. What actually shipped is a `snapshot.Cache` interface with
**no implementation**, plus an errata claiming `snapshot/natskv` exists — it
does not; `snapshot/` holds one file. Meanwhile the runtime's write-through is
unconditional: every successful append marshals the entire folded state and
calls `Save`, whether or not the stream is long enough to need it
(`aggregate/runtime.go:426-436`).

That combination is a trap. An adopter who implements the interface to speed up
one hot aggregate pays a full state marshal per command on **every** aggregate
type that runtime serves, and an adopter who reads ADR 0007 has no way to know
whether enabling the cache helps or hurts. The trigger here is therefore not a
p99 measurement on one service; it is that the shipped surface cannot be used
as designed. This ADR supersedes 0007's "no code until measured" **for the
cache implementation and its write policy only**; 0007's trigger still governs
whether an adopter turns it on, and its non-goals stand.

Two things about the interface shape the decision. `Cache` is keyed by
canonical stream id and has no workspace parameter (`snapshot/snapshot.go`),
so an implementation is workspace-scoped by construction. And `Save` receives
only the new snapshot, never the previous one, so any policy evaluated *inside*
the cache must either `Load` first (a round trip per command) or keep
process-local state (writes multiply by replica count). The only place the
policy can be both free and exact is the runtime, which has just read the
snapshot it seeded from.

## 2. Goals, non-goals, anti-goals

**Goals.**

- Make enabling the cache safe by default: a stream shorter than the threshold
  is never *automatically* written, and a load never folds more than the
  threshold once a snapshot exists.
- Keep `snapshot.Cache` a dumb keyed store — `Load`, `Save`, no policy.
- Ship the two adapters the library's own shape implies, so the interface is
  usable without writing storage code.
- Keep the cache out of the append transaction and out of the chain's claims.
- Make erasure and workspace isolation as explicit for the cache as for the
  log.

**Non-goals.**

- A background snapshotter, or enumerating streams by length (no `Store`
  method offers it).
- An asynchronous writer in this version (see §9).
- Attestation of cached state (see §3.3).
- Declaring the threshold in the protos: it depends on deployment scale, not
  on contracts.
- Per-stream thresholds. The knob is per `Runtime`, which is per aggregate
  type.

**Anti-goals.**

- A cache whose failure mode is silent *and* unrecoverable. Lost writes and
  log resets must heal without operator action.
- A cache that changes what a load returns. Every failure path falls through
  to a full fold.
- A second schema-change mechanism. The table ships in the shared ledger, not
  in adapter code.

## 3. Decision

### 3.1 The write policy

The decision belongs to `Runtime`, keyed on the gap since the **seed** — the
snapshot version this command's load actually used — and it is evaluated after
the append returns and **before** `stateCodec.Encode`:

```go
// WithSnapshotEvery writes a snapshot only when the gap since the snapshot
// this command loaded (the seed) reaches n. Not calling it keeps today's
// write-through. n == 0 disables writes (a read-only cache).
func (r *Runtime[S, C, E]) WithSnapshotEvery(n uint64) *Runtime[S, C, E]

func (r *Runtime[S, C, E]) snapshotDue(seed, to uint64) bool {
    switch {
    case !r.snapshotPolicySet:
        return true // today's behaviour
    case r.snapshotEvery == 0:
        return false
    default:
        return to > seed && to-seed >= r.snapshotEvery
    }
}
```

`seed` is zero when the load did not seed — absent snapshot, fold-version
mismatch, decode failure, or a seed discarded by the lineage guard. In `Handle`
it is the `from` that `load` already computes (`aggregate/runtime.go:505-541`);
the public `Load` variants keep their signatures, so only the private plumbing
changes.

Four consequences, which are the point:

1. **The decision precedes the encode.** The marshal and the save happen once
   per `n` appends instead of once per append.
2. **The invariant is exact, not approximate.** A load seeds at most `n`
   events behind, even after a lost write: the next append past the gap
   retries. The crossing rule (`to/n != from/n`), by contrast, waits for `2n`
   after a failure and therefore only guarantees `2n−1`.
3. **It heals resets.** A log restored below the cached version makes the
   lineage guard discard the seed; `seed` becomes 0, and the first append with
   `to >= n` rewrites the row. No operator step, and no wedge.
4. **It is replica-safe and batch-proof without process state.** Both operands
   come from shared stores — the append's own result and the snapshot the load
   read — and a batch that moves the version by `k` is measured on the
   resulting version.

What it gives up: snapshots land on `n, 2n, 3n` in normal operation, but drift
off multiples exactly when healing (a lost write, a reset, a cache outage at
load). Absolute predictability was worth less than the guarantee.

**Explicit warm-up.** The policy runs only on the append path, so a stream that
does not append — an archived aggregate read by dashboards, or any long stream
the first time the cache is enabled — would never get a snapshot and would keep
full-folding. `Runtime.WarmSnapshot(ctx, sid)` closes that hole without making
reads write: it folds the stream once and saves one snapshot at its head. It
deliberately ignores the threshold (the caller is explicitly asking for a
write, including when the policy is `0`), and errors when no cache is
configured. It is never called from `Load`, `LoadStale` or any read path; an
operator or a job decides when to pay for it.

### 3.2 The in-tree adapters

Both adapters store opaque bytes — they never decode, encrypt, or interpret
state. State sealing and MACs are the caller's (ADR 0025).

**Postgres.** `postgres.NewSnapshotCache(store *Store, workspace string)`.
Table `snapshots` in the events schema:

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    workspace_id text        NOT NULL,
    stream_id    text        NOT NULL,   -- canonical "type:id"
    version      bigint      NOT NULL,
    fold_version bigint      NOT NULL,
    recorded_at  timestamptz NOT NULL,   -- the runtime's Snapshot.RecordedAt
    state        bytea       NOT NULL,
    PRIMARY KEY (workspace_id, stream_id)
);
```

Migration `0013`, in the same Postgres ledger module as the store
(`migrations.goose_migrations`, module `eventstore`), so a database that
shares the ledger also shares the table's history. RLS follows the feed tables
rather than `events`: `ENABLE` + `FORCE` and a workspace-only policy with **no
bypass arm** — the relay never reads snapshots, and erasure is workspace-scoped
(`Purge`) — so this table does not join the four policies that carry the
role-gated `es_relay` arm. Migrations issue no grants; the deploy runbook does,
per principal.

Because the interface has been public while unimplemented, an adopter may
already own a table called `snapshots` with a different shape. The migration
must therefore **refuse loudly** when a `snapshots` table exists whose columns
are not the ones above, rather than no-op through `IF NOT EXISTS` and record
itself as applied: the runtime ignores `Load`/`Save` errors, so a wrong-shaped
table would degrade to permanent full folds with no signal. The mechanism is
the builder's (a shape assertion in a `DO` block, or a plainly failing
`CREATE TABLE` with the runbook covering a rename); the requirement is
acceptance-tested. The name is documented as reserved in the runbook.

**SQLite.** `sqlite.NewSnapshotCache(store *Store)`. Migration `0011`, one
table, no workspace column (SQLite is single-workspace), `recorded_at` as
fixed-width RFC3339 UTC text like `events.recorded_at`, `state BLOB`.

`Save` is last-writer-wins:

```sql
insert into snapshots (...) values (...)
on conflict (workspace_id, stream_id) do update set ...
```

No monotonicity guard. Per-stream appends are already serialized by
expected-version conflicts, so synchronous writes cannot reorder; if two
replicas' snapshot writes do arrive out of order, the older row costs one load
at most `n` extra folded events and the next gap boundary rewrites it. A guard
would buy that back only by permanently wedging a stream after a log reset,
because the stale row can never be replaced from a lower version. §5 of the
superseded design sketch had such a guard; it is deliberately not here.

**Erasure.** Both adapters expose `Purge(ctx) error` — delete every snapshot
this cache holds (workspace-scoped on Postgres). It is not on
`snapshot.Cache`, so the interface stays two methods and existing
implementations do not break; the shred procedure holds the concrete adapter.
ADR 0025 §4 already makes this the caller's obligation; this ADR gives it a
method and a runbook line.

`Purge` is **best-effort and application-coordinated**, like the projection
purge in ADR 0004 §4: a command already in flight, or one that starts
immediately after, can re-insert a row, so the runbook says to quiesce the
workspace — or, for sealed state, to destroy the key first, which makes any
racing re-insert inert. Making the purge durable against a live writer needs a
workspace tombstone every `Save` consults, which imports workspace lifecycle
into a droppable cache; that is out of scope here (§9) and belongs with
whatever component owns erasure ordering.

### 3.3 The trust boundary

A snapshot cache is **trusted infrastructure**, as protected as the log. The
lineage guard proves that the seed's *version* exists, not that its *state*
matches the log; a forged row at a valid version with a matching `fold_version`
drives `Decide` on forged state while every chain and anchor stays intact.
`WithChainVerification` recomputes only the envelopes it folds, so a seeded
load verifies the tail and trusts the seed.

No library-side fix is possible: binding the state to the chain would require
re-deriving it, and es-lite is keyless by ADR 0025. The mitigation is
client-side and already available: the caller's `stateCodec` can MAC or seal
the bytes on encode and verify on decode, so a forged row fails at `Decode` and
the runtime falls through to a full fold. The README and the knob's doc comment
say this plainly, and nothing in the audit output claims the chain attests the
cache.

The shipped default assumes the same protection as `events`: same database,
same RLS, same grant discipline. A cache that lives somewhere less trusted is
the caller's decision and needs the MAC.

### 3.4 Not chosen, and why

- **Crossing rule `to/n != from/n`.** Stateless and independent of the read
  path, but after a lost write it waits for `2n`, and the headline invariant
  degrades to `2n−1`. Rejected for the exact gap rule above.
- **Gap evaluated in a `Cache` decorator.** `Save` sees only the new snapshot,
  so the decorator must `Load` per command or keep process-local state; and it
  cannot skip the encode, because by then the bytes exist. The runtime is the
  only place that has the seed for free.
- **The gap as a backend `WHERE` clause.** It cannot skip the encode, and it
  makes the policy a property of each backend. Dropped along with the guard.
- **A background snapshotter.** It needs stream enumeration by length, which
  no `Store` method offers, and it writes snapshots for streams nothing loads.
- **An asynchronous writer now.** §9.
- **A separate schema.** It does not change physical-backup or PITR scope —
  only selective logical dumps, which `--exclude-table` already covers — and a
  runtime-configurable schema cannot be created by a static migration under the
  least-privilege role the repo tests. If a namespace is ever wanted, the
  clean form is a fixed name shipped in the migration, not a knob.
- **A separate database now.** One pool parameter later; shipping it now forces
  adopters to create the table themselves and degrades silently if they do not.
- **Attestation of cached state.** Deferred; see §3.3.
- **A proto knob for `n`.** A deployment property, not a contract.

## 4. Why the seed is the right counter

- **It is the cost.** A load seeded at `seed` folds at most `n` events, by
  construction. The knob is denominated in the thing the adopter is buying.
- **Short streams opt out of writes by construction.** A stream that never
  reaches `n` is never written. Note the precise claim: the threshold removes
  the *write* and the lineage-guard read, not the per-load `cache.Load` —
  a short stream still pays one lookup every load (§5).
- **Each stream keeps its own schedule**, independent of workspace volume; the
  global position would be exactly the wrong counter.
- **No new concept.** The version is the number optimistic concurrency already
  turns on, and the seed is the number the load already read.

## 5. Costs, accepted knowingly

- **One cache lookup per load, always.** In-tree, this is one indexed local
  query; on a short stream it is pure overhead. The alternative — learning
  that a stream has no snapshot without asking — needs either a negative cache
  or a schema change, and neither is worth it for a local lookup.
- **A stale row after a reset** costs two extra probes per load (the cache
  read plus the lineage probe) until the first append with `to >= n` replaces
  it. A reset stream is short; the fold is cheap.
- **A `fold_version` bump** makes every load full-fold until the next append
  that crosses the gap (`seed` is 0, so the first append with `to >= n`
  rewrites). For a stream that never appends again, `WarmSnapshot` is the
  explicit way to repopulate; without it, that stream keeps folding fully.
- **Derived PII now lives in the events database.** RLS and the same grant
  discipline apply; erasure is a `Purge` the runbook must not forget, and it is
  only as durable as the quiesce it runs under (§3.2).
- **No async writer**, so a remote cache pays its put latency on the command
  path once per `n` appends. That is bounded by the put, not by `n`, and it is
  the right trade at 1-in-n.
- **The policy is per runtime**, not per stream: an aggregate type with a
  mixed population of short and long streams pays snapshots only for the long
  ones (short ones never cross), but cannot use different `n` for them.

## 6. Consequences

- **Backward compatible.** `WithSnapshotEvery` is new; not calling it keeps the
  current write-through; `snapshot.Cache` is unchanged.
- **ADR 0007 is corrected, not rewritten.** Its errata now names the two
  in-tree adapters instead of the nonexistent `snapshot/natskv`; its trigger
  still governs adoption, its four invalidation surfaces stand, and its
  "written asynchronously" discipline is honoured in substance — the write is
  outside the append transaction and appends never depend on it — while this
  ADR records the synchronous, 1-in-`n` choice and why (§3.4, §9).
- **Operations.** Postgres gains a table, a policy and a grant obligation; the
  deploy runbook gains the snapshot grants, the `--exclude-table` advice for
  logical backups, and the post-restore expectation (missing snapshots rebuild
  lazily). SQLite gains a table, no rebuild.
- **Tests.** Migration-count assertions move (Postgres 12 → 13, SQLite 10 →
  11); the adapter tests cover the contract on both backends; the policy is
  unit-tested against a fake cache.

## 7. Acceptance criteria

- **Policy.** With the method uncalled, every successful append writes. With
  `0`, none does. With `n`, writes land at the first append whose version is
  `>= seed+n`; a batch that jumps the boundary writes once; no-op and
  constraint-only appends never write; a lost write is retried at the next
  append past the gap; a seed discarded by the lineage guard leads to a write
  at the first append with `to >= n`.
- **Warm-up.** `WarmSnapshot` writes a snapshot at the stream's head even when
  the threshold says no, and even for a short stream; it errors without a
  cache; no read path calls it.
- **Adapters.** Round trip preserves `version`, `fold_version`, `recorded_at`
  and the exact state bytes; absent is `ok=false`; a second `Save` replaces;
  `Purge` empties. Postgres additionally: the migration applies from scratch
  (0013 is additive — one table and its policy, no `ALTER`, no backfill); a
  pre-existing `snapshots` table with a different shape **fails the migration**
  rather than being recorded as applied; RLS isolates workspaces and has no
  bypass arm; the ledger records `0013`; `WithoutAutoMigrate` fails
  `Load`/`Save` observably rather than silently succeeding.
- **Integration.** A runtime with a snapshot adapter seeds and folds only the
  tail; a fold-version mismatch and a lineage discard both fall through to a
  full fold; `LoadStale` still honours `maxAge` against the stored
  `recorded_at`.
- **Docs.** README's snapshot section names the adapters, the policy and the
  trust boundary; the ADR index row exists; ADR 0007's errata is corrected.

## 8. What is left to the builder

- The mechanism for threading the seed out of `load` (an extra private return,
  a small struct, or a field on a per-call value), and whether `Handle`'s
  retry loop needs it reset per attempt.
- Column types and casts (SQLite's integer/uint64 mapping; Postgres's bigint
  for `fold_version`), and the exact RFC3339 encoding for SQLite times.
- `Purge`'s signature and return, and whether it belongs on a small adapter
  interface.
- Test structure: mirrored per-adapter tests are expected; a shared
  conformance helper is optional and should not become a new public package
  without a reason.
- The deploy runbook text and the least-privilege grant list for the new table.

## 9. Deferred

- **The asynchronous writer.** When a remote cache exists, the shape is a
  bounded per-stream queue, coalesced, non-blocking send, drop on full, with
  `OnDrop`/`OnError` because `Runtime` discards `Save`'s error. It must own its
  lifetime (`Run(ctx)`); handing `Save`'s request context to a worker cancels
  every write when the handler returns, and nothing reports it — the symptom is
  "snapshots did not help". Reintroducing it also reintroduces the ordering
  question this ADR dropped; that writer must solve it with an epoch or a
  delete, not a bare version comparison.
- **A separate database** (a pool parameter) and a **fixed-name separate
  schema** (a migration, not a knob).
- **Attestation** of cached state, which needs keys es-lite does not hold.
- **A proto knob** and **per-stream thresholds**.
- **A conformance package** for adopters' own `Cache` implementations.
