# ADR 0037: Commit-Safe Feed Reads on Postgres

- **Status:** Accepted (implemented)
- **Date:** 2026-11-05
- **Related:** ADR 0001 §5 (the delivery gap this closes), ADR 0002 (delivery
  topology; its LISTEN/NOTIFY promise is corrected here), ADR 0004 (workspaces),
  ADR 0010 (migrations — the index-build caveat), ADR 0029 (the chain that pins
  delivered order), ADR 0035 (caller-owned transactions), ADR 0036 (no broker)

## Context

`global_position` on Postgres is assigned by a sequence at INSERT time, but
rows become visible at COMMIT time, and commit order ≠ assignment order. A
cursor-based feed that reads `WHERE global_position > :cursor`, sees position
101 (committed), advances past it, and only then watches 100 commit, has lost
100 forever — the monotonic checkpoint makes the skip permanent. This is the
P0 of the 2026-11-04 production review (`docs/review/20261104_feed_review.md`,
R1), the exact hazard ADR 0001 §5 deferred with the Postgres adapter.

The hazard is worst under **caller-owned append transactions** (ADR 0035):
an append that shares the caller's transaction draws its position long before
it commits, widening the window in which assignment and commit order diverge.
Any fix must be correct for that shape, not only for es-lite-owned short
appends.

Two spikes (2026-11-04, throwaway, both recorded below) settled the
mechanism: the first falsified the settled-ceiling design the plan started
with; the second verified every assumption of the replacement specified here.

## Decision: the horizon fence

Every feed-facing Postgres read is bounded by a *fence*: a recorded sequence
value `L` paired with a transaction id `R` drawn by the **reader** immediately
after reading `L`. Rows at positions ≤ `L` are delivered only once no
transaction that already existed when `R` was drawn is still in flight —
observed as `pg_snapshot_xmin(pg_current_snapshot()) >= R`. Per
`ReadAllBounded` call:

1. **Idle check** — read `L = last_value` of `events_global_position_seq`,
   outside any transaction. If `L <= from` there is nothing new: return an
   empty page without drawing anything. Idle streams burn no transaction ids.
2. **Fence establishment** — draw `R = pg_current_xact_id()` in a short
   transaction, then roll it back. The abort settles `R` immediately: the
   commit horizon advances to `R` itself.
3. **Fence check** — `SELECT pg_snapshot_xmin(pg_current_snapshot()) >= R`.
   True → deliver rows in `(from, L]` (the existing page query plus
   `AND global_position <= L`). False → store `(boundL, fenceR)` on the
   `Workspace` and return an empty page; the feed's wait cycle retries.
4. **Fenced delivery** — a later call checks the stored fence first; when it
   clears, it delivers ≤ `boundL` and drops the state. Every delivered page
   re-establishes a fresh fence on the next call.

The fence state is two integers on the adapter's per-scope `Workspace` (one
per stream, mutex-guarded). No schema, no new tables, no cross-process state.
`Head` and `MatchingHead` share the same fence; `RawHead` is the plain
unfiltered committed maximum, for stall detection and the resume clamp only.

### Why it is lossless

Two facts make the fence airtight:

- **P1 — the append draws its xid before its position.** The append path
  issues one bare read-only statement, `SELECT pg_current_xact_id()`, as its
  transaction's first statement, before the stream-head read and before any
  `nextval` position draw. Every event row's insert-xid is therefore assigned
  before its position is drawn. The bare statement is load-bearing: folding
  the xid draw into the head-read `SELECT` was tried and **falsified on empty
  streams** by the second spike — the head read returns no row there, so the
  target list never evaluates and no xid is assigned; every stream's *first*
  append would lose P1.
- **P2 — positions are monotonic.** Any row at a position ≤ `L` drew that
  position before the reader read `L`, and therefore (by P1) has an
  insert-xid assigned before `R` was drawn — an xid `< R`.

While any xid `< R` is in flight, `pg_snapshot_xmin` stays below `R`: an
in-flight xid *below* the commit horizon appears in the snapshot's in-flight
list and lowers `xmin`, and an in-flight xid *at or above* the horizon pins
the horizon itself, which is `xmin`'s floor when nothing older runs. The
reader's own `R`-abort is what raises the horizon past the appenders' xids
and makes them snapshot-visible in the first place. So `xmin >= R` implies
every xid `< R` is committed or aborted. Combined with P2: when the fence
clears, every row at a position ≤ `L` is either already visible or was
aborted — nothing inside the delivered range can appear later. Aborted
appends leave permanent holes above the cursor, which the type filter
already treats as ordinary cross-workspace gaps.

The argument is reproduced as a comment on the `Workspace` implementation
(`postgres/wsstore.go`) and pinned by the ordering tests
(`TestReadAllBoundedWaitsForOutOfOrderCommit`,
`TestReadAllBoundedWaitsForStraddledCommit`,
`TestReadAllBoundedPassesAbortedHole`, `TestHeadIsSettledWhileAppendInFlight`)
and by the chaos gate (`task test:feedgate`: 8 concurrent appenders with
randomized insert/commit separation, ≥100k events, set-equality oracle,
kills at the ack windows).

### Spike records

**Spike 1 (Task 1) — the settled ceiling is falsified.** The original design
bounded each page by the sequence value recorded one round earlier, delivered
once the snapshot's `xmax` had passed the previous round's `xmax`. On
PostgreSQL 17.11 this is provably unsafe: snapshot `xmax` is
`latestCompletedXid+1` — the *commit horizon* — and in-flight xids at or
above the horizon are skipped from the snapshot's in-flight list entirely
(`procarray.c`), so a lone in-flight append looks *idle* to every snapshot
(`xmin == xmax`). At the straddle gap — tx1 committed, tx2 in flight at the
next position — the spike recorded snapshot `789:789` with tx2 provably in
flight: the fast-path "settled" condition fires exactly there and the
original mechanism would deliver past tx2's position and lose its event.
Committing a higher xid surfaces lower in-flight xids (`772:774:772`);
committing a lower xid does not pass a higher in-flight one (`776:776`).
Sequence `last_value ≥` every drawn position held at every measurement
point, cross-connection and mid-transaction.

**Spike 2 (Task 1b) — all four fence assumptions verified**, with the
documented fallback for the one that failed in its first shape:
aborting the reader's `R` advances the horizon (`R=744` → `xmin=745`); the
fence engages on the straddle (unfenced `748:748` vs fenced `748:750:748` at
the same state — the fence sees what the settled ceiling could not — and
clears to `750:750` delivering `[1 2 3]`); the xid draw assigns before the
position draw (the `readHead` fold falsified on empty streams, the bare
statement verified on both stream states, the xid stable across the whole
append transaction); a lone in-flight append is waited out and delivered on
commit (`757:759:757` → `759:759`). Full reports:
`.superpowers/sdd/2026-11-04-postgres-production-grade/task-1-report.md`
and `task-1b-report.md`.

## Rejected alternatives, and the interleavings that killed them

**Per-row `xmin` filter** (return only rows whose insert transaction has
provably finished). Filters in *xid space* while the cursor advances in
*position space*, and the two orders diverge when a transaction's first
write precedes its append — the shape ADR 0035 exists for. Killer
interleaving: T1 opens, writes another table (drawing an old xid), then
appends at `gp=101` and commits; T2 opens later (newer xid), appends
`gp=100` earlier, commits later. At the moment T2 is in flight a filtered
page returns 101 alone; the cursor acks past 100 and its later commit is
never delivered. Not fixable by tweaking the predicate: the reader cannot
distinguish "will commit later" from "already aborted".

**Pre-reserved commit watermark** (positions reserved in a committed
registration transaction before the insert). Broken for caller-owned
transactions: the registration would have to commit *before* the caller's
own transaction that wraps the append — impossible without breaking the
atomicity the caller asked for. It also costs one extra commit/fsync per
append call on the es-lite-owned path and needs advisory-lock liveness
machinery to stay correct. Killer interleaving: any ADR 0035 append —
`AppendTx` into a caller's open transaction simply has no earlier point at
which the reservation could have committed.

**Settled ceiling** (two-phase, position-space: bound delivery by the
sequence value recorded one round earlier, once the snapshot horizon has
passed that round's `xmax`). Falsified empirically by spike 1. Killer
interleaving: the straddle — tx1 commits position 101 while tx2 holds 100
in flight; the snapshot is `789:789` (idle-looking) because tx2's xid sits
at the commit horizon and is skipped from the in-flight list; the
"settled" condition fires, the page delivers 101, the cursor passes 100,
and tx2's commit is lost.

**Per-workspace contiguous positions** (a counter row taken `FOR UPDATE`).
Correct and ADR-0035-compatible, but it serializes every append in a
workspace on one hot row — a real write-path cost. Kept only as the
fallback if the fence's assumptions are ever falsified in production.

**Gap detection on existing positions.** The sequence is shared across all
workspaces; gaps within one workspace are legitimate (aborts, other
workspaces' draws) and indistinguishable from in-flight inserts. Rejected.

**`track_commit_timestamp`.** Off by default and unavailable on most
managed Postgres. Rejected.

## What the fence costs, accepted knowingly

- **Read side — one real transaction id per non-idle poll round per
  subscriber.** The idle check (statement 1) costs nothing; but a subscriber
  whose types are rare on a busy database draws a fence on nearly every poll
  (the sequence moves, so `L > from`). At the documented envelope (spec
  §5.12: 100 subscribers, ~4 polls/s) that is on the order of **34 million
  xids per day** — roughly 1.6% of the ~2·10⁹ xids in a wraparound epoch.
  The direct consequence is operational: autovacuum must advance frozenxid
  on schedule, so a fleet-sized feed deployment pulls wraparound maintenance
  from "may never run" into the regular operating window. Alert on
  `age(datfrozenxid)` per database (the runbook carries the guidance).
- **Write side — one extra round trip per append transaction.** The bare
  `SELECT pg_current_xact_id()` before the head read measured ≈ **106.8 µs
  per call, ≈ 3.2% of a batch-20 append** (Docker-on-macOS; the ratio, not
  the absolute, is the transferable figure). This is the entire write-path
  cost of commit-safe reads — no extra commits, no reservation writes, no
  new locks — and it was accepted at the Gate 0 exit review.
- **The empty-log edge.** A fresh sequence reports `last_value = 1` before
  any draw, so on a zero-row database `L > from` holds and each poll draws a
  fence even though there is nothing to deliver. A known, minor cost of an
  empty log; the first append makes it moot.
- **Future mitigation (not built).** At large fleet sizes the per-poll
  `(snapshot, last_value)` round can be shared per process per workspace —
  one read fanning out to that workspace's streams (spec §5.12) — cutting
  the xid burn and round trips without touching correctness. Deferred
  until the envelope demands it.

## Stall semantics: delay, never loss

Because the fence waits for the commit horizon, it waits for *any* live
transaction that existed when `R` was drawn — including a wedged long-lived
transaction elsewhere on the primary (an analytical query gone wrong, a
forgotten `BEGIN`). While the horizon is pinned, all feed pages for all
subscribers on that database stall. This is the deliberate trade: the
failure mode of the fence is *delay*, never *loss*. Three properties keep
the delay honest:

- **It is visible.** The stream's empty-page branch probes `RawHead`
  (rate-limited to at most one probe per stream per few seconds) and reports
  `Observer.OnStall(settledHead, rawHead)` when committed backlog sits above
  the settled position — exactly once per stall episode. A stall shows up
  in metrics within seconds, not as an unexplained silence.
- **It never corrupts resume.** `Head`/`MatchingHead` report 0 while the
  fence is blocked; the resume clamp therefore consults `RawHead` and only
  clamps when `RawHead < checkpoint` (the restored-log case). A blocked
  fence can cause a transiently negative `Status.Lag`, never a full-log
  replay.
- **It is diagnosable elsewhere.** The runbook directs stall analytics to a
  replica: `pg_stat_activity` on the primary during a stall episode finds
  the horizon-pinning transaction, but heavy diagnosis queries should not
  run there.

Waiting behind a long *caller-owned append* is not a stall but the same
mechanism working: those events are genuinely in flight at positions below
what would otherwise be delivered next, and delivering past them is the
loss this ADR exists to prevent.

## Fallback gates

The spike protocol is part of the decision. If the abort-advances-horizon
assumption ever fails on a future Postgres, the fallback is to keep the `R`
transaction open until delivery (costing one connection per stream). If the
xid-before-position assumption ever fails, the bare-statement shape is the
documented repair (it already is the design). Anything newly falsified goes
back to a human decision, not a silent patch — the settled-ceiling
falsification is the model.

## Index build order

The feed's page and head queries need two indexes migration 0003 adds:

```sql
CREATE INDEX events_ws_pos_idx      ON events (workspace_id, global_position);
CREATE INDEX events_ws_type_pos_idx ON events (workspace_id, type_url, global_position);
```

The build order is operational, not incidental. Migrations run in one
transaction (ADR 0010), which forbids `CREATE INDEX CONCURRENTLY`; a plain
build on a ≥10M-row partitioned table blocks appends for its whole duration
— and with auto-migrate-on-`Open`, the first upgraded replica would take
that lock at boot. Postgres (through at least 18) additionally cannot run
`CREATE INDEX CONCURRENTLY` directly on a partitioned parent at all. The
runbook therefore builds both indexes manually **before** the migration is
allowed to run, using the partition-aware pattern — build each partition's
child index with `CONCURRENTLY`, create the parent index with `ON ONLY
events`, then `ALTER INDEX ... ATTACH PARTITION` for each partition (the
last two steps are metadata-only and instant). The migration's
`CREATE INDEX IF NOT EXISTS` statements remain as the enforcement net: an
instant no-op when the runbook was followed, a blocking build only on small
databases that skipped it. The EXPLAIN gate (`task test:feedgate`) asserts
the page query is served by the composite index and never sequentially
scans an events partition.

## RLS rollout

The fence protects *position* correctness; row-level security protects
*workspace* correctness, and the two landings were ordered deliberately:

- **The feed tables have no bypass arm at all.** `feed_checkpoints`,
  `feed_leases` and `feed_dead_letters` (migration 0004) are RLS-enabled
  with `FORCE ROW LEVEL SECURITY` and admit only the workspace arm
  (`workspace_id = current_setting('app.workspace_id', true)`). The feed is
  per-workspace; no non-superuser, non-`BYPASSRLS` session — not even one
  holding `app.bypass_rls` — may reach another workspace's subscription
  state. The conformance suite pins
  cross-workspace refusal against a non-superuser probe role.
- **The pre-existing bypass arms are role-gated.** The `events` and
  `unique_claims` policies (migration 0001) trusted any session that set
  `app.bypass_rls=on`. Migration 0005 additionally requires
  `pg_has_role(current_user, 'es_relay', 'member')`. `Drain` — the
  cross-workspace claim relay — and the anchor/verify path (`Anchor`,
  `CheckAnchor`, `ListStreams`, `ListAnchors`) are the only bypass readers.
  **Rollout ordering is part of the mechanism:** the moment 0005 applies,
  the bypass silently stops working for every non-member — so the runbook
  creates `es_relay` and grants it to the relay and anchor/verify principals
  *before* the migration runs, and both paths verify membership once per
  `Store` at startup, failing fast with `postgres.ErrRelayRoleMissing`
  instead of silently reading nothing (which on the anchor path would
  compute a root over zero heads and record it as a real anchor).

## Scope: what is fenced and what is not

Truthfulness requires naming the boundary. On Postgres, **the feed-facing
reads are fenced**: `Workspace.ReadAllBounded`, `Head` and `MatchingHead` —
everything `feed.Log` is implemented over. The raw `Workspace.ReadAll`
cursor — used by `projection.LogFeed`/`Replay` for in-process tails and by
the legacy `delivery.Poller` — is deliberately *not* fenced: it remains the
naive cursor ADR 0001 §5 warned about, suited to single-writer workspaces,
rebuilds against a quiescent log, and tests. `Drain` carries its own
commit-safety (the `published_at` claim relay with `FOR UPDATE SKIP
LOCKED`). On SQLite, the single-connection writer makes assignment and
commit effectively serialized, so every read is gap-free by construction.
Consumers that need commit-safe delivery under concurrent appends on
Postgres must go through `feed` (or `Drain`) — the README's guarantees
block says exactly this.

## LISTEN/NOTIFY: corrected and deferred

ADR 0002 promised a Postgres `LISTEN/NOTIFY` push path; none ships. The
feed polls (~250ms default, ±20% jitter) and exposes a `Waiter` seam for a
future wakeup source, but a LISTEN/NOTIFY waiter requires an append-time
`NOTIFY` — a write-path cost this design's anti-goals forbid — so it stays
a post-gate, separately-ADR'd decision (spec §5.12 notes the ~1000-
subscriber scale where it pays off). One deferred question seeds that
future ADR: the `Waiter` contract cannot distinguish "woken because new
events exist" from "timed out" — both return nil. The server therefore
treats every return as "maybe" and re-reads the page, which keeps correctness
but costs the latency win unless the contract gains a signal. The
*page-confirmed floor* option preserves both: on an early nil return, read
the page; skip the remainder of the poll interval only if the page is
non-empty. That keeps wakeup latency single-digit for a real push source
without reopening the spin hole a misbehaving `Waiter` could create.

## Consequences

### Positive

- **No event is ever skipped by the feed on Postgres** under any append
  concurrency, including caller-owned append transactions — the P0 is
  closed, and pinned by deterministic ordering tests plus the chaos gate.
- The write path pays one read-only statement per append and nothing else:
  no extra commits, no locks, no reservation table, no sequence changes.
- Stalls are visible (`OnStall`), safe (the fence-aware clamp), and honest
  (delay, never loss); the failure mode of every interleaving degrades to
  waiting.
- The fence is two integers of state per stream and three cheap statements
  per round — it adds no cross-process coordination anywhere.

### Negative

- **The feed trails by one wait cycle** (~250ms by default) instead of
  delivering same-snapshot under sustained load — the accepted price of
  never delivering past an in-flight lower position.
- **A wedged long-lived transaction stalls every feed page** on the
  database until it ends; the stall is observable but the delay is real
  (runbook: find the horizon-pinning transaction, run analytics on a
  replica).
- **The fence burns transaction ids** at a rate that makes autovacuum
  wraparound maintenance a live operational concern at fleet scale; the
  monitoring burden (age-of-frozenxid alerting) moves to the runbook.
- **One extra round trip on every append** (~3.2% of a batch-20 append,
  measured) — permanent, accepted, and the honest number is recorded here
  rather than in a benchmark nobody re-runs.
