# Postgres Production-Grade Feed — Design

- **Date:** 2026-11-04 (rev 3 — Task 1 spike falsified the settled ceiling; horizon fence chosen)
- **Status:** proposed (awaiting approval)
- **Owner:** es-lite
- **Implements:** the Postgres-relevant actions of `docs/review/20261104_feed_review.md` (Gate 0 in full; Gate 1 items R2, R4, R5, R7, R8 including the consumer adapter, R11; Gate 2 test/CI items)
- **Spike:** 1 day, throwaway, before any production code (§5.1.4)

---

## 1. Intent and why

The 2026-11-04 review found that the Postgres feed path is not production-ready. The root cause is structural: `global_position` is assigned by a sequence at INSERT, not at COMMIT, so a cursor-based reader can advance past a lower position that commits later and lose that event permanently. Around that P0 sit five P1 gaps: unfenced acknowledgements, a poison-event wedge with no recovery path, a missing composite index, silent skips when a projection's consumed types change, and the absence of the Postgres feed adapters — and the consumer adapter — that would make any of this testable end to end.

This design makes the Postgres feed path trustworthy: no silent loss, no silent wedges, no silent skips, and observable behavior when the system must wait.

## 2. Goals

1. **No event is ever skipped by the feed on Postgres** under any append concurrency, including caller-owned append transactions (ADR 0035).
2. Acks cannot move a subscription's cursor unless the acker holds the lease.
3. An oversized or poisoned event has a bounded blast radius and an operator recovery path that is recorded.
4. Feed page queries are index-served at 10M+ events per database.
5. Changing a projection's consumed type set fails closed instead of skipping history.
6. The Postgres `feed.Log`, `CheckpointStore`, `LeaseStore` adapters and the `feed.Server` → `projection.Feed` consumer adapter ship in this repo and pass one shared conformance suite.
7. A stalled feed (waiting on in-flight transactions) is visible, not silent.
8. One feed process sustains ≥100 concurrent Postgres subscribers at a documented, bounded resource cost (§5.12).

## 3. Non-goals and anti-goals

**Non-goals (tracked separately in the review doc):**

- `BufferedMarker` durability and `Runner` retry budgets (review A7) — projection-layer, backend-agnostic; separate plan.
- Core `Config.Authorize` hook — transport authorization belongs to the platform; this design keeps structural defenses (RLS, workspace pinning, role-gated bypass).
- Legacy `delivery.Poller`/`Relay` disposition (review A14).
- Audit trail beyond the dead-letter record (review A15).

**Anti-goals (would make the design a failure):**

- Any write-path throughput or latency regression on the append path, **other than the one read-only xid-draw statement per append that P1 requires** (§5.1.3 — Task 1b falsified the zero-cost fold on empty streams; the statement's measured cost is recorded at the Gate 0 benchmark and accepted in the exit review). Otherwise: no extra commits, no position reservation, no new locks taken by appends.
- A mechanism whose failure mode is *loss* rather than *delay*.
- New runtime dependencies (no metrics library; the observer is an interface).
- Breaking the hash chain, the opaqueness of payloads (ADR 0025), or SQLite behavior (SQLite needs none of the read-side changes).

## 4. Approaches considered

| Approach | Verdict | Why |
|---|---|---|
| **A1. Read-side per-row `xmin` filter** — pages return only rows whose insert transaction provably finished | **rejected during design review** | Provably lossy under ADR 0035: it filters in *xid space* while the cursor advances in *position space*, and the two orders diverge when a transaction's first write precedes its append (old xid, late position). The reader can deliver `gp=101` and permanently skip an in-flight `gp=100`. Found by the builder check; not fixable by tweaking the predicate. |
| **A2. Pre-reserved commit watermark** — positions reserved in a committed registration transaction before insert | **rejected** | Broken for caller-owned transactions: the registration cannot commit before the caller's transaction that wraps the append. Also +1 commit/fsync per append call and advisory-lock liveness machinery for the non-ADR-0035 path. |
| **A3. Settled-ceiling reads (two-phase, position-space)** — each page bounds delivery by the sequence value recorded one round earlier, delivered only once the snapshot horizon has passed that round's xmax | **falsified by the Task 1 spike** | PG's snapshot xmax is the *commit* horizon and skips in-flight xids ≥ it, so a lone in-flight append looks "idle" to every snapshot; the fast-path condition fires at the exact straddle gap (observed `789:789` with tx2 provably in flight) and loses the gap event. Kept as the record of what was tried; see §5.1.6. |
| **A3′. Horizon-fence reads** — the reader records the sequence value `L`, draws its own transaction id `R` immediately after reading `L`, and delivers below `L` only once every snapshot reports `xmin ≥ R`; appends draw their xid with one bare read-only statement before their first query, so the xid provably precedes the position draw | **chosen** (§5.1, rev 3; verified by the Task 1b re-spike) | Read-side only; the only write-path cost is one extra read-only statement per append (the zero-cost `readHead` fold was falsified on empty streams — see §5.1.7); `xid8` comparison is wrap-safe; correct for any interleaving including ADR 0035 (proof in §5.1.3, validated against both spikes' recorded snapshots); degrades to delay, never loss. |
| B. Per-workspace contiguous positions (counter row, `FOR UPDATE`) | fallback only (§5.1.5) | Correct and ADR-0035-compatible, but serializes all appends per workspace on a hot row — a real write-path cost, violating an anti-goal unless forced. |
| C. Gap detection on existing positions | rejected | The sequence is shared across workspaces; gaps within a workspace are legitimate and indistinguishable from in-flight inserts. |
| D. `track_commit_timestamp` | rejected | Off by default, unavailable on most managed Postgres. |

**What A3 costs, accepted knowingly:** under sustained append load the feed trails by one wait cycle (the feed's existing poll cadence, ~250ms default) instead of delivering same-snapshot, and it waits behind long *caller-owned appends* — which is correct behavior, since their events are genuinely in flight and sit at positions below what would otherwise be delivered next. Residual exposure: long-lived transactions elsewhere on the primary delay the ceiling advance; §5.8 makes the stall observable and the runbook directs analytics to a replica.

## 5. Decided design

### 5.1 Commit-safe reads (the P0): the horizon fence

> **Revision 3 (2026-11-04).** The Task 1 spike falsified the original settled-ceiling mechanism (§4 row A3; record in §5.1.6). This section now specifies the replacement chosen by the human: the horizon fence. Its losslessness argument (§5.1.3) is validated against the spike's own recorded observations.

**5.1.1 Mechanism.** Every feed-facing Postgres read is bounded by a *fence*: a recorded sequence value `L` paired with a transaction id `R` drawn by the reader immediately after reading `L`. Rows at positions ≤ `L` are delivered only once no transaction that existed when `R` was drawn is still in flight — observed as `pg_snapshot_xmin(pg_current_snapshot()) >= R`.

Per `ReadAllBounded` call:

1. **Idle check (statement 1, no fence):** read `L = last_value` of `events_global_position_seq`. If `L <= from` there is nothing new — return an empty page without drawing anything (idle streams burn no transaction ids).
2. **Fence establishment (statement 2, short tx, then rollback):** draw `R = pg_current_xact_id()` and roll the tx back (the abort settles `R` immediately — Task 1b verifies).
3. **Fence check (statement 3):** `SELECT pg_snapshot_xmin(pg_current_snapshot()) >= R`. True → deliver rows in `(from, L]` (the existing query plus `AND global_position <= L`). False → store `(boundL, fenceR)` on the `Workspace` and return an empty page (`truncated=false`); the feed's existing wait cycle retries.
4. **Fenced delivery on a later call:** with stored state, run the fence check first; when it clears, deliver `≤ boundL` and drop the state. Every delivered page re-establishes a fresh fence on the next call.

`Head` and `MatchingHead` (§5.8) use the same fence. `RawHead` is the plain unfiltered max, for stall detection only.

**5.1.2 Where the state lives.** `(boundL, fenceR)` is two integers on the adapter's per-scope `Log` instance — the `*Workspace` that `Logs.Open` returns once per stream (`feed/server.go:279`). Guarded by a mutex, since `Status` may touch the same instance concurrently. No schema, no new tables, no cross-process state.

**5.1.3 Why it is lossless (the argument the tests must encode).** Two facts make the fence airtight:

- **P1 — the append draws its xid before its position.** The append issues one bare read-only statement — `SELECT pg_current_xact_id()` — as its transaction's first statement, before `readHead` and before any `nextval` position draw. Hence every event row's insert-xid was assigned before its position was drawn. (Folding the xid draw into `readHead`'s SELECT was tried and **falsified on empty streams** by the Task 1b spike: `readHead` returns no row on an empty stream, so the target list never evaluates and no xid is assigned — every stream's *first* append would lose P1. The bare statement assigns unconditionally; the xid is stable across the whole append transaction — verified, §5.1.7.)
- **P2 — positions are monotonic.** Any row at a position ≤ `L` drew that position before the reader read `L`, and therefore (by P1) has an insert-xid assigned before `R` was drawn, i.e. an xid `< R`.

While any xid `< R` is still in flight, `pg_snapshot_xmin` stays below `R` — the spike verified both halves: an in-flight xid *below* the commit horizon appears in the snapshot's in-flight list and lowers `xmin`; an in-flight xid *at or above* the horizon pins the horizon itself, which is `xmin`'s floor when nothing older runs. So `xmin ≥ R` implies every xid `< R` is committed or aborted. Combined with P2: when the fence clears, every row at a position ≤ `L` is either already visible or was aborted — nothing inside the delivered range can appear later. Aborted appends leave permanent holes above `from`, which the type filter already treats as ordinary cross-workspace gaps. The argument is reproduced as a comment on the implementation and pinned by the ordering tests (§5.10.1).

**5.1.4 Re-spike (Task 1b, throwaway) — run 2026-11-04, all four assumptions verified, (3) with the documented fallback.** The load-bearing assumptions, on the `task test:pg` Docker (`postgres:17`):

1. **Abort advances the horizon:** draw `R` in a tx, roll it back; a fresh statement from another connection observes `pg_snapshot_xmin(...) >= R`. ✅ verified (`R=744` → `xmin=745`).
2. **The fence engages on the straddle:** replay the Task 1 straddle (§5.1.6); at the gap step (tx1 committed, tx2 in flight) the reader's freshly drawn `R` yields `xmin < R`; after tx2 commits, `xmin >= R` and delivery ≤ L yields `[1, 2, 3]`. ✅ verified (unfenced `748:748` vs fenced `748:750:748` at the same state; cleared `750:750` → `[1 2 3]`).
3. **The xid draw assigns before the position draw:** ✅ verified **with the fallback** — the `readHead` fold is falsified on empty streams (no row → target list never evaluates → no xid); the bare `SELECT pg_current_xact_id()` statement before `readHead` assigns unconditionally on both stream states, and the xid is stable across the whole append transaction.
4. **The lone in-flight append:** a single uncommitted `AppendTx` — the fence waits, then clears on commit. ✅ verified (`757:759:757` → `759:759`, delivers `[1]`).

**5.1.5 Fallback gate.** Assumption (1) failing would be handled by keeping the `R` tx open until delivery (not needed — (1) verified); (3) failing was handled by the extra-statement shape, now the specified design (P1). Anything newly falsified goes back to the human.

**5.1.7 Re-spike record (Task 1b, 2026-11-04, throwaway).** On `PostgreSQL 17.11`: (a) aborting the reader's own `R` advances the commit horizon to `R` itself, which is what makes in-flight appenders snapshot-visible — at the straddle gap the unfenced snapshot is `748:748` (Task 1's falsification, reproduced) while the fenced snapshot at the same state is `748:750:748` with `xmin` pinned at tx2's xid; the fence sees what the settled ceiling could not. (b) The fold's empty-stream failure is not an edge case: every stream's first append hits it. (c) `L = last_value` reflects uncommitted draws (conservative, safe — delivery waits on the fence, not on L). (d) When the fence clears with nothing older in flight, `xmin` equals `R+1` — the fence clears promptly, no spurious waiting. Full report preserved at `.superpowers/sdd/2026-11-04-postgres-production-grade/task-1b-report.md`.

**5.1.6 Spike record (Task 1, 2026-11-04, throwaway).** Observed on `PostgreSQL 17.11`: snapshot `xmax` is `latestCompletedXid+1` (the commit horizon); in-flight xids ≥ the horizon are skipped from the snapshot's in-flight list entirely (`procarray.c`), so a lone in-flight append looks idle (`xmin == xmax`). At the straddle gap step the spike recorded `789:789` while tx2 was provably in flight — the original fast-path condition fires there and would lose the gap event. Committing a higher xid surfaces lower in-flight xids (`772:774:772`); committing a lower xid does not pass a higher in-flight one (`776:776:`). Sequence `last_value ≥` every drawn position held at every measurement point, cross-connection and mid-transaction. Full report preserved at `.superpowers/sdd/2026-11-04-postgres-production-grade/task-1-report.md`.

### 5.2 Indexes

```sql
CREATE INDEX events_ws_pos_idx      ON events (workspace_id, global_position);
CREATE INDEX events_ws_type_pos_idx ON events (workspace_id, type_url, global_position);
```

**Build order is operational, not incidental** (found by the builder check): migrations run in one transaction (ADR 0010), which forbids `CREATE INDEX CONCURRENTLY`, and a plain build on a 10M+ row partitioned table blocks appends for the whole build — with auto-migrate-on-`Open`, the first upgraded replica would take that lock at boot. Therefore: the runbook builds both indexes manually with `CONCURRENTLY` **before** the migration is allowed to run; the migration carries `CREATE INDEX IF NOT EXISTS` as the enforcement net (instant no-op when the runbook was followed; a blocking build only on small databases that skipped it). ADR 0010 gains a note. The EXPLAIN gate (§5.10.2) asserts the page query uses `events_ws_type_pos_idx` and heap-fetches no other workspace's rows.

### 5.3 Payload caps and the poison path

**One authoritative cap, one backstop.** The cap lives on the **store**, set at `Open` via a functional option (`sqlite.Option`, `postgres.Option`; both packages use the functional-option pattern — there is no `OpenOptions`): `WithMaxPayloadBytes(n)`; default `8 << 20` (matching the feed's `defaultMaxPayloadBytes`); `0` = default; negative = unlimited. The store enforces it at Append — rejecting with a new sentinel `es.ErrPayloadTooLarge` (wrapped with stream, version, type URL and actual size; never the payload) — and honors it in its page reads. `feed.Config.MaxPayloadBytes` remains as a defense-in-depth backstop for adapters without SQL-side size checks; the conformance suite pins that a store-configured cap is honored in both append and read, and the docs state the store cap is authoritative. (Builder question 4, resolved.)

**Read-side (no materialization).** `ReadAllBounded` in both adapters becomes a two-step page:

1. Fetch `(position, octet_length(payload))` for candidate rows within the byte budget — the cap check happens in SQL, so an over-cap payload is never materialized.
2. Fetch full rows for the chosen positions only.

If the *first* row of a page is over the store's cap, the adapter returns `*feed.EventTooLargeError` (position, type, size, limit — no payload); if a *later* row is over, stop the page before it and report `truncated`, so the next page makes it the first row. The server's own check stays for adapters that don't implement this (documented in the port).

**Recovery.** New `feed.Server.Skip(ctx, req Request, position uint64, actor, reason string) error`:

- Refuses while a lease is held unless forced (same rule as `ResetCheckpoint`; the normal flow is that the poison error already killed the stream and released the lease). `Skip` is a position advance, not a redefinition: it **preserves** the stored definition fingerprint (§5.5).
- Advances the checkpoint to `position` (resume reads strictly after it) and records a dead-letter row.

New optional port and table, both backends (SQLite migration 0004; Postgres migration 0003):

```sql
CREATE TABLE feed_dead_letters (
    workspace_id text NOT NULL, projection text NOT NULL, instance text NOT NULL,
    position bigint NOT NULL, type_url text NOT NULL, size_bytes int NOT NULL,
    reason text NOT NULL, actor text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, projection, instance, position)
);
```

`Config.DeadLetters feed.DeadLetterStore` is optional; nil means Skip still advances but records nothing (documented). This is the only audit surface in scope (partially addresses review R13).

**DLQ lifecycle.** A dead-letter row is a *pointer*, not a copy: the event itself remains in the immutable log, so nothing is duplicated into a second retention surface and nothing is ever deleted from the source of truth. Triage is by `(workspace, projection, reason)`: fix the cause — raise the cap, add an upcast, fix the handler — then either accept the gap as a recorded, audited omission, or replay by resetting the checkpoint to before the position, which re-delivers the dead-lettered event like any other. Rows are tiny and double as the audit record; retention is an ops policy, not a mechanism. `OnPoison` plus a DLQ-depth gauge feed the alerting. Handler-level poison (a dispatch that always fails) is *not* this design's DLQ: it stays in the fail-stop path here and gets a bounded-retry budget writing into the same `feed_dead_letters` table in the companion projection plan (review A7).

### 5.4 Acknowledgement fencing (API change, pre-1.0)

**Owner.** `Request` gains `Owner string` — a caller-supplied, unique-per-subscription token (the transport's subscription id). When empty the server mints one as today, and such a stream is not ackable (documented). `Subscribe` uses `req.Owner` verbatim as the lease owner. Owner collision between two concurrent streams of the same key is safe: the second acquire fails with `ErrLeaseHeld`, or a reconnect reusing its own token steals from its own zombie (fast self-failover) and the loser's next renewal fails closed. `newPrefix` propagates entropy errors instead of falling back to the constant `"feed"` (review D-3).

**Fenced ack.** Acks go through one value type instead of a growing parameter list:

```go
type Ack struct {
    Scope, Projection, Instance string
    Owner      string // the subscription token from Request.Owner
    Position   uint64
    Definition string // Definition.Hash() of the acking consumer's type set (§5.5)
}
func (s *Server) Checkpoint(ctx context.Context, ack Ack) error
```

The server verifies via `LeaseStore.Holder` that the holder's owner equals `ack.Owner` before saving: not held → `ErrLeaseLost`; held by another → `*LeaseHeldError`. A rejected ack never touches the cursor. Known and accepted: the holder check and the save are not one atomic step with a concurrent renewal loss; the failure mode is a rejected ack and a reconnect, never an advanced cursor — documented on the method.

`Subscribe`'s blocking shape is unchanged. `ResetCheckpoint`/`DeleteSubscription` keep their `force` semantics and now also clear the definition fingerprint (a reset is a deliberate re-subscription under a new identity).

**Port reshaping (found by the builder check).** `LeaseStore` and `CheckpointStore` methods currently take the NUL-joined `LeaseKey` string, which forces the Postgres adapter to split NUL bytes out of a TEXT column to recover the workspace for RLS. The ports change to take `(scope, projection, instance)` as three parameters; adapters compose their own storage keys. `feed.LeaseKey` becomes an internal helper or is deleted. With §5.9's NUL validation, no key ever carries an ambiguous byte.

### 5.5 Definition fingerprint (fail closed on drift)

- `projection.Definition` gains `Hash() string` — deterministic SHA-256 over `name` + NUL + sorted `Consumes` joined by NUL. One implementation, shared by core and codegen output.
- `feed.CheckpointStore.Save/Reset` signatures gain `definition string` (the hash; empty = unset). The `feed_checkpoints` schema gains `definition_hash text NOT NULL DEFAULT ''` (SQLite migration 0004; the new Postgres table has it from day one).
- The ack persists the hash (§5.4's `Ack.Definition`), so the first subscription records its identity. The ack path is cross-process-safe: any process can recompute `Definition.Hash()` from its catalogue, so no in-memory coupling exists (builder question 3, resolved).
- `Subscribe` reads the stored hash via `CheckpointStore.Checkpoint` — `feed.Checkpoint` gains a `DefinitionHash` field — and, when the stored hash is non-empty and differs from `req.Definition.Hash()`, fails immediately with a new `feed.ErrDefinitionChanged` (transport-mappable to FailedPrecondition; the error names the projection and points at `ResetCheckpoint`).
- `Skip` preserves the hash; `ResetCheckpoint` clears it.

Result: adding an event type to a deployed projection refuses to serve until an operator explicitly resets — no silent history skip.

### 5.6 Postgres feed adapters and the consumer adapter (new, in this repo)

New file `postgres/feed.go` (+ migration 0003, workspace-scoped tables with the same RLS pattern as `events`, **without** any bypass arm — the feed is per-workspace and never reads across tenants):

- **`Logs`** — `Open(ctx, scope)` returns the existing `*Workspace`, which implements `feed.Log` and is commit-safe via §5.1. Workspace pinning uses the existing `setWorkspace` transaction pattern.
- **`CheckpointStore`** — `feed_checkpoints(workspace_id, projection, instance, position, definition_hash, updated_at)`, RLS-enabled; monotonic Save via `GREATEST`; timestamps from `clock_timestamp()` (DB clock — sidesteps review R10 for Postgres).
- **`LeaseStore`** — `feed_leases(workspace_id, projection, instance, owner, consumer, expires_at)` with RLS; expiry and takeover comparisons use `clock_timestamp()` in SQL; single-statement owner-guarded upsert mirroring the SQLite adapter. Keys are the three columns — no NUL-joined strings in storage (§5.4).
- **`eventfeed` consumer adapter** (review R8, the load-bearing half the design previously omitted — builder question 2, resolved: in scope): a new package implementing `projection.Feed` over `feed.Server`. Its `Open` supplies `Request.Owner` (a fresh UUID per attempt, or a stable token for self-stealing reconnects — builder decides, both safe); its `Sink.Send` invokes the projection `handle`; it calls `Server.Checkpoint` with `Ack{Owner, Position, Definition: Definition.Hash()}` **after** `handle` returns nil — the at-least-once invariant, in code and under test, instead of a doc comment at `projection/runner.go:16`. It wraps itself in `ReconnectingFeed` and reports poison via the observer rather than swallowing it. **Acks are coalesced at page boundaries** — one fenced ack per delivered page, plus an ack on the idle tick when events trickled in sub-page, never per event: 100 subscribers cannot stampede the lease/checkpoint tables, and the crash-redelivery window is bounded by one page (≤ `MaxBatch`).

**Conformance suite.** New package `feedtest` exports `RunLog`, `RunCheckpoints`, `RunLeases` (and a combined `RunAll`) encoding the port contracts the review found unpinned: monotonic-vs-rewinding Save, expiry boundaries (`<=` acquire / `>` holder), owner-guarded renew/release, empty types, first-row-over-budget, `truncated` at exact budget, `*EventTooLargeError` without materialization, checkpoint round-trip including definition hash, and — for multi-workspace fixtures — cross-workspace access refusal (RLS). SQLite and Postgres adapter tests both call it; the platform's future adapters must too.

### 5.7 RLS bypass hardening

The `events_workspace_isolation` policy's bypass arm trusts any session that sets `app.bypass_rls=on` — and `unique_claims` has the *identical* arm, which the original design missed (builder question 5). Both policies change to:

```sql
OR (current_setting('app.bypass_rls', true) = 'on'
    AND pg_has_role(current_user, 'es_relay', 'member'))
```

**Rollout ordering is part of the design:** the moment the migration applies, the bypass silently stops working for everyone (the relay sees no rows — a silent delivery outage, not an error). Therefore: the runbook creates the `es_relay` role and grants it to the relay principal **before** the migration is applied, and `postgres.Store` gains a startup verification — when `Drain` runs with the bypass set, it checks `pg_has_role(current_user, 'es_relay', 'member')` once and fails fast with a typed, actionable error instead of silently reading nothing. The migration does not attempt `CREATE ROLE` (migrator privileges vary); CI's superuser container creates it in the test setup.

### 5.8 Observability

- `feed.Config.Observer` — new optional interface, no-op default, no dependencies:

```go
type Observer interface {
    OnPage(events, bytes int)             // per page
    OnCheckpoint(position uint64)         // successful fenced ack
    OnLeaseLost(key, owner string)        // renewal failure
    OnPoison(err *EventTooLargeError)     // oversized first row
    OnStall(settledHead, rawHead uint64)  // empty settled page with unsettled backlog
}
```

- The stream's empty-page branch consults `RawHead`, rate-limited to at most one probe per stream per few seconds, and reports `OnStall(settled, raw)` when `raw > pos` — the §4 stall made visible without adding per-poll cost at fleet scale (§5.12).
- `feed.Log` gains `MatchingHead(ctx, types) (uint64, error)`; `Server.Status(ctx, req Request) (Status, error)` replaces the type-blind variant, and `Lag` becomes `matchingHead − position` for the subscription's actual type set. The old `Status` signature is removed (pre-1.0), and its duplicate checkpoint read with it (review R15).

### 5.9 Validation and small API fixes folded in

- `validateKey`/`validateRequest` reject NUL and control bytes in scope/projection/instance, validate `instance` (previously unchecked), and require `Request.Projection == Request.Definition.Name` (a mismatch silently cross-contaminates checkpoints; review D-2).
- `Config` zero/negative semantics unified and documented: negative `MaxPayloadBytes` disables the backstop; negative `MaxBatchBytes` means "default", never silently "unlimited".
- `Subscribe`'s error join drops `context.DeadlineExceeded` when the caller's ctx expired (duplication; review A-7).

### 5.10 Tests, CI, and the chaos gate

**5.10.1 Deterministic correctness tests (per-PR CI, both backends where applicable):**

1. **Out-of-order commit, same-order insert** (two append transactions, higher gp commits first) — `ReadAllBounded` returns nothing until the lower commits, then both in order; the cursor never passes the gap. Repeated with roles swapped and with the lower transaction aborting (a hole, no stall).
2. **Xid/position inversion** (the case that killed approach A1): T1 writes early (old xid), appends late at `gp=101`, commits; T2 (newer xid) appends early at `gp=100`, commits later. The feed must deliver `100` then `101`, never `101` alone.
3. **Stale ack:** A holds, lease expires, B takes over; A's ack is rejected; B's cursor is unmoved.
4. **Concurrent SQLite writers:** N goroutines × M appends; positions contiguous; an interleaved reader never observes gp N+1 before N.
5. **Poison recovery:** over-cap append rejected; one forced in via direct insert; stream dies with `*EventTooLargeError`; `Skip` records the dead letter; reconnect resumes past it.
6. **Definition drift:** subscribe `{A}`, checkpoint, resubscribe `{A,B}` → `ErrDefinitionChanged`; after `ResetCheckpoint`, replay includes B's history.
7. **Stall detection:** an in-flight append holds the ceiling; the observer reports `OnStall`; after commit, delivery resumes.
8. **Skip while held:** `Skip` refuses a live lease without force; with force it advances and records.

**5.10.2 The heavy gate — `task test:feedgate`** (manual + nightly, not per-PR; builder question 7, resolved): the full chaos matrix — 8 concurrent appenders whose INSERT and COMMIT are **separated by randomized 0–500ms delays** (forcing assignment order to diverge from commit order), ≥100k events, 3 consumers with kills injected at the critical windows: after send/before ack, after ack, mid-lease, and across a store close/reopen. The oracle is **set equality, not counts**: each consumer's applied-position set must equal the source's positions filtered by that consumer's type set — any difference is loss or invention; duplicates are tolerated only within one ack window (§5.6) and must be deduplicated by the marker. Per-stream hash-chain continuity (ADR 0029) is the internal order oracle: every delivered stream must arrive version-contiguous and hash-linked. The gate also runs a steady-state scenario with ≥100 idle subscribers asserting the resource envelope (§5.12), the EXPLAIN assertion on a ≥1M-row seeded fixture, and the full matrix under `-race`. Phase 1's exit criterion runs this once and records the output in the PR. Per-PR CI keeps reduced deterministic versions of 5.10.1 plus `task test:pg -race -shuffle=on`; a `coverage` task gets a floor at the current level; `golangci-lint` (errcheck, govet, staticcheck) becomes a CI job.

### 5.11 Docs and ADRs

- **ADR 0037 — Commit-safe feed reads on Postgres**: the settled-ceiling mechanism and its losslessness argument (§5.1.3), the rejected A1/A2 with the interleavings that killed them, stall semantics, the fallback gate, the index build ordering, and the RLS role rollout.
- ADR 0001 §5 cross-references 0037; ADR 0010 records the non-`CONCURRENTLY` caveat and the runbook-first index order; ADR 0036 and the README delivery section state the settled-head semantics and a guarantees block.
- Runbook (`docs/deploy/README.md`): poison/Skip, the stall alert and "analytics go to a replica", lease RTO (≤ `LeaseTTL`), `es_relay` creation and grant ordering, index build ordering.

### 5.12 Scalability envelope and resource model

**The unit of scale is the subscription, and subscriptions are independent.** No cross-subscriber coordination exists anywhere in the feed: each stream owns one lease, one checkpoint row, one `Log` instance with two integers of ceiling state, and two goroutines (stream + renewer). Placement *is* the lease: run k feed processes and each subscription is claimed by exactly one — horizontal scaling needs no sharding logic beyond "more processes".

Per-subscriber steady-state cost on Postgres (measured shape from the perf review; re-measured in Phase 1):

| Metric | Per subscriber (idle) | 100 subscribers |
|---|---|---|
| Polls | 4/s (250ms) | 400/s |
| Round trips | ~16–20/s | ~1,600–2,000/s |
| Postgres CPU | µs-scale index probe per poll | < 5% of one core |
| Network | < 2 KB/s | < 200 KB/s |
| Memory | 2 goroutines + ≤16 MiB transient page | trivial |
| Acks | coalesced per page (§5.6) | ≈ consumed events / page |

Three changes make this envelope honest rather than accidental:

1. **Idle poll jitter (±20%)** in `feed.Server.wait` — without it, 100 streams started together poll in lockstep and micro-burst the pool.
2. **Pool sizing guidance** in the runbook — pgx's default `max(4, NumCPU)` starves ~50+ concurrent streams; a feed process runs an explicitly sized pool (≥ 2× expected concurrent streams, bounded by Postgres `max_connections` across all feed processes).
3. **Wait floor enforcement** in the stream loop — a `Waiter` returning immediately must not turn the loop into a spin (perf finding); the server sleeps the remainder of the poll interval.

**SQLite is not in this envelope.** One connection serializes appends and every read; a catch-up replay blocks appends for the scan duration, worsening with log size. SQLite stays positioned as single-node/embedded/tests, as documented; this design changes nothing there.

**Not needed at 100, noted for ~1000:** the per-poll `(snapshot, last_value)` round could be shared per process per workspace (one read fanning out to that workspace's streams), and a LISTEN/NOTIFY `Waiter` (one listener connection per process) would collapse idle polling and cut delivery latency from ~250ms to single-digit ms — but it requires an append-time `NOTIFY`, a write-path cost this design's anti-goal forbids, so it stays a post-gate, separately-ADR'd decision.

## 6. Phasing and sizing

| Phase | Contents | Exit criterion | Est. |
|---|---|---|---|
| **0. Spike** | §5.1.4 on PG 17 | findings recorded; fallback decided if falsified | 1 d |
| **1. Gate 0** | Failing chaos + inversion tests → §5.1 horizon fence + §5.2 indexes → §5.3 append cap | `task test:feedgate` green once, recorded; EXPLAIN gate green; append benchmark shows no write-path change | 4–6 d |
| **2. Protocol** | §5.4 fencing + port reshaping, §5.5 fingerprint, §5.3 Skip/dead letters | Stale-ack, drift, skip-while-held tests green | 3–4 d |
| **3. Adapters & ops** | §5.6 adapters + `eventfeed` + conformance, §5.7 RLS role, §5.8 observer, §5.12 jitter/wait floor/pool guidance, §5.10 CI, §5.11 docs | Postgres adapters pass `feedtest`; stall observable; ADR merged | 6–7 d |

Total ≈ 14–18 engineer-days for one senior Go/Postgres engineer. Phases 1 and 2 are independently shippable; Phase 3 depends on both. There is no append benchmark today; creating the baseline is part of Phase 1 (routine, not a risk).

## 7. What is left to the builder

- The exact statement shape that reads `(snap, L)` — one query, a CTE, or two statements — pinned only by "one snapshot per round" and "the horizon check and the row read share a snapshot".
- Whether `RawHead` is a separate method or a field on the round's metadata — internal to the adapters.
- The `feedtest` fixture API shape (a `feedtest.Fixture` struct is suggested, not required).
- The `eventfeed` reconnect token policy: fresh UUID per attempt vs stable token for self-stealing reconnects — both safe (§5.4), the builder picks and documents.
- Migration numbering conflicts if other work lands first (next free: Postgres 0003, SQLite 0004).
- Test names, per repo convention.
- Anything the spike falsifies — that goes back to the human, not the builder.

## 8. Review focus (failure modes this design must not reintroduce)

1. **Delivering a position while a lower position is still in flight** — including the xid/position inversion that killed approach A1 — pinned by tests 5.10.1.1 and .2, which must interleave *multi-write* transactions, not append-only ones.
2. **A stale ack that slips through** a check-then-save race — pinned by the stale-ack test asserting the cursor is unmoved, not merely that an error returns.
3. **A two-step page that silently skips an over-cap row** instead of stopping before it — pinned by the poison test's "resume re-hits it as first row" step.
4. **`Skip` as an unfenced admin hammer, or a skip that clears the definition identity** — pinned by the skip-while-held test and a fingerprint-preservation assertion.
5. **The new RLS tables missing their workspace policy, or carrying a bypass arm** — pinned by the conformance suite's cross-workspace refusal.
6. **A relay that silently reads nothing after the RLS migration** — pinned by the startup role verification failing fast.
7. **A subscriber fleet in lockstep exhausting the connection pool** — pinned by the gate's ≥100-idle-subscriber scenario against the guided pool (§5.10.2, §5.12).
