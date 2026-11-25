# ADR 0045: Read-Your-Writes Tokens and Applied Projection Checkpoints

- **Status:** Accepted (implemented)
- **Date:** 2026-11-25
- **Related:** ADR 0004 (workspaces), ADR 0005 (projection rebuild),
  ADR 0010 (schema migrations), ADR 0025 (client-owned crypto),
  ADR 0030 (projector codegen), ADR 0035 (caller-owned transactions),
  ADR 0036 (no broker), ADR 0037 (commit-safe feed reads),
  ADR 0039 (scoped feed wake-ups; its `NotifyListener` shape is reused),
  the [read-your-writes guide](../guides/read-your-writes.md) and
  [proposal](../proposals/es-lite-read-your-writes.md)

## 1. Context

Projections are asynchronous. A command commits and returns; the projection
that maintains the read model applies the same event milliseconds later. A
client that immediately lists the resource can therefore miss its own write —
the read model has not applied the position the append returned.

The write side already knows where its event landed: `AppendResult.Position`
is the highest `global_position` the append committed. The read side needs a
bounded way to say **"do not answer from this projection until it has applied
at least that position"**, without making projections synchronous and without
holding a database connection while waiting.

The requirements:

- a successful append exposes its position in a form a client can store and
  return — small, opaque, versioned, and safe in a header;
- a query can wait, with a deadline, on one named projection in one workspace;
- a timeout is stale data, not an error, and a failing checkpoint store can be
  made non-fatal to the query path;
- the wait works across processes (a projection process and a separate query
  service) and on both backends;
- waiting must not make the append path slower or a checkpoint hint able to
  fail a write;
- a rebuild must never be reported as "reached" over a half-rebuilt read
  model;
- no broker: ADR 0036 removed NATS/JetStream, so the cross-process hint must
  ride Postgres `LISTEN`/`NOTIFY` or nothing.

## 2. Decision

### 2.1 The token is a scope and a position

```go
type Token struct {
    Scope    string // the workspace; "" on SQLite
    Position uint64 // highest global_position the command committed
}
```

The wire form is one version byte (`1`), a big-endian `u16` scope length, the
scope bytes, then a big-endian `u64` position, base64url-encoded without
padding. `""` is the zero token. `ParseToken` rejects an unknown version, a
truncated body, a length that does not match, and a scope over 512 bytes (the
same bound the feed uses for a workspace id).

`AppendResult.Position` is the highest global position in the append; it is
`0` for an append with no events. `aggregate.Result.Token` carries the token —
`TokenFor(scope, results...)` takes the highest position across several
appends — so a multi-event command yields one token covering the whole
command: a reader never observes half of it. A highest position of 0 yields
the zero token, because there is nothing to wait for. `MaxToken(a, b)` returns
the further-along token and is `ErrScopeMismatch` for two different non-empty
scopes, which is the client-side bug worth catching.

Scopes are limited to 512 bytes: `Token.String` still encodes a longer scope,
but `ParseToken` rejects it, so `TokenFor` with an oversized scope produces an
unusable token. Keep the scope (the workspace id) within that bound.

The token is a **position hint, not an authorization claim**. It carries no
rights and no data; the scope a query waits in comes from the authenticated
workspace, never from the token. A token for another scope is rejected rather
than followed.

### 2.2 The applied checkpoint is not the delivery cursor

`feed_checkpoints` records a consumer's **delivery ack** for
`(scope, projection, instance)`, written by `feed.Server` after a handler
returns. The read model's progress is a different fact: a consumer can be
acknowledged ahead of its handler's read-model write, and a rebuild restarts
the read model without restarting delivery. Reusing the delivery cursor would
make a wait trust an ack that does not describe the read model.

The applied checkpoint therefore gets its own table,
`projection_checkpoints`, one row per `(scope, projection)`:

```sql
CREATE TABLE projection_checkpoints (
    workspace_id text        NOT NULL,
    projection   text        NOT NULL,
    position     bigint      NOT NULL DEFAULT 0,
    state        text        NOT NULL DEFAULT 'live'
        CHECK (state IN ('live','rebuilding','stalled')),
    updated_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, projection)
);
```

- **Grain:** one row per projection and workspace. `position` is the highest
  global position the read model has applied (or deliberately filtered).
- **States:** `live`, `rebuilding`, `stalled`. `StatusUnknown` means "no row"
  and is never persisted; the text form is validated on write and read.
- **Monotonic within a state:** a save at or below the stored position in the
  same state leaves the position alone; a state change replaces it, because a
  rebuild legitimately restarts at zero. `updated_at` refreshes on every
  successful save; readers compare position and state, never the timestamp.
- **RLS mirrors `event_tags`:** `ENABLE` + `FORCE`, the workspace arm plus the
  role-gated `app.bypass_rls` + `es_relay` arm. No grants in the migration;
  deployment issues them (runbook).
- **Unpartitioned and small:** one row per projection per workspace, not per
  event or per subscription.

Postgres migration `0017`; SQLite migration `0015` uses `scope` instead of
`workspace_id`, no RLS, and a text `updated_at` in the adapter's fixed-width
layout. SQLite is one database per workspace, so the application supplies the
empty scope on both sides.

### 2.3 The checkpoint is write-through, not same-transaction

`projection.Runner` dispatches a read-model write and then persists the
checkpoint as two calls. There is no transaction seam through `Dispatch`, and
`OnApplied` is documented best-effort; requiring a same-transaction checkpoint
would change the handler contract for every projection and could not be
enforced by the library.

The guarantee is therefore:

> `SaveProgress` returns only after the read-model write it describes has
> committed. If the process dies between those two steps, the event is
> redelivered (handlers are idempotent) and the checkpoint never advances
> early.

Specifics that make this hold:

- **One save per `Apply` call, one per event on the live path.** `Apply` saves
  once at the last successfully applied position of its batch, not once per
  envelope, and a failed dispatch saves the applied prefix once before
  returning the error. `Run`/`Feed` deliver one envelope at a time, so the live
  path makes one save per event; `Rebuild` saves once per page. Hints are
  coalesced per scope, so the hint rate does not follow the save rate.
- **Publish after the save.** The in-process `Progress` publisher is called
  only after `SaveProgress` returns, so a waiter cannot see a position the
  store does not hold. A `BufferedMarker` is not a `ProgressStore`; progress
  stores are write-through by contract.
- **`Reset` publishes what the store returned.** `Runner.Reset` replaces the
  row with position 0 and state `rebuilding` before the application clears or
  swaps the read model, then publishes that returned checkpoint to in-process
  waiters. Publishing the state alone would leave a warm cache at the old
  high-water mark, so a rebuild below it could be reported as reached. The
  legacy `Marker` path, which has no checkpoint row, publishes the status
  change only.
- **`Rebuild` goes live last, fail-closed.** The replay runs entirely in
  `rebuilding`; the row is set `live` only after the store's `SetStatus`
  succeeds, and a failed status write leaves both row and runner
  `rebuilding`. A later advance cannot write `live` over a half-rebuilt model.
- **A restart seeds the state.** `loadMarker` seeds the runner's lifecycle
  state from a found checkpoint, so a process that crashes during a rebuild
  restarts `rebuilding` rather than flipping `live` on its first save. The
  projection stays fail-closed until an operator completes or re-runs the
  rebuild.

### 2.4 `WaitFor` is bounded, per scope, and fail-open by choice

```go
type WaitResult struct {
    Reached    bool
    Checkpoint Checkpoint
    Waited     time.Duration
    Status     Status
}

wr, err := waiter.WaitFor(ctx, scope, "users", tok, 500*time.Millisecond)
```

`Reached == true` means: for this scope, projection `users` has applied every
event at or below `tok.Position`, or deliberately filtered it. That is all it
means — no cross-projection or cross-workspace claim.

The algorithm and its bounds:

1. A zero token returns `Reached: true` immediately, before any store access,
   so a client that sends no token never touches the checkpoint store. Once
   the scope check passes, a token with `Position: 0` is likewise reached
   immediately.
2. A token scope over 512 bytes is `ErrInvalidToken`; a token scope that
   differs from the caller's scope is `ErrScopeMismatch`; both fail even under
   best effort.
3. On a cold key the waiter reads `projection_checkpoints` once, bounded by the
   wait deadline rather than only the caller's context. A read that expires
   with the wait deadline is a timeout result, not a store error, even without
   `WithBestEffort`. A missing row is position 0: it waits, because "never ran"
   is not "caught up".
4. The **status gate comes before the position gate**. A `rebuilding` or
   `stalled` checkpoint returns `Reached:false` with that status even when its
   stale position is at or above the token.
5. Only a token the cache did not satisfy reaches the waiter caps (`1024` per
   key, `16384` total by default). Above either cap, the result is
   `Reached:false` with a nil error — shed load rather than queue an unbounded
   number of blocked requests. A cached reached token still returns reached
   under load.
6. The waiter registers for the key, re-checks (an advance may have landed in
   between), then blocks on a Go channel, its context, and the deadline. It
   holds no connection, transaction or lock while blocked.
7. When the deadline fires, the waiter performs one last bounded store read
   (at most `min(200 ms, timeout)`), installs a found checkpoint and decides
   against it. A hint lost for the whole wait therefore costs latency, not a
   false timeout.
8. The default deadline is 500 ms; values above 5 s are clamped to 5 s. A
   timeout is `Reached:false` with a nil error, carrying the last checkpoint
   and `Waited`.

`WithBestEffort` converts store errors into `Reached:false, err:nil` so a query
endpoint can serve stale data. It never swallows a bad token, a scope mismatch
or context cancellation. `WithFallback` changes the refresh interval (100 ms
default, jittered ±20%) and `WithMaxWaiters` the caps.

### 2.5 Hints are never data

The Postgres progress store emits an opt-in hint after a checkpoint mutation
commits. Three properties are load-bearing:

- **Post-commit and best-effort.** `NewProgressStore(store, channel)` owns a
  bounded notifier; `SaveProgress`, `ResetProgress` and `SetStatus` mark their
  scope only after the transaction commits. The notifier coalesces scopes and
  flushes on a ~10 ms window (early when its batch fills) with
  `pg_notify(channel, scope)` on the pool. A failed flush is counted
  (`NotifyDropped`) and dropped; a rolled-back save never marks; a same-state
  save at or below the stored position does not move the row and marks
  nothing; an oversized scope (over 512 bytes) skips the hint instead of
  failing the save. **A hint can never fail or roll back a checkpoint write.**
- **The payload is the scope alone.** There is no position and no state in
  the notification. A forged or replayed hint can cause a store read; it
  cannot raise a cached position or produce a false `reached`.
- **The listener only enqueues.** `postgres.NewProgressListener` wraps the
  ADR 0039 `NotifyListener`: `OnNotify(scope)` calls
  `EnqueueScopeRefresh(scope)` and a reconnect calls `EnqueueRefreshAll()`.
  Neither runs a store read on the listener goroutine. A single refresh worker
  in the waiter reads the authoritative checkpoint only for keys with **local**
  waiters; a hint for a scope where nothing is waiting performs no read. An
  unchanged checkpoint is not broadcast.

Because the payload is stable per scope, the listener's existing 10 ms
per-scope throttle coalesces a burst; because the hint is a wake, not a
delivery, a lost or late hint costs at most one fallback tick. The 100 ms
fallback read satisfies every wait even with hints disabled.

An empty channel disables hints completely: no notifier goroutine, no
statement, and `NotifyDropped()` reports `0`. The fallback path is unchanged.
SQLite is in-process: the runner's `Progress` publisher wakes the same
process's waiter directly, and there is no cross-process hint.

### 2.6 Rebuilds: status plus blue/green, not a token epoch

A token cannot carry a projection epoch. One append feeds many projections,
and a projection can be rebuilt without the event store knowing. Safety comes
from the checkpoint state and a deployment contract instead:

1. `Reset` sets `rebuilding` **before** the read model is cleared or swapped.
2. Clear/replay, or build the new read-model version alongside the old.
3. `Rebuild` sets `live` only after the replay succeeds and the status write
   commits.

While `rebuilding`, `WaitFor` returns immediately with `Reached:false` and
`StatusRebuilding`; it never pretends the old checkpoint is meaningful.
Serving reads during a rebuild therefore requires **blue/green** (route to the
previous version until the new one reaches the token) or **quiesced reads**. A
token outstanding across a non-blue/green rebuild is stale and is treated as
such by the client. A crashed rebuild leaves `rebuilding` set — fail-closed
until an operator completes or re-runs it.

`StatusStalled` has no producer in the library: the runner writes only `live`
and `rebuilding`. To mark a projection stalled, stop its runner, write
`state='stalled'` (or `live` to clear) with the position unchanged, then
restart. `loadMarker` seeds the state from the row, so the restart stays
fail-closed; a still-running runner would re-persist its in-memory state on the
next save, which is why it must be stopped first.

## 3. Consequences

### 3.1 Write and read cost

- **One checkpoint write per `Apply` call**, after the read-model write. The
  live `Run`/`Feed` path calls `Apply` with one envelope at a time, so it pays
  one small upsert **per event**; an explicit multi-event `Apply` batch and
  each `Rebuild` page pay one each. There is no extra append-path statement:
  the checkpoint belongs to the projection, not the append.
- **The query path pays one primary-key read** on a cold key, then none while a
  cached checkpoint is warm. A blocked wait holds no connection; 100 waiters on
  one key share one store read per wake or fallback tick, and each waiter does
  one bounded read at its own deadline. Tokenless requests pay nothing.
- **Hints add one `pg_notify` per coalesced window per scope**, emitted after
  the checkpoint commit. They are coalesced per scope, so the hint volume does
  not scale with the per-event save rate, and a lower same-state save emits
  nothing. They are optional; the fallback tick and the deadline read are the
  safety net.
- **`LogFeed` is the single-writer path.** Over Postgres it pages raw `ReadAll`
  without a settlement fence. Run it with a single writer or a quiescent log;
  with concurrent appenders use `eventfeed`, which owns the fenced delivery
  cursor.

### 3.2 Grants and placement

The projection runner needs `SELECT, INSERT, UPDATE` on
`projection_checkpoints`; the query/waiter service needs `SELECT`. **No
`DELETE` on the normal path** — rows are replaced in place, and a rebuild
resets rather than deletes. A store-wide projection uses the scope `"*"` as
its row key and the same grants.

Placement is a deployment decision: co-locate the table with the read model
(so the query service uses one pool for both) or grant the query service
`SELECT` on the event database (a second pool). A wait never reads the read
model, so it cannot compensate for querying a lagging replica: the caller must
read the same copy the projection writes.

### 3.3 Hints have the C2 costs and preconditions

The progress hint reuses the ADR 0039 C2 listener shape, so it inherits its
operating costs:

- **One dedicated `LISTEN` connection per channel per listening process**
  (`processes_that_listen × channels`). It is never pooled; PgBouncer
  *transaction* pooling breaks `LISTEN`, so use a direct or session-pooled
  connection. A process that also runs a feed C2 listener on another channel
  holds two backends.
- **NOTIFY is not RLS-filtered.** Notifications are visible to every role that
  can connect, and any such role can spoof one. The precondition is a
  dedicated database or a fully trusted set of connect-capable roles — the same
  precondition as ADR 0039 C2, not a per-table grant.
- **Queue exhaustion.** The async notification queue is bounded. When it is
  exhausted, **NOTIFY-ing commits can fail** — notably a transactional append
  hint under ADR 0039 C2. The progress hint is post-commit, so exhaustion
  costs a dropped hint, never the checkpoint write; the fallback read still
  satisfies waits. Alert on `pg_notification_queue_usage()` and disable
  progress hints first if an append path also uses C2.
- **No two-phase commit.** A transaction that executed `NOTIFY` cannot be
  prepared for 2PC. The progress hint runs outside the save transaction, but an
  append path using C2's transactional hint must not flow through a 2PC
  preparer.
- **A slow or dead listener costs delay, not correctness.** Reconnect backoff
  and `OnReconnected → EnqueueRefreshAll` repair missed hints; the fallback tick
  bounds the rest.

### 3.4 Rebuild contract and backups

A rebuild is an operational sequence, not a single library call: set
`rebuilding`, clear/swap the read model, replay, set `live` (runbook). Reads
are quiesced or blue/green for its duration.

`projection_checkpoints` is **derived progress, not state**. A restore resets
and replays it; a checkpoint row left ahead of a restored read model would make
waiters claim freshness the read model does not have. It is not part of the
backup set and is safe to exclude or truncate on restore.

### 3.5 Additive, and no API break

The change is additive: a new `es.Token` type and two `Result` fields, a
`ProgressStore`/`ProgressPublisher`/`Waiter` seam in `projection`, one optional
runner field pair, and Postgres/SQLite adapters plus migrations. A runner
without a `ProgressStore` keeps the legacy `Marker` path and pays nothing. A
store that never enables the channel emits no hints. No existing stored event,
hash, anchor or checkpoint changes shape.

## 4. Alternatives considered

- **A broker (NATS/JetStream or equivalent).** Rejected by ADR 0036. It would
  add an operational dependency for a latency hint whose loss already costs at
  most one fallback tick. The listener seam is small enough that an application
  with its own bus can deliver the same wake; the library does not need one.
- **Write the checkpoint in the same transaction as the read model.** The
  strongest guarantee, but `Dispatch` has no transaction seam and a projection
  handler may write any store. Enforcing it would change the generated handler
  contract and could not be verified by the library. Write-through after the
  commit is the strongest guarantee that is enforceable, and a crash window
  only causes idempotent redelivery.
- **Put an epoch in the token.** One append feeds many projections and a
  projection can be rebuilt without the log changing, so there is no stable
  epoch to carry. The checkpoint state plus blue/green or quiesced reads is the
  deployment contract; a future epoch can be added behind the token version
  byte if blue/green proves unacceptable.
- **Reuse `feed_checkpoints` as the applied checkpoint.** The delivery ack has
  a different writer, a different grain (per instance) and different failure
  semantics; a consumer can ack ahead of the read-model write. Sharing it would
  make "reached" mean "delivered to a handler", which is exactly the stale read
  this ADR removes.
- **Make projections synchronous.** Rejected as the default: it couples every
  write to every read model, turns a read-model outage into a write outage, and
  changes the handler contract. Read-your-writes is a bounded wait on the
  existing async path, not a new consistency model for all reads.
- **A token carrying the projection name or a read-model id.** The token is one
  value reused across every projection the response depends on; the projection
  name belongs in the `WaitFor` call, not the token. This keeps one token per
  workspace and lets `MaxToken` work.
- **Poll-only, no hints.** Viable and always available — it is the fallback —
  but it spends a store read per active key per 100 ms at idle. Hints make the
  common case one read per advance without weakening the fallback.

## 5. Rollout

- **Migrations:** Postgres `0017_projection_checkpoints.sql` (table, check,
  RLS; no grants); SQLite `0015_projection_checkpoints.sql` (scope-keyed table,
  no RLS). The table starts empty and is catalog-only: no backfill, no
  concurrent index build.
- **Runtime:** `es.Token`/`ParseToken`/`TokenFor`/`MaxToken`;
  `AppendResult.Position`; `aggregate.Result.Position`/`Token`;
  `projection.Checkpoint`/`Status`/`ProgressStore`/`ProgressPublisher`/
  `Waiter`; `projection.Runner.ProgressStore`/`Progress`/`Scope`.
- **Adapters:** `postgres.NewProgressStore(store, channel)` and
  `postgres.NewProgressListener(dsn, channel, refresher)` (channel `""`
  disables hints); `sqlite.NewProgressStore(store)` (in-process only).
- **Grants and operations:** the runbook adds the two grants, the placement
  contract, the C2 trust/channel/queue checks, the connection budget, the
  listener and drop metrics, the rebuild order and the backup rule.
- **Example:** [`examples/readwrite`](../../examples/readwrite) is a runnable
  SQLite command→token→wait→read flow; the
  [guide](../guides/read-your-writes.md) is the integration recipe.
- **Release note (v0.24.0):** read-your-writes consistency tokens,
  `projection.Waiter` and the applied `projection_checkpoints` table; Postgres
  migration 0017 / SQLite 0015. Additive public API; no break. Release notes
  are generated from commit subjects by `.github/workflows/release.yml`; there
  is no `CHANGELOG.md` to edit.
- **No default-path change:** hints are opt-in, a runner without a
  `ProgressStore` keeps the legacy marker, and a query without a token returns
  immediately.

## 6. References

- [Read-your-writes guide](../guides/read-your-writes.md) — the four-step
  integration, failure table and sizing.
- [Proposal](../proposals/es-lite-read-your-writes.md) — the design record
  this ADR implements.
- [Deployment runbook](../deploy/README.md#projection-checkpoints-and-read-your-writes-migration-0017)
  — grants, placement, channel, connection budget, health, rebuild and backup.
- [Example](../../examples/readwrite) — the runnable SQLite flow.
- ADR 0039 — the `NotifyListener`, per-scope throttle and C2 trust
  precondition reused here.
- ADR 0005 — `Reset`/`Rebuild`; ADR 0037 — the commit-safe delivery cursor
  that this table is deliberately not.
