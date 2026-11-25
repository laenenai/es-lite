# es-lite — Read-Your-Writes (WaitFor) — Revised Proposal

- **Status:** Proposal (revised; supersedes the 2026-11-24 draft)
- **Date:** 2026-11-25
- **Related:** ADR 0004 (workspaces), ADR 0005 (projection rebuild), ADR 0029
  (the chain), ADR 0036 (Postgres/SQLite only, no broker), ADR 0037
  (commit-safe feed reads), ADR 0039 (scoped wake-ups)

## 1. Problem

Projections are asynchronous. After `Handle` returns, the caller's own events
may not have reached the read model, so a list query can flicker, miss a row or
show a stale value. The write side knows the position it committed; the read
side needs a bounded way to say "do not answer from this projection until it
has applied at least position P."

## 2. Goals and non-goals

Goals:

- Every successful append exposes its highest `global_position`; a token is a
  `(scope, position)` value that can be returned to a client and passed back.
- A query can wait, with a deadline, until a named projection's **applied
  checkpoint** is at least the token, for one scope (a workspace).
- Waiting is notification-driven in process and across processes, with a
  bounded database-read fallback. Timeouts return `Reached: false` and stale
  data, never an error and never a hang.
- Works on Postgres and SQLite, embedded and in a separate projection process.

Non-goals:

- Global linearizability or cross-projection atomicity.
- Cross-region consistency.
- Making projections synchronous. This is the async default with a wait.
- A broker. ADR 0036 removed NATS/JetStream; there is no `task test:nats`.

## 3. Decisions settled by this revision

### 3.1 No broker; Postgres `LISTEN/NOTIFY` or an application notifier

In-process waits use a hub. Cross-process waits use Postgres `LISTEN/NOTIFY`
on a checkpoint channel, built on the existing `postgres.NotifyListener`
shape. If the read model lives outside Postgres, the application supplies a
`Notifier`; the fallback still works without one. SQLite is in-process only.

A hint must never fail a write and must never make a read lie. The progress
hint is therefore emitted **after** the checkpoint transaction commits, from a
bounded notifier that dedupes scopes and flushes at most every 10 ms; a failed
flush is counted and dropped, never propagated to the save. The payload is the
**scope only** — no position and no state — so the listener cannot be tricked
into raising a cached position; a wake turns into a store read for the keys
that have local waiters. The listener's 10 ms per-scope throttle now coalesces
a burst, because the payload is stable per scope. Waiting never holds a
database connection: the worker reads the checkpoint, releases it, and waiters
block on Go channels. One process needs one `LISTEN` connection, not one per
request.

### 3.2 The applied checkpoint is not the delivery cursor

`feed_checkpoints` records a **consumer's delivery ack** for
`(scope, projection, instance)` and is written by `feed.Server.Checkpoint`.
`projection.Marker` records what the **read model has applied**. They can
diverge: a consumer can ack a page after its handler returns, while a runner's
marker is a separate write. `WaitFor` is about the read model, so it reads the
applied checkpoint, not the delivery ack and not `Drain`.

The applied checkpoint gets its own table, `projection_checkpoints`, with
`(scope, projection) → (position, state, updated_at)`. It is not the dropped
post-0008 `checkpoints` table and not a feed cursor. This is deliberate: one
row per projection and scope, one meaning.

### 3.3 A write-through marker, not the same transaction

`projection.Runner.apply` dispatches the handler and then saves the marker as
two calls. There is no transaction passed through `Dispatch`, and `OnApplied`
is documented as best-effort and may precede durability with a
`BufferedMarker`. Requiring the checkpoint to commit in the same transaction as
an arbitrary read-model write would change the handler contract and cannot be
enforced by the library.

The guarantee is therefore:

> `SaveProgress` returns only after the handler's read-model write has
> committed. If the process dies between those two steps, the event is
> redelivered (idempotent) and the checkpoint never advances early.

That is sufficient for read-your-writes and keeps the current `Runner` shape.
A marker that buffers is not a `ProgressStore`; `ProgressStore` implementations
are write-through.

### 3.4 Scope and position

Postgres `global_position` is store-global. A workspace-scoped projection's
checkpoint advances over events from other workspaces; that is fine, because
the guarantee is scoped:

> For scope S and projection P, checkpoint ≥ X means every event with
> `global_position ≤ X` **in S** has been handled or deliberately filtered by
> P.

A store-wide projection uses a separate scope (for example `"*"`) and a
store-wide checkpoint; on Postgres that row is outside the standard workspace
RLS arm, so the runner needs the role-gated bypass (or a dedicated policy).
A token from one scope is never compared with another scope's checkpoint.
`WaitFor` takes the authenticated scope explicitly and rejects a token whose
scope differs (`ErrScopeMismatch`). SQLite is
single-workspace and its envelopes carry an empty workspace, so its scope is
the empty string; the caller must pass the same empty scope. A workspace token
never matches the empty scope (and vice versa).

### 3.5 Rebuild safety is status plus blue/green, not a token epoch

A token cannot carry a projection epoch: one append feeds many projections, and
a projection can be rebuilt without the event store knowing. Instead:

- `Reset`/`Rebuild` sets the checkpoint state to `rebuilding` **before** the
  read model is cleared or replaced.
- `WaitFor` returns immediately with `Reached: false` and
  `Status == rebuilding` while that state is set; it does not pretend the old
  checkpoint is still meaningful.
- A rebuild that must serve reads uses **blue/green**: route the query to the
  previous read-model version until the new projection reaches the token. A
  non-blue/green rebuild must quiesce reads; a token outstanding across it is
  treated as stale by the client.
- A crashed rebuild leaves `rebuilding` set, which is fail-closed; the operator
  clears it after the rebuild is complete.

This is a deployment contract, documented, not a field in the token. A future
epoch can be added if blue/green is not acceptable.

### 3.6 Token encoding

The token is opaque **[version u8][scope length u16][scope][position u64]**,
base64url-encoded. It is a small `es` type, not a protobuf message in `es.v1`;
a wire proto would make the internal encoding a public schema commitment. The
version byte allows a future change.

## 4. Public API

```go
// es
type Token struct {
    Scope    string
    Position uint64
}
func TokenFor(scope string, results ...AppendResult) Token // highest position; scope from the caller
func (t Token) String() string
func ParseToken(s string) (Token, error) // "" -> zero token
func MaxToken(a, b Token) (Token, error) // ErrScopeMismatch if scopes differ

// AppendResult gains:
Position uint64 // highest global_position in Envelopes; 0 for no events

// aggregate.Result gains:
Position uint64
Token    es.Token
```

`Handle`/`HandleAt` set `Position`/`Token` from the append. A zero-event no-op
reads the stream's last envelope position (one bounded read; best-effort: a
read error yields a zero token, not a failed command). An empty stream yields a
zero token.

```go
// projection
type Status uint8
const (
    StatusUnknown Status = iota
    StatusLive
    StatusRebuilding
    StatusStalled
)

type Checkpoint struct {
    Scope, Projection string
    Position          uint64
    State             Status
    UpdatedAt         time.Time
}

type ProgressStore interface {
    LoadProgress(ctx context.Context, scope, projection string) (Checkpoint, bool, error)
    // SaveProgress is write-through: callers invoke it after the read-model
    // write commits. It must not lower a position within one state.
    SaveProgress(ctx context.Context, scope, projection string, cp Checkpoint) error
    // ResetProgress zeroes the position and sets state (rebuilding), for a rebuild.
    ResetProgress(ctx context.Context, scope, projection string, state Status) (Checkpoint, error)
    // SetStatus changes state without moving the position.
    SetStatus(ctx context.Context, scope, projection string, state Status) error
}

type ProgressPublisher interface {
    Advance(cp Checkpoint)
    SetStatus(scope, projection string, state Status)
}

type Waiter struct{ /* hub, source, options */ }

type WaiterOption func(*waiterOptions)
func WithFallback(d time.Duration) WaiterOption     // refresh interval; default 100ms, jittered
// WithBestEffort turns store errors into Reached:false (stale data) instead of
// an error, so a query endpoint can fail open. Token and scope errors still
// fail, as does context cancellation.
func WithBestEffort() WaiterOption
// WithMaxWaiters bounds the registry; above the cap WaitFor fails open.
func WithMaxWaiters(perKey, total int) WaiterOption

func NewWaiter(store ProgressStore, opts ...WaiterOption) *Waiter
func (w *Waiter) EnqueueScopeRefresh(scope string) // non-blocking hint from the listener
func (w *Waiter) EnqueueRefreshAll()               // listener reconnect repair
func (w *Waiter) Close()                           // stop the refresh worker
func (w *Waiter) WaitFor(ctx context.Context, scope, projection string, tok es.Token, timeout time.Duration) (WaitResult, error)
func (w *Waiter) Checkpoint(ctx context.Context, scope, projection string) (Checkpoint, bool, error)
func (w *Waiter) Status(ctx context.Context, scope, projection string) (Status, error)
func (w *Waiter) Advance(cp Checkpoint)                    // ProgressPublisher
func (w *Waiter) SetStatus(scope, projection string, state Status)

type WaitResult struct {
    Reached    bool
    Checkpoint Checkpoint
    Waited     time.Duration
    Status     Status
}
```

Errors are reserved for: malformed token, scope mismatch, context cancellation,
and store errors. A timeout is `Reached: false, err == nil`. A zero token is
immediately reached. The default timeout is 500 ms; values above 5 s are
clamped to 5 s.

The runner opts in:

```go
type Runner struct {
    // existing fields...
    Scope         string          // workspace, or "*" for a store-wide runner
    ProgressStore ProgressStore   // optional; nil keeps the legacy Marker path
    Progress      ProgressPublisher // optional; updated after a durable save
}
```

`Runner.advance` saves through `ProgressStore` when set (else the legacy
`Marker`), then calls `Progress.Advance`. `Runner.Reset` calls
`ResetProgress(..., StatusRebuilding)` before the application clears the read
model; `Runner.Rebuild` sets `live` at the end. `Runner.Status` is exposed
through the publisher and the stored state.

## 5. Semantics and wait algorithm

For `WaitFor(ctx, scope, projection, tok, timeout)`:

1. A zero token returns `Reached: true` immediately, before any store access.
2. Validate `tok.Scope == scope` (`ErrScopeMismatch`) and parse validity; a
   token scope over 512 bytes is invalid.
3. Enforce the waiter caps (`WithMaxWaiters`); above them return
   `Reached:false, err:nil` (fail open).
4. Read the cached checkpoint; on a miss, `LoadProgress`. A missing row is
   position 0 in a live-like state and waits. If the state is `rebuilding` or
   `stalled`, return `Reached:false` with that status (the caller serves
   stale/optimistic data). Only then compare the position.
5. Register for the `(scope, projection)` wake-up, then re-check (an advance
   may have landed between 4 and 5).
6. Wait on the wake-up, `ctx` and the deadline. Compare on each wake.
7. Unregister on return.

One refresh worker per `Waiter` owns every store read: it drains non-blocking
scope hints and a jittered ticker (≤100 ms), reads only keys with local
waiters, and advances the cache with the authoritative checkpoint. A
notification for a scope with no local waiters performs no read. The payload
never carries a position, so a forged hint can only cause a read, never a false
`reached`. On reconnect the listener enqueues every active key.

A zero token returns before any store access, so a query endpoint whose callers
send no token never touches the checkpoint store. `WaitFor` never reads the
read model; it only observes the checkpoint. The caller must read the same
read-model copy that the projection writes.

## 6. Backends, migrations, notifications

**Postgres.** Migration `0017_projection_checkpoints.sql` creates:

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

with the standard workspace RLS policy (workspace arm plus the role-gated
`es_relay` arm), `ENABLE` + `FORCE`. The table is small and unpartitioned.
`postgres.NewProgressStore(store *Store, channel string)` implements
`projection.ProgressStore`. After any checkpoint mutation commits (`SaveProgress`, `ResetProgress`,
`SetStatus`), it marks the scope on a bounded notifier; the notifier dedupes scopes and flushes at most every 10 ms
with `pg_notify(channel, scope)` **outside** the save transaction. A failed
flush is counted and dropped: a hint never fails or rolls back a checkpoint
write, and a rolled-back save never marks at all. The payload is the scope
only, so a forged notification cannot raise a cached position. The listener's
per-scope throttle now coalesces a burst. `postgres.NewProgressListener(dsn,
channel, refresher)` wraps the existing `NotifyListener`: `OnNotify(scope)`
enqueues a scope refresh and `OnReconnected()` enqueues a refresh of every
active key; neither runs a store read on the listener goroutine. `Close()`
flushes once, best-effort, and stops the notifier.

**SQLite.** Migration `0015_projection_checkpoints.sql` creates the same table
with `scope` instead of `workspace_id`, no RLS, and `updated_at TEXT`.
`sqlite.NewProgressStore(store *Store)` implements the interface in-process;
there is no cross-process notification.

**Delivery cursor.** `feed_checkpoints` and `feed.Server.Checkpoint` are
unchanged. A consumer may ack ahead of the applied checkpoint; that is its
delivery state, not the read-model guarantee.

**Fallback.** If a notification is lost, the waiter re-reads
`projection_checkpoints` at most every 100 ms (jittered) and once at the
deadline. A counter/instrumentation hook should expose fallback reads;
metrics/exporters remain application-owned.

## 7. Rebuilds

```go
runner.ProgressStore.ResetProgress(ctx, scope, name, projection.StatusRebuilding)
clearOrSwapTheReadModel()
last, err := runner.Rebuild(ctx, source, batch)
runner.Progress.SetStatus(scope, name, projection.StatusLive)
```

`Rebuild` itself still does not reset (ADR 0005); `Reset` is explicit. While
`rebuilding` is set, `WaitFor` returns `Reached: false` immediately with
`StatusRebuilding`; the query path returns stale/optimistic data. A blue/green
deployment points new traffic at the new version only after `Rebuild` returns
and the status is `live`.

## 8. Usage

```go
// command endpoint: return the committed position
res, err := runtime.Handle(ctx, sid, cmd, meta)
// res.Token.String() in the response body / ES-Consistency header

// list endpoint: wait for the projection, then read the same store
tok, err := es.ParseToken(r.Header.Get("ES-Consistency"))
if err != nil { /* 400 */ }
if tok.Scope != authenticatedWorkspace { /* 403 */ }
wr, err := waiter.WaitFor(r.Context(), authenticatedWorkspace, "iam.user_list", tok, 500*time.Millisecond)
if err != nil { writeErr(w, err); return }
rows := readModel.ListUsers(r.Context(), filterFrom(r))
if tok.Position > 0 { // a tokenless read has nothing to be consistent with
    w.Header().Set("ES-Consistent", strconv.FormatBool(wr.Reached && wr.Status == projection.StatusLive))
}
writeJSON(w, rows)
```

The client keeps the latest token per workspace, uses `MaxToken` for several
commands, and retries with the same token once if `ES-Consistent: false`.

### Query services without a hard dependency on es-lite

`WaitFor` is one bounded checkpoint read plus an in-process condition
variable; it is not a call into the write path and it does not proxy the
read-model query. A fleet of query endpoints still should not lose
availability because the checkpoint store is down:

- **Co-locate.** Put `projection_checkpoints` in the same Postgres database as
  the read model. The wait then costs a primary-key lookup on a connection the
  service already has. The projection process is the only writer; waiters are
  read-only.
- **Fail open.** Use `WithBestEffort()` on the query path: a store error, a
  missing row or a timeout becomes `Reached: false` and the endpoint serves
  what it has with `ES-Consistent: false`. Malformed tokens, scope mismatches
  and client cancellation still fail the request.
- **Skip when tokenless.** A request without `ES-Consistency` is a zero token
  and returns before any store access.
- **Do not hold anything across the wait.** No open transaction, no pooled
  connection, no lock. Read after the wait in a short transaction.
- **One listener per process.** `postgres.NewProgressListener` owns a single
  connection and fans out into the `Waiter` hub; it is created once at
  startup and `Run` in a goroutine, never per request.

The dependency shape is: command → token; projection → checkpoint row +
notification; query → checkpoint read + its own read model. The query path
never contacts the append path.

## 9. Testing

Unit:

- Token: round trip, malformed/unknown version, zero token, `MaxToken` scope
  mismatch, URL-safe output.
- `AppendResult.Position` and `Handle` token: normal append, multi-event batch,
  zero-event no-op on a normal and a snapshot-seeded stream, empty stream.
- Progress store: save/load/reset/set-status; position never lowers within a
  state; scope isolation.
- Runner: saves through the progress store after dispatch; `Apply` and a
  rebuild page save and publish once per batch, and save the last successful
  position before a mid-batch error; calls the publisher in order; `Reset` sets
  `rebuilding` before the first replayed event; legacy `Marker` still works.
- Waiter: empty token, already reached (no blocking), wake on advance, no wake
  below, the register/re-check race, timeout with no error, ctx cancellation,
  scope mismatch, rebuilding status, fallback read, 10k waiters and goroutine
  leaks (`goleak` is a new test-only dependency). Also: `WithBestEffort`
  turns a store error into `Reached:false, err:nil` while the default returns
  it; N waiters on one key share one refresher (the store read count is per
  tick, not per waiter); no waiter holds a connection or a `store` call while
  blocked; the waiter caps fail open; a forged `Advance` is corrected by the
  next refresh; a scope with no local waiters performs no read; a refresh-all
  repairs every active key.
- Postgres: migration shape/RLS, save/load/reset; the hint is emitted after
  commit and coalesced per scope; a rolled-back save emits none; a failed
  flush does not fail the save; the listener enqueues a refresh and a
  reconnect enqueues a refresh-all; a forged payload can only cause a read.
- SQLite: migration shape, save/load/reset, in-process advance.
- Rebuild: waiters see `rebuilding`, then `live`; an outstanding token is
  satisfied after the rebuilt projection passes it.

Integration (Postgres-gated):

- Handle → token → WaitFor on a real async `projection.Runner` → query; 1,000
  iterations with zero stale reads when `Reached && live`.
- Two connections/processes: writer saves a checkpoint, a waiter in another
  connection is released by `LISTEN/NOTIFY`; dropping the notification still
  satisfies via the fallback.
- Out-of-order commits: the projection checkpoint must not pass an uncommitted
  lower position; a wait for the higher position does not return early.

## 10. Acceptance

- `Handle` and `AppendResult` expose the highest position; `TokenFor` covers
  single and multi-append.
- `projection.ProgressStore` and `Waiter` exist for Postgres and SQLite, with
  in-process and Postgres cross-process notification.
- The checkpoint is written after the read-model write commits; the hint is
  emitted after the commit, never inside it, so a hint cannot fail a write; a
  rolled-back save emits none; a lost hint falls back.
- A hint cannot make a read lie: the payload carries only a scope, and the
  waiter always re-reads the authoritative position before advancing.
- Waiting is cheap at the read edge: no connection is held while blocked, a
  tokenless or warm-cache read performs no store access, N waiters on one key
  share one fallback refresher, and notifications are coalesced per batch with
  no message for a no-op save.
- A query endpoint using `WithBestEffort` keeps serving with `Reached:false`
  when the checkpoint store errors, times out or is missing.
- Rebuilds are status-gated; `WaitFor` never reports reached while
  `rebuilding`.
- Tests above pass in CI, `-race` clean, no goroutine leaks.
- An example in `examples/` shows a list view using the token end to end.
- ADR 0045 records the decision and the blue/green rebuild contract.

## 11. Deferred

- A durably versioned projection epoch (needed only if blue/green is not
  possible).
- An adapter that reads a raw `feed.CheckpointStore` as the applied
  checkpoint; v1 uses `projection.Runner`'s progress store.
- A checkpoint mirror for a read model in a separate database. V1 expects
  `projection_checkpoints` to be co-located with the read model or reachable
  from the query service; otherwise the query path needs a connection to the
  event database just for the wait.
- Cross-region consistency and global linearizability.
- Automatic status clearing after a crashed rebuild (v1: operator action).
