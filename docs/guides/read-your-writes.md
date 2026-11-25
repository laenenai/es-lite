# Read-your-writes for query APIs

- **Status:** shipped in v0.24.0; [ADR 0045](../adr/0045-read-your-writes.md)
  is Accepted (implemented). See also
  [the proposal](../proposals/es-lite-read-your-writes.md).
- **Audience:** anyone exposing a query endpoint over an asynchronous read
  model.
- **Runnable example:** [`examples/readwrite`](../../examples/readwrite) — a
  SQLite command → token → wait → read flow, plus the reusable
  [`WaitFor` middleware](../../examples/readwrite/middleware.go).

## 1. The problem

A command commits an event and returns. The projection that maintains the read
model applies that event a few milliseconds later. A client that immediately
lists the resource can therefore miss its own write.

The write side knows exactly where its event landed in the log. The read side
needs a bounded way to say: *do not answer from this projection until it has
applied at least that position.*

## 2. The contract

```go
type Token struct {
    Scope    string // the workspace; "" on SQLite
    Position uint64 // highest global_position the command committed
}

type WaitResult struct {
    Reached    bool
    Checkpoint projection.Checkpoint
    Waited     time.Duration
    Status     projection.Status
}

wr, err := waiter.WaitFor(ctx, scope, "users", tok, 500*time.Millisecond)
```

`wr.Reached == true` means: **for this scope, projection `users` has applied
every event at or below `tok.Position`, or deliberately filtered it.**

That is all it means. It is not a global linearizability claim, not a
transaction across projections, and not a statement about other workspaces.
The state gate comes before the position gate: a `rebuilding` or `stalled`
checkpoint returns `Reached:false` immediately, even if its stale position is
at or above the token. The default deadline is 500 ms; values above 5 s are
clamped to 5 s. A timeout is **not an error** — it returns `Reached:false` with
stale data.

The zero token (`Token{}`, wire form `""`) is reached immediately, before any
store access, so a tokenless request never touches the checkpoint store. A
token with `Position: 0` for a matching scope is treated the same way. Do not
claim freshness for either: `ES-Consistent` is only meaningful when the caller
supplied a token with a position.

The token is a **position hint, not an authorization claim**. It carries no
rights and no data; the scope a query waits in comes from authentication. A
token scope over 512 bytes is `es.ErrInvalidToken`; a token for another scope
is `es.ErrScopeMismatch`. Both fail even under best effort.

## 3. Integration in four steps

```
POST /users
  Handle @ 1234 ──► ES-Consistency: <token(ws_a,1234)>
                          │
                          ▼
GET /users   ES-Consistency: <token>
  1. ParseToken + resolve scope   (no es-lite access)
  2. check token scope == scope   (403 otherwise)
  3. WaitFor(ws_a,"users",tok)    (cached checkpoint, or one read, then a Go channel)
  4. SELECT ... FROM users        (the same read model)
  5. ES-Consistent: true|false
```

### Step 1 — the command returns a token

```go
// Postgres: the runtime's store is workspace-scoped, so every operation
// (including the append and the RLS setting) runs as this workspace.
wsStore := store.WorkspaceStore(ws)
runtime := aggregate.NewRuntime(wsStore, decider, codec)
res, err := runtime.Handle(ctx, sid, CreateUser{Name: "alice"}, es.Meta{})
if err != nil { /* ... */ }
w.Header().Set("ES-Consistency", res.Token.String())
```

`es.Meta` carries audit fields, not the workspace: the scope comes from the
store handle the runtime was built over. On SQLite the store is
single-workspace and the scope is `""`.

`res.Token` carries `res.Position`, the highest position in the append. A
multi-event command therefore yields a token that covers the whole command:
the read never observes half of it. An append that bypasses `aggregate` can
build the same token with `es.TokenFor(scope, results...)`; a highest position
of 0 yields the zero token, because there is nothing to wait for.

### Step 2 — the client passes it back

The client keeps the latest token per workspace and sends it on the next read:

```
GET /users
ES-Consistency: AQAEd3NfYQAAAAAAAATS
```

If the client has no token, it sends nothing. The endpoint skips the wait and
does not claim freshness; it must not manufacture an `ES-Consistent` header.
When a response depends on several commands, send the higher token produced by
`es.MaxToken` (section 6).

### Step 3 — wait on the projection

```go
raw := r.Header.Get("ES-Consistency")
if raw == "" {
    // Tokenless: no wait, no checkpoint read, no freshness claim.
    serveUsers(w, r)
    return
}
tok, err := es.ParseToken(raw)
if err != nil {
    http.Error(w, "invalid consistency token", http.StatusBadRequest)
    return
}
if tok.Scope != authenticatedWorkspace {
    http.Error(w, "token is for another workspace", http.StatusForbidden)
    return
}
wr, err := waiter.WaitFor(r.Context(), authenticatedWorkspace, "users", tok, 500*time.Millisecond)
if err != nil {
    // only: bad token, scope mismatch, ctx cancelled, or store error without
    // WithBestEffort. A timeout is not an error.
    writeError(w, err)
    return
}
```

Never take the scope from the token. The scope comes from authentication; the
token is a position hint, not an authorization claim.

The reusable wrapper
[`WaitFor`](../../examples/readwrite/middleware.go) does exactly these steps,
sets `ES-Consistent`, and turns scope resolution failures into 400/403 and a
strict store failure into 503. It is `package main` only because the runnable
example is; copy it into your query service:

```go
mux.Handle("GET /users", WaitFor(waiter, "users", workspaceFromContext, 500*time.Millisecond, usersHandler))
```

### Step 4 — read the same read model

```go
rows, err := readModel.ListUsers(r.Context(), authenticatedWorkspace)
// ...
// Only when the caller supplied a token:
w.Header().Set("ES-Consistent", strconv.FormatBool(
    wr.Reached && wr.Status == projection.StatusLive))
writeJSON(w, rows)
```

`WaitFor` never reads the read model. It only observes the projection's
checkpoint, so you must query the same copy the projection writes (same
database, not a lagging replica).

## 4. The dependency model and what the wait costs

The query path does **not** call an es-lite service. It reads one small table.
If `projection_checkpoints` lives in the same database as the read model, the
dependency is a table, not a network hop or a new pool.

| Request | Checkpoint access | Connection held while blocked |
|---|---|---|
| no `ES-Consistency` header, or zero token | **none** (returns before the store) | none |
| token already reached (warm cache) | none | none |
| token ahead of the checkpoint | one primary-key read on a cold key, then a Go-channel wake | **none** |
| store unavailable + `WithBestEffort` | one failed read | none |

Additional properties:

- **Tokenless requests never touch the store.** A missing header is handled
  before parsing or scope resolution; a zero token is `Reached:true` before any
  store access.
- **One refresh worker per process**, started lazily by the first wait that
  actually has to block. It reads only keys with **local** waiters; a hint for
  a scope where nothing is waiting performs no read, and an unchanged
  checkpoint is not broadcast.
- **One hint per scope per window, after commit.** The hint is emitted outside
  the checkpoint transaction and coalesced for ~10 ms. A failed hint is counted
  and dropped; it can never fail or roll back the save. An oversized scope
  skips its hint rather than failing the write. On Postgres, every checkpoint
  mutation — `SaveProgress`, `ResetProgress`, `SetStatus` — marks its scope.
- **Hints are never data.** The payload carries the scope only — no position,
  no state. A forged hint can only cost a store read; the position always comes
  from `projection_checkpoints`, so it can never produce a false `reached`.
- **The listener only enqueues.** One `LISTEN` connection per process, created
  once and started in a goroutine
  (`postgres.NewProgressListener(dsn, channel, waiter)`, then
  `listener.Run(ctx)`); it is not one per request. `OnNotify` calls
  `EnqueueScopeRefresh(scope)` and a reconnect calls `EnqueueRefreshAll()`,
  which refreshes every active key to repair missed hints. Neither runs a store
  read on the listener goroutine.
- **An empty channel is a kill switch.** No notifier goroutine, no hint
  statement, `NotifyDropped() == 0`; the ~100 ms jittered fallback read still
  satisfies every wait, more slowly but correctly.
- **Wake-ups are cheap.** One read per coalesced wake updates the cache; 100
  waiters on that key then wake and return without touching the database. At
  the deadline each waiter also performs one last bounded store read, so a
  hint lost for the whole wait still ends reached.
- **Save volume on the live path.** `Run`/`Feed` call `Apply` with one envelope
  at a time, so the live path saves the checkpoint **once per event**. An
  explicit multi-event `Apply` batch and each `Rebuild` page save once each.
  That is cheap and write-through, and hints are coalesced per scope, so the
  hint volume follows scopes and windows, not saves. A lower same-state save
  (one that does not move the position) emits nothing.
- **`LogFeed` is the single-writer path.** Over Postgres, `projection.LogFeed`
  pages raw `ReadAll` without a settlement fence. Run it with a single writer
  or a quiescent log; with concurrent appenders use `eventfeed`, which owns the
  fenced delivery cursor.
- **Co-locate the table with the read model.** The same database, one pool for
  the wait and the query. `WaitFor` never reads the read model, so it cannot
  detect that you then query a lagging replica: read the same copy the
  projection writes.

That is the answer to “do all my query APIs now depend on es-lite?”: they
depend on one row in a table you can co-locate with the read model, and they
can degrade to stale data instead of failing.

## 5. Failing open

```go
waiter := projection.NewWaiter(progressStore,
    projection.WithBestEffort(),      // store failure -> Reached:false, nil
    projection.WithMaxWaiters(1024, 16384), // defaults: per key, total
)
```

`WithBestEffort` turns store errors into `Reached:false, err:nil`, so the
endpoint serves stale data. A timeout is already `Reached:false, err:nil`; a
missing row is position 0, so the wait continues and, if nothing advances,
times out as stale. Best effort **never** masks a malformed token, a scope
mismatch or context cancellation.

`WithMaxWaiters(perKey, total)` bounds the hub. The defaults are 1024 waiters
on one `(scope, projection)` key and 16384 overall; above either cap `WaitFor`
returns `Reached:false, err:nil` immediately — fail open rather than queue
unbounded blocked requests. A non-positive value keeps the default. The cap is
checked after the cached-checkpoint decision, so an already-satisfied token
still returns `Reached:true` when the hub is full.
`WithFallback(d)` changes the fallback refresh interval (100 ms default,
jittered ±20%).

| Condition | `WaitFor` | Endpoint |
|---|---|---|
| reached, `live` | `Reached:true`, `nil` | 200, `ES-Consistent: true` |
| deadline passes | `Reached:false`, `nil` | 200, stale, `ES-Consistent: false` |
| checkpoint store down | `Reached:false`, `nil` (best effort) | 200, stale, `ES-Consistent: false` |
| checkpoint store down, strict | store error | 503 (or 500 — your choice) |
| malformed token | `ErrInvalidToken` | 400 |
| token scope ≠ auth scope | `ErrScopeMismatch` | 403 |
| request cancelled | context error | client gone |
| projection `rebuilding` | `Reached:false`, `StatusRebuilding` immediately | 200, stale, `ES-Consistent: false` |
| projection `stalled` | `Reached:false`, `StatusStalled` immediately | 200, stale, `ES-Consistent: false` |
| projection not running / stuck | timeout | 200, stale, `ES-Consistent: false` |
| waiter cap reached | `Reached:false`, `nil` immediately | 200, stale, `ES-Consistent: false` |

A projection that is down cannot block reads: the wait is bounded by the
deadline.

Set `ES-Consistent` only when the request carried a token. The value is
`wr.Reached && wr.Status == projection.StatusLive`; a `true` for a tokenless
request would be a claim you did not earn.

## 6. Several projections, several commands

- One `WaitFor` per projection the response depends on. Usually that is one.
- The token is the same `(scope, position)` for all of them; only the
  projection name changes.
- After several commands, send the highest token: `es.MaxToken(a, b)` on the
  client. It returns `ErrScopeMismatch` if the scopes differ, which is exactly
  the bug you want to catch.
- If a response joins projections, wait for each one; there is no
  cross-projection barrier. Claim `ES-Consistent: true` only when every wait
  reached and every status is `live`.

## 7. Rebuilds

```go
// Before touching the read model:
err := runner.Reset(ctx)              // checkpoint -> position 0, state rebuilding
// clear or swap the read model
last, err := runner.Rebuild(ctx, source, 500) // state -> live on success
```

While the state is `rebuilding`, `WaitFor` returns `Reached:false` with
`StatusRebuilding` immediately — it never pretends the old checkpoint is
meaningful. Serve stale data, or route to the previous read-model version
(blue/green). Quiescing reads is the alternative to blue/green; a token that
spans a non-blue/green rebuild is treated as stale by the client.

`Reset` is explicit; `Rebuild` does not reset on its own (ADR 0005). A crashed
rebuild leaves `rebuilding` set, which is fail-closed until an operator clears
it. Both `ResetProgress` and `SetStatus` mark the scope, so a wait blocked on
the old position is woken to observe `rebuilding` (and later `live`). `Reset`
publishes the checkpoint the store returned (position 0, `rebuilding`), so a
warm waiter cache cannot keep the old high-water mark and report a rebuild
below it as reached.

## 8. Sizing

| Scenario | Cost |
|---|---|
| 100 concurrent queries, one key, one advance | 1 coalesced notification, 1 store read by the refresher, 100 wake-ups, **0 reads on wake**, no extra connections |
| 100 waiters on one key, hints lost | 1 store read per ~100 ms tick (jittered ±20%), one shared worker |
| Hints with no local waiters | 0 store reads |
| 1,024 waiters on one key (the default per-key cap) | ~2 KB stack + one timer each; above the cap the rest fail open immediately |
| 10 API pods | 10 `LISTEN` connections per enabled channel; 0 with hints disabled |

The number to watch is **waiters × deadline**, i.e. how many requests are
simultaneously blocked, not the notification rate. A client that retries a
timed-out request immediately with the same token doubles the waiters; back off
or accept `ES-Consistent: false`.

## 9. Testing your integration

The useful test is not “a wait returns reached”; it is “the query never misses
its own write”:

```go
for i := 0; i < 1000; i++ {
    res := createUser(t, i)          // command, returns token
    wr := wait(t, res.Token)         // synchronous wait in the test
    rows := listUsers(t)             // HTTP call with ES-Consistency
    if wr.Reached && wr.Status == projection.StatusLive && !contains(rows, i) {
        t.Fatalf("stale read at iteration %d", i)
    }
}
```

Also test the failure paths: stop the projector and assert the read returns
`ES-Consistent:false` within the deadline; reset the projection and assert
`rebuilding`; send a token for another workspace and assert 403; drop a store
error in best-effort mode and assert the read still returns 200 with `false`.
The example's
[`middleware_test.go`](../../examples/readwrite/middleware_test.go) covers the
last two plus the tokenless path: no header means **zero** `LoadProgress`
calls, a reached live token yields `ES-Consistent: true`, a timeout yields
`false`, a malformed token is 400 and a foreign-scope token is 403.

## 10. Operations

- Postgres migration `0017_projection_checkpoints.sql`; SQLite
  `0015_projection_checkpoints.sql`.
- Grants: the projection runner needs `SELECT, INSERT, UPDATE`; the query
  service needs `SELECT`. No `DELETE` on the normal path. See the
  [deployment runbook](../deploy/README.md#grants-the-privilege-contract).
- `projection_checkpoints` is derived progress, not state. A restore resets and
  replays it; do not back it up as a source of truth.
- **Kill switch:** an empty progress channel (`NewProgressStore(store, "")`)
  disables hints completely — no notifier goroutine, no `NOTIFY`, and the
  fallback read still satisfies every wait. Use it when the notification queue
  alarms, a listener stays disconnected, or untrusted roles can connect.
- **Caps:** `WithMaxWaiters(1024, 16384)` are the defaults; above either, waits
  fail open immediately. Watch `waiters × deadline`, not the hint rate.
- **Health:** a reconnect enqueues a refresh for every active key;
  `NotifyListener.PID() == 0` for a sustained period and a rising
  `ProgressStore.NotifyDropped()` are the actionable signals. See
  [health and metrics](../deploy/README.md#health-and-metrics).
- `LISTEN`/`NOTIFY` are optional: without them the fallback read still
  satisfies waits, more slowly. Hints never change correctness, only latency.

## 11. When not to use it

- **Clients with no token.** There is nothing to wait for; read directly and
  omit `ES-Consistent`.
- **Analytics over old data.** A consistency token is irrelevant to a query
  that is already expected to be minutes behind.
- **Cross-region reads.** The guarantee is per scope and per database; it says
  nothing about a replica in another region.
- **Synchronous projections.** If the read model is written in the same
  transaction as the append, the wait is unnecessary.
