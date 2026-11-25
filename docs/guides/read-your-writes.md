# Read-your-writes for query APIs

- **Status:** proposed; ships with the `es.Token` / `projection.Waiter` work
  (v0.24.0). See [the proposal](../proposals/es-lite-read-your-writes.md) and
  ADR 0045.
- **Audience:** anyone exposing a query endpoint over an asynchronous read
  model.

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

wr, err := waiter.WaitFor(ctx, scope, "users", tok, 500*time.Millisecond)
```

`wr.Reached == true` means: **for this scope, projection `users` has applied
every event at or below `tok.Position`, or deliberately filtered it.**

That is all it means. It is not a global linearizability claim, not a
transaction across projections, and not a statement about other workspaces.
The default deadline is 500 ms; values above 5 s are clamped to 5 s. A timeout
is **not an error** — it returns `Reached:false` with stale data.

## 3. Integration in four steps

```
POST /users
  append @ 1234 ──► ES-Consistency: <token(ws_a,1234)>
                          │
                          ▼
GET /users   ES-Consistency: <token>
  1. parse + check scope        (no es-lite access)
  2. WaitFor(ws_a,"users",tok)  (one checkpoint read, then a Go channel)
  3. SELECT ... FROM users      (your read model)
  4. ES-Consistent: true|false
```

### Step 1 — the command returns a token

```go
res, err := runtime.Handle(ctx, sid, CreateUser{Name: "alice"}, es.Meta{Workspace: ws})
if err != nil { /* ... */ }
w.Header().Set("ES-Consistency", res.Token.String())
```

`res.Token` is set from `AppendResult.Position`, the highest position in the
append. A multi-event command therefore yields a token that covers the whole
command: the read never observes half of it.

### Step 2 — the client passes it back

The client keeps the latest token per workspace and sends it on the next read:

```
GET /users
ES-Consistency: AQACd3NfYQAAAAAAAATc
```

If the client has no token, it sends nothing. That is a zero token: the
endpoint skips the wait and should not claim freshness.

### Step 3 — wait on the projection

```go
tok, err := es.ParseToken(r.Header.Get("ES-Consistency"))
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

### Step 4 — read the same read model

```go
rows, err := readModel.ListUsers(r.Context(), authenticatedWorkspace)
// ...
if tok.Position > 0 { // only claim freshness when the caller supplied a token
    w.Header().Set("ES-Consistent", strconv.FormatBool(
        wr.Reached && wr.Status == projection.StatusLive))
}
writeJSON(w, rows)
```

`WaitFor` never reads the read model. It only observes the projection's
checkpoint, so you must query the same copy the projection writes (same
database, not a lagging replica).

## 4. What the wait actually costs

The query path does **not** call an es-lite service. It reads one small table.
If `projection_checkpoints` lives in the same database as the read model, the
dependency is a table, not a network hop or a new pool.

| Request | Checkpoint access | Connection held while blocked |
|---|---|---|
| no `ES-Consistency` header | none | none |
| token already reached (warm hub) | none (in-memory) | none |
| token ahead of the checkpoint | one primary-key read, then a wake-up | **none** |
| store unavailable + `WithBestEffort` | one failed read | none |

Additional properties:

- **One `LISTEN` connection per process**, created once
  (`postgres.NewProgressListener`), not one per request. Ten API pods = ten
  connections.
- **One notification per checkpoint write.** The runner saves once per applied
  batch and a rebuild once per page; a save that does not move the position
  emits nothing; the listener throttles per scope at 10 ms.
- **One fallback refresher per `(scope, projection)` key**, shared by every
  waiter on that key. If notifications are lost, 100 waiters cost one read per
  tick, not 100.
- **Wake-ups are free.** `Advance` updates the hub cache before closing the
  channel, so 100 waiters wake and return without a database read.

That is the answer to “do all my query APIs now depend on es-lite?”: they
depend on one row in a table you can co-locate with the read model, and they
can degrade to stale data instead of failing.

## 5. Failing open

```go
waiter := projection.NewWaiter(progressStore, projection.WithBestEffort())
```

`WithBestEffort` turns store errors, missing rows and timeouts into
`Reached:false, err:nil`. Malformed tokens, scope mismatches and client
cancellation still fail the request.

| Condition | `WaitFor` | Endpoint |
|---|---|---|
| reached, `live` | `Reached:true`, `nil` | 200, `ES-Consistent: true` |
| deadline passes | `Reached:false`, `nil` | 200, stale, `ES-Consistent: false` |
| checkpoint store down | `Reached:false`, `nil` (best effort) | 200, stale, `ES-Consistent: false` |
| checkpoint store down, strict | store error | 503 or 500 — your choice |
| malformed token | `ErrInvalidToken` | 400 |
| token scope ≠ auth scope | `ErrScopeMismatch` | 403 |
| request cancelled | `context.Canceled` | client gone |
| projection `rebuilding` | `Reached:false`, `StatusRebuilding` | 200, stale, `ES-Consistent: false` |
| projection not running / stuck | timeout | 200, stale, `ES-Consistent: false` |

A projection that is down cannot block reads: the wait is bounded by the
deadline.

## 6. Several projections, several commands

- One `WaitFor` per projection the response depends on. Usually that is one.
- The token is the same `(scope, position)` for all of them; only the
  projection name changes.
- After several commands, send the highest token: `es.MaxToken(a, b)`. It
  returns `ErrScopeMismatch` if the scopes differ, which is exactly the bug you
  want to catch.
- If a response joins projections, wait for the slowest one by waiting for
  each; there is no cross-projection barrier.

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
it.

## 8. Sizing

| Scenario | Cost |
|---|---|
| 100 concurrent queries, one key, one advance | 1 notification, 1 wake broadcast, 0 reads on wake |
| 100 waiters on one key, notifications lost | 1 store read per ≤100 ms tick (shared refresher) |
| 10,000 waiters | ~2 KB stack + one timer each; avoid unbounded long-polling |
| 10 API pods | 10 `LISTEN` connections |

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
`rebuilding`; send a token for another workspace and assert 403.

## 10. Operations

- Postgres migration `0017_projection_checkpoints.sql`; SQLite
  `0015_projection_checkpoints.sql`.
- Grants: the projection runner needs `SELECT, INSERT, UPDATE`; the query
  service needs `SELECT`. No `DELETE` on the normal path. See the
  [deployment runbook](../deploy/README.md#grants-the-privilege-contract).
- `projection_checkpoints` is derived progress, not state. A restore resets and
  replays it; do not back it up as a source of truth.
- `LISTEN`/`NOTIFY` are optional: without them the fallback read still
  satisfies waits, more slowly.

## 11. When not to use it

- **Clients with no token.** There is nothing to wait for; read directly and
  omit `ES-Consistent`.
- **Analytics over old data.** A consistency token is irrelevant to a query
  that is already expected to be minutes behind.
- **Cross-region reads.** The guarantee is per scope and per database; it says
  nothing about a replica in another region.
- **Synchronous projections.** If the read model is written in the same
  transaction as the append, the wait is unnecessary.
