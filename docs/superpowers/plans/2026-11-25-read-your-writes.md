# Read-Your-Writes (WaitFor) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every successful append a position token and let a query wait, with a deadline, until a named projection has applied that position — in process and across Postgres processes — without a broker.

**Architecture:** A token is `(scope, position)`. The write side exposes `AppendResult.Position` and `aggregate.Result.Token`. The read side reads a new `projection_checkpoints` row (position + state), waits on an in-process hub, and falls back to a bounded checkpoint read. A Postgres checkpoint write publishes `pg_notify` in the same transaction; `postgres.NotifyListener` wakes waiters in other processes. SQLite is in-process. The existing `projection.Marker` and `feed_checkpoints` delivery cursors are unchanged.

**Tech Stack:** Go 1.27, Postgres 14+, SQLite, the `es`/`aggregate`/`projection` packages, Postgres `LISTEN/NOTIFY`, `go.uber.org/goleak` for the waiter tests.

**Spec:** `docs/proposals/es-lite-read-your-writes.md` (the revised proposal; read it before starting).

## Global Constraints

- **No broker.** ADR 0036 is in force: no NATS/JetStream/KV, no `task test:nats`. Cross-process notification is Postgres `LISTEN/NOTIFY`; SQLite is in-process.
- **Applied checkpoint, not delivery cursor.** `feed_checkpoints` and `feed.Server.Checkpoint` are untouched. `projection_checkpoints` is the only source for `WaitFor`.
- **Write-through.** `ProgressStore.SaveProgress` is called only after the read-model write commits. No same-transaction requirement; a crash replays, never advances early.
- **Scope.** The guarantee is “all events `≤ P` **in scope S**.” Postgres positions are store-global; a workspace runner advances over other workspaces’ positions. A store-wide runner uses scope `"*"`.
- **Token wire form.** Version `1`, then u16 scope length, scope bytes, u64 position; base64url. `""` parses as the zero token.
- **Wait policy.** Default timeout 500 ms; values above 5 s clamp to 5 s. A timeout is `Reached:false, err == nil`. Errors are reserved for a malformed token, scope mismatch, context cancellation and store errors.
- **Fallback.** One refresher per `(scope, projection)` key re-reads at most every 100 ms, jittered, and once at the deadline; every waiter on that key shares it. Notifications are an optimization.
- **Query-path cost.** A waiter holds no database connection while blocked. A tokenless request and a warm-cache hit perform no store access. `WithBestEffort` is the query path's fail-open mode; commands and health checks keep the default.
- **Notification volume.** One notification per coalesced window per scope: the live `Run`/`Feed` path saves once per event, an `Apply` batch and each `Rebuild` page save once, and a lower same-state save (one that does not move the position) emits nothing.
- **Migrations.** Postgres `0017_projection_checkpoints.sql`; SQLite `0015_projection_checkpoints.sql`. Existing applied migrations are not edited.
- **Backward compatibility.** `projection.Marker`, `eventfeed`, `feed`, `Drain` and the feed checkpoint schema keep their current behavior. The new paths are additive and opt-in.
- **Rebuilds.** `rebuilding` is set before the read model is cleared; `WaitFor` returns `Reached:false` with that status. Blue/green or quiesced reads are a deployment contract.

## Review Focus

These are the failure modes most likely to bite a user and are not obvious from a happy-path test:

1. **A token from one workspace compared with another workspace’s progress.** Must fail closed (`es.ErrScopeMismatch`), never sit until timeout as if it were unsatisfied.
2. **A checkpoint advanced or notified before the read-model write commits.** A crash or rollback must not make `Reached:true` true over missing data.
3. **A rebuild serving empty data while the old checkpoint still looks high.** `WaitFor` must return `rebuilding` rather than `reached`; the test resets the read model between the reset and the replay.
4. **A lost notification.** The fallback must satisfy the wait without a notification, including after a listener reconnect.
5. **A snapshot-seeded no-op command.** It has no new envelopes; the token must still carry the stream’s last global position (or a zero token for an empty stream), not an empty/incorrect scope.
6. **Every query endpoint now touches the checkpoint store.** The tokenless path must not read it, a wait must not pin a pooled connection, and `WithBestEffort` must keep the endpoint serving when the store is unavailable. A test with 100 blocked waiters should show one refresher, one notification and no pool exhaustion.

---

### Task 1: Tokens and append positions

**Files:**
- Create: `es/token.go`, `es/token_test.go`
- Modify: `es/store.go` (`AppendResult`), `es/errors.go` (`ErrInvalidToken`, `ErrScopeMismatch`)
- Modify: `postgres/wsstore.go`, `sqlite/store.go` (set `AppendResult.Position`)
- Modify: `aggregate/runtime.go` (`Result.Position`, `Result.Token`, no-op head read)
- Test: `aggregate/token_test.go`

**Interfaces:**
- Consumes: `es.Envelope.GlobalPosition`, `es.AppendResult`, `es.Store.ReadStream`.
- Produces:
  ```go
  type Token struct{ Scope string; Position uint64 }
  func TokenFor(scope string, results ...AppendResult) Token
  func (t Token) String() string
  func ParseToken(s string) (Token, error)
  func MaxToken(a, b Token) (Token, error)

  type AppendResult struct {
      FromVersion, ToVersion uint64
      Envelopes []Envelope
      Position uint64 // highest global_position; 0 for no events
  }

  type aggregate.Result[S, E any] struct {
      State S; Events []E; FromVersion, ToVersion uint64; Envelopes []es.Envelope
      Position uint64
      Token es.Token
  }
  ```

- [ ] **Step 1: Write the failing tests**

In `es/token_test.go`:
```go
func TestTokenRoundTrip(t *testing.T) {
    tok := es.Token{Scope: "ws_a", Position: 42}
    got, err := es.ParseToken(tok.String())
    if err != nil || got != tok { t.Fatalf("round trip = (%+v, %v)", got, err) }
    if tok.String() == "" || strings.ContainsAny(tok.String(), "+/=") { t.Fatalf("not URL-safe: %q", tok.String()) }
}

func TestParseTokenRejectsMalformed(t *testing.T) {
    for _, s := range []string{"!!!!", "AQ", "AQAAAAA"} { // truncated / bad version body
        if _, err := es.ParseToken(s); !errors.Is(err, es.ErrInvalidToken) { t.Errorf("%q: %v", s, err) }
    }
}

func TestZeroTokenAndMaxToken(t *testing.T) {
    if got, err := es.ParseToken(""); err != nil || got != (es.Token{}) { ... }
    a := es.Token{Scope: "ws", Position: 5}; b := es.Token{Scope: "ws", Position: 9}
    if got, err := es.MaxToken(a, b); err != nil || got.Position != 9 { ... }
    if _, err := es.MaxToken(a, es.Token{Scope: "other", Position: 9}); !errors.Is(err, es.ErrScopeMismatch) { ... }
}

func TestTokenFor(t *testing.T) {
    got := es.TokenFor("ws", es.AppendResult{Position: 3}, es.AppendResult{Position: 8})
    if got.Position != 8 || got.Scope != "ws" { ... }
}
```

Add `TestMaxTokenZero` to the `es` block: a zero token (empty scope, position
0) is compatible with any scope and returns the other token.

In `aggregate/token_test.go`, name the tests so the Step 2 filter runs them:
```go
func TestHandleToken(t *testing.T) {
    // append two events; Handle returns Position == the last envelope position
    // and a token whose scope is the envelope workspace and position matches.
}
func TestAppendResultPosition(t *testing.T) {
    // a direct openStore(t).Append returns Position == the last envelope.
}
func TestNoOpToken(t *testing.T) {
    // a no-op command (Decide returns nil) returns the same head position and
    // a token whose scope is the stream's workspace; repeat with the snapshot
    // cache warmed so the no-op has no tail envelopes; an empty stream's no-op
    // returns the zero token.
}
```
Use the existing `openStore`, `newSnapshotRuntime` and simple counter/decider fixtures from `aggregate/*_test.go`.

Also extend the existing Postgres append round-trip test (or add `TestAppendResultPosition` in `postgres/postgres_test.go`) with `res.Position == res.Envelopes[len-1].GlobalPosition`; the Postgres path sets the field independently of SQLite.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./es ./aggregate -run 'TestToken|TestParseToken|TestAppendResultPosition|TestHandleToken|TestNoOpToken' -v`
Expected: build failures — `Token`, `AppendResult.Position`, `Result.Token` undefined.

- [ ] **Step 3: Implement**

`es/token.go`: a 1-byte version, u16 scope length, scope, u64 position; base64url. `ParseToken("")` returns the zero token; any other length/version is `ErrInvalidToken`. `TokenFor` takes the highest `AppendResult.Position` and returns the zero token when it is 0 (nothing to wait for). `MaxToken` treats a zero token as compatible with any scope (returns the other one) and rejects two different non-empty scopes with `ErrScopeMismatch`.

`es/errors.go`: add `ErrInvalidToken`, `ErrScopeMismatch`.

Set `AppendResult.Position` in Postgres’ `Workspace.append` and SQLite’s `appendTx` to `envs[len(envs)-1].GlobalPosition` when `len(envs) > 0`.

`aggregate/runtime.go`: after a successful append, set `Position` from `res.Position` and `Token = es.TokenFor(workspaceOf(res.Envelopes), res)`. For the no-op branch, add:
```go
func (r *Runtime[S, C, E]) headPosition(ctx context.Context, sid es.StreamID, version uint64) (uint64, string) {
    if version == 0 { return 0, "" }
    envs, err := r.store.ReadStream(ctx, sid, version-1, version)
    if err != nil || len(envs) == 0 { return 0, "" } // best-effort token
    return envs[len(envs)-1].GlobalPosition, envs[len(envs)-1].Workspace
}
```
and use it in the `len(events)==0 && len(constraints)==0` branch: set `Position` and `Token = es.Token{Scope: ws, Position: pos}` from the returned workspace (`es.StreamID` carries no workspace, so the head envelope is the only source). A read error must not fail the no-op command; it yields a zero token.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./es ./aggregate ./postgres ./sqlite -run 'TestToken|TestParseToken|TestAppendResultPosition|TestHandleToken|TestNoOpToken' -v`
Expected: PASS. Then `go test ./es ./aggregate ./postgres ./sqlite` (Postgres with `PG_DSN` on the test container).

- [ ] **Step 5: Commit**

```bash
git add es/token.go es/token_test.go es/store.go es/errors.go postgres/wsstore.go sqlite/store.go postgres/postgres_test.go aggregate/runtime.go aggregate/token_test.go
git commit -m "feat(es): add consistency tokens and append positions"
```

---

### Task 2: Projection checkpoint contract and memory store

**Files:**
- Create: `projection/checkpoint.go`, `projection/checkpoint_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  ```go
  type Status uint8
  const (StatusUnknown Status = iota; StatusLive; StatusRebuilding; StatusStalled)

  type Checkpoint struct {
      Scope, Projection string
      Position uint64
      State Status
      UpdatedAt time.Time
  }

  type ProgressStore interface {
      LoadProgress(ctx context.Context, scope, projection string) (Checkpoint, bool, error)
      SaveProgress(ctx context.Context, scope, projection string, cp Checkpoint) error
      ResetProgress(ctx context.Context, scope, projection string, state Status) (Checkpoint, error)
      SetStatus(ctx context.Context, scope, projection string, state Status) error
  }

  type ProgressPublisher interface {
      Advance(cp Checkpoint)
      SetStatus(scope, projection string, state Status)
  }

  func NewMemoryProgressStore() *MemoryProgressStore
  ```

- [ ] **Step 1: Write the failing tests**

`projection/checkpoint_test.go`: save/load a checkpoint; saving a lower position in the same state keeps the higher one; saving a higher one advances; `ResetProgress` sets position 0 and `rebuilding`; `SetStatus` changes state without moving position; two scopes do not see each other; loading an absent key returns `found=false`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./projection -run 'TestMemoryProgressStore' -v`
Expected: build failure — the types do not exist.

- [ ] **Step 3: Implement**

A mutex-guarded map keyed by `(scope, projection)`. `SaveProgress` enforces: same state → `max(position)`; different state → replace the position. `ResetProgress` replaces with `{Position: 0, State: state}`. `SetStatus` creates a zero-position row when absent and, when present, changes only `State` and `UpdatedAt` — never the position. `UpdatedAt` reflects the last successful save even when the position did not move; Task 7's change detection compares position and state, not the timestamp. Timestamps use `time.Now().UTC()` in memory; the SQL stores use their database clocks.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./projection -run 'TestMemoryProgressStore' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add projection/checkpoint.go projection/checkpoint_test.go
git commit -m "feat(projection): add checkpoint progress contract"
```

---

### Task 3: Runner scope, progress store and status

**Files:**
- Modify: `projection/runner.go`
- Test: `projection/runner_progress_test.go`

**Interfaces:**
- Consumes: `projection.ProgressStore`, `projection.ProgressPublisher`, `projection.Checkpoint`, `projection.Status` from Task 2.
- Produces:
  ```go
  type Runner struct {
      // existing fields...
      Scope         string
      ProgressStore ProgressStore   // optional; nil keeps the Marker path
      Progress      ProgressPublisher // optional
  }
  func (r *Runner) Reset(ctx context.Context) error
  ```

- [ ] **Step 1: Write the failing tests**

A recording publisher and the memory progress store. Assert:
- `Apply` dispatches, then saves `Checkpoint{Scope:"ws", Projection:"p", Position: e.GlobalPosition, State: Live}`, in that order.
- `Apply` over a three-event batch saves and publishes **once**, at the last position (a recording store counts the saves).
- `Apply` stops at a failing event: it saves once with the last successfully applied position, publishes only that, and returns the error.
- A filtered event saves and publishes too.
- A save error does not call `Advance` and is returned.
- `Apply` while the runner is rebuilding persists the new position with `StateRebuilding`; the state becomes `live` only after a successful `Rebuild`.
- `Reset` sets position 0/state `rebuilding`, clears the runner’s cached marker, and publishes the checkpoint the store returned (position and state), so a warm cache cannot keep an old high-water mark.
- `Reset` calls `Discard(name)` when the legacy `Marker` implements it (a buffered pending position must not resurrect the old marker).
- `Rebuild` over a two-event source sets `live` only on success; a source error leaves `rebuilding`.
- A failing `SetStatus(live)` during `Rebuild` returns the error and leaves both the row and `r.state` `rebuilding`.
- A new runner loading a stored `rebuilding` checkpoint seeds `r.state=rebuilding`; its next `advance` persists `rebuilding`, not `live`.
- The legacy `Marker` path still works with `ProgressStore == nil`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./projection -run 'TestRunnerProgress|TestRunnerReset|TestRunnerRebuildStatus' -v`
Expected: build failure — `Runner.Scope`/`ProgressStore`/`Reset` do not exist.

- [ ] **Step 3: Implement**

- `loadMarker`: when `ProgressStore != nil`, call `LoadProgress(ctx, r.Scope, r.Name)`; a found row supplies the marker (position) **and the state** (unless the state is `StatusUnknown`), so a restart during a rebuild stays fail-closed; otherwise position 0 and the default `live`. Keep the legacy `Marker.Load` path when nil.
- `advance`: when `ProgressStore != nil`, call `SaveProgress(ctx, r.Scope, r.Name, Checkpoint{Scope:r.Scope, Projection:r.Name, Position:pos, State:r.state})`; otherwise `Marker.Save`. On success, call `Progress.Advance(cp)` when `Progress != nil`. Never publish before the save returns.
- **Rebuild state.** Track the runner's state in an unexported field, default `StatusLive`; `Reset` sets `StatusRebuilding`; `Rebuild` sets `StatusLive` only after `Replay` returns nil. `advance` persists that state. Without this, the first replayed page would write `live` mid-rebuild and a `WaitFor` could return reached over a half-rebuilt read model (Review Focus 3).
- **Batch coalescing.** Split dispatch from save: `Apply` dispatches every envelope in the batch, tracks the last successful position, and calls `advance` **once** at the end — and once with the last successful position before returning a dispatch error (otherwise a redelivery after a partial batch restarts the page). `Rebuild` reuses `Apply`, so a rebuild saves and notifies once per page. `Run`/`Feed` delivers one envelope at a time, so the live path saves once per event; that is acceptable because the Postgres store suppresses a notify when the save does not move the position (Task 7) and the listener throttles per scope at 10 ms. Keep the in-memory `r.marker` updated per applied envelope so an in-process redelivery still skips. Call `OnApplied` for every applied envelope **after** the batch save, preserving its documented “called after the marker is saved” ordering while still firing once per envelope.
- `Reset`: `ResetProgress(ctx, r.Scope, r.Name, StatusRebuilding)` when the store is set and publish the returned checkpoint with `Progress.Advance(cp)` — a state change replaces the cached position, so a warm cache moves to 0. Otherwise `Discard(r.Name)` if the `Marker` implements `interface{ Discard(string) }` and then `Marker.Save(...,0)`, publishing `Progress.SetStatus(..., StatusRebuilding)`. Set `r.state=StatusRebuilding`, `r.marker=0`, `r.loaded=true` in both paths.
- `Rebuild`: after `Replay` succeeds, call `ProgressStore.SetStatus(..., StatusLive)` when the store is set **first**, and set `r.state=StatusLive` only after it returns nil; then `Progress.SetStatus(..., StatusLive)`. On any error, restore/leave `StatusRebuilding` in both the row and `r.state`, and return. Keep `OnApplied` after the save and after `Advance`.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./projection -run 'TestRunnerProgress|TestRunnerReset|TestRunnerRebuildStatus|TestRunnerOnApplied' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add projection/runner.go projection/runner_progress_test.go
git commit -m "feat(projection): persist scoped progress and rebuild status"
```

---

### Task 4: Waiter

**Files:**
- Create: `projection/wait.go`, `projection/wait_test.go`
- Modify: `go.mod` / `go.sum` (add `go.uber.org/goleak` as a test dependency)

**Interfaces:**
- Consumes: `projection.ProgressStore`, `projection.Checkpoint`, `projection.Status`, `es.Token`, `es.ErrScopeMismatch`.
- Produces:
  ```go
  type WaitResult struct {
      Reached bool
      Checkpoint Checkpoint
      Waited time.Duration
      Status Status
  }

  type WaiterOption func(*waiterOptions)
  func WithFallback(d time.Duration) WaiterOption     // refresh interval; default 100ms, jittered
  func WithBestEffort() WaiterOption                  // store errors -> Reached:false
  func WithMaxWaiters(perKey, total int) WaiterOption // 0 uses the defaults (1024 / 16384)

  func NewWaiter(store ProgressStore, opts ...WaiterOption) *Waiter
  func (w *Waiter) WaitFor(ctx context.Context, scope, projection string, tok es.Token, timeout time.Duration) (WaitResult, error)
  func (w *Waiter) Checkpoint(ctx context.Context, scope, projection string) (Checkpoint, bool, error)
  func (w *Waiter) Status(ctx context.Context, scope, projection string) (Status, error)
  func (w *Waiter) Advance(cp Checkpoint)   // ProgressPublisher: cache + wake
  func (w *Waiter) SetStatus(scope, projection string, state Status)
  func (w *Waiter) EnqueueScopeRefresh(scope string) // non-blocking hint from the listener
  func (w *Waiter) EnqueueRefreshAll()               // listener reconnect repair
  func (w *Waiter) Close()                           // stop the refresh worker
  ```

- [ ] **Step 1: Write the failing tests**

Table tests in `projection/wait_test.go`:
- zero token → `Reached:true`, no store read;
- a token with `Position: 0` and a matching scope → `Reached:true`, no store read;
- a cold read that blocks past the deadline → a timeout result (`Reached:false, err == nil`) without `WithBestEffort`;
- already satisfied → returns without blocking;
- `Advance` past the token releases a waiter, an advance below it does not;
- the register/re-check race: loop 1,000 times, `Advance` between the fast path and registration, every wait returns reached (`-race`);
- timeout 20 ms → `Reached:false`, `err == nil`, `Waited` ≥ the timeout;
- context cancellation → `context.Canceled`;
- a token for another scope → `errors.Is(err, es.ErrScopeMismatch)`; a token with the empty scope is accepted when the caller scope is empty (SQLite) and rejected against a real workspace;
- a checkpoint at a position at or above the token but in `rebuilding` or `stalled` → `Reached:false`, the matching `Status`, no timeout (the status check must beat the position check);
- a missing checkpoint row → does not short-circuit: it waits and is released by an `Advance`; `Status` on a missing row returns `StatusUnknown` with a nil error;
- a lost notification: do not call `Advance`; advance the memory store behind the waiter and set `WithFallback(5*time.Millisecond)`; the wait returns reached;
- a lost notification for the whole wait: a store that returns the token's position only on a second read, with `WithFallback(time.Hour)` and a short timeout, ends reached via the one read at the deadline;
- a shared refresher: after warming the cache, 100 waiters on one key waiting for a position above it, with a 5 ms fallback, perform roughly one store read per tick, not one per waiter (count `LoadProgress` calls);
- a notification is a hint, not data: `Advance` a forged position 10^9, then `EnqueueScopeRefresh(scope)`; the cached and returned position is the store's authoritative one, and a token at 10^9 is not reached;
- no local waiters: `EnqueueScopeRefresh` and `EnqueueRefreshAll` perform no store read (count calls) and do not block;
- reconnect repair: two active keys, `EnqueueRefreshAll`, both are refreshed from the store;
- caps: with `WithMaxWaiters(1, 1)`, a second waiter on the key and a first waiter on another key each return `Reached:false, err:nil` immediately without blocking; an already-satisfied cached token still returns reached under the same cap;
- best effort: a store returning an error makes the default `WaitFor` return it, and `WithBestEffort` return `Reached:false, err:nil`; `ErrScopeMismatch` and `context.Canceled` still fail under `WithBestEffort` — including a context error returned by a ctx-aware `LoadProgress` that is gated and cancelled mid-read;
- idle eviction: after waits on more distinct keys than the idle cap, the hub stays bounded, and an evicted key re-reads the store on its next wait;
- status stickiness: two gated cold reads, `live@10` and `rebuilding@0`, installed in either order, leave the cache `rebuilding`; a token at 10 is not reached;
- no spurious wake-ups: with an unchanged checkpoint, three fallback ticks do not wake a blocked waiter (the worker broadcasts only on a change);
- 10,000 concurrent waiters on one key are all released by one `Advance` — construct the waiter with `WithMaxWaiters(20000, 20000)` so the default cap does not shed them — and `goleak.VerifyNone(t)` passes at the end after `Close()`;
- `Status` returns the stored state when the cache is cold;
- a token whose scope exceeds 512 bytes is `es.ErrInvalidToken`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./projection -run 'TestWaiter' -v`
Expected: build failure — `Waiter`/`WaitResult` do not exist.

- [ ] **Step 3: Implement**

A hub keyed by `(scope, projection)` holding the cached `Checkpoint`, the key's wake channel, and its waiter count. One refresh worker per `Waiter` owns every store read:

- It selects on a bounded scope-refresh queue, a refresh-all signal, a jittered ticker (`WithFallback`, default 100 ms) and `Close`.
- On any wake it computes the keys that currently have waiters, dedupes them, drops any whose waiter count already fell to zero, `LoadProgress`es each, `Advance`s the authoritative checkpoint, and wakes a key **only when found, state or position changed**.
- An instance with no waiters performs no reads, and a hint for a scope with no local waiters is dropped before any store access.
- A read error is counted and ignored; the next signal retries. Hint queue overflow drops the hint; the ticker repairs it.
- Idle keys are bounded by an internal cap (8192) with LRU eviction of keys that have no waiters; active keys are bounded by `WithMaxWaiters`. An evicted key re-reads on its next wait.

`WaitFor`:
1. A zero token returns reached immediately, before any store access; after the scope check, a token with `Position: 0` does the same.
2. If `tok.Scope != scope` → `es.ErrScopeMismatch`; an empty token scope matches only an empty caller scope (SQLite), and a token scope longer than 512 bytes is `es.ErrInvalidToken`. `ParseToken` also caps its input length before allocating.
3. Read the cache; on a miss, `store.LoadProgress` under a context bounded by the wait deadline. A read that expires with that deadline is a timeout result, not a store error. A missing row (`found=false`) is position 0 in a live-like state: continue waiting. `rebuilding` or `stalled` returns false immediately with that status. A position at or above the token returns reached. A cold read installs fail-closed: `rebuilding`/`stalled` sticks against a `live` read, otherwise a state change wins and a same-state read takes the higher position (two concurrent cold reads must not let a stale `live` erase a fresh `rebuilding`).
4. Enforce `WithMaxWaiters` only for a token the cache did not satisfy: above the per-key or total cap, return `Reached:false, err:nil` immediately (fail open; the caller serves stale data). Never queue an unbounded waiter, and never let the cap turn an already-satisfied token into a fail-open.
5. Lock, register the key, unlock, re-read the cache; if the advance happened, return reached.
6. Wait on the key's channel, `ctx.Done()` and the deadline. The worker, not the waiter, reads the store. When the deadline fires, perform one last bounded `LoadProgress` (`min(200ms, timeout)`), install a found checkpoint and decide; a lost hint then costs latency, not a false timeout.
7. Unregister; the worker idles when no key has waiters.

Clamp a timeout above 5 s to 5 s; `0` uses 500 ms. `Advance` stores the checkpoint as authoritative (it must never be called with a value the store did not return), ignores a lower position in the same state and replaces on a state change. `SetStatus` updates the cache and broadcasts. `Status` returns `StatusUnknown` when no row exists.

**Hints are never trusted.** The listener payload carries only a scope; `EnqueueScopeRefresh` turns it into a store read, so a forged or replayed notification cannot raise a cached position and cannot produce a false `reached`. `EnqueueRefreshAll` (listener reconnect) does the same for every key with waiters. Both are non-blocking and bounded; the 100 ms ticker is the repair path for anything dropped.

`WithBestEffort()` converts store errors into `Reached:false, err:nil`, so a query endpoint can fail open to stale data — but never `ErrInvalidToken`, `ErrScopeMismatch`, or context cancellation: check `ctx.Err()` and `errors.Is(err, context.Canceled/DeadlineExceeded)` before swallowing, because a ctx-aware store can return those from the read itself. It is for the read path; commands and health checks should keep the default. `Close()` stops the worker; it does not cancel in-flight waits.

- [ ] **Step 4: Run to verify pass**

Run: `go test -race ./projection -run 'TestWaiter' -v`
Expected: PASS, no goroutine leaks.

- [ ] **Step 5: Commit**

```bash
git add projection/wait.go projection/wait_test.go go.mod go.sum
git commit -m "feat(projection): add notification-driven WaitFor"
```

---

### Task 5: Postgres projection_checkpoints

**Files:**
- Create: `postgres/migrations/0017_projection_checkpoints.sql`, `postgres/progress.go`, `postgres/progress_test.go`
- Modify: `postgres/migrations_test.go`, `postgres/postgres_test.go`, `postgres/wsstore_test.go` (version counts / truncate lists)

**Interfaces:**
- Consumes: `projection.ProgressStore`, `projection.Checkpoint`, `projection.Status`.
- Produces:
  ```go
  var _ projection.ProgressStore = (*ProgressStore)(nil)
  func NewProgressStore(s *Store, channel string) *ProgressStore // channel is used by Task 7; "" disables it
  ```

- [ ] **Step 1: Write the failing tests**

- `TestProjectionCheckpointsSchema`: the table exists with `workspace_id`, `projection`, `position`, `state`, `updated_at`; the primary key is `(workspace_id, projection)`; the state CHECK admits only the three values.
- `TestProjectionCheckpointsRLS`: `relrowsecurity`/`relforcerowsecurity` are true and the policy is `projection_checkpoints_workspace_isolation`.
- `TestPostgresProgressStore`: save/load, monotonic within a state, reset to 0/rebuilding, set-status, scope isolation (two workspaces).
- `TestMigrationVersionSeventeen`: change the existing “latest version” assertions from 16 to 17.

- [ ] **Step 2: Run to verify failure**

Run: `PG_DSN=... go test ./postgres -run 'TestProjectionCheckpoints|TestPostgresProgressStore|TestMigrationVersionSeventeen' -v`
Expected: no `projection_checkpoints` table; `NewProgressStore` undefined.

- [ ] **Step 3: Implement**

The migration creates the table from the proposal with the standard workspace RLS policy (`ENABLE` + `FORCE`, workspace arm plus the role-gated `es_relay` arm). `ProgressStore` uses `store.runWorkspace(ctx, scope, ...)`:

```sql
INSERT INTO projection_checkpoints (workspace_id, projection, position, state)
VALUES ($1,$2,$3,$4)
ON CONFLICT (workspace_id, projection) DO UPDATE SET
  position = CASE
      WHEN projection_checkpoints.state = excluded.state
      THEN GREATEST(projection_checkpoints.position, excluded.position)
      ELSE excluded.position END,
  state = excluded.state,
  updated_at = clock_timestamp()
```

`ResetProgress` writes `(0, state)`. `SetStatus` upserts `(0, state)` when absent and, when present, updates only `state` and `updated_at` (never `position`). Map the `state` text to `projection.Status` and reject unknown values on read. Store the constructor’s `channel` on the struct; Task 7 uses it. Task 5's tests construct with `""` (notify is off until Task 7). Add `projection_checkpoints` to the test truncate lists and update the migration-count tests (17).

- [ ] **Step 4: Run to verify pass**

Run: `PG_DSN=... go test ./postgres -run 'TestProjectionCheckpoints|TestPostgresProgressStore|TestMigration' -v`
Expected: PASS, then the full Postgres suite.

- [ ] **Step 5: Commit**

```bash
git add postgres/migrations/0017_projection_checkpoints.sql postgres/progress.go postgres/progress_test.go postgres/migrations_test.go postgres/postgres_test.go postgres/wsstore_test.go
git commit -m "feat(postgres): add the projection checkpoint store"
```

---

### Task 6: SQLite projection_checkpoints

**Files:**
- Create: `sqlite/migrations/0015_projection_checkpoints.sql`, `sqlite/progress.go`, `sqlite/progress_test.go`
- Modify: `sqlite/migrate_test.go`, `sqlite/populated_test.go` (version counts)

**Interfaces:**
- Consumes: the same `projection.ProgressStore`.
- Produces:
  ```go
  var _ projection.ProgressStore = (*ProgressStore)(nil)
  func NewProgressStore(s *Store) *ProgressStore
  ```

- [ ] **Step 1: Write the failing tests**

Schema (`scope`, `projection`, `position`, `state`, `updated_at`, PK), save/load, monotonic, reset, set-status, scope isolation. Update the latest migration version from 14 to 15.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./sqlite -run 'TestSQLiteProjectionCheckpoints|TestSQLiteProgressStore|TestSQLiteMigration' -v`
Expected: no table; `NewProgressStore` undefined.

- [ ] **Step 3: Implement**

The SQLite table uses `scope` and `updated_at TEXT` (the store’s timestamp format). The same upsert semantics, using `s.db` directly: `SetStatus` updates only `state` (and the timestamp) when the row exists. No RLS, no notification. Update the migration-count tests.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./sqlite -run 'TestSQLiteProjectionCheckpoints|TestSQLiteProgressStore|TestSQLiteMigration' -v`
Expected: PASS, then `go test ./sqlite`.

- [ ] **Step 5: Commit**

```bash
git add sqlite/migrations/0015_projection_checkpoints.sql sqlite/progress.go sqlite/progress_test.go sqlite/migrate_test.go sqlite/populated_test.go
git commit -m "feat(sqlite): add the projection checkpoint store"
```

---

### Task 7: Postgres cross-process notifications

**Files:**
- Modify: `postgres/progress.go` (constructor/publish)
- Create: `postgres/progress_notify.go`, `postgres/progress_notify_test.go`
- Test: use the existing `listenConn`/`assertNoNotification` helpers from `postgres/notify_integration_test.go` where possible.

**Interfaces:**
- Consumes: `postgres.ProgressStore`, `postgres.NotifyListener`, `projection.Checkpoint`.
- Produces:
  ```go
  func NewProgressStore(s *Store, channel string) *ProgressStore
  func (p *ProgressStore) Close() error

  type ScopeRefresher interface {
      EnqueueScopeRefresh(scope string)
      EnqueueRefreshAll()
  }
  func NewProgressListener(dsn, channel string, r ScopeRefresher) *NotifyListener
  ```

- [ ] **Step 1: Write the failing tests**

- A committed `SaveProgress` marks the scope; the notifier flushes one `pg_notify(channel, scope)` within the window (poll with a deadline).
- A committed `ResetProgress` or `SetStatus` also marks the scope: a rebuild or status change must wake remote waiters, not wait for the fallback.
- Two saves on the same scope inside one window produce one notification; a lower same-state save (the upsert does not move the position) emits no hint at all.
- The live path's per-event saves coalesce per scope: many close saves produce one notification per window, so hint volume does not scale with the save rate.
- A save that never commits (a failed or rolled-back transaction) produces no notification: `Mark` runs only after `runWorkspace` returns nil.
- A failing notifier flush (injected through the notifier constructor seam, not by mutating a live goroutine) does not fail `SaveProgress` and leaves the stored row intact.
- A scope longer than the notify limit is saved without a hint (counted as dropped), never an error.
- Two `SaveProgress` calls on the same scope inside one window produce one notification (drive coalescing through the store, not `Mark` directly).
- `Mark` after `Close` is a no-op; the pending map stays empty and nothing is flushed.
- `NewProgressStore` panics on an invalid non-empty channel, matching `NewNotifyListener`.
- `NewProgressListener`'s `OnNotify(payload)` calls `EnqueueScopeRefresh(payload)`; `OnReconnected` calls `EnqueueRefreshAll`; start the listener with `Run`, save, and wait on a fake refresher's channel.
- The payload carries no position, so a forged notification can only trigger a refresh; the waiter's position always comes from the store.

- [ ] **Step 2: Run to verify failure**

Run: `PG_DSN=... go test ./postgres -run 'TestProgressNotify' -v`
Expected: the constructor accepts a channel but ignores it; `NewProgressListener` and the notification are missing.

- [ ] **Step 3: Implement**

Use the constructor’s `channel` (Task 5 already accepts it). **Emit after commit, never inside the save transaction.** Each of `SaveProgress`, `ResetProgress` and `SetStatus` returns from `runWorkspace` nil and then calls `Mark(scope)` on a bounded notifier started by `NewProgressStore`; `SaveProgress` adds `RETURNING position` and skips `Mark` when the returned position is greater than the requested one (a lower same-state save moved nothing). `Close()` stops it. The notifier dedupes scopes in a map, flushes every 10 ms (or when the map is full) with one statement:

```sql
SELECT pg_notify($1, s) FROM unnest($2::text[]) AS s
```

A flush error is counted and the batch dropped — the checkpoint row is already committed, so a hint can never fail or roll back the save. An oversized scope (>512 bytes) is skipped and counted the same way. An empty channel disables the notifier entirely. Expose the dropped/failed count (for example `NotifyDropped() uint64`) so the runbook's metric is reachable; `Close` sets a closed flag so a later `Mark` is a no-op rather than an unflushable map entry.

`NewProgressListener(dsn, channel, r ScopeRefresher)` wraps the existing `NotifyListener` with a handler whose `OnNotify(payload string)` calls `r.EnqueueScopeRefresh(payload)` and whose `OnReconnected()` calls `r.EnqueueRefreshAll()`. `NotifyListener` passes the raw payload, and the payload is the scope — the same shape as the two existing channels, so its 10 ms per-scope throttle coalesces a burst. The handler must not block: both calls are bounded, non-blocking queue writes; the store read happens on the waiter's worker, never on the listener goroutine.

Add a `Close` path (store or notifier) that flushes once, best-effort, and stops the goroutine; the waiter tests use it to satisfy `goleak`.

- [ ] **Step 4: Run to verify pass**

Run: `PG_DSN=... go test ./postgres -run 'TestProgressNotify|TestProjectionCheckpoints|TestPostgresProgressStore' -v`
Expected: PASS, then the full Postgres suite.

- [ ] **Step 5: Commit**

```bash
git add postgres/progress.go postgres/progress_test.go postgres/progress_notify.go postgres/progress_notify_test.go
git commit -m "feat(postgres): notify checkpoint advances"
```

---

### Task 8: Rebuild status, waiter integration and example

**Files:**
- Test: `projection/rebuild_progress_test.go`
- Create: `examples/readwrite/main.go`, `examples/readwrite/main_test.go`
- Test: `eventfeed` wiring note/test if the example uses the feed path.

**Interfaces:**
- Consumes: `projection.Runner`, `projection.Waiter`, `sqlite.NewProgressStore`, `es.TokenFor`.
- Produces: a runnable example and a tested rebuild/wait flow.

- [ ] **Step 1: Write the failing tests**

`projection/rebuild_progress_test.go`:
- Reset a runner with a memory progress store, then advance the checkpoint to a position **above** the token while the state is still `rebuilding`, and clear a fake read model; a `WaitFor` for that token must return `Reached:false, Status:rebuilding` — the position alone would satisfy it, so the test isolates the status gate.
- Rebuild over a two-event source; assert the state becomes `live` and a wait for the last position is reached.
- A source error leaves the state `rebuilding`.

`examples/readwrite/main_test.go`:
- Append two events to a SQLite store with a projection runner; take the token; `WaitFor`; read the projection; assert the rows and `Reached && live`. A no-op token is not exercised here — the example has no command runtime; Task 1 owns that case.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./projection -run 'TestRebuildProgress' -v` and `go test ./examples/readwrite -v`
Expected: the rebuild test may already pass once Task 3 is in; the example does not exist.

- [ ] **Step 3: Implement**

Add the rebuild test wiring: `Reset` before clearing the model; `Rebuild`; `WaitFor`. The example uses `sqlite.Open`, a `projection.Runner` whose `Dispatch` updates an in-memory list, `runner.ProgressStore = sqlite.NewProgressStore(store)`, `runner.Scope = ""` (SQLite is single-workspace and its envelopes carry an empty workspace), and a `projection.NewWaiter(progressStore)` assigned to `runner.Progress`. It prints the token, the wait result and the list.

Add a short `eventfeed` note in the example README: the `eventfeed` server is the delivery path; the runner’s progress store is the applied checkpoint, and `WaitFor` uses the latter.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./projection -run 'TestRebuildProgress' -v` and `go test ./examples/readwrite -v`
Expected: PASS. Then `go test ./projection ./examples/...`.

- [ ] **Step 5: Commit**

```bash
git add projection/rebuild_progress_test.go examples/readwrite
git commit -m "feat(projection): gate rebuilds and add a read-your-writes example"
```

---

### Task 9: ADR, runbook and README

**Files:**
- Create: `docs/adr/0045-read-your-writes.md`
- Modify: `docs/adr/README.md`, `docs/deploy/README.md`, `README.md`
- Test: relative-link check and `git diff --check`

**Interfaces:**
- Consumes: the whole feature.
- Produces: the operator-facing record.

- [ ] **Step 1: Write the ADR**

Follow the ADR 0044 shape: context, decision (token, applied checkpoint vs delivery cursor, write-through guarantee, scope, status/blue-green, Postgres NOTIFY), consequences (one extra checkpoint write per `Apply` — per event on the live path — grants, rebuild contract), alternatives (NATS, same-transaction checkpoint, token epoch, feed-checkpoint reuse), rollout (migrations 0017/0015, version v0.24.0, example).

- [ ] **Step 2: Update the runbook**

Add:
- the `projection_checkpoints` table, RLS and grants: the projection runner needs `SELECT, INSERT, UPDATE`; the query/waiter service needs `SELECT`; no `DELETE` on the normal path. State the placement contract: co-locate the table with the read model (one pool for the query service) or grant the query service `SELECT` on the event database — and say which one the deployment uses;
- the notification channel/role: the projection process and waiters need `LISTEN`/`NOTIFY` privileges (Postgres grants `NOTIFY` on the database and `LISTEN` on the channel by default for a login role; document the deployment check). The progress hint is emitted **after commit, best-effort**; a failed hint never fails the checkpoint write;
- the kill switch: an empty progress channel disables hints, and the fallback read still satisfies waits. When `pg_notification_queue_usage()` alarms, disable hints first;
- the connection budget: one dedicated backend per channel per listening process (`processes_that_listen × channels`); document the gateway/relay or multi-channel-listener option, and the PgBouncer session/direct requirement;
- health and metrics to export: listener PID (0 = disconnected), queue usage, dropped hints, fallback reads, active waiters; alert when a listener stays disconnected;
- the rebuild order: set `rebuilding`, clear/swap the read model, replay, set `live`; blue/green or quiesced reads;
- backup: `projection_checkpoints` is derived progress, not state; a restore resets/replays it; do not treat it as a backup set.

- [ ] **Step 3: Update the README**

Mention the token/`WaitFor` capability in the responsibilities table and the security model (tokens are position hints, not authorization; scope comes from the authenticated workspace). Keep the Documentation link and the read-your-writes pointer that already exist in the README from the plan commit.

- [ ] **Step 4: Verify**

Run the repo’s relative-link check over the changed docs and `git diff --check`; then `task test` once.

- [ ] **Step 5: Commit**

```bash
git add docs/adr/0045-read-your-writes.md docs/adr/README.md docs/deploy/README.md README.md
git commit -m "docs: record read-your-writes and checkpoint progress"
```

---

### Task 10: Usage guide and query middleware

**Files:**
- Create: `docs/guides/read-your-writes.md`
- Modify: `README.md` (Documentation list and the “Projections and reactions” pointer)
- Create: `examples/readwrite/middleware.go`, `examples/readwrite/middleware_test.go`
- Test: relative links and `git diff --check`

**Interfaces:**
- Consumes: everything from Tasks 1–9.
- Produces: the user-facing integration recipe and a reusable HTTP middleware in the example.

- [ ] **Step 1: Write the guide**

The guide already exists (committed with the plan); update it against the shipped API, keeping this coverage, in this order:
- the contract: what `Reached` means, scope, the deadline, rebuilds;
- the four integration steps: command returns the token → client sends `ES-Consistency` → `WaitFor` → read the same read model;
- **the dependency model**: tokenless requests never touch the store, a warm cache needs no read, one `LISTEN` per process, no connection held while blocked, and co-locating `projection_checkpoints` with the read model;
- fail-open with `WithBestEffort` and the `ES-Consistent` response header;
- a failure table: timeout, checkpoint store down, `rebuilding`, token from another workspace, projection not running;
- several projections in one response and `MaxToken` on the client;
- sizing: 100 waiters on a key = 1 notification, 0 reads on wake, 1 refresher, no extra connections;
- a “test your integration” loop: 1,000 handle→wait→query iterations with zero stale reads when `Reached && live`;
- when **not** to use it: clients with no token, analytics over historical data, cross-region reads.

- [ ] **Step 2: Add the middleware and its test**

`examples/readwrite/middleware.go` exports:

```go
const ConsistencyHeader = "ES-Consistency"
const ConsistentHeader = "ES-Consistent"

// WaitFor wraps next: it parses the token, resolves the authenticated scope
// with scopeOf (the example reads it from the request context), checks the
// token's scope, waits on the named projection and sets ES-Consistent.
// Missing header = no wait, no store access and no ES-Consistent claim.
// Invalid token = 400. Scope mismatch = 403. With WithBestEffort a store
// failure still calls next with ES-Consistent:false.
func WaitFor(w *projection.Waiter, projectionName string, scopeOf func(*http.Request) (string, error), timeout time.Duration, next http.Handler) http.Handler
```

`middleware_test.go` asserts: no header → `next` runs with no `LoadProgress` call (counting store); a reached token → `next` sees `ES-Consistent: true`; a timeout → `false`; a malformed token → 400; a token for another workspace → 403.

- [ ] **Step 3: Link the guide**

Add it to the README Documentation list and point to it from “Projections and reactions”.

- [ ] **Step 4: Verify**

Run the relative-link check over the changed docs, `git diff --check`, and `go test ./examples/readwrite`.

- [ ] **Step 5: Commit**

```bash
git add docs/guides/read-your-writes.md README.md examples/readwrite/middleware.go examples/readwrite/middleware_test.go
git commit -m "docs: add the read-your-writes usage guide and middleware"
```

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-11-25-read-your-writes.md`. The spec is `docs/proposals/es-lite-read-your-writes.md`. Tasks 1–4 build the core; 5–7 add the Postgres/SQLite stores and notification; 8–10 add the rebuild flow, example, ADR and usage guide. The interfaces are shared across tasks (token → progress → runner → waiter), so implement them in order. The Postgres tasks need the test DSN on `:5434`; SQLite and the core are local.
