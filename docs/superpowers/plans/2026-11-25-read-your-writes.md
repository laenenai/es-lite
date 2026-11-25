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
- **Notification volume.** One notification per checkpoint write: `Apply`/`Rebuild` save once per batch/page, and a save that does not move the position emits nothing.
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

`es/token.go`: a 1-byte version, u16 scope length, scope, u64 position; base64url. `ParseToken("")` returns the zero token; any other length/version is `ErrInvalidToken`. `TokenFor` takes the highest `AppendResult.Position` (0 if none). `MaxToken` treats a zero token as compatible with any scope (returns the other one) and rejects two different non-empty scopes with `ErrScopeMismatch`.

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
- `Reset` sets position 0/state `rebuilding`, clears the runner’s cached marker, and publishes the status.
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
- **Batch coalescing.** Split dispatch from save: `Apply` dispatches every envelope in the batch, tracks the last successful position, and calls `advance` **once** at the end — and once with the last successful position before returning a dispatch error (otherwise a redelivery after a partial batch restarts the page). `Rebuild` reuses `Apply`, so a rebuild saves and notifies once per page. `Run`/`Feed` still delivers one envelope at a time and saves per event; that is acceptable because the Postgres store suppresses a notify when the stored position does not move (Task 7) and the listener throttles per scope at 10 ms. Keep the in-memory `r.marker` updated per applied envelope so an in-process redelivery still skips. Call `OnApplied` for every applied envelope **after** the batch save, preserving its documented “called after the marker is saved” ordering while still firing once per envelope.
- `Reset`: `ResetProgress(ctx, r.Scope, r.Name, StatusRebuilding)` when the store is set, otherwise `Discard(r.Name)` if the `Marker` implements `interface{ Discard(string) }` and then `Marker.Save(...,0)`; set `r.state=StatusRebuilding`, `r.marker=0`, `r.loaded=true`; call `Progress.SetStatus(..., StatusRebuilding)`.
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
  func WithFallback(d time.Duration) WaiterOption // default 100ms, jittered
  func WithBestEffort() WaiterOption              // store errors -> Reached:false

  func NewWaiter(store ProgressStore, opts ...WaiterOption) *Waiter
  func (w *Waiter) WaitFor(ctx context.Context, scope, projection string, tok es.Token, timeout time.Duration) (WaitResult, error)
  func (w *Waiter) Checkpoint(ctx context.Context, scope, projection string) (Checkpoint, bool, error)
  func (w *Waiter) Status(ctx context.Context, scope, projection string) (Status, error)
  func (w *Waiter) Advance(cp Checkpoint)   // ProgressPublisher
  func (w *Waiter) SetStatus(scope, projection string, state Status)
  ```

- [ ] **Step 1: Write the failing tests**

Table tests in `projection/wait_test.go`:
- zero token → `Reached:true`, no store read;
- already satisfied → returns without blocking;
- `Advance` past the token releases a waiter, an advance below it does not;
- the register/re-check race: loop 1,000 times, `Advance` between the fast path and registration, every wait returns reached (`-race`);
- timeout 20 ms → `Reached:false`, `err == nil`, `Waited` ≥ the timeout;
- context cancellation → `context.Canceled`;
- a token for another scope → `errors.Is(err, es.ErrScopeMismatch)`; a token with the empty scope is accepted when the caller scope is empty (SQLite) and rejected against a real workspace;
- a checkpoint at a position at or above the token but in `rebuilding` or `stalled` → `Reached:false`, the matching `Status`, no timeout (the status check must beat the position check);
- a missing checkpoint row → does not short-circuit: it waits and is released by an `Advance`; `Status` on a missing row returns `StatusUnknown` with a nil error;
- a lost notification: do not call `Advance`; advance the memory store behind the waiter and set `WithFallback(5*time.Millisecond)`; the wait returns reached;
- a shared refresher: after warming the cache, 100 waiters on one key waiting for a position above it, with a 5 ms fallback, perform roughly one store read per tick, not one per waiter (count `LoadProgress` calls);
- best effort: a store returning an error makes the default `WaitFor` return it, and `WithBestEffort` return `Reached:false, err:nil`; `ErrScopeMismatch` and `context.Canceled` still fail under `WithBestEffort`;
- 10,000 concurrent waiters on one key are all released by one `Advance`; `goleak.VerifyNone(t)` at the end;
- `Status` returns the stored state when the cache is cold.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./projection -run 'TestWaiter' -v`
Expected: build failure — `Waiter`/`WaitResult` do not exist.

- [ ] **Step 3: Implement**

A hub keyed by `(scope, projection)` holding the cached `Checkpoint` and a `chan struct{}` that is closed and replaced on each advance. `WaitFor`:
1. A zero token (empty scope and position) returns reached immediately.
2. If `tok.Scope != scope` → `es.ErrScopeMismatch`. An empty token scope matches only an empty caller scope, which is the SQLite case; a workspace token never matches another workspace.
3. Read the cache; on a miss, `store.LoadProgress`. A missing row (`found=false`) is position 0 in a live-like state: continue waiting, do not short-circuit. If the state is `rebuilding` or `stalled`, return false immediately with that status. If the position is at or above the token, return reached.
4. Lock, register the key’s channel, unlock, re-read; if the advance happened, return reached.
5. Loop on `select` of the key's channel, `ctx.Done()`, the key's refresher signal and the deadline. On each wake, read the cache. The refresher, not the waiter, loads from the store and `Advance`s it.
6. Remove the registration; stop the key's refresher when its waiter count reaches zero.

Clamp a timeout above 5 s to 5 s; `0` uses 500 ms. `Advance` ignores a checkpoint with a lower position in the same state and replaces on a state change. `SetStatus` updates the cache and broadcasts. `Status` returns `StatusUnknown` when no row exists.

**The fallback is hub-level, not waiter-level.** The first waiter on a key starts one refresher goroutine (jittered ≤100 ms) that serves every waiter on that key and stops when the last one leaves. 100 waiters on one key cost one store read per tick, not 1,000 per second, and the refresher is the only place that re-reads after the initial lookup. A refresher read error is not returned to anyone: it retries on the next tick, and `WithBestEffort` only governs the waiter's own lookup. Waiting never holds a pooled connection: the initial `LoadProgress` returns it, and the waiter blocks on Go channels only.

`WithBestEffort()` converts store errors (but not `ErrInvalidToken`, `ErrScopeMismatch` or context cancellation) into `Reached:false, err:nil`, so a query endpoint can fail open to stale data. It is for the read path; commands and health checks should keep the default.

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
  func NewProgressListener(dsn, channel string, advance func(projection.Checkpoint)) *NotifyListener
  ```

- [ ] **Step 1: Write the failing tests**

- A committed `SaveProgress` sends one JSON notification on `channel` with the scope, projection, position and state.
- A rolled-back transaction sends none: run the same upsert+notify statements in a transaction, `ROLLBACK`, and assert no notification arrives. An invalid-state failure is a separate validation test — it fails before the notify and does not prove `pg_notify` is transactional.
- Saving the same position *and* state twice emits one notification; the second save is a no-op. Run the two saves sequentially; two concurrent first inserts may legitimately both notify, which is harmless.
- A batch save from Task 3 (`Apply` over three envelopes) produces one notification, not three.
- `NewProgressListener` decodes the payload and calls `advance` with the `Checkpoint`; start it with `Run`, save, and wait on a channel.
- A notification for another scope/projection does not call the wrong advance callback (the callback receives the payload; the waiter keys it).

- [ ] **Step 2: Run to verify failure**

Run: `PG_DSN=... go test ./postgres -run 'TestProgressNotify' -v`
Expected: the constructor accepts a channel but ignores it; `NewProgressListener` and the notification are missing.

- [ ] **Step 3: Implement**

Use the constructor’s `channel` (Task 5 already accepts it). In the `runWorkspace` callback, read the current `(position, state)` in the same transaction (`SELECT ... FOR UPDATE`), then upsert, then call `SELECT pg_notify($1,$2)` with a JSON payload **only when the position or state actually changed**; `pg_notify` is transactional, so a rollback sends nothing. Skip when the channel is empty or the row did not move. Cap the payload at the NOTIFY limit; return an error if it is over 7 KB (the workspace/projection lengths are not otherwise bounded by the library).

`NewProgressListener` wraps the existing `NotifyListener` with a handler whose `OnNotify(payload string)` unmarshals the JSON and calls `advance`. (`NotifyListener` passes the raw payload to `OnNotify`; the parameter is named `scope` in the feed use, but nothing validates that shape.) Its `OnReconnected` is a no-op: the 100 ms fallback repairs a notification lost during a reconnect. It validates the channel through `NotifyListener` and returns a listener the caller runs.

The listener's 10 ms throttle keys on the whole payload, so it drops exact duplicates but not distinct positions. Notification volume is bounded by the runner's batch save (Task 3) and the no-op suppression above, not by the listener.

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

Follow the ADR 0044 shape: context, decision (token, applied checkpoint vs delivery cursor, write-through guarantee, scope, status/blue-green, Postgres NOTIFY), consequences (one extra checkpoint write per batch, grants, rebuild contract), alternatives (NATS, same-transaction checkpoint, token epoch, feed-checkpoint reuse), rollout (migrations 0017/0015, version v0.24.0, example).

- [ ] **Step 2: Update the runbook**

Add:
- the `projection_checkpoints` table, RLS and grants: the projection runner needs `SELECT, INSERT, UPDATE`; the query/waiter service needs `SELECT`; no `DELETE` on the normal path. State the placement contract: co-locate the table with the read model (one pool for the query service) or grant the query service `SELECT` on the event database — and say which one the deployment uses;
- the notification channel/role: the projection process and waiters need `LISTEN`/`NOTIFY` privileges (Postgres grants `NOTIFY` on the database and `LISTEN` on the channel by default for a login role; document the deployment check);
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
