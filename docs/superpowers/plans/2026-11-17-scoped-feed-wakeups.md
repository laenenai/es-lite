# Scoped Feed Wake-Ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a committed append wake only the feed streams of its workspace, so an idle subscriber sees it in single-digit milliseconds instead of waiting out the 250 ms poll, without changing delivery semantics or the default append path.

**Architecture:** Add an optional `feed.ScopedWaiter` interface and route the server's idle wait through the stream's `Request.Scope`. Ship an in-memory `feed.Hub` implementing both `Waiter` and `ScopedWaiter` with a close-and-remove generation per scope. Wake sources stay outside the default append path: an app hint via `es.WithAfterAppend` (recommended) or an opt-in Postgres `LISTEN/NOTIFY` source whose write-path cost is confined to deployments that enable it.

**Tech Stack:** Go 1.27.3, pgx v5.10.0, standard library only. No new module dependencies.

**Spec:** `docs/adr/0039-scoped-feed-wakeups.md` (revision 2; the panel review is `docs/review/20261117_adr0039_panel_review.md`). The plan argues from the ADR; executors read both. Section references like §3.2 point at the ADR.

## Global Constraints

- **No new module dependencies.** The Hub is stdlib; `pgx` already provides `WaitForNotification`.
- **The default append path is unchanged.** `WithAppendNotify` off means no additional statement, no additional lock, no behavioural change. C2's commit-time notify-queue lock is accepted only when a deployment opts in (ADR §2, §6).
- **A wake is a hint.** Correctness remains the horizon fence (ADR 0037). A missed, late or duplicated wake may cost at most one `Poll`; it may never lose an event.
- **`feed` stays storage- and transport-neutral.** It must not import `postgres`; the new `postgres.NotifyListener` takes a `NotifyHandler`, not `*feed.Hub`.
- **`WithAfterAppend` recovers a panic from its callback.** The append is already durable; a hint failure must never surface as a failed append (ADR §3.3, §6).
- **The Hub's `Wake` closes and removes the scope entry.** Never "replace and leave" — that leaks a generation per workspace (ADR §3.2, panel CTO-2).
- **The NOTIFY channel is a capability.** Validate it as a strict PostgreSQL identifier; never log it. The payload is the workspace ID and nothing else (ADR §6).
- **No schema change, no new grants, no RLS bypass arm.**
- **Tests:** plain `GOWORK=off go test ./...` must stay green without services; Postgres suites run via `task test:pg` (Docker `postgres:17`) and skip without `PG_DSN`. Race runs use `task test:race`.
- **Conventional commits** matching history (`feat(feed): ...`, `test(postgres): ...`, `docs: ...`).

## Review Focus

The failure modes the ADR implies but no existing test exercises, most likely first. Each is pinned by the named test in its task.

1. A `Waiter` decorator implements only `Wait`, silently downgrading a scoped deployment to global wakes — `TestUnscopedDecoratorDoesNotForwardScope` (Task 1).
2. A wake lands between a waiter parking and exiting, leaving a permanent map entry — `TestHubWakeDuringParkLeavesNoEntry` (Task 2).
3. A panic in the after-append hook turns a committed append into a caller-visible failure — `TestWithAfterAppendRecoversPanic` (Task 3).
4. A claim-only append (zero envelopes) panics the documented hook, or panics a wake callback that indexes `Envelopes[0]` — `TestWithAfterAppendClaimOnlyAppend` (Task 3).
5. A listener reconnect stops delivering wakes because `LISTEN` was not re-issued or `WakeAll` was not called — `TestNotifyListenerReconnectsAndWakesAll` (Task 7).

---

## Phase A — the scoped seam

### Task 1: `ScopedWaiter` and scoped server routing

**Files:**
- Modify: `feed/ports.go` (add `ScopedWaiter`; extend the `Waiter` doc)
- Modify: `feed/server.go` (`wait`, `idleWait`, call sites in `stream`)
- Test: `feed/server_test.go` (external package), `feed/server_internal_test.go` (internal package)

**Interfaces:**
- Consumes: `Waiter` (`feed/ports.go:192-194`), `Request.Scope` (`feed/ports.go:267-269`), `Server.stream`'s `req` (`feed/server.go:389`).
- Produces: `feed.ScopedWaiter` with `WaitScope(ctx context.Context, scope string, timeout time.Duration) (woke bool, err error)`; unexported `func (s *Server) waitFor(ctx context.Context, scope string, timeout time.Duration) (bool, error)`; `wait(ctx, scope string) error`; `idleWait(ctx, iterStart time.Time, scope string) error`.

- [ ] **Step 1: Write the failing external tests** in `feed/server_test.go`:
  - `scopedWakingWaiter{delay time.Duration}` implements both `Wait` (records calls) and `WaitScope` (records the scopes; returns `(true, nil)` after `delay`), guarded by a mutex with `scopesSnapshot()` and `waitCalls()`.
  - `TestScopedWaiterReceivesStreamScope`: empty `newMemLogs()`, `Poll: 250ms`, `Waiter: &scopedWakingWaiter{delay: 10ms}`, stream started with `testRequest("live")` (Scope `"tenant"`). Wait until one `WaitScope` call; assert the recorded scope is exactly `"tenant"`. Then wait for the second log read and assert it happened within 100 ms (a genuine wake bypasses the 250 ms floor). Cancel and assert `context.Canceled`.
  - `TestUnscopedDecoratorDoesNotForwardScope` (Review Focus 1): `type waitOnlyWaiter struct{ inner *scopedWakingWaiter }` implements only `Wait`, delegating to `inner.Wait`. Pass it as `Config.Waiter`. Assert `len(inner.scopesSnapshot()) == 0` and `inner.waitCalls() > 0` after the first idle wait.
  - `TestScopedWaiterErrorStopsStream`: a `WaitScope` returning `errors.New("wake source failed")` makes `Subscribe` return an error matching that cause.
- [ ] **Step 2: Write the failing internal test** in `feed/server_internal_test.go`:
  - `scopedInstantWaiter` returns `(false, nil)` immediately and records the scope.
  - `TestScopedWaiterTimeoutKeepsFloor`: pin jitter to identity (`srv.jitter = func(d time.Duration) time.Duration { return d }`), `Poll: 100ms`, empty `idlePageLog`; assert consecutive page reads span at least `Poll` and the waiter saw scope `"tenant"`.
- [ ] **Step 3: Run** `GOWORK=off go test ./feed/...` — expected: FAIL (`ScopedWaiter` undefined; `waitFor` missing; signature mismatch).
- [ ] **Step 4: Implement.**
  - In `feed/ports.go`, add after `Waiter`:
    ```go
    type ScopedWaiter interface {
        WaitScope(ctx context.Context, scope string, timeout time.Duration) (woke bool, err error)
    }
    ```
    Extend the `Waiter` doc with: a decorator that wraps a `Waiter` must forward `WaitScope` when the wrapped value implements it; embedding is the safe pattern; a `Wait`-only decorator silently downgrades to global wakes.
  - In `feed/server.go`, change the call sites: `s.wait(ctx)` → `s.wait(ctx, req.Scope)` (`:503`), `s.idleWait(ctx, iterStart)` → `s.idleWait(ctx, iterStart, req.Scope)` (`:573`).
  - Add:
    ```go
    func (s *Server) waitFor(ctx context.Context, scope string, timeout time.Duration) (bool, error) {
        if sw, ok := s.cfg.Waiter.(ScopedWaiter); ok {
            return sw.WaitScope(ctx, scope, timeout)
        }
        return s.cfg.Waiter.Wait(ctx, timeout)
    }
    ```
  - `wait` discards `woke`: `_, err := s.waitFor(ctx, scope, s.cfg.Poll)`. `idleWait` uses `woke, err := s.waitFor(ctx, scope, d)` and keeps the existing floor and jitter logic unchanged.
- [ ] **Step 5: Run** `GOWORK=off go test -race ./feed/...` — expected: PASS, including every existing `Waiter` test unchanged.
- [ ] **Step 6: Commit** — `feat(feed): route idle waits by subscription scope`

### Task 2: `feed.Hub`

**Files:**
- Create: `feed/hub.go`
- Test: `feed/hub_test.go` (`package feed`, internal, so the map size is observable)

**Interfaces:**
- Consumes: `Waiter`, `ScopedWaiter` from Task 1.
- Produces: `NewHub() *Hub`; `(*Hub).Wake(scope string)`; `(*Hub).WakeAll()`; `(*Hub).Wait(ctx context.Context, timeout time.Duration) (bool, error)`; `(*Hub).WaitScope(ctx context.Context, scope string, timeout time.Duration) (bool, error)`; `(*Hub).Stats() HubStats`; `type HubStats struct{ Wakes, WakeAlls uint64 }`.

- [ ] **Step 1: Write the failing tests** in `feed/hub_test.go`:
  - `TestHubWakeIsScoped`: park waiters on `"a"` and `"b"`; `Wake("a")`; assert `"a"` returns `(true, nil)` within 100 ms and `"b"` is still blocked; then `Wake("b")`.
  - `TestHubWakeAll`: park waiters on `"a"`, `"b"` and an unscoped `Wait`; `WakeAll`; all three return `(true, nil)`.
  - `TestHubWakeWakesUnscopedWaiter`: an unscoped `Wait` returns on `Wake("a")` (compatibility path).
  - `TestHubWaitScopeTimeout`: `WaitScope(ctx, "a", 10ms)` returns `(false, nil)`.
  - `TestHubWaitScopeCancel`: cancel the context; `WaitScope` returns `(false, context.Canceled)`.
  - `TestHubWakeOnAbsentScopeLeavesMapEmpty`: `Wake("never-registered")` leaves `len(h.scopes) == 0`.
  - `TestHubWakeDuringParkLeavesNoEntry` (Review Focus 2): start a waiter on `"a"`; poll until `h.scopes["a"] != nil`; call `Wake("a")`; wait for the waiter to return; assert `len(h.scopes) == 0`. This is the interleaving the "replace and leave" lifecycle leaks.
  - `TestHubTenThousandScopesLeaveNoEntries`: for 10 000 distinct scopes, call `WaitScope` with an already-cancelled context, and every tenth scope with `Wake` racing it; assert `len(h.scopes) == 0` at the end and `runtime.NumGoroutine()` has not grown by more than 2 over a baseline.
  - `TestHubZeroValuePanics`: `assertPanics` for `(*Hub)(nil).Wake("a")`, `(*Hub)(nil).Wait`, and `var h Hub; h.WakeAll()`.
  - `TestHubStats`: `Wake` twice and `WakeAll` once; `Stats()` returns `Wakes: 3, WakeAlls: 1` (Wake counts calls, not scopes).
  - `TestHubConcurrentWakeAndWait`: 32 goroutines × 200 iterations of `WaitScope` with a 1 ms timeout and a concurrent `Wake`/`WakeAll`; run under `-race`.
- [ ] **Step 2: Run** `GOWORK=off go test ./feed/ -run TestHub -count=1` — expected: FAIL (`Hub` undefined).
- [ ] **Step 3: Implement** `feed/hub.go` with exactly these invariants:
  ```go
  type Hub struct {
      mu      sync.Mutex
      scopes  map[string]*scopeGen
      all     *scopeGen
      wakes   uint64
      wakeAll uint64
  }
  type scopeGen struct {
      ch   chan struct{}
      refs int
  }
  ```
  - `NewHub()`: `scopes: make(map[string]*scopeGen)`, `all: &scopeGen{ch: make(chan struct{})}`.
  - A private `check()` called at the top of every exported method: `if h == nil || h.scopes == nil || h.all == nil { panic("feed: Hub must be created with NewHub") }`.
  - `Wake(scope)`: lock; `h.wakes++`; if `g := h.scopes[scope]; g != nil { close(g.ch); delete(h.scopes, scope) }`; `close(h.all.ch); h.all.ch = make(chan struct{})`; unlock.
  - `WakeAll()`: lock; `h.wakeAll++`; for each `g` in `h.scopes`, `close(g.ch)` and delete; then close and replace `h.all.ch`; unlock.
  - `WaitScope`: lock; get-or-create `g` for the scope; `g.refs++`; `ch := g.ch`; unlock. Defer: lock; `g.refs--`; if `g.refs == 0 && h.scopes[scope] == g { delete(h.scopes, scope) }`; unlock. Then select on `ch`, `time.NewTimer(timeout)`, `ctx.Done()`; return `(true, nil)`, `(false, nil)`, `(false, ctx.Err())` respectively.
  - `Wait`: lock; `ch := h.all.ch`; unlock; select as above without touching `refs`.
  - `Stats`: lock; return a copy of `HubStats{Wakes: h.wakes, WakeAlls: h.wakeAll}`.
  - No method holds the lock while blocking on a channel. `Wake` and `WakeAll` only close and remove/swap.
- [ ] **Step 4: Run** `GOWORK=off go test -race ./feed/ -run TestHub -count=1` — expected: PASS, no leak.
- [ ] **Step 5: Run** `GOWORK=off go test -race ./feed/...` — expected: all feed tests PASS.
- [ ] **Step 6: Commit** — `feat(feed): add a scoped in-memory wake hub`

---

## Phase B — the application hint

### Task 3: `es.WithAfterAppend`

**Files:**
- Create: `es/afterappend.go`
- Test: `es/afterappend_test.go` (`package es_test`)
- Test: `aggregate/afterappend_test.go` (`package aggregate_test`; uses the existing `openStore(t)` SQLite helper)

**Interfaces:**
- Consumes: `es.Store`, `es.AppendResult`.
- Produces: `func WithAfterAppend(store es.Store, fn func(context.Context, es.AppendResult)) es.Store`.

- [ ] **Step 1: Write the failing `es` tests**:
  - Define `appendStub` embedding `es.Store`, with `Append` recording calls and returning a configurable `(AppendResult, error)`, and `ReadStream` recording calls.
  - `TestWithAfterAppendFiresOnceWithResult`: success with one envelope; the hook sees the exact `FromVersion`, `ToVersion` and envelope position; fires exactly once.
  - `TestWithAfterAppendDoesNotFireOnError`: `es.ErrConflict` and a generic error each leave the hook uncalled.
  - `TestWithAfterAppendRecoversPanic` (Review Focus 3): the hook panics; `Append` returns the successful result and does not panic.
  - `TestWithAfterAppendClaimOnlyAppend` (Review Focus 4): `AppendResult{}` with zero envelopes; the hook fires once, and reading `len(res.Envelopes)` is 0 (no panic in the library).
  - `TestWithAfterAppendNilFnReturnsStore`: `WithAfterAppend(base, nil) == base`.
  - `TestWithAfterAppendPassesOtherMethodsThrough`: `wrapped.ReadStream` delegates to the stub exactly once.
  - `TestWithAfterAppendHasNoAppendTx`: `reflect.TypeOf(wrapped).MethodByName("AppendTx")` is invalid.
- [ ] **Step 2: Run** `GOWORK=off go test ./es/ -run TestWithAfterAppend -count=1` — expected: FAIL (`WithAfterAppend` undefined).
- [ ] **Step 3: Implement** `es/afterappend.go`:
  ```go
  type afterAppendStore struct {
      es.Store
      fn func(context.Context, es.AppendResult)
  }
  var _ es.Store = (*afterAppendStore)(nil)

  func WithAfterAppend(store es.Store, fn func(context.Context, es.AppendResult)) es.Store {
      if fn == nil {
          return store
      }
      return &afterAppendStore{Store: store, fn: fn}
  }

  func (s *afterAppendStore) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
      res, err := s.Store.Append(ctx, p)
      if err != nil {
          return res, err
      }
      s.call(ctx, res)
      return res, nil
  }

  func (s *afterAppendStore) call(ctx context.Context, res es.AppendResult) {
      defer func() { _ = recover() }()
      s.fn(ctx, res)
  }
  ```
  The godoc must state: runs once after every successful append, synchronously, after commit and before `Append` returns; never on error; no error return because the append is durable and a failed hint must not look like a failed append; a panic is recovered for the same reason; the workspace must be closed over (a claim-only append has zero envelopes and `Envelope.Workspace` is empty on SQLite); `AppendTx` and `aggregate.WithAppend` are not covered; the wrapper returns `es.Store`, so it erases concrete methods.
- [ ] **Step 4: Run** `GOWORK=off go test -race ./es/...` — expected: PASS.
- [ ] **Step 5: Write the failing `aggregate` test** `TestAfterAppendHookDoesNotFireWhenWithAppendReplacesTheAppend` in `aggregate/afterappend_test.go`:
  - `base := openStore(t)`; `wrapped := es.WithAfterAppend(base, counter)`; `rt := aggregate.NewRuntime(wrapped, counter.Decider, counterv1.EventCodec{})`.
  - `Handle(..., aggregate.WithAppend(base.Append))` for an `Init`; assert the hook was not called.
  - A following `Handle` without `WithAppend` for an `Increment`; assert the hook was called once.
- [ ] **Step 6: Run** `GOWORK=off go test -race ./aggregate/...` — expected: PASS.
- [ ] **Step 7: Commit** — `feat(es): add a post-commit append hint decorator`

---

## Gate — measure C1 before any write-path change

### Task 4: End-to-end C1 latency and read amplification (gate evidence)

**Files:**
- Modify: `postgres/wsstore.go` (add the `FenceDraws` accessor)
- Create: `postgres/feed_latency_test.go` (`package postgres_test`, no build tag; `t.Skip` unless `PG_DSN` is set)

**Interfaces:**
- Consumes: `postgres.Open`, `postgres.NewLogs`, `postgres.NewCheckpointStore`, `postgres.NewLeaseStore`, `feed.NewServer`, `feed.NewHub`, `es.WithAfterAppend`.
- Produces: `func (w *Workspace) FenceDraws() uint64`; `TestFeedWakeLatency`; the `percentile(ds []time.Duration, p float64) time.Duration` helper (nearest-rank); and the test-only `countingLogs` wrapper. Task 8 reuses them.

- [ ] **Step 1: Add the fence-draw accessor** to `postgres/wsstore.go`:
  ```go
  // FenceDraws reports the fence transaction ids this Workspace's rounds have
  // drawn (the F9 counter). It is diagnostic only: reading it takes the same
  // mutex a read round does, and it changes no read or append behaviour.
  func (w *Workspace) FenceDraws() uint64 {
      w.mu.Lock()
      defer w.mu.Unlock()
      return w.draws
  }
  ```
- [ ] **Step 2: Build the harness.** One test, three subtests (`poll`, `hub`, `contended`), 50 iterations each:
  - Open one store per subtest with `postgres.Open(ctx, dsn)`; `wsID := "lat-" + subtest`; `ws := store.WorkspaceStore(wsID)`; `hub := feed.NewHub()`.
  - `countingLogs{inner: postgres.NewLogs(store)}` wraps the logs: `Open` records every `feed.Log` it returns; `fenceDraws()` sums `FenceDraws()` across the recorded logs via `interface{ FenceDraws() uint64 }`. Use it as `feed.Config.Logs`, plus `postgres.NewCheckpointStore(store)`, `postgres.NewLeaseStore(store)`, `Waiter` per subtest, `Poll: 250ms`.
  - Matching request: `Scope: wsID`, `Projection: "latency.view"`, `Instance: "i"`, `Owner: "latency-owner"`, `Definition: projection.Definition{Name: "latency.view", Consumes: []string{"latency.v1.Event"}}`.
  - Second, non-matching subscription: `Projection: "latency.other"`, `Instance: "i"`, `Consumes: []string{"latency.v1.Other"}`, a `nullSink`; it never delivers. It exists so the wake's read amplification is measurable: every append wakes it too, and it draws a fence for an empty page.
  - `envelopeSink{ch chan delivered}` where `delivered{at time.Time; position uint64}`; `Send` records `time.Now()` and the position, then sends.
  - Subscribe both in goroutines; a test loop receives from the matching sink, acks via `srv.Checkpoint(ctx, feed.Ack{Scope: wsID, Projection: "latency.view", Instance: "i", Owner: "latency-owner", Position: d.position, Definition: def.Hash()})`, and records `d.at.Sub(appendAt)`.
  - Warm up with one append/delivery before measuring.
  - Between iterations `time.Sleep(30 * time.Millisecond)` so the streams are parked; then `appendAt := time.Now()` and `appendStore.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: v, Events: []es.EventData{{TypeURL: "latency.v1.Event", SchemaVersion: 1, Payload: []byte("x")}}})`.
  - `poll`: `Waiter: nil`; `appendStore := es.Store(ws)`.
  - `hub`: `Waiter: hub`; `appendStore := es.WithAfterAppend(ws, func(context.Context, es.AppendResult) { hub.Wake(wsID) })`.
  - `contended`: same as `hub`, but before each append open `tx, _ := store.Pool().Begin(ctx)` and `tx.Exec(ctx, "SELECT pg_current_xact_id()")`; commit it 50 ms after the append (`time.AfterFunc`). This pins the fence, which is the point.
  - Assert: `poll` p50 ≥ 100 ms (the poll is real); `hub` p50 < 10 ms; `contended` p50 ≥ 50 ms (the fence engaged) and p99 ≤ 2×`Poll`+150 ms. `t.Logf` p50/p99 and `fenceDraws()/iterations` for all three. The `hub` draws/append minus the `poll` draws/append is the wake-driven read amplification for the two subscriptions.
- [ ] **Step 3: Run** `task test:pg` — expected: PASS. Copy the three p50/p99 pairs and draws/append into the PR description; Task 9 records them in the ADR.
- [ ] **Step 4: Commit** — `test(postgres): measure scoped-wake latency and read amplification`

> **GATE — do not start Task 6 until both hold:** Task 4's `hub` p50 is below 10 ms on the local Postgres, and Task 5's C2 operating model is merged. If the p50 misses, stop and revisit the ADR before touching the append path.

### Task 5: C2 operating model in the runbook (gate document)

**Files:**
- Modify: `docs/deploy/README.md`

**Interfaces:**
- Produces: the operational preconditions Task 6/7 implement against. No code.

- [ ] **Step 1: Add a "Feed wake-ups" section** with these exact rules:
  - Two sources: C1 (application hint, recommended) and C2 (Postgres `LISTEN/NOTIFY`, opt-in).
  - C2 is enabled only when every role that can `CONNECT` to the database is trusted, or the database is dedicated to the append/feed path. A co-tenant database with untrusted roles uses C1.
  - The channel name is a capability: high-entropy, strict identifier, never logged.
  - One listener connection per process; direct or session-pooled (PgBouncer transaction pooling breaks `LISTEN`).
  - Reconnect backoff floor ≥ `Poll`; alert on `pg_notification_queue_usage()`; a transaction that executed NOTIFY cannot be prepared for two-phase commit.
  - When to prefer C1: any shared database, any deployment without a trusted listener role, any append rate where the extra commit-time lock is unwelcome.
- [ ] **Step 2: Verify** `grep -n "Feed wake-ups\|trusted\|pg_notification_queue_usage\|two-phase" docs/deploy/README.md` returns the new section.
- [ ] **Step 3: Commit** — `docs(deploy): define the opt-in feed wake-up operating model`

---

## Phase C — opt-in Postgres hint (gated)

### Task 6: `WithAppendNotify` and the notify statement

**Files:**
- Modify: `postgres/store.go` (`openConfig.notifyChannel`, `WithAppendNotify`, validation, `Store.notifyChannel`)
- Modify: `postgres/wsstore.go` (`append` calls `notifyAppend` after `applyClaims`)
- Test: `postgres/notify_test.go` (`package postgres`, unit; no `PG_DSN`)
- Test: `postgres/notify_integration_test.go` (`package postgres_test`, `PG_DSN`-gated)

**Interfaces:**
- Consumes: `DBTX` (`postgres/wsstore.go:29-33`), `Option`/`openConfig` (`postgres/store.go:43-56`), `Workspace.append` (`postgres/wsstore.go:185-371`).
- Produces: `func WithAppendNotify(channel string) Option`; `func (w *Workspace) notifyAppend(ctx context.Context, db DBTX, p es.AppendParams) error`; `const maxNotifyWorkspaceBytes = 512`; `func validNotifyChannel(s string) error`.

- [ ] **Step 1: Write the failing unit tests** in `postgres/notify_test.go` with a `recordingDBTX` implementing `Exec` (append a record), and `Query`/`QueryRow` that panic:
  - `notifyChannel == ""` → `notifyAppend` issues no `Exec`.
  - channel set, one event → exactly one `Exec` whose SQL contains `pg_notify` and whose args are the channel and `w.ws`.
  - channel set, zero events → no `Exec`.
  - channel set, `len(w.ws) > maxNotifyWorkspaceBytes` → error before any `Exec`.
  - `validNotifyChannel`: `"es_lite_appended"` ok; `""` error; 64-byte name error; `"bad-name"` error; `"select"` ok.
  - `TestOpenConfigRejectsInvalidNotifyChannel`: `cfg := newOpenConfig([]Option{WithAppendNotify("bad-name")}); cfg.validate()` returns an error; `WithAppendNotify("es_lite_appended")` validates.
  Construct the `Workspace` directly: `&Workspace{store: &Store{notifyChannel: ...}, ws: ...}` (same package).
- [ ] **Step 2: Write the failing integration tests** in `postgres/notify_integration_test.go`:
  - `TestAppendNotifyFiresOnCommit`: `postgres.Open(ctx, dsn, postgres.WithAppendNotify(channel))`; open a dedicated `pgx.Conn`, `LISTEN` the channel; `ws.Append` one event; `WaitForNotification` within 50 ms; assert `Channel == channel` and `Payload == wsID`.
  - `TestAppendNotifyRollbackDoesNotFire`: begin a pool tx, `ws.AppendTx`, roll back; no notification within 100 ms.
  - `TestAppendNotifyClaimOnlyDoesNotFire`: a claim-only append (`Events` empty, one `es.Claim`); no notification within 100 ms.
  - `TestAppendNotifyOffDoesNotFire`: open without the option; append; no notification within 100 ms.
  - `TestWithAppendNotifyRejectsInvalidChannel`: `postgres.Open` with `bad-name` returns an error naming the channel.
- [ ] **Step 3: Run** `GOWORK=off go test ./postgres/ -run 'TestNotify|TestWithAppendNotify' -count=1` — expected: FAIL (option and helper undefined).
- [ ] **Step 4: Implement.**
  - `openConfig` gains `notifyChannel string`; `WithAppendNotify(channel string) Option` sets it. Add `func (c openConfig) validate() error` returning the `validNotifyChannel` error.
  - Fail fast: in `Open`, move `cfg := newOpenConfig(opts)` above `pgxpool.New` and call `cfg.validate()` before opening the pool. In `OpenWithPool`, call it after the nil-pool check and before `probePostgresFloor`. Both constructors set `Store.notifyChannel = cfg.notifyChannel`.
  - `Store` gains `notifyChannel string`.
  - `validNotifyChannel`: non-empty; `len(s) <= 63` (NAMEDATALEN−1); first byte a letter or `_`; remaining bytes letters, digits or `_`. Error text names the channel but never logs it elsewhere.
  - `notifyAppend`:
    ```go
    func (w *Workspace) notifyAppend(ctx context.Context, db DBTX, p es.AppendParams) error {
        if w.store.notifyChannel == "" || len(p.Events) == 0 {
            return nil
        }
        if len(w.ws) > maxNotifyWorkspaceBytes {
            return fmt.Errorf("postgres: workspace id is %d bytes, over the %d-byte notify bound", len(w.ws), maxNotifyWorkspaceBytes)
        }
        if _, err := db.Exec(ctx, `SELECT pg_notify($1, $2)`, w.store.notifyChannel, w.ws); err != nil {
            return fmt.Errorf("notify append: %w", err)
        }
        return nil
    }
    ```
    Call it as the last statement of `append`, after `applyClaims` and before the `AppendResult` is returned. A notify error aborts the transaction (the hint is failure-contained by the operator, not by silently committing).
- [ ] **Step 5: Run** the unit tests without a database, then `task test:pg` — expected: all PASS.
- [ ] **Step 6: Run** `GOWORK=off go test -race ./postgres/...` — expected: unit tests PASS; integration skipped without `PG_DSN`.
- [ ] **Step 7: Commit** — `feat(postgres): add an opt-in append notify hint`

### Task 7: `NotifyListener`

**Files:**
- Create: `postgres/notify.go`
- Test: `postgres/notify_listener_test.go` (`package postgres`, `PG_DSN`-gated; internal for the connection PID)

**Interfaces:**
- Consumes: `pgx.ConnectConfig`, `pgx.Identifier.Sanitize`, `*pgx.Conn.WaitForNotification`.
- Produces:
  ```go
  type NotifyHandler interface {
      OnNotify(scope string)
      OnReconnected()
  }
  func NewNotifyListener(dsn, channel string, handler NotifyHandler) *NotifyListener
  func (l *NotifyListener) Run(ctx context.Context) error
  func (l *NotifyListener) PID() uint32 // current backend PID, 0 when disconnected
  ```
  Defaults: reconnect minimum 250 ms, maximum 30 s (exponential, ±20% jitter); per-scope throttle window 10 ms.

- [ ] **Step 1: Write the failing tests:**
  - `TestNotifyListenerDelivers`: store with `WithAppendNotify(channel)`, listener with a recording handler; `Run` in a goroutine; wait for `l.PID() != 0`; append; `OnNotify` receives the workspace within 50 ms.
  - `TestNotifyListenerReconnectsAndWakesAll` (Review Focus 5): after the first delivery, `SELECT pg_terminate_backend($1)` on the listener PID from a separate connection; wait for `OnReconnected`; append again; assert a second `OnNotify`. The reconnect must re-`LISTEN`.
  - `TestNotifyListenerThrottles`: set the unexported `throttle` to 200 ms; append twice within 20 ms; assert exactly one `OnNotify` for that scope (the second is dropped).
  - `TestNotifyListenerQuotesChannel`: a channel named `select` (valid identifier, reserved word) works — append and observe the wake.
  - `TestNotifyListenerStopsOnCancel`: cancel `Run`'s context; `Run` returns and the connection is closed.
- [ ] **Step 2: Run** `task test:pg` — expected: FAIL (`NotifyListener` undefined).
- [ ] **Step 3: Implement** `postgres/notify.go`:
  - `Run`: loop until `ctx.Done()`:
    - `cfg, err := pgx.ParseConfig(l.dsn)`; set `cfg.RuntimeParams["application_name"] = "es-lite-notify"`; `conn, err := pgx.ConnectConfig(ctx, cfg)`.
    - `conn.Exec(ctx, "LISTEN "+pgx.Identifier{l.channel}.Sanitize())`; store `l.pid.Store(uint32(conn.PgConn().PID()))`.
    - If `connected` was already true, call `l.handler.OnReconnected()`; set `connected = true`.
    - Inner loop: `n, err := conn.WaitForNotification(ctx)`; on error, close the connection, `l.pid.Store(0)`, back off and reconnect; on success, `l.deliver(n.Payload)`.
    - On `ctx.Done()`, close the connection and return `ctx.Err()`.
  - `deliver(scope)` applies the throttle (single goroutine, no mutex): `if now.Sub(last[scope]) < l.throttle { return }`; record; prune when `len(last) > 4096` (delete entries older than `2*l.throttle`); then `l.handler.OnNotify(scope)`.
  - `PID()` returns `l.pid.Load()`; it is diagnostic (triage, tests) and never blocks.
  - Backoff: start at `reconnectMin`, double to `reconnectMax`, multiply by `0.8 + 0.4*rand.Float64()`; reset after a successful `LISTEN`.
  - Panic on a nil handler or an invalid channel in `NewNotifyListener` (reuse `validNotifyChannel`); document that `Run` is not safe for concurrent calls and that `OnNotify` must not block (the gateway wires it to `hub.Wake`, which is non-blocking).
- [ ] **Step 4: Run** `task test:pg` — expected: PASS.
- [ ] **Step 5: Run** `task test:race` — expected: PASS.
- [ ] **Step 6: Commit** — `feat(postgres): add a reconnecting notify listener`

### Task 8: C2 latency, chaos reconnect, and the append benchmark

**Files:**
- Modify: `postgres/feed_latency_test.go` (add the `notify` subtest)
- Create: `postgres/feedgate_notify_test.go` (`//go:build feedgate`)

**Interfaces:**
- Consumes: Task 4's harness, `countingLogs`, `percentile`, and `FenceDraws`; `WithAppendNotify` and `NotifyListener` from Tasks 6/7.
- Produces: the numbers Task 9 records.

- [ ] **Step 1: Add the `notify` subtest** to `TestFeedWakeLatency`: open the store with `WithAppendNotify(channel)`, start `postgres.NewNotifyListener(dsn, channel, handler)` where `handler.OnNotify` calls `hub.Wake` and `OnReconnected` calls `hub.WakeAll`; measure exactly like `hub` (including the non-matching subscription and the fence draws). Assert p50 < 10 ms and log p50/p99 and draws/append.
- [ ] **Step 2: Add the chaos reconnect test** `TestFeedGateNotifyReconnect` to `postgres/feedgate_notify_test.go` (build tag `feedgate`; reuse the `feedgate_test.go` helpers):
  - Open a store with `WithAppendNotify(channel)`, a `feed.Server` over `postgres.NewLogs`/`NewCheckpointStore`/`NewLeaseStore` with a `feed.Hub` waiter, and one subscriber.
  - Start a producer appending one event per iteration (for example 200 events with a 5 ms gap); start a `NotifyListener` whose `OnNotify` calls `hub.Wake` and `OnReconnected` calls `hub.WakeAll`.
  - Mid-run, `SELECT pg_terminate_backend($1)` with `listener.PID()` on a separate connection; wait for `OnReconnected`; continue appending.
  - Assert the subscriber eventually delivered exactly the appended position set (the set-equality oracle) and `OnReconnected` fired at least once. The listener outage must cost delay only; the poll is the safety net.
- [ ] **Step 3: Write the benchmark** `BenchmarkWorkspaceAppendNotify` with `b.Run` subtests for `notify=off|on` and `appenders=1|8|32`:
  - Open one store per notify mode. For `notify=on`, start a listener with a no-op handler and wait for `pid != 0`.
  - For each appender count G, create G distinct streams (`bench-notify-<mode>-<G>-<i>`), each with `ExpectedVersion` starting at 0; `b.ResetTimer`; start G goroutines each appending `b.N/G` single-event batches to its own stream; `sync.WaitGroup`; `b.StopTimer`; report `appends/s` and `events/s`.
- [ ] **Step 4: Run** the latency test: `PG_DSN=... go test ./postgres/ -run TestFeedWakeLatency -count=1 -v` — expected: PASS, four p50/p99 pairs and four draws/append lines logged.
- [ ] **Step 5: Run** the benchmark:
  ```bash
  PG_DSN='postgres://postgres:pw@127.0.0.1:5433/eslite?sslmode=disable' \
    go test -tags feedgate -run '^$' -bench BenchmarkWorkspaceAppendNotify \
    -benchtime=2s ./postgres/
  ```
  Record every number; the `off` rows are the no-regression evidence.
- [ ] **Step 6: Commit** — `test(postgres): measure opt-in notify latency, draws and append cost`

### Task 9: Docs and the recorded result

**Files:**
- Modify: `docs/adr/0039-scoped-feed-wakeups.md` (measurements; status)
- Modify: `docs/adr/0037-commit-safe-feed-reads.md` (note at "LISTEN/NOTIFY: corrected and deferred")
- Modify: `docs/adr/0002-deployment-topology-and-delivery.md` (errata under the Signal section)
- Modify: `README.md` ("Delivery guarantees" → "Push")
- Modify: `docs/adr/README.md` (0039 status)

**Interfaces:**
- Consumes: Task 4's `poll`/`hub`/`contended` p50/p99, Task 8's `notify` numbers and benchmark table.

- [ ] **Step 1: Add "Measurements (2026-11-17)" to ADR 0039** with:
  - the environment (Docker `postgres:17`, local machine),
  - a p50/p99 table for `poll`, `hub`, `contended`, `notify`, clean and contended as measured,
  - the append benchmark table (`appenders` × `notify`), appends/s and the delta,
  - the fence `draws`/append for `poll`, `hub`, `contended` and `notify` from Tasks 4 and 8, with the read-amplification delta between the matching and non-matching subscriptions,
  Then change the status to `Accepted (implemented)`.
- [ ] **Step 2: ADR 0037 note:** at the LISTEN/NOTIFY section, add:
  > **Update (ADR 0039, 2026-11-17):** the deferral is lifted in part. An
  > opt-in `postgres.WithAppendNotify`/`NotifyListener` source and the
  > application-hint seam ship; the fence is unchanged. The paragraph's claim
  > that the `Waiter` "cannot distinguish ... both return nil" is stale: the
  > landed port returns `(woke bool, err error)` (`feed/ports.go:192-194`).
- [ ] **Step 3: ADR 0002 errata:** under the Signal section, add a dated note: the core still ships no broker and no default push source; ADR 0039 adds the scoped `Waiter`/`Hub` seam, an opt-in Postgres source, and the app-hint decorator.
- [ ] **Step 4: README:** replace the "Push" bullet with: wake-ups are hints; delivery remains fenced; with a source wired, clean-fence p50 is single-digit ms and blocked-fence p99 is ~`Poll`; a lost wake costs at most one poll; C1 (app hint) is recommended, C2 is opt-in.
- [ ] **Step 5: ADR index:** update 0039 to `Accepted (implemented)`.
- [ ] **Step 6: Verify** `GOWORK=off go test ./...` and `git diff --check` — expected: green, no whitespace errors.
- [ ] **Step 7: Commit** — `docs: record scoped wake-up measurements and guarantees`

---

## Self-review notes (coverage)

- ADR §3.1 → Task 1. §3.2 → Task 2. §3.3 C1 → Task 3. §3.3 C2 → Tasks 6/7. §3.4 (not chosen) → no code, recorded in the ADR. §4 → pinned by the existing feedgate chaos gate plus Task 4's `contended` subtest and Task 8's listener-kill chaos test. §5 → Task 4 (p50/p99 and contention), Task 8 (benchmark, draws per append). §6 → Task 5 (runbook), Task 6 (validation, payload bound), Task 7 (channel quoting, throttle), Task 3 (panic contract). §7 → Task 9. §8 → spread across Tasks 1-8; the leak, decorator fallback, claim-only, panic, reconnect and chaos criteria each have a named test. §9 → this task order. §10 → recorded in the ADR.
- No task adds a schema migration, a grant, a new dependency, or an unopt-in append-path statement.
