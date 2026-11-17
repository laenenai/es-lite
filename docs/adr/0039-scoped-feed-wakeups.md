# ADR 0039: Scoped Feed Wake-Ups

- **Status:** Accepted (implemented)
- **Date:** 2026-11-17
- **Related:** ADR 0002 (delivery topology; its "no push source ships"
  statement is amended here), ADR 0035 (caller-owned transactions), ADR 0036
  (no broker), ADR 0037 (commit-safe feed reads; its LISTEN/NOTIFY deferral is
  lifted here for an opt-in source), ADR 0025 (payload opaqueness), the
  2026-11-04 Postgres production-grade spec §3 (anti-goals), the README
  delivery guarantees, and the 2026-11-17 panel review
  (`docs/review/20261117_adr0039_panel_review.md`).
- **Supersedes in part:** ADR 0037 §"LISTEN/NOTIFY: corrected and deferred".
  The read-side horizon fence, delivery semantics, checkpoints and leases are
  unchanged.

## 1. Context

The feed's idle path is a poll: `defaultPoll = 250ms`, scaled by ±20% jitter
(`feed/server.go:21,164`). A committed append is therefore visible to a
subscriber on average after one half-poll, and a realtime gateway that feeds
client channels from two projections per workspace (a realtime publisher and a
notification projection) pays that delay on every message. ADR 0037 already
built the seam for a wake source and proved the server can use one without
weakening correctness:

- `feed.Waiter` (`feed/ports.go:182-194`) is an optional wakeup source.
  `Wait` returns `(woke, err)`; `woke == false` on a timeout.
- `Server.idleWait` (`feed/server.go:616-649`) is a *wake-confirmed* floor:
  a genuine wake (`woke == true`) bypasses the rest of the poll, and a
  timeout keeps it. A waiter that cannot distinguish a wake from a timeout
  must report `false`, so a misbehaving waiter cannot turn the loop into a
  spin against the log.
- The clamp loop's `Server.wait` (`feed/server.go:584-597`) uses the same
  seam without a floor; it only re-heads a blocked fence.

What the seam lacks is **scope**. One `Waiter` is shared by every stream of a
`Server`. A wake for workspace A wakes every stream. Post-errata, a woken
stream of an *unrelated* scope still short-circuits on its own committed
maximum (a RawHead-shaped round trip, no xid drawn — ADR 0037 Errata §1,
`postgres/wsstore.go:689-701`), but every stream of the *same* scope runs a
fenced page read that draws a transaction id (ADR 0037: "one real transaction
id per non-idle poll round"). Under per-workspace partitioning — many streams
per server — a global wake turns one append into a round trip for every
stream and a fence draw for every stream of the appending workspace. That is
precisely the cost ADR 0037 documented and asked deployments to manage. A
wake must therefore be routable by the subscription's scope.

Two constraints frame the decision:

1. **The append path must not regress** beyond the one read-only xid draw ADR
   0037 already accepts (spec §3: no extra commits, no new locks). A
   `pg_notify` inside the append transaction adds a statement and takes the
   notify-queue lock at commit, which briefly serialises committing
   notifiers. It cannot be the default.
2. **es-lite ships no broker** (ADR 0036). The library must not depend on
   NATS. Cross-process wake delivery is the application's transport; es-lite
   supplies only the backend-agnostic seam.

Correctness does not move. The horizon fence remains the delivery boundary; a
wake is a hint that new events *may* exist. A missed, late or duplicated wake
can only cost delay, never loss.

## 2. Goals, non-goals, anti-goals

**Goals.**

- A committed append reaches an idle subscriber in single-digit milliseconds
  on a clean fence, without changing what is delivered or in what order.
- Wakes are routable: a wake for one scope wakes only that scope's streams
  (and any explicitly unscoped waiter).
- The default build and the default append path are unchanged: no new
  statement, no new lock, no new module dependency.
- The hook is usable by `aggregate.Runtime` and generated command services
  without changing their call sites.

**Non-goals.**

- Changing the fence, checkpoint, lease, ack or rebuild semantics.
- Exactly-once delivery. Wake-ups only reduce latency.
- A running service or daemon in es-lite (ADR 0036).
- Type-scoped wakes (a wake for every stream consuming the appended type).
  Scope is the workspace; the per-type read amplification is recorded as a
  cost, not eliminated.

**Anti-goals.**

- Any wake mechanism whose failure mode is *loss* rather than *delay*.
- A default-on append-time `NOTIFY`, or any new lock on the **default**
  append path. The opt-in C2 path deliberately takes the commit-time
  notify-queue lock; this amends spec §3's "no new locks taken by appends" to
  "no new locks on the **default** append path", and the opt-in cost is
  measured and recorded rather than assumed away (see §6).
- A wake that can turn a committed append into an apparent failure.
- An unbounded per-scope map, goroutine, or connection leak.

## 3. Decision

### 3.1 `feed.ScopedWaiter` — an optional, backward-compatible extension

```go
// ScopedWaiter is an optional extension of Waiter. When the configured
// Waiter also implements it, the server waits through WaitScope with the
// stream's Request.Scope, so a wake source can wake only the streams of the
// scope that received events. Same contract as Waiter.Wait.
type ScopedWaiter interface {
    WaitScope(ctx context.Context, scope string, timeout time.Duration) (woke bool, err error)
}
```

`Server.stream` already carries `req` (`feed/server.go:389`), so both
`wait` and `idleWait` gain the stream's scope. A small helper resolves the
interface once per wait:

```go
if sw, ok := w.(ScopedWaiter); ok {
    return sw.WaitScope(ctx, scope, timeout)
}
return w.Wait(ctx, timeout)
```

Floor, jitter and error semantics are unchanged. An unscoped waiter behaves
exactly as it does today, byte for byte.

One operational rule goes in the `Waiter` doc: **a decorator that wraps a
`Waiter` must forward `WaitScope` when the wrapped value implements it.**
Embedding the wrapped waiter is the safe pattern. A decorator that only
implements `Wait` silently downgrades the deployment to global wakes; there
is no runtime signal for that, so it must be documented.

### 3.2 `feed.Hub` — an in-memory, dependency-free wake hub

`NewHub()` returns a `Hub` that implements both `Waiter` and `ScopedWaiter`.
Producers call:

- `Wake(scope string)` — wakes every waiter currently blocked on that scope,
  and every unscoped waiter (the compatibility path).
- `WakeAll()` — wakes everyone, including every scope. Used after a wake
  source reconnects, because hints emitted while it was down may have been
  missed.

The implementation is a broadcast generation per scope plus one hub-wide
generation for `Wait`:

```go
type Hub struct {
    mu     sync.Mutex
    scopes map[string]*scopeGen
    all    *scopeGen
}
type scopeGen struct {
    ch   chan struct{}
    refs int
}
```

The invariants are part of the decision, because the obvious implementation
leaks or loses wakes:

1. **`WaitScope` registers under the lock.** Lock, get-or-create the scope's
   generation, increment `refs`, copy `ch`, unlock. The decrement and the
   conditional delete run in a deferred block under the same mutex. An entry
   is deleted when `refs` reaches zero and the map still holds that same
   generation.
2. **`Wake(scope)` closes and removes the scope entry.** Under the lock, if
   an entry exists, close its channel and delete the map entry. Waiters that
   already copied the channel observe the close; the entry is re-created
   lazily by the next `WaitScope`. Removing on wake is load-bearing: if the
   entry were merely replaced, a wake that lands between a waiter parking and
   exiting would leave a fresh generation with no registrant and no one to
   delete it — a permanent map entry per workspace, which is the normal
   "appends but no live subscriber" case. Removing on wake also coalesces
   repeated wakes while no stream is parked.
3. **`Wake(scope)` on an absent scope is a no-op for the map.** There is no
   waiter to wake, and creating an entry would grow the map with every
   appended workspace that has no subscriber.
4. **`Wake` also closes and replaces the hub-wide generation**, so a legacy
   unscoped `Wait` preserves today's "any append may wake me" behaviour.
5. **`WakeAll` closes and removes every live scope entry and closes and
   replaces the hub-wide generation**, all under the lock.
6. **`Wait` (unscoped) uses the same generation pattern** on `all`. It is
   never deleted; one channel pair per hub.
7. **No method blocks on a channel while holding the lock.** `Wake` and
   `WakeAll` only close channels and remove or swap them; `WaitScope` only
   copies a channel reference.
8. **The zero value is not usable;** construct with `NewHub`. Calling `Wake`,
   `WakeAll` or `Wait` on a nil or uninitialised `Hub` panics. The panic is
   the contract, and a test pins it.

`WaitScope` returns `(true, nil)` when the channel closes, `(false, nil)` on
the timer, and `(false, ctx.Err())` when the context ends. The timer is
created and stopped inside the call; no goroutine per waiter is needed.

**The accepted gap.** A wake that lands between a stream's page read and its
`WaitScope` registration is not observed by that round; nor is a wake that
lands while the stream is sending a page. The next read is at most one poll
away (1.2 × `Poll` with jitter). This is the same "delay, never loss" failure
class ADR 0037 accepts; v1 documents it rather than engineering it away. A
future generation-counter contract (register before the read, or pass the
observed generation into the wait) would close it, but it changes the seam and
is not needed for the target latency.

### 3.3 Wake sources

Both sources live outside the default append path. Only C1 is recommended for
the first deployment.

#### C1 — application hint (default, recommended)

Whoever appends calls `hub.Wake(scope)` after `Append` returns successfully.
In-process this is sufficient. Cross-process, the **application** relays the
hint over its own bus; es-lite does not learn about NATS.

To avoid touching `aggregate.Runtime` or generated command services, es-lite
supplies one store decorator:

```go
// WithAfterAppend wraps store so fn runs once, synchronously, after every
// successful Append — after the append transaction has committed and before
// Append returns to the caller. fn is not called on any error.
//
// fn has no error return on purpose: the append is already durable, and a
// failed hint must never surface as a failed append (which would invite a
// retry and a duplicate). A panic from fn is recovered and discarded for the
// same reason: the append stays committed, the hint is lost, and delivery
// falls back to the poll. Log inside fn and do not block.
//
// The workspace must be closed over by the caller. It is NOT reliably
// available from AppendResult: a claim-only append succeeds with zero
// envelopes, and Envelope.Workspace is empty on single-workspace backends.
func WithAfterAppend(store es.Store, fn func(context.Context, es.AppendResult)) es.Store
```

Every other `es.Store` method passes through unchanged. Three boundaries are
deliberate and must be documented:

- **`AppendTx` is not hooked.** The caller owns the transaction; only the
  caller knows when it commits. Wake after your own commit.
- **`aggregate.WithAppend` is not hooked** (`aggregate/runtime.go:212-219`,
  used at `398-401`). It *replaces* `store.Append` for the call, so the
  decorator is bypassed. That is the caller-owned-transaction shape; the same
  rule applies — wake after the commit.
- The wrapper returns `es.Store`, so it erases concrete methods a caller may
  have used (`*postgres.Workspace.AppendTx`/`ReadStreamTx`,
  `postgres/store.go:355-370`). Wrap only the handle used for plain appends.
  There is no adapter-level substitute for the caller-owned path: `DBTX`
  exposes no `Commit` (`postgres/wsstore.go:29-33`), so only the caller (or
  the transaction wrapper it owns) knows when the commit happened.

**Product consequence.** ADR 0035 exists because the platform appends inside
a caller-owned transaction. A service that forgets to wake after its own
commit does not fail: it silently falls back to polling for exactly the
events the gateway cares most about, and no existing signal distinguishes a
missing wake from a blocked fence (`Observer.OnStall` covers only the
latter). Phase A/B therefore ships a cheap wake diagnostic — Hub counters
(`Wake`/`WakeAll`, and wake-driven reads where observable) — so an operator
can answer "are wakes arriving for this scope?". C2, whose notify commits
with the caller's transaction, is the only zero-call-site coverage for the
caller-owned path.

The recommended gateway wiring is unchanged from the brief's intent, with the
scope closed over rather than read from the result:

```go
hub := feed.NewHub()
srv, _ := feed.NewServer(feed.Config{ /* ... */ Waiter: hub })

ws := es.WithAfterAppend(store.Workspace(wsID), func(context.Context, es.AppendResult) {
    nc.Publish("es.appended."+subjectToken(wsID), nil) // best-effort; encode the scope to one NATS token
})
nc.Subscribe("es.appended.*", func(m *nats.Msg) {
    hub.Wake(subjectScope(m.Subject))
})
```

`subjectToken` must encode or hash the workspace so it is one valid NATS
subject token; raw workspace IDs may contain separators or wildcard
characters. The appender and the gateway are different processes in this
example, and `fn` runs on the command path: the publish must be bounded and
async (a NATS client with a full pending buffer can block while
disconnected), never a synchronous round trip.

#### C2 — Postgres LISTEN/NOTIFY (opt-in, off by default)

`postgres.WithAppendNotify(channel string) Option` (`postgres/store.go:43` is
the existing option pattern). When set, `Workspace.append`
(`postgres/wsstore.go:185`) runs `SELECT pg_notify($1, $2)` once per non-empty
batch, after the events, the `log_order` row and the claims, with the channel
name and the workspace ID as parameters. NOTIFY is transactional: an aborted
append sends nothing, and a caller-owned transaction notifies only when the
caller commits.

`postgres.NotifyListener` owns **one dedicated connection** (never a pooled
connection that is returned), sets an `application_name`, runs
`LISTEN <channel>`, loops on `conn.WaitForNotification(ctx)`, and invokes a
handler. It takes a two-method interface, not `*feed.Hub`, so the adapter
stays testable and the coupling stays one-way:

```go
type NotifyHandler interface {
    OnNotify(scope string)
    OnReconnected() // the gateway wires this to hub.WakeAll()
}
```

The channel is validated as a strict PostgreSQL identifier at `Open`
(length ≤ `NAMEDATALEN−1`) and quoted with `pgx.Identifier.Sanitize` for
`LISTEN`; it is a security capability (see §6), is never logged, and must be
high-entropy. On connection loss the listener reconnects with jittered
backoff whose floor is `Poll`, re-issues `LISTEN`, and calls
`OnReconnected`. It throttles per scope: a notification for a scope already
woken within a small window is dropped, because parked waiters already
received the close. That bounds spoofed or duplicated notification
amplification without touching the hub's semantics or the latency claim.
Duplicate notifications are harmless. The `pgconn.Notification` PID enables
the standard self-notify filter if the listener process also appends.

Costs, stated plainly and carried into the runbook:

- One extra statement and round trip per non-empty append, comparable in
  shape to the accepted xid draw, plus the commit-time notify-queue lock that
  serialises committing notifiers. Suitable for moderate append rates; above
  that, C1.
- A direct or session-pooled connection. PgBouncer **transaction** pooling
  breaks `LISTEN`.
- One listener connection per process, not per workspace. A fleet of gateway
  replicas each needs its own listener.
- The payload limit is made unreachable, not validated per append: reject
  workspace IDs longer than a documented bound (for example 512 bytes) when
  the `*Workspace` is created, so the 8000-byte NOTIFY limit cannot be hit.
- C2 has two honest commit-time failure modes: an exhausted async
  notification queue makes NOTIFY-ing transactions fail at commit, and a
  transaction that executed NOTIFY cannot be prepared for two-phase commit.
  The listener throttle, bounded reconnect, `pg_notification_queue_usage()`
  alerting and the 2PC exclusion in §6 are the controls. The hint is not
  "failure-free"; it is failure-contained and operationalised.
- `pg_notify` is not RLS-filtered, and any role that can `CONNECT` can
  `LISTEN` or spoof a notification. See §6 for the hard precondition.

SQLite is unchanged: in-process apps call `hub.Wake(scope)` after append (C1),
the same pattern ADR 0002 already describes for `delivery.Signal`.

### 3.4 Not chosen, and why

- **Logical decoding as the wake source.** No write-path cost, but it needs
  `wal_level=logical`, a replication slot, and slot-lag operations — a
  bigger operational surface than the latency is worth in v1. Revisit if
  append rates make C2's commit lock unacceptable.
- **Trigger-based `NOTIFY`.** Same cost as C2 and less explicit about where
  the hint is emitted.
- **`delivery.Signal` / `delivery.Poller`.** They tail the unfenced
  `ReadAll` cursor; a wake there would hint the wrong read path (ADR 0037
  §"Scope: what is fenced and what is not").
- **A global (unscoped) wake only.** Correct but expensive: one append wakes
  every stream on the server. Streams of unrelated scopes short-circuit on
  their committed max (a round trip each, no xid), but every stream of the
  appending workspace draws a fence xid. Scope routing is the point of this
  ADR.
- **Per-row `xmin` filtering or a pre-reserved commit watermark.** Already
  rejected in ADR 0037 for loss and write-path reasons respectively; the
  wake does not reopen them.

## 4. Why a wake cannot lose an event

The fence is untouched. A wake only causes an earlier `ReadAllBounded`, and
that read is bounded by the same horizon fence as a poll. Two cases cover the
wake's own failure modes:

- **The wake arrives.** The stream reads a fenced page. If the append is
  settled, it is delivered. If the fence is blocked by an in-flight
  transaction, the page is empty and the stream waits; the committed event is
  not lost, because the cursor did not move. The cost is up to one poll.
- **The wake is lost, late, duplicated, or the listener was down.** The
  stream's poll fires. The fenced read delivers the event. Nothing depends on
  the hint for completeness.

`WakeAll` after a listener reconnect is a latency repair, not a correctness
one. A missed hint is indistinguishable from a slow poll, and both are
bounded by `Poll`.

## 5. Costs, accepted knowingly

- **Fence contention still governs p99.** A wake bypasses the floor, but if
  any transaction anywhere on the database is in flight when the reader draws
  its fence, the read stores a fence and returns empty. The committed event
  is not re-notified, so the stream waits a full jittered poll. Single-digit
  p50 applies to a clean fence; under concurrent writers, p99 remains
  ~`Poll` unless the blocking writer is itself an es-lite append whose
  commit emits a wake. The latency test must measure both regimes and the
  ADR must report them separately.
- **Read amplification.** One wake causes a fenced page read in every stream
  of that scope. With two projections per workspace that is two reads per
  append, at least one of which is usually a no-op for a type filter. At high
  append rates this is per-append xid burn — the cost ADR 0037 documented.
  Scope routing bounds it to a workspace; a per-scope coalescing window
  (wake at most once per small interval) is the future knob, not a v1
  requirement.
- **The floor is bypassed on every genuine wake.** A producer that calls
  `Wake` in a tight loop spins every stream of the scope. `Wake` is a trusted
  in-process API; it must not be exposed over a network. The `NotifyListener`
  calls it once per notification, and `WakeAll` once per reconnect.
- **A wake source adds an operational dependency.** C1 adds a bus publish on
  the command path; C2 adds a listener connection and its reconnect
  behaviour. Both are optional; with neither configured, behaviour is
  exactly today's polling.

## 6. Security and tenancy

These are load-bearing for the panel review, not afterthoughts.

- **C2's hard precondition: trusted `CONNECT` or a dedicated database.**
  LISTEN is not row-filtered ("notifications are visible to all users"), and
  any role that can `CONNECT` can both `LISTEN` on a nameable channel and
  `pg_notify` on it. C2 is therefore enabled only when **every role that can
  connect to that database is trusted**, or the database is dedicated to the
  append/feed path. A shared database with untrusted tenant roles must use
  C1. This is stricter than "run the listener under a trusted role": the
  precondition is about all connect-capable roles, because eavesdropping and
  spoofing are open to every one of them.
- **The channel name is a capability.** Use a high-entropy, unguessable
  channel name. Validate it as a strict PostgreSQL identifier (length ≤
  `NAMEDATALEN−1`) and quote it with `pgx.Identifier.Sanitize` for `LISTEN`;
  never log it. An unguessable name mitigates both eavesdropping and
  injection, but it is not a substitute for the trust precondition. Roles
  holding `pg_read_all_stats` can read other sessions' query text and thereby
  learn the name.
- **The payload is the workspace ID and nothing else.** The store already
  treats workspace IDs as non-secret metadata; the *new* exposure is
  cross-workspace correlation and per-token volume/timing, not the ID itself.
  Bound workspace IDs at `WorkspaceStore` creation (for example 512 bytes) so
  the 8000-byte NOTIFY limit is unreachable. An optional opaque mode (an
  HMAC of the scope) is defense-in-depth only: the appender emits the
  payload, so the key must be shared by the appender and the listener; the
  listener needs a token→scope directory; token equality still correlates a
  workspace's events, and per-token volume/timing still leaks activity. It
  reduces incidental log legibility; it does not make C2 safe on an
  untrusted database.
- **C1 over a shared bus has the same shape.** A NATS subject containing the
  workspace ID exposes it to every subscriber of the account. The gateway's
  subject authorization is the control; an opaque token is the defensive
  option. Encode the scope to a single valid subject token.
- **No new privileges, schema or rows.** The listener needs only `CONNECT`;
  C2 makes no schema change and writes no table. The append path gains no
  grants, and no RLS bypass arm is touched.
- **Honest failure domain.** A hint can fail an append in two specific ways,
  both at commit: an exhausted async notification queue makes NOTIFY-ing
  transactions fail at commit, and a transaction that executed NOTIFY cannot
  be prepared for two-phase commit. A wedged listener (alive but not
  draining) can pin queue cleanup, so sustained appends can begin failing.
  Controls: one listener per process, a bounded reconnect with a `Poll`
  floor, the per-scope throttle, an alert on `pg_notification_queue_usage()`,
  and a documented 2PC exclusion. The 8000-byte limit is not a failure mode
  because the payload is length-bounded by construction.
- **Denial of service.** `Wake` storms and a flapping listener's `WakeAll`
  can spin streams. The hub is in-process and trusted; never expose `Wake`
  over a network. The listener throttle is the C2 abuse bound; coalescing in
  the hub remains a future cost knob. The notify queue itself can grow if
  listeners are slow; one listener per process and the bounded reconnect are
  the controls.
- **The hook runs user code on the command path.** The decorator recovers a
  panic from `fn`: the append is already durable, the hint is lost, and
  delivery falls back to the poll. Re-panicking would present a committed
  append as a failure and invite a duplicate retry — the exact outcome the
  no-error design exists to prevent. `fn` must still not block; a NATS client
  with a full pending buffer can block, so use a bounded/async publish.

## 7. Consequences

### Positive

- Idle delivery latency drops from ~125–250 ms to single-digit milliseconds
  on a clean fence, with no change to delivered sets, order, or ack fencing.
- Scope routing removes the global-wake cost that made the existing `Waiter`
  seam unattractive for per-workspace partitioning.
- Default builds and default appends are unchanged; SQLite is unchanged; no
  new module dependency (`pgx` already provides `WaitForNotification`).
- The seam remains transport-neutral: NATS, a bus, or a database hint all
  reduce to `hub.Wake(scope)`.

### Negative

- Two new public surfaces (`feed.ScopedWaiter`, `feed.Hub`) plus one
  decorator (`es.WithAfterAppend`) and, for C2, one Postgres option and one
  listener type.
- C2 carries a real write-path cost when enabled and a cross-workspace
  metadata exposure that must be configured around.
- The best-case latency claim needs careful measurement; the fence, not the
  wake, sets p99 under contention.
- `WithAfterAppend` does not cover `AppendTx` or `aggregate.WithAppend`, and
  it narrows a concrete workspace to `es.Store`. Those boundaries are
  documented, not eliminated.

## 8. Acceptance criteria

- **Default build:** all existing tests pass unchanged; the append path
  issues no additional statement and takes no additional lock; the feed's
  delivery behaviour is byte-for-byte identical.
- **Scoping:** a `ScopedWaiter` receives exactly the stream's
  `Request.Scope`; an unscoped waiter's `Wait` calls are unchanged; a wake
  for scope A does not wake scope B; `WakeAll` wakes both.
- **Liveness:** a missed wake delays delivery by at most one `Poll`; the
  feedgate chaos gate (set-equality oracle) stays green with a wake source
  wired.
- **Leaks:** 10k scopes that come and go leave no goroutines and no map
  entries; `-race` is clean under concurrent `Wake`/`WaitScope`. The test
  includes the wake-after-last-waiter interleaving (a wake between park and
  exit) and a `Wake` on a never-registered scope; both must leave the map
  empty.
- **Contracts:** the Hub's zero-value panic is pinned; a waiter that
  implements only `Wait` still routes through `Wait` (the unscoped
  compatibility path).
- **Latency (local Postgres, record both):** append→`Sink` p50 < 10 ms with
  a wake on a clean fence; p50/p99 with a concurrent in-flight appender,
  reported as the contended regime. The wake-off baseline is ~`Poll`.
- **Append benchmark (1, 8, 32 appenders; C2 on vs off):** the default (off)
  shows no regression; the on case records the measured delta honestly. Report
  the fence `draws` counter delta (wake on vs off) so read amplification is a
  measured number, not a paragraph.
- **Hook contract:** `WithAfterAppend` fires once on success with the full
  result, never on an error; a panic in `fn` is contained; the hook does not
  fire when `aggregate.WithAppend` replaces the append.
- **C2 correctness:** append → wake observed in < 50 ms; a rolled-back
  `AppendTx` produces no wake; a claim-only append emits no notify; the
  option off issues no notify; a killed listener reconnects and `WakeAll`
  reaches waiters. The chaos matrix injects a listener kill/restart
  mid-run and re-asserts set equality.
- **Docs:** this ADR (with measurements filled in), a note in ADR 0037 that
  also corrects its stale "the Waiter cannot distinguish a wake from a
  timeout" rationale (the port has returned `(bool, error)` since it landed),
  an errata note in ADR 0002, the README guarantees block (wake-ups are
  hints; delivery is fenced; a lost wake costs at most one poll; single-digit
  p50 on a clean fence, p99 ~`Poll` under a blocked fence), and
  `docs/deploy/README.md` (direct connection, one listener per process,
  channel entropy, `pg_notification_queue_usage()` alerting, 2PC exclusion,
  when to prefer C1).

## 9. Implementation outline (after this ADR is approved)

Phased so the low-risk seam lands first and the write-path change lands
last, with measurements.

**Phase A — feed seam and hub (no append-path change).**

1. `feed/ports.go`: `ScopedWaiter`; `Waiter` doc gains the decorator rule.
2. `feed/server.go`: thread `req.Scope` through `wait`/`idleWait`; route via
   the optional interface. Extend `wakingWaiter`/`blockingWaiter` tests:
   scope received exactly, `woke` bypasses the floor, `false` keeps it,
   unscoped path unchanged, and a `Wait`-only waiter still routes through
   `Wait`.
3. `feed/hub.go` + `feed/hub_test.go`: the invariants of §3.2, including
   close-and-remove on `Wake`, the stale-generation interleaving, a wake on
   a never-registered scope, the zero-value panic, timeout, cancellation,
   scope isolation, `WakeAll`, refcount cleanup at 10k scopes, and `-race`
   concurrency. Add cheap counters (`Wake`/`WakeAll`, wake-driven reads) and
   a `Stats()` accessor for the "are wakes arriving?" diagnostic.

**Phase B — the application hint.**

4. `es/afterappend.go` + tests: fires once on success with the full result,
   never on `ErrConflict`/other errors, recovers a panic from `fn`, passes
   every other method through, does not hook `AppendTx` (compile-time
   interface assertion), does not fire when `aggregate.WithAppend` replaces
   the append, and is documented for the caller-owned path.

**Phase C — opt-in Postgres hint and evidence (conditional).** Start only
after Phase A/B's clean-fence latency is measured and §6's C2 security and
failure model is in the runbook.

5. `postgres.WithAppendNotify` + the `pg_notify` statement (non-empty
   batches only, after claims) + `postgres/notify.go` with the
   `NotifyHandler` listener, the per-scope throttle, reconnect/`OnReconnected`,
   and `application_name`. Integration tests gated by `PG_DSN`, plus a
   deterministic no-notify test with a recording `DBTX` and a claim-only
   boundary test.
6. End-to-end latency test (`feed.Server` + `Hub` + `NotifyListener`, one
   subscription) with p50/p99, wake on vs off, clean vs contended; report
   the `draws` delta.
7. Append benchmarks at 1/8/32 appenders, C2 on vs off; record the numbers
   in this ADR. Inject a listener kill/restart into the feedgate chaos run.
8. Docs: 0037 note (including the stale `Waiter` rationale), 0002 errata,
   README guarantees, deploy runbook (trust precondition, channel entropy,
   queue-usage alert, 2PC exclusion, reconnect floor).

## 10. Questions resolved at panel review

1. **Workspace scope is sufficient for v1.** The non-matching projection's
   no-op read is one fence round per append, bounded by scope and
   index-served. Type scoping would multiply the wake key and buy little at
   the append rates where the wake matters. Revisit only with measured read
   amplification and the coalescing knob.
2. **Coalescing is deferred as a cost knob, required as an abuse bound.**
   A wake-driven empty read returns to the wait immediately (the floor is
   bypassed on a genuine wake), and a timeout-driven empty read is already
   paced by `Poll`; a hub-side coalescing window would only merge bursts of
   wakes, so it saves little. The C2 listener's per-scope throttle is
   required, and it lives in the listener, not the hub.
3. **Trusted `CONNECT` is the hard precondition; an opaque payload is
   optional defense-in-depth.** C2 is enabled only when every role that can
   connect to the database is trusted or the database is dedicated. An
   unguessable channel name is mandatory. HMAC payloads reduce incidental log
   legibility; they do not make C2 safe on a co-tenant database, because token
   equality and per-token volume/timing still correlate.
4. **A/B ship first; C2 is gated.** C2 ships after the C1 clean-fence win is
   measured and the C2 security/failure model is in the runbook. Its real
   advantage — zero per-call-site wiring, including caller-owned commits — is
   why it is worth gating properly rather than shipping half-specified.

## Measurements (2026-11-17)

Recorded from the implementation branch. Environment: local Docker
`postgres:17` (the `task test:pg` shape, localhost), macOS host, `-race`
builds for the latency runs. Latency shapes are 50 iterations each
(30 ms parks between appends, a matching and a non-matching subscription
per shape); the benchmark is `-benchtime=2s` with single-event batches.
The `notify` shape wires `postgres.WithAppendNotify` + `NotifyListener`
with the gateway wiring (`OnNotify` → `hub.Wake(scope)`,
`OnReconnected` → `hub.WakeAll()`); `poll` is the no-waiter baseline,
`hub` the in-process `WithAfterAppend` → `hub.Wake` hint, `contended`
the hub shape with a fence pinned ~50 ms per append.

### Wake latency and fence draws (p50 / p99, append → delivery)

| shape      | p50       | p99      | fence draws/append (steady-state) |
|------------|-----------|----------|------------------------------------|
| poll       | 219.7 ms  | 272.7 ms | 2.08 (2.00)                        |
| hub        | 7.03 ms   | 22.99 ms | 2.08 (2.00)                        |
| notify     | 7.09 ms   | 8.84 ms  | 2.08 (2.00)                        |
| contended  | 264.5 ms  | 309.5 ms | 2.08 (2.00)                        |

- The clean-fence gate holds: `hub` p50 7.03 ms and `notify` p50 7.09 ms,
  both under the 10 ms criterion, and essentially identical to each other —
  the `pg_notify` round trip costs nothing measurable over the in-process
  hint. The poll baseline is real (~`Poll`), and the contended shape
  confirms §5: a pinned fence costs ~`Poll` (264.5 ms ≈ 250 ms), with p99
  bounded (309.5 ms, well under 2×`Poll`+150 ms) — delivery falls back to
  the poll, never stalls.
- **Read amplification, measured and framed honestly.** All four shapes
  draw ~2 fences per append in steady state — 2.00 draws/append is one
  fence draw per subscription of the scope per append (two subscriptions
  in the harness). The wake does *not* add a net draw over the poll
  baseline in this test: the poll shape already burns the same ~2
  draws/append on its own schedule (the never-acking non-matching
  subscription draws a fence on every idle poll cycle). What the wake
  contributes structurally is the non-matching subscription's empty-fence
  draw per append — an empty page read for a type filter that never
  matches — and that draw is the same one the poll baseline was already
  paying. So the measured amplification delta is ~0, not a win: the wake
  buys 219.7 ms → ~7 ms latency at no additional steady-state fence-draw
  cost, and §5's "one fenced page read per stream of the scope per wake"
  is the per-append structure, confirmed at 2.00 steady-state.

### Append benchmark (opt-in C2 on vs off, single-event batches)

| appenders | notify=off appends/s | notify=on appends/s | on vs off |
|-----------|----------------------|---------------------|-----------|
| 1         | 980.0                | 795.6               | −18.8%    |
| 8         | 2,718                | 2,313               | −14.9%    |
| 32        | 4,375                | 2,273               | −48.0%    |

- The `notify=off` rows are the no-regression evidence: the default
  append path is untouched, and `WithAppendNotify` is opt-in.
- The `notify=on` cost is real and recorded honestly: ~19% at 1 appender,
  ~15% at 8, and it flattens at 32 appenders (~2,273/s vs ~4,375/s off).
  The per-append `pg_notify` statement plus its queue drain appears to
  serialise above ~2.3k notifications/s. This is the measured opt-in
  cost §3.3 warns about ("suitable for moderate append rates; above
  that, C1") — not a default-path regression, and the reason C1 stays
  the recommended source at high append concurrency.

### Chaos (listener kill mid-run)

The feedgate chaos run with `WithAppendNotify` wired: a real listener
backend was killed with `pg_terminate_backend` at event 60 of 200 while
the producer kept appending through the reconnect outage. Result: 201/201
events delivered exactly (set equality against the appended set), 0
duplicates, 1 reconnect with `OnReconnected` → `WakeAll`. §4 holds under
a real connection loss: the outage cost delay only, never loss, because
the missed hints fell back to the poll.

## Appendix: panel review changes (2026-11-17)

The panel review (`docs/review/20261117_adr0039_panel_review.md`) returned
"approve with changes". This revision applies the gating items:

- The Hub lifecycle is close-and-remove on `Wake`, get-or-create on
  `WaitScope`, with the stale-generation interleaving test (§3.2, §8).
- §2's anti-goal is rescoped to the **default** append path, with an explicit
  amendment to spec §3 for the opt-in C2 lock.
- C1's caller-owned-transaction gap is stated as a product consequence, and
  the wake diagnostic is a Phase A deliverable (§3.3, §9).
- The `WithAfterAppend` panic contract is resolved in favor of recovery, so a
  durable append is never presented as a failure (§3.3, §6).
- C2's security model is rewritten: trusted `CONNECT` as a hard precondition,
  mandatory high-entropy channel name, optional HMAC with correct key
  placement, a required listener throttle, and the honest commit-time
  failure domain (queue exhaustion, 2PC exclusion, queue-usage alerting)
  (§3.3, §6).
- Accuracy corrections: §1 and §3.4 now reflect the post-errata idle
  short-circuit; the ADR 0037 note will correct the stale "cannot
  distinguish a wake from a timeout" rationale.
