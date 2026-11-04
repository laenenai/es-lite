# es-lite Feed — CTO Review, Risk Register & Action Plan

- **Review date:** 2026-11-04
- **Module:** `github.com/laenenai/es-lite`
- **Scope:** committed `main` @ `e2d08ea`, worktree clean, review range `bf1214c..HEAD` (10 commits / ~4.5k lines) covering:
  - `feed/` — transport-agnostic feed core (`Subscribe`, leases, checkpoints, bounded reads)
  - `sqlite/feed.go` + `sqlite/migrations/0003_feed.sql` — SQLite log/checkpoint/lease adapters
  - `postgres/wsstore.go` — Postgres feed-facing reads (`ReadAll`, `ReadAllBounded`, `Head`)
  - `projection/` — `ReconnectingFeed`, `BufferedMarker`, `Runner`
  - `delivery/` — `Poller`, `Relay`, `Signal`
- **Method:** seven independent expert reviewers run in parallel on **GLM 5.3 (non-flash)**, each with an isolated brief and read-only mandate:
  1. distributed-systems / feed protocol
  2. storage adapters / SQL
  3. projection & delivery consumer correctness
  4. Go API & architecture
  5. testing / CI quality
  6. security & multi-tenancy
  7. performance / scalability / operations
- **Lead adjudication:** the CTO pass independently reproduced the load-bearing claims (NUL collision, missing index, unfenced acks, SQLite single-writer invariant) and corrected one reviewer inference (see §6).
- **Prior context:** `docs/review/20261028_review.md` (pre-feed). Findings there are only restated where the new feed code re-introduces or worsens them.

---

## 1. Executive verdict

**SQLite single-node: close to production-ready after the P1 items.**
The feed core is small, carefully written, and unusually well tested. Lease lifecycle ordering, cancellation, error joining, page continuation, and checkpoint clamping are all correct. The SQLite adapter is genuinely gap-free because one connection plus the file write lock makes `global_position` assignment commit-ordered.

**Postgres multi-tenant: NOT production-ready.**
`Workspace.ReadAllBounded` is, as written, the naive monotonic cursor that the repo's own ADR 0001 §5 forbids. `global_position` is assigned by a sequence at INSERT, not at COMMIT. Under concurrent appenders a consumer can advance its checkpoint past a lower position that commits later, and that event is then **silently and permanently skipped**. The `Drain` relay solved this with a claim/outbox pattern; the new live feed did not carry that protection across.

**The three things that matter most**

1. **P0 — Postgres feed can silently lose events** (`postgres/wsstore.go:345-364` + `feed/server.go:302-319`). Fix or hard-disable before any Postgres feed deployment.
2. **P1 — Acknowledgements are not fenced by the lease** (`feed/server.go:364-372`). A stale consumer can move a shared cursor past events a new consumer has not delivered. The lease protects delivery, not acking.
3. **P1 — A single oversized or poisoned event wedges a subscription forever** (`feed/server.go:307-314`, no append-side cap, no skip/DLQ). Every reconnect re-hits the same row; there is no operator recovery path except a forced checkpoint reset.

**Also material:** no composite `(workspace_id, global_position)` index; no `Consumes` fingerprint on checkpoints (adding an event type silently skips its history); `BufferedMarker`'s "1s crash window" is not actually time-bounded; zero feed observability and a `Lag` metric that is meaningless under type filtering; the consumer adapter that is supposed to bind `feed.Server` to `projection.Feed` does not exist in this repo.

---

## 2. Scorecard

Ratings: 5 = production-ready, 4 = good with minor gaps, 3 = adequate, 2 = weak, 1 = absent/broken.

| Dimension | SQLite | Postgres | Justification |
|---|---:|---:|---|
| Feed protocol & cursor correctness | 4 | 1 | Core loop is correct; Postgres cursor inherits the commit-order gap. |
| Storage adapter correctness | 4 | 2 | SQLite adapters are solid; Postgres lacks the index and the commit-safe read. |
| Lease / checkpoint durability | 4 | n/a in-repo | Owner-guarded, monotonic, correctly released; Postgres adapters live in the platform and are unreviewed. |
| Consumer / projection path | 2 | 2 | `BufferedMarker` and reconnect logic have real gaps; the binding adapter is missing. |
| API design & boundaries | 3 | 3 | Clean ports, but ack fencing, definition identity, and `feed`→`projection` coupling need work. |
| Security & tenancy | 3 | 3 | Crypto boundary holds; isolation is fully delegated with no fail-closed hook; RLS bypass is a GUC. |
| Observability & operations | 1 | 1 | No metrics, logs, readiness, runbook, or server binary. `Lag` is misleading. |
| Testing & CI | 4 | 3 | Strong unit/race suite; no ordering/ack/poison tests; PG job lacks `-race`; no coverage/lint gate. |
| Documentation truthfulness | 2 | 2 | README still promises `global_position`-tail at-least-once; no feed ADR; ADR 0002 is stale. |
| **Overall** | **3.5** | **1.5** | SQLite is one P1 pass away; Postgres is blocked on the P0. |

---

## 3. Verification performed

```text
GOWORK=off go build ./...                  OK
GOWORK=off go vet ./...                    OK
GOWORK=off go test -count=1 ./...          all 14 packages ok (~29s)
GOWORK=off go test -race -count=1 ./...    all packages ok
```

- 25 of 26 Postgres tests skip locally without `PG_DSN`. **They do run in CI** via `task test:pg` and a Docker Postgres (`ci.yml:44-54`, `Taskfile.yml:85-92`). CI does **not** run that job with `-race` and has no test for out-of-order commits.
- Independently reproduced:
  - `LeaseKey("a\x00b","c","d") == LeaseKey("a","b\x00c","d")` → `true`; `validateKey` does not reject NUL (`feed/server.go:480-488`).
  - `events` indexes (`postgres/migrations/0001_initial_schema.sql:51-55`): `(global_position)`, partial `(global_position) WHERE published_at IS NULL`, `(workspace_id, correlation_id)`, `(workspace_id, actor_principal)`, `(workspace_id, stream_id, recorded_at)`. **No `(workspace_id, global_position)` and no `type_url` index.**
  - `sqlite/store.go:84` sets `SetMaxOpenConns(1)`; `:158` enables WAL + 5s busy timeout.
  - `Server.Checkpoint` performs no `LeaseStore` lookup (`feed/server.go:364-372`), while `ResetCheckpoint` and `DeleteSubscription` do.
  - No feed daemon: `cmd/` contains only `es-migrate`, `es-verify`, `protoc-gen-es-lite`.

---

## 4. Consolidated findings

Severity: **P0** data loss / breach in production; **P1** must fix before production or before the platform freezes these ports; **P2** important, bounded; **P3** polish/backlog.
Confidence reflects reviewer + lead agreement after code verification.

| ID | Sev | Finding | Evidence | Confidence |
|---|---|---|---|---|
| R1 | **P0** | Postgres commit-order vs `global_position` causes silent permanent skips | `postgres/wsstore.go:345-364`, `postgres/migrations/0001:12-16`, `feed/server.go:302-319` | high |
| R2 | **P1** | Checkpoint acks are not fenced to the lease owner | `feed/server.go:153,364-372`, `sqlite/feed.go:128-141` | high |
| R3 | **P1** | Oversized/poison event wedges a subscription permanently; payload fully materialized | `feed/server.go:307-314`, `sqlite/feed.go:83-91`, `wsstore.go:379-388` | high |
| R4 | **P1** | No Postgres index for the feed page/head query | `postgres/migrations/0001:51-55`, `wsstore.go:360-364,432-435` | high |
| R5 | **P1** | Adding a consumed type silently skips all history for that type | `feed/ports.go:44-51,99-114`, `sqlite/feed.go:110-124` | high |
| R6 | **P1** | `BufferedMarker` crash window unbounded by time; no shutdown flush; reset can be resurrected | `projection/buffered_marker.go:100-107,124-146`, `projection/runner.go:182-191,204-207` | high |
| R7 | **P1** | No feed observability; `Status.Lag` is meaningless with type filters | `feed/server.go:410-443`, no metrics/log hooks | high |
| R8 | **P1** | The `feed.Server` → `projection.Feed` adapter does not exist; the ack-ordering invariant is unenforced | `projection/runner.go:16,142-169`; no `eventfeed` package | high |
| R9 | P2 | SQLite feed reads hold the single connection, stalling appends during replay | `sqlite/store.go:84`, `sqlite/feed.go:67-96` | high |
| R10 | P2 | Lease fencing trusts process clocks; NUL key collision; `instance` unvalidated | `sqlite/feed.go:213-287`, `feed/ports.go:134-137`, `feed/server.go:480-488` | high |
| R11 | P2 | Tenant authorization is 100% delegated with no fail-closed hook; RLS bypass is an unguarded GUC | `feed/ports.go:99-114`, `postgres/migrations/0001:64-68`, `postgres/drain.go:30` | high |
| R12 | P2 | Reconnect/backoff hot loop; errors swallowed; legacy `delivery` dies on first error; closed `Wake` busy-spins | `projection/reconnecting.go:60-88,99`, `delivery/poller.go:90-105` | high |
| R13 | P2 | No audit trail for `force` reset, delete, or checkpoint changes | `feed/server.go:364-405`, `sqlite/migrations/0003_feed.sql` | high |
| R14 | P2 | Feed has no ADR, guarantees section, runbook, or SLO; ADR 0002 still promises LISTEN/NOTIFY that does not ship | `README.md:49,104-107`, `docs/adr/0002-*` | high |
| R15 | P3 | API nits: inconsistent zero/negative semantics, `Load`/`Checkpoint` overlap, double read in `Status`, `newPrefix` constant fallback, deadline error duplication | `feed/server.go:94-99,129-135,219-230,414-423` | high |
| R16 | P3 | `maxBytes` applied after materializing the stopping row; huge first row always read | `sqlite/feed.go:79-91`, `wsstore.go:373-389` | high |
| R17 | P3 | SQLite `IN`-list variable cap; migration cross-process race; NUL bytes in TEXT keys; per-page RLS tx overhead | `sqlite/feed.go:56-65`, `sqlite/store.go:111-149`, `wsstore.go:352-359` | medium |

### R1 — the P0 in detail

`global_position` is a Postgres sequence assigned inside the append transaction (`RETURNING global_position`, `wsstore.go:167-182`), so it orders *insert attempts*, not commits.

Interleaving:

1. TX-A inserts `gp=100` and stalls before commit.
2. TX-B inserts `gp=101`, commits.
3. Feed page `WHERE global_position > 99` (READ COMMITTED) sees 101; `feed/server.go:318` advances `pos=101`; consumer acks 101.
4. TX-A commits. Next page reads `> 101`. **gp=100 is never delivered.**

Monotonic checkpoint `Save` (`MAX`/`GREATEST`) makes it permanent. No gap detection exists in `feed/`. `Drain` avoided exactly this with `FOR UPDATE SKIP LOCKED` + `published_at` (`postgres/drain.go:11-16,34-39`), and ADR 0001 §5 says the Postgres path “must not rely on a naive monotonic cursor.” The new read API contradicts the repo's own decision record.

**Does every deployment hit it?** It requires concurrent appenders on the same workspace and is more likely at high write concurrency. A single-writer workspace may never see it — which makes it worse, because it will pass tests and fail in production under load.

**SQLite is safe:** one connection + file write lock means sequence assignment and commit are effectively serialized. That is an invariant, not an accident — and it is currently untested (see R18).

### R2 — why ack fencing matters

`Subscribe` acquires a lease and streams; the consumer acknowledges out-of-band via `Server.Checkpoint(ctx, scope, projection, instance, position)`. That method never verifies the caller is the lease holder. After a takeover:

- the new stream redelivers from the last ack;
- a stale ack from the old consumer (e.g. an in-flight RPC) advances the checkpoint;
- the new consumer's lower ack is swallowed by monotonic `Save`.

Result: the new consumer skips events it never delivered. Delivery is fenced; acknowledgement is not. This must be fixed in the core, not delegated to the transport adapter.

### R3 — the poison wedge

There is no append-time payload cap in either store. `MaxPayloadBytes` (8 MiB default) is checked only after the page is read, and both adapters deliberately include the first row regardless of size. One 100 MB event:

- is materialized in memory (per reconnect), then rejected with a terminal error;
- `ReconnectingFeed` swallows the error and retries forever;
- every projection consuming that type is stuck at the same position, with no signal and no skip path.

The only recovery is an operator who parses the error position and calls `ResetCheckpoint(force=true)` — which R2/R11 make an unaudited, unfenced admin action.

### R5 — definition drift

The checkpoint key is `(scope, projection, instance)` only. The consumed type set is supplied per request and never recorded. Deploying a projection that adds `invoice.v1.Issued` at checkpoint 50,000 silently omits all `Issued` events up to 50,000 — no error, no warning, and a projection that looks healthy while incomplete. This is a one-way data-quality bug in the read model.

### R6 — the buffering illusion

`BufferedMarker` claims the crash window is bounded by `MaxInterval` (1s). In practice the interval is checked **only when `Save` is called again** (`buffered_marker.go:105`), and there is no timer goroutine. A stream that goes quiet leaves the last batch pending indefinitely. Worse, `Runner.Run` does not flush on return, and a shutdown flush using the cancelled context would be rejected by a DB-backed marker. `Save(0)` cannot clear a pending position, and `Load` flushes pending *back* over an out-of-band reset — so “reset the marker, then rebuild” can silently rebuild nothing.

### R7/R8 — the operability hole

There are no counters, no structured events, no readiness hook, and no runbook. `Status.Lag = head − position`, where `head` is the max over *all* types while `position` advances only on *matching* types — a fully caught-up filtered projection reports ever-growing lag. Separately, the adapter that is supposed to turn `feed.Server` into a `projection.Feed` and take the ack after the read-model commit is referenced in a comment (`runner.go:16`) but does not exist. The most important correctness invariant on the consumer path is currently a doc comment.

---

## 5. Risk register

Exposure = likelihood × impact, before mitigation. “Wired?” means the risk is live only if the platform connects the Postgres read API to `feed.Log` / exposes the admin RPCs.

| ID | Risk | Likelihood | Impact | Exposure | Trigger |
|---|---|---|---|---|---|
| R1 | Silent event loss on Postgres | High under concurrency | Critical | **Critical** (if wired) | Two appenders commit out of assignment order |
| R2 | Stale ack skips events | Medium | High | **High** | Lease takeover + in-flight ack |
| R3 | Subscription wedged by poison event | Medium | High | **High** | Any payload > 8 MiB or handler that never succeeds |
| R4 | Feed page degrades to partition scan | High at ≥10M events | High | **High** | Sparse type filter / large log |
| R5 | Projection silently incomplete | High as definitions evolve | High | **High** | Adding an event type to a released projection |
| R6 | Replay/rebuild after crash or reset is wrong | Medium | Medium | **Medium** | Quiet period + deploy; marker reset in-process |
| R7 | Stuck projection undetected | High | High | **High** | Any of the above |
| R8 | Consumer adapter breaks at-least-once | High (not built) | High | **High** | Platform writes its own ack path |
| R9 | SQLite appends stall during replay | Medium | Medium | **Medium** | Large catch-up on single node |
| R10 | Two active subscribers via clock skew / key collision | Low–Medium | High | **Medium** | Shared SQLite file, no NTP; NUL in names |
| R11 | Cross-tenant read / unauthorized reset | Low–Medium | Critical | **Critical** (if multi-tenant) | Adapter passes user-supplied scope; force RPC exposed |
| R12 | Reconnect storm / delivery stops silently | Medium | Medium | **Medium** | Feed flaps; transient DB error on legacy path |
| R13 | No audit of administrative actions | High | Medium | **Medium** | Compliance review / incident forensics |
| R14 | Operators cannot run or reason about the feed | High | Medium | **Medium** | First production incident |

**Risk posture:** R1, R2, R3, R5 and R11 are *silent* failures. That is the dominant theme: the system fails without an error, which means only external reconciliation catches it. Any production gate must include an end-to-end reconciliation/chaos test, not just unit tests.

---

## 6. Expert disagreements and CTO adjudication

| Disagreement | Resolution |
|---|---|
| Storage reviewer inferred “Postgres feed behavior is not exercised in CI” from 25 local skips. | **Incorrect.** CI runs `task test:pg` against Docker Postgres (`ci.yml:44-54`). The real gap is narrower: no out-of-order-commit test, and no `-race` on the PG job. |
| Protocol reviewer rated the SQLite cursor “safe”; storage reviewer rated the same area P0. | **Both right about different backends.** SQLite is gap-free by construction (single writer). Postgres is not. Keep the severity split in the scorecard. |
| Consumer reviewer called the missing adapter P1; API reviewer treated it as a documentation/dangling-reference issue. | **P1.** It is the load-bearing path for the at-least-once invariant. A dangling comment is not a contract. |
| Security reviewer rated tenant delegation P1; API reviewer treated authorization as an accepted design boundary. | **Accepted boundary, but not fail-closed.** The core's assumption is defensible; the absence of an optional `Authorize` hook and the unguarded RLS GUC are the actionable parts. Treat as a platform go-live gate, not a core blocker. |
| Performance reviewer measured SQLite page costs; storage reviewer flagged `IN`-list limits. | **No conflict.** Both are real; both are P2/P3 relative to R1–R3. |

---

## 7. Action plan

Estimates are engineer-days (ed) for one senior Go/Postgres engineer, including tests and review.

### Gate 0 — stop-the-line (do not wire the Postgres feed to production)

| # | Action | Addresses | Effort | Acceptance criteria |
|---|---|---|---:|---|
| A1 | Decide and implement the Postgres read contract. **Preferred:** add a commit watermark so `ReadAllBounded` only returns positions below a provably-committed threshold (e.g. filter on `xmin` below `pg_snapshot_xmin(pg_current_snapshot())`, or an in-flight/commit table maintained by the append path). **Fallback:** make `Workspace` *not* satisfy `feed.Log` and ship a separate adapter that requires a single writer per workspace, with the limitation documented and enforced. Record the decision in an ADR. | R1 | 4–6 | New chaos test: 8 concurrent appenders, randomized commit order, ≥100k events, 3 consumers; after replay, the consumed set equals the appended set exactly. No skips over 100 runs. |
| A2 | Fence acknowledgements. `Subscribe` returns a handle carrying the lease owner; `Checkpoint` requires that owner and verifies `Holder == owner` (or not held). Delete the owner-less overload or make it internal-only. | R2 | 2–3 | Test: consumer A holds, consumer B takes over after expiry, A's ack is rejected; B's cursor never jumps. |
| A3 | Payload/poison policy. Enforce `MaxPayloadBytes` at append in both stores; add a durable skip/quarantine operation (dead-letter row with position, type, error) and an operator-facing `Skip`/`DeadLetter` API. Push a size predicate into SQL so oversized rows are not materialized. | R3 | 2–3 | Test: poison event → stream surfaces a typed error → `Skip` → subscription advances → the dead-letter row is queryable and audited. |
| A4 | Test and CI hardening. Add the four missing tests (out-of-order commit, stale ack, concurrent SQLite writers, poison recovery); run the PG job with `-race -shuffle=on`; add a coverage gate and `golangci-lint`. | R1–R3, R18 | 2–3 | CI fails on any regression of the four scenarios; `-race` on Postgres. |
| A5 | Add `(workspace_id, global_position)` and `(workspace_id, type_url, global_position)` indexes; add an `EXPLAIN` assertion/benchmark for the feed page query. | R4 | 0.5 | Page query uses an index scan with no cross-workspace heap fetches at 10M rows; benchmark recorded. |

**Gate 0 total: ~11–15 ed.** No Postgres feed deployment, and no “production ready” claim, until A1–A4 are green.

### Gate 1 — before the platform freezes these ports / external consumers

| # | Action | Addresses | Effort | Acceptance criteria |
|---|---|---|---:|---|
| A6 | Definition identity. Store a fingerprint of `{Name, sorted Consumes}` with the checkpoint; reject `Subscribe` on mismatch with a typed, transport-mappable error instructing `ResetCheckpoint`. Enforce `Request.Projection == Definition.Name` (or remove the duplicate field). | R5, R15 | 2 | Test: subscribe `{A}`, checkpoint, resubscribe `{A,B}` → `ErrDefinitionChanged`; forced reset replays the new type. |
| A7 | Marker durability. Add a background flush timer at `MaxInterval`; flush with `context.WithoutCancel` during `Runner.Run` teardown; add `Reset`/`DiscardPending` semantics and document that rebuild requires a fresh marker or process restart. | R6 | 2 | Test: save without reaching `MaxCount`, advance the clock past `MaxInterval`, assert the inner marker is written; shutdown flush persists; reset is not resurrected. |
| A8 | Ship the consumer adapter (`eventfeed` or equivalent) that binds `feed.Server` to `projection.Feed`: `Sink.Send` → `handle`, ack only after `handle` returns nil, reconnect resumes from the acked checkpoint. Add a public conformance suite for `Log`/`CheckpointStore`/`LeaseStore` so the platform's Postgres adapters pass the same tests. | R8 | 3 | End-to-end test: kill the stream mid-batch, reconnect, assert no skip and only bounded duplicates; adapter is the only supported path (README + ADR). |
| A9 | Fail-closed authorization hook. Add optional `Config.Authorize(ctx, scope, projection, instance) error` (deny by default in multi-tenant mode); assert `env.Workspace == req.Scope` when non-empty; gate the RLS bypass arm on a relay role (`pg_has_role`), not just a GUC. | R11 | 2 | Adapter without `Authorize` cannot serve multiple scopes; RLS bypass fails for a normal role; test covers a cross-tenant attempt. |
| A10 | Observability. Define an `Observer` interface (or metrics hooks) with pages, events, bytes, lag, checkpoint age, lease lost, reconnect attempts, poison/quarantine. Add a matching head per definition and make `Status.Lag` either filter-aware or explicitly documented as an upper bound. Add readiness. | R7 | 2–3 | A stuck projection is detectable from metrics alone in <1 min; dashboard/runbook query provided. |
| A11 | Correct the docs. New ADRs: feed ordering/commit watermark, ack fencing, poison policy, definition identity. Update ADR 0001 §5, ADR 0002 (no LISTEN/NOTIFY ships), ADR 0036. Add a README “guarantees” block and a runbook. | R14 | 1–2 | README no longer claims a guarantee the Postgres path does not provide; runbook covers stuck/poison/lease/rebuild. |

**Gate 1 total: ~12–16 ed.**

### Gate 2 — hardening and scale

| # | Action | Addresses | Effort |
|---|---|---|---:|
| A12 | SQLite read path: open a second read-only connection (WAL allows one writer + N readers) for feed/checkpoint/lease reads, or document the replay-time write stall. Add a benchmark at 1M/10M rows. | R9 | 2 |
| A13 | Lease clock source: use DB time (`unixepoch('subsec')` in SQLite, `clock_timestamp()` in Postgres) for expiry and comparison; document NTP if app clocks remain. Reject NUL and validate `instance`; length-prefix or column-encode keys. | R10 | 1 |
| A14 | Reconnect/delivery hygiene: reset backoff only after sustained health (N events or T seconds); add an observer hook for swallowed errors; handle a closed `Wake` channel; decide the fate of legacy `delivery.Poller`/`Relay` (harden with retry + singleton lease, or mark deprecated). | R12 | 2 |
| A15 | Audit trail: thread actor/reason through `Checkpoint`, `ResetCheckpoint(force)`, `DeleteSubscription`; emit structured audit records. | R13 | 1–2 |

### Backlog (P3)

- Inconsistent zero/negative config semantics (`MaxPayloadBytes < 0` disables, `MaxBatchBytes < 0` defaults); make both explicit.
- Remove the `Load`/`Checkpoint` duplication and the double read in `Status`.
- `newPrefix` should fail loudly instead of falling back to `"feed"`.
- Drop `context.DeadlineExceeded` from the joined error when the caller's ctx expired.
- Dedupe/limit the `types` list for SQLite’s variable cap.
- Wrap the SQLite migration check in `BEGIN IMMEDIATE`.
- Avoid materializing the stopping row; apply `maxBytes` before scanning payloads.
- Consider a read-only `pgx.TxOptions` for feed pages.

---

## 8. Sequencing

| Sprint | Focus | Output |
|---|---|---|
| 1 | A1 (design spike + implementation), A4 (tests first) | An ADR for order-safe reads and a failing chaos test that proves the bug, then the fix. |
| 2 | A2, A3, A5 | Ack fencing, poison policy, indexes. Gate 0 exit review. |
| 3 | A6, A7, A8 | Definition identity, marker durability, consumer adapter + conformance suite. |
| 4 | A9, A10, A11 | Security gate, observability, docs/ADRs/runbook. Gate 1 exit review. |
| 5+ | A12–A15 + backlog | Hardening, scale benchmarks, audit. |

**Do not:** tag v1.0, publish a “production ready” claim, or let the platform implement its own ack path before Gate 0 and Gate 1 are green.

---

## 9. Definition of done (production gate)

A release is feed-ready only when all of the following pass repeatably in CI:

1. **Reconciliation chaos test** — N concurrent appenders with randomized commit order; M consumers with random kills; after replay, the applied event set equals the appended set, with duplicates allowed only within the replay window.
2. **Takeover test** — two consumers race a lease; at most one delivers at a time; a stale ack is rejected; no cursor jump.
3. **Poison test** — an oversized and a permanently-failing event are quarantined, audited, and do not block later events.
4. **Definition-drift test** — adding a consumed type without a reset fails closed.
5. **Crash/rebuild test** — kill after apply but before marker flush; restart; verify bounded redelivery and correct state.
6. **Operational test** — a stuck projection is visible in metrics/readiness within one minute; the runbook path resolves it.
7. **Postgres EXPLAIN gate** — the feed page uses the composite index at 10M rows.

---

## Appendix — Panel scope and evidence base

| Reviewer | Primary files | Headline contribution |
|---|---|---|
| Distributed systems | `feed/*.go` | Lease lifecycle is sound; ack fencing and poison gaps; commit-order hazard |
| Storage / SQL | `sqlite/feed.go`, `postgres/wsstore.go`, migrations | P0 commit-order skip; missing index; lease clock semantics; byte-budget correctness |
| Consumer / projection | `projection/*`, `delivery/*` | Missing adapter; `BufferedMarker` crash window and reset resurrection; reconnect hot loop |
| API / architecture | `feed/ports.go`, `projection/definition.go`, codegen | Ack fencing API, definition drift, `feed`→`projection` coupling, docs truthfulness |
| Testing / CI | all `*_test.go`, workflows | Strong core tests; four missing failure tests; PG job lacks `-race`; no coverage/lint gate |
| Security / tenancy | feed + Postgres RLS, ADR 0004/0025 | Crypto boundary holds; delegated auth has no fail-closed hook; RLS bypass is a GUC; audit gap |
| Performance / ops | feed loop, queries, runner | Missing index impact quantified; 4 RT/page; single-connection SQLite stall; zero observability |

**Review artifacts:** this document supersedes the feed-specific portions of `docs/review/20261028_review.md`; the pre-feed findings there remain valid where not restated.
