# es-lite Production-Grade Closure — What Was Done

- **Date:** 2026-11-06
- **Scope:** the full arc from the production review to the merged-ready branch `feat/postgres-production-grade-feed` (PR #16) — 29 commits, 19 planned tasks, three phases, one falsified design recovered mid-flight.
- **Companion documents:** the review that started it (`docs/review/20261104_feed_review.md`), the binding design (`docs/superpowers/specs/2026-11-04-postgres-production-grade-design.md`, rev 3), the plan (`docs/superpowers/plans/2026-11-04-postgres-production-grade.md`), and the decision record (**ADR 0037**).

---

## 1. Where it started

On 2026-11-04 a seven-reviewer production review (distributed systems, storage/SQL, consumers, API, testing, security, performance, with CTO adjudication) examined the newly built feed and returned a split verdict:

- **SQLite single-node: close to production-ready** (overall 3.5/5)
- **Postgres multi-tenant: NOT production-ready** (overall 1.5/5)

Seventeen findings (R1–R17) were recorded with a risk register and an action plan in three gates. The dominant theme: **the system failed silently** — events skipped, cursors jumped, projections stuck, with no error anywhere. Only external reconciliation would have caught it. The review's opening line for the P0: *"Fix or hard-disable before any Postgres feed deployment."*

| The findings that mattered | Sev | In one line |
|---|---|---|
| R1 | **P0** | Postgres `global_position` is assigned at INSERT but ordered by COMMIT — a consumer can advance past a lower position that commits later and **silently, permanently skip it** |
| R2 | P1 | Acknowledgements were not fenced by the lease — a stale consumer could move a shared cursor past events a new consumer never delivered |
| R3 | P1 | One oversized/poison event wedged a subscription forever; payloads fully materialized before rejection |
| R4 | P1 | No composite index served the feed page query — a partition scan at scale |
| R5 | P1 | Adding a consumed event type silently skipped all history of that type |
| R7 | P1 | Zero feed observability; `Status.Lag` was meaningless under type filters |
| R8 | P1 | The consumer adapter binding `feed.Server` to `projection.Feed` did not exist — the platform would have written its own ack path |
| R11 | P2 | Tenant authorization fully delegated; the RLS bypass was an unguarded GUC |
| R14 | P2 | Docs promised guarantees the Postgres path did not provide (truthfulness 2/5) |

---

## 2. The design — including the one that died

A design spec and a 19-task plan were written against the review. The original P0 fix was a **"settled ceiling"**: read the sequence's `last_value`, and deliver only below a bound that is provably committed.

**Task 1's spike falsified it.** Postgres snapshot `xmax` is the *commit* horizon, and in-flight xids at or above it are invisible to every snapshot — a lone in-flight append looks "idle" (observed `789:789` with transaction 2 provably in flight). The losslessness proof was invalid. Per the spec's own escalation gate, the finding went to the human, who chose the replacement: the **horizon fence**. The spec was amended to rev 3 with the spike record preserved, and Task 1b re-verified every assumption of the new mechanism on live Postgres.

### The horizon fence (what actually shipped)

Per page round on a `Workspace`:

1. Read the sequence's `last_value` → `L` (the highest position ever *assigned*).
2. Draw our own xid `R` (a real one, then roll back) and record `(L, R)`.
3. Deliver only rows `≤ L`, and only once `pg_snapshot_xmin ≥ R` — i.e. once every transaction that existed when we drew `R` has committed or aborted.

**Why it is lossless:** (P1) the append path draws its xid *before* it draws any position, so any transaction holding a position ≤ L has xid < R; (P2) `last_value` is ≥ every position ever drawn, so `xmin ≥ R` proves every row in `(from, L]` is final. The argument is reproduced as the implementation comment (`postgres/wsstore.go`) and recorded in ADR 0037.

**Cost, measured:** one extra round trip per append (106.8 µs ≈ **3.2%** of a batch-20 append) and one real xid per non-idle poll round per subscriber. At fleet scale the xid burn pulls autovacuum freeze in from "never" to days — ADR 0037 states the arithmetic (100 subscribers × ~4 polls/s ≈ 34.6M xids/day ≈ 1.6% of the 2³¹ space) and the runbook adds `age(datfrozenxid)` alerting.

Everything the fence needs survives a wedged long-lived transaction as *delay*, never loss — and the stall is observable (`RawHead` probe → `OnStall`).

---

## 3. What was built (19 tasks, three phases)

### Phase 1 — Gate 0 (the stop-the-line items)

- **Commit-safe reads:** the fence in `ReadAllBounded`; settled `Head` and fence-bounded `MatchingHead`; unbounded `RawHead` for stall detection. Pinned by deterministic red-first tests: out-of-order commit, straddled commit (one tx owning positions 1 and 3 across another's 2), aborted hole.
- **Composite indexes** `(workspace_id, global_position)` and `(workspace_id, type_url, global_position)` (migration 0003), with a runbook-first, partition-aware `CONCURRENTLY → ON ONLY → ATTACH` build order for large databases.
- **Append-time payload cap** (`WithMaxPayloadBytes`, 8 MiB default, both stores, caller-owned transactions included) and **two-step pages** that select sizes before payloads — an over-cap row is never materialized; the first over-cap row surfaces as a typed `*EventTooLargeError` carrying position/type/size/limit and *never the payload*.
- **`task test:feedgate`** — the standing nightly gate: chaos (8 appenders × 12.5k events, randomized 0–500 ms insert→commit separation, 3 consumers with disjoint type filters, deterministic kill windows), EXPLAIN plan gate at 1M rows, 100-subscriber idle-fleet burst, and the append benchmark.

### Phase 2 — the feed protocol

- **Fail-closed validation:** NUL/control bytes rejected in keys, projection/definition name must match, prefix entropy errors propagate, config semantics documented.
- **Key-triple ports:** lease and checkpoint stores take `(scope, projection, instance)`; the collision-prone `LeaseKey` string API deleted.
- **Lease-fenced acknowledgements:** `Server.Checkpoint(Ack)` verifies the holder — not held → `ErrLeaseLost`, foreign holder → `*LeaseHeldError`; a rejected ack never moves the cursor (asserted before and after).
- **Definition fingerprints:** `Definition.Hash()` over name + sorted consumed types; a changed definition fails `Subscribe` with `ErrDefinitionChanged` and a recovery path; reset *replaces* the identity; `Skip` preserves it.
- **Skip + dead letters:** an operator-facing `Skip` that refuses a held lease without `force`, records a dead-letter row (enriched store-side via a metadata-only lookup — payload never scanned) *before* advancing, and fails closed if the record fails.

### Phase 3 — adapters, operability, truth

- **In-repo Postgres feed adapters** (`Logs`, `CheckpointStore`, `LeaseStore`, `DeadLetterStore`): workspace-scoped, RLS-pinned with **no bypass arm** (migration 0004), DB clocks, mirroring sqlite semantics exactly.
- **`feedtest` conformance suite:** one contract suite both backends pass; the chaos gate now runs the real adapters.
- **Role-gated bypass:** migration 0005 gates the legacy `Drain` and anchor-path bypass arms on `pg_has_role(current_user, 'es_relay', 'member')`, and both fail fast (`ErrRelayRoleMissing`) instead of silently reading nothing. The review round here caught that the anchor path would otherwise have *silently written wrong anchor rows* post-gating — reproduced RED (a committed root over zero heads), then fixed with the same one-line gate.
- **Observability:** the `Observer` interface (pages, checkpoints, lease loss, poison, stalls), filter-aware `Status.Lag`, rate-limited stall probes, and the **fence-aware resume clamp** — previously a reconnect during write load replayed the entire log.
- **`eventfeed` consumer adapter:** binds `feed.Server` to `projection.Feed` — ack strictly *after* `handle` returns (the invariant that was once only a doc comment), page-coalesced acks, stream-end flush, self-steal reconnect token. Mutation-pinned: deliberately reordering the ack before the handler fails the test with the exact "acked before its handler succeeded" signature.
- **Fleet hygiene:** ±20% idle-poll jitter + a waiter floor — 100 idle subscribers now spread their polls across ≥12 of 15 buckets instead of stampeding in lockstep.
- **CI gates:** golangci-lint (errcheck/govet/staticcheck, two narrow documented exclusions), coverage ratchet (42.7% floor), `test:pg` under `-race -shuffle=on`, nightly feedgate cron, and the EXPLAIN flake fixed for real (`SET STATISTICS 1000`, six consecutive greens).
- **Truthful docs:** ADR 0037 (mechanism, both spike records, killer interleavings for every rejected alternative, xid costs, monitoring); ADRs 0001/0002/0010/0029/0036 corrected; a README **guarantees block** audited claim-by-claim against the code; a runbook covering roles, index ordering, poison triage, stall alerting, pool sizing, and lease RTO ≤ `LeaseTTL`.

---

## 4. The proof

Every task landed test-first (red evidence recorded), was independently reviewed, and the branch went through a three-pass whole-branch review plus one fix wave. The production gate (review §9) results:

| # | Scenario | Result |
|---|---|---|
| 1 | **Chaos reconciliation** — 100,000 events, 8 concurrent appenders with randomized insert/commit separation, 3 consumers with mid-page kill windows, store close/reopen | **PASS** — delivered set equals appended set exactly: 0 lost, 0 invented; per-stream hash chains intact; resume-clamp replays eliminated (redeliveries per kill: ~9,000 → 43/73/143, 0 clamp replays) |
| 2 | **Takeover** — stale ack after lease takeover | **PASS** — rejected; stored position asserted unchanged |
| 3 | **Poison** — oversized event | **PASS** — typed error, never materialized, `Skip` advances with an audited dead letter |
| 4 | **Definition drift** — add a consumed type without reset | **PASS** — fails closed with recovery path |
| 5 | **Crash/rebuild** — kill between handle and ack | **PASS** — bounded redelivery (≤ one page when acks are healthy), correct resume from checkpoint |
| 6 | **Operational** — stuck projection | **PASS** — visible via `OnStall`/`RawHead` in seconds; runbook maps every signal |
| 7 | **EXPLAIN gate** — feed page at 1M rows | **PASS** — composite index child serves it; no Seq Scan; stable across 6 runs |

Full-gate evidence is recorded in the SDD ledger and referenced from ADR 0037 and the runbook. Postgres suites run in CI under `-race -shuffle=on`; the feedgate runs nightly on `main`.

---

## 5. What it does NOT guarantee (stated on purpose)

- **Not exactly-once** — crashes and failovers redeliver bounded ranges; markers dedup.
- **No cross-stream causality** — sequence order is not real-time order.
- **No push wakeups** — polls at 250 ms ±20%; the `Waiter`/`Signal` seams exist for a future LISTEN/NOTIFY ADR.
- **Not every read API is fenced** — `LogFeed`, `Replay`, and `delivery.Poller` read raw; `Drain` is separately safe via its claim/outbox pattern. The fenced surface is the feed: `ReadAllBounded`, `Head`, `MatchingHead`.
- **RLS is not absolute** — no non-superuser, non-`BYPASSRLS` session can cross workspaces; superusers can (Postgres design; the runbook says connect feed processes as ordinary roles).
- **Same-token zombie overlap** — a reconnecting consumer that re-acquires its own lease duplicates for a bounded window; duplicates, never loss; lease epoch deferred as YAGNI.

## 6. Deliberately out of scope (backlog-filed)

`BufferedMarker` durability (R6/A7), the SQLite second read connection (R9/A12), the reconnect-error observer hook (R12/A14 — carries the SA4006 lint-exclusion deletion trigger), an audit trail beyond dead letters (R13/A15), the `Config.Authorize` fail-closed platform hook (A9). Each has a rationale in the ledger and an entry in `docs/backlog/`.

## 7. Migration notes for consumers

Pre-1.0 breaking port changes ship in this branch: key-triple stores, fenced `Ack`, the `definition` parameter on `Save`/`Reset`, `Log.RawHead`/`MatchingHead`, `Status(ctx, req)`. Consumers should:

- **ecas (v0.10.0)** and the **contracts plugin pin (v0.10.0)**: mechanical bumps once the next release is tagged.
- **platform (v0.9.0)**: its hand-written `adapters/postgres/{leases,checkpoints}.go` and local `eventfeed` package are superseded by the in-repo, conformance-tested adapters and `eventfeed.New` — the correct migration is mostly deletion, and it closes the review's "unreviewed platform adapters" and "platform writes its own ack path" (R8) exposures as a side effect.

---

*Methodology note: implement → independent review → fix rounds per task, a whole-branch final review, and a single fix wave — all verdicts, rulings, and red/green evidence recorded in the SDD ledger; the git history is the record.*
