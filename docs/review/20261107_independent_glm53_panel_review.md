# es-lite — Independent GLM 5.3 Panel Review

- **Review date:** 2026-11-07
- **Commit reviewed:** `main` @ `2fd3a76` (merge of `feat/postgres-production-grade-feed`)
- **Method:** six independent expert reviewers, each run as an isolated GLM 5.3 agent in a **fresh worktree** of the commit under review. The worktree contained no working-tree files from the main checkout, and each reviewer was explicitly barred from reading `docs/review/`, `docs/backlog/`, and `docs/superpowers/` — so no reviewer had access to any earlier review of this code, including `20261107_principal_review.md`. Findings were then adjudicated by the lead (this document records the adjudication, including one retraction of an earlier-pass claim).
- **Reviewer mandates:** read-only; verify every suspected issue against the exact call path before reporting; targeted test runs allowed (unit only — no Postgres/Docker); confidence required per finding.
- **Convergence evidence:** six findings from the earlier single-reviewer pass (anchor torn heads, relay-role latch, duplicate-`EventID` mapping, `Skip` rewind, filtered-stall false positive, sequence `SELECT` privilege) were **independently rediscovered by blinded reviewers**, several with their own throwaway reproductions or planner/`EXPLAIN` evidence.

---

## 1. Panel verdicts

| # | Domain | Score | One-line verdict |
|---|---|---:|---|
| R1 | Core model (`es`, `aggregate`, `upcast`, `snapshot`, `migrate`) | 4/5 | Production-ready core; one must-fix in the tamper-evidence path (verifier blind to head truncation) and a dead error contract. |
| R2 | Postgres backend | 4/5 | Fence/RLS/feed design is sound and superbly tested; anchor path lacks the fence, plus two liveness hazards (role-check latch, sequence-cache assumption). |
| R3 | SQLite backend | 4/5 | Correct for its role; missing `type_url` index and two constructor-contract holes. No P0/P1. |
| R4 | Feed protocol + `eventfeed` | 4/5 | Core is production-grade; the consumer adapter swallows the core's most important fail-closed error (`ErrDefinitionChanged`) into an infinite silent retry. |
| R5 | Projections, delivery, codegen, `cmdsvc` | 3.5/5 | Strong invariant thinking; two must-fix operational gaps (LogFeed silent wedge, `Idempotency-Key` without dedup) and a hot-spin trap. |
| R6 | Security, tenancy, operability, docs | 4/5 | RLS discipline and honesty are real; runbook never documents a single required grant (day-one breakage for least-privilege deployments), and the `anchors` policy was left un-role-gated. |

**Consensus:** no P0 (no silent data loss under default configurations). The P1 list is longer than any single reviewer's, because each domain contributed one or two.

---

## 2. Consolidated findings

Severity: **P1** = must fix before production; **P2** = important, bounded; **P3** = polish.
Attribution: `[R#]` = the blinded reviewer(s) that reported it; `repro` = reproduced live against PostgreSQL 17.11 during adjudication.

### 2.1 P1 — must fix before production

#### P1-1 Postgres `Anchor`/`CheckAnchor` record and then fail on torn heads under concurrent appends `[R2]` `repro`

- **Evidence:** `postgres/anchor.go:58-92` (`upto` from `MAX(global_position)` in one statement; `heads()` at `anchor.go:169-177` in a second statement — a different snapshot under READ COMMITTED; `bypassTx` sets no fence), `anchor.go:121-137` (`CheckAnchor` recomputes later), `cmd/es-verify/main.go` maps mismatch to the broken exit code.
- **What:** an append that drew a lower position and is still in flight when the heads query runs is invisible to it, while `upto` includes higher committed positions. The recorded root covers the stream's *older* head; when the in-flight append commits, `CheckAnchor` reports `ErrAnchorMismatch` with no tampering. The caller-supplied `upto > 0` variant is unstable by construction (a range still being filled). The repo already built the remedy — the horizon fence (`wsstore.go:407-494`) — but the bypass path never uses it.
- **Impact:** the runbook's anchor CronJob against a live database will, under sustained concurrent appends, record roots that later verification fails on: false tamper alarms, and a culture of dismissing anchor mismatches that a real attacker benefits from.
- **Live repro (adjudication):** committed A at pos 1, held B at pos 2 open, committed C at pos 3, anchored (`upto=3, streams=2`), committed B → `CheckAnchor` = `ErrAnchorMismatch` (streams 2, recomputed 3).
- **Fix:** apply the fence on this path — clamp `upto` to `last_value`, draw `R`, wait for `pg_snapshot_xmin >= R`, then compute heads; after that, heads at `<= upto` are permanently stable. Add a concurrent-append anchor test (none exists; `TestAnchorsPostgres` runs on a quiescent log).
- **Confidence:** high (mechanism certain; reproduced live).

#### P1-2 `VerifyStream` passes a stream whose earliest rows were deleted — head truncation is invisible to the default audit `[R1]`

- **Evidence:** `es/verify.go:80-94` (pagination from version 0, no first-version check), `es/verify.go:181-184` (a first envelope with version > 1 is accepted as a mid-stream start: `prev_hash` taken as given), `es/verify.go:96-99` (`Strict` only compares `ChainedFrom`), `cmd/es-verify/main.go` (strict off by default), `es/anchor.go:61-84` (anchor root covers head version+hash only — deleting v1..v5 of a 10-version stream leaves both the head and the root byte-identical), `es/verify_test.go:176-187` (the mid-stream pass is pinned as *intended* for snapshot-seeded loads).
- **What:** versions are 1-based and contiguous by contract, so a first surviving version > 1 is proof rows were deleted. The checker treats it as a legitimate mid-stream start and reports `Intact()`. `--strict` catches it only for fully-chained stores; for streams populated before migration 0002 (`ChainedFrom > 1` legitimately) strict cannot be used, and the default invocations (`es-verify stream`, `es-verify all`) pass silently.
- **Impact:** an operator with DB write access deletes a stream's oldest rows — squarely inside ADR 0029's threat model — and the default audit is green with `chained_from=N` and exit 0. For pre-0002-populated streams it is the one tamper that leaves no signal anywhere.
- **Fix:** in `VerifyStreamWith`, record the first envelope's version; if it is > 1, mark the report broken (`BrokenAt = firstVersion`, "head truncated"). Do **not** put the check in `ChainChecker.Next`, where mid-stream starts are legitimate (snapshot-seeded runtime loads). Note the interplay with P2-11: a *zero*-envelope read must be reported as "empty/not found" rather than "broken", so the two cases need distinct handling.
- **Confidence:** high (traced `check()` for a surviving-version-6 start; the existing test pins the wrong direction as passing).

#### P1-3 `eventfeed` swallows terminal Subscribe errors — `ErrDefinitionChanged` retries forever with zero signal `[R4]` `repro (throwaway test)`

- **Evidence:** `eventfeed/eventfeed.go:243-259` (`attempt.Feed` returns the Subscribe error only when the caller's ctx is still live), `projection/reconnecting.go:26, 56-88` (inner errors never surfaced; backoff → reopen forever), `feed/ports.go` (`Observer` has no subscribe-error hook), `feed/server.go:369-375` (drift fails after lease acquire, before any read — so not even `OnPage` fires).
- **What:** the core correctly fails closed on definition drift and names `ResetCheckpoint` as the recovery. The reference consumer classifies that terminal error exactly like a transient blip: retry with backoff, forever. Reviewer verified with an out-of-repo test: `Feed()` spun the full window retrying `ErrDefinitionChanged`, delivered nothing, called no observer method, and returned only when ctx ended; each attempt also acquires+releases the lease, so the key flaps. A persistent `ErrLeaseHeld` (duplicate deployment) dies the same silent death.
- **Impact:** any deploy that adds a consumed event type — the exact scenario the definition fingerprint exists for — leaves the projection silently stopped while the process looks healthy. No alert, no log, nothing an app can wire.
- **Fix:** give `ReconnectingFeed` a fatal-error escape (an `IsFatal func(error) bool` or a wrapped `*FatalError` type it propagates), mark `*feed.DefinitionChangedError` fatal in `eventfeed`, and at minimum add an observer hook (`OnSubscribeError`).
- **Confidence:** high (verified against the real `feed.Server` + `eventfeed` + `ReconnectingFeed`).

#### P1-4 `checkRelayRole` latches transient probe failures for the Store's lifetime `[R2]` `repro`

- **Evidence:** `postgres/drain.go:31-51` — inside `relayRoleOnce.Do`, *any* probe failure (including a canceled caller context) is stored as `relayRoleErr` and cached forever.
- **Live repro:** first `Drain` with a cancelled context → `es_relay membership check: context canceled`; a subsequent healthy `Drain` on the same Store → the same cached error.
- **Impact:** one shutdown-time or transient call permanently disables Drain *and* the anchor path in that process; anchors fail quietest of all (the CronJob simply records nothing). Tests cover the member/non-member halves, never the transient half.
- **Fix:** latch only the definitive verdict; on probe error, leave the once un-triggered and return the error so the next call re-probes. Probe on a Store-owned context (`context.WithoutCancel(ctx)` + short timeout), not the first caller's.
- **Confidence:** high (reproduced live).

#### P1-5 Required privileges are undocumented; the fence needs `SELECT` on the sequence, which `USAGE` does not confer `[R2, R6]` `repro`

- **Evidence:** `postgres/wsstore.go:407-415` (`SELECT last_value FROM events_global_position_seq`), `docs/deploy/README.md:249-250` ("grant them exactly the schema access they need" — no enumeration anywhere in the runbook), zero `GRANT` statements in migrations, CI connects as the container superuser so it cannot catch this. Also needed and undocumented: `SELECT` on `schema_migrations` (anchor path), `INSERT`+`DELETE` on `unique_claims` (Claim/Release), full CRUD on the feed tables, `SELECT` on events + `SELECT,INSERT` on `anchors` for the scheduler.
- **Live repro:** a role granted only `USAGE` on the sequence (the normal grant for the `nextval` default) gets `permission denied for sequence` on the fence statement — a hard outage of the entire delivery path, discovered only in production. `pg_sequence_last_value('...')` works with `USAGE` alone but returns `NULL` for a never-used sequence (needs `COALESCE`).
- **Fix:** add a per-principal grant list to the runbook (appender, feed process, relay, anchor scheduler), highlighting the sequence's split privilege; or switch the code to `COALESCE(pg_sequence_last_value(...), 0)`. Add a least-privilege end-to-end test — every CI Postgres test today runs as superuser.
- **Confidence:** high (reproduced live).

#### P1-6 `Idempotency-Key` is audit metadata only; retries double-write `[R5, lead]`

- **Evidence:** `cmdsvc/cmdsvc.go:26-29, 63-83` (header → `Meta.CommandID` only); `sqlite/migrations/0001_initial_schema.sql:44` (`events_command_idx` is a plain index, not unique); no dedup check anywhere in `aggregate/runtime.go`; `cmdsvc/example/example_test.go:130-137` — the example test itself proves a second request with the same key commits a second event.
- **Impact:** a client that times out after the server commits and retries with the same key gets a double write for any non-self-guarding command. The header name carries the industry meaning (draft-ietf-httpapi-idempotency-key), so the name invites trust the stack does not deliver. There is also no seam in the generated handler to add dedup.
- **Fix:** either rename (e.g. `Command-Key`) and document loudly that dedup must live in deciders, or provide an opt-in dedup seam (`Deps.Seen(ctx, sid, commandID)`, or the documented recipe: unique index on `(stream_id, command_id)` + `ErrConstraintViolated` → `AlreadyExists`, which `MapError` already maps).
- **Confidence:** high (schema, runtime, generated handler, and the example's own behavior verified).

#### P1-7 `LogFeed` retries a failing handler forever, silently, discarding the error `[R5]`

- **Evidence:** `projection/runner.go:59-66` — `for { if err := handle(...); err == nil { break }; sleep(ctx, poll) }`: no attempt cap, no backoff growth, no dead-letter escape, and the error is never surfaced, logged, or counted. Contrast `projection/reconnecting.go:84` (backoff) — and note `projection/` has no logging or observer seam at all.
- **Impact:** any persistent handler error (read-model DB down, a poison event, `LoadAsOfVersion` returning `ErrVersionOutOfRange` for a truncated stream — a *permanent* error) head-of-line blocks the projection forever with the process looking "up" to every supervisor. This is the documented live path for tests and single-node SQLite.
- **Fix:** an observability seam (`OnRetry func(env, err, attempt)` or an `OnError` callback), growing delay, and special-casing permanent errors; keep at-least-once semantics. `projection/runner.go` currently has **no direct tests at all** — add them with the fix.
- **Confidence:** high.

### 2.2 P2 — important, bounded

#### P2-1 The fence's lossless argument silently breaks if the sequence is given `CACHE > 1` `[R2]`

- **Evidence:** `postgres/migrations/0001_initial_schema.sql:12` (nothing pins `CACHE 1`), `wsstore.go:61-64` (P2 of the proof: "any row at a position ≤ L drew that position before the reader read L"), `wsstore.go:404-406` (the comment acknowledges only the fresh-sequence overstatement).
- **What:** with `CACHE n>1`, `last_value` advances a full cache block on the first `nextval`, and the cached positions are *drawn later* — after a fence round may have read L, cleared, and delivered past them. A later append (xid > R, invisible to the fence) then inserts a row at a position ≤ the already-acked bound: a permanently skipped event — the exact P0 class the fence exists to prevent. `ALTER SEQUENCE ... CACHE 32` is a reasonable-looking throughput tweak an operator could make.
- **Fix:** assert at startup (`SELECT seqcache FROM pg_sequence WHERE sequencename = 'events_global_position_seq'`) and fail loudly if > 1; document the invariant in the migration and runbook.
- **Confidence:** high on the mechanism (sequence cache semantics; the proof's needed direction is the converse of the one the spike verified).

#### P2-2 The migration advisory lock can leak onto a healthy pooled session and wedge all future migrations `[R2, lead]`

- **Evidence:** `postgres/store.go:188-191` — the deferred `pg_advisory_unlock` runs on the caller's (possibly cancelled) context and its error is discarded; if it fails, the connection is *clean* (transaction already ended), so pgxpool returns it to the pool still holding a session-level lock (verified against pgxpool source: only busy/in-tx connections are destroyed). No `lock_timeout` is set, so the next `Migrate` blocks forever.
- **Fix:** unlock on `context.Background()` (or use `pg_advisory_xact_lock` inside one transaction), and set a `lock_timeout` so a future Migrate fails loudly instead of hanging.
- **Confidence:** high (pgxpool/tx source verified).

#### P2-3 The `anchors` table's bypass arm was left un-role-gated; migration 0005's comment misstates its own coverage `[R6]`

- **Evidence:** `postgres/migrations/0002_hash_chain.sql:32-35` (`anchors_admin_only` is GUC-only), `0005` rewrites only `events` and `unique_claims`, while its comment says "Both policies that carry the arm (events and unique_claims, the pair 0001 created …)" — three policies carried the arm; `drain_test.go:266` concedes "the anchors policy is GUC-only, so the write succeeds".
- **What:** the Go path fails fast (`bypassTx` → `checkRelayRole`), but any principal holding `INSERT` on `anchors` can forge anchor rows (and with `UPDATE`, rewrite past roots in place) by setting a GUC any session can set. Off-box exports of *past* roots remain the real trust anchor, but a forged latest row can be exported by an unwary operator during a scheduler outage.
- **Fix:** ship a `0006` adding the same role gate to `anchors_admin_only`; correct the 0005 comment and ADR 0029's wording; state in the runbook that only the anchor principal gets `INSERT` on `anchors` and nobody gets `UPDATE`/`DELETE`. Add a migration-level policy assertion (today only the feed tables' policies are asserted).
- **Confidence:** high (policy text and absence of a later migration verified).

#### P2-4 `global_position` is covered by neither chain nor anchor; the docs claim otherwise `[R6, lead]`

- **Evidence:** `es/hash.go:45-46` ("attested by anchors instead"), `docs/adr/0029:86-89`, vs `es/anchor.go:61-85` (root hashes workspace, stream_id, version, head hash — no position) and `0001` (PK/UNIQUE constrain identity, not position).
- **What:** an attacker with DB write access can `UPDATE events SET global_position = ...` — reordering delivery across streams — without breaking the chain (position is not hashed) or the anchor root. Within-stream order is pinned by the hashed `version`/`prev_hash`; cross-stream order is unaudited.
- **Fix:** make the docs match the mechanism, or fold position into attestation in a future migration (insert-then-attest is already the anchor's pattern for the cutoff).
- **Confidence:** high.

#### P2-5 Append-only is convention-only at the DB level; the relay is over-granted `[R6, R2, lead]`

- **Evidence:** `0001`'s events policy `WITH CHECK` admits any workspace-scoped `UPDATE`/`DELETE`; no trigger, no column-level privilege, no `REVOKE` guidance; `Drain` only ever updates `published_at` (`drain.go:120-123`) but nothing scopes the grant; even the test harness grants blanket `UPDATE` (`drain_test.go:170-172`).
- **Fix:** extend the grants section (P1-5): appender roles get `SELECT,INSERT` on events only; the relay gets `GRANT UPDATE (published_at) ON events`; no role gets `DELETE`/`TRUNCATE`. Optionally a trigger rejecting updates to any other column.
- **Confidence:** high.

#### P2-6 `es-verify` reports success for streams that don't exist `[R6]`

- **Evidence:** `es/verify.go:80-94` (empty read → intact report), `cmd/es-verify/main.go` `verifyOne` default branch prints `ok ... head=v0 chained_from=0` and exits 0; Postgres `readStream` returns an empty slice with no not-found error; `es/verify_test.go:28-31` pins the tolerance as intended; `cmd/es-verify/` has no tests at all. `es-verify all` on an empty DB prints "0 streams checked" and exits 0.
- **Impact:** a typo'd stream id — or a real id under the wrong workspace argument — audits nothing and reports a false-clean result. Unacceptable shape for an audit tool.
- **Fix:** treat `rep.Head == 0` as an explicit outcome ("empty/not found"), exit non-zero for `stream` unless `--allow-empty`; same for `all` with zero streams. (Distinct from P1-2's broken-head case, which must read as *tamper*.)
- **Confidence:** high.

#### P2-7 `OnStall` false-positives for caught-up filtered subscriptions `[R4, lead]` `repro`

- **Evidence:** `feed/server.go:413-426` (probe compares the subscription's `pos` against the **unfiltered** `RawHead`), `feed/ports.go:117`.
- **What/Impact:** for a subscription caught up on its own types, `raw > pos` holds trivially whenever other types exist above it — verified: a fully idle subscriber reported `OnStall(settled=1, raw=901)`. In any multi-type workspace every sparse subscriber emits bogus stalls (re-armed by every page and reconnect), training operators to ignore the signal the fence exists to produce.
- **Fix:** the `Log` port lacks the needed view — add a raw *matching* head and compare against the subscription's own types; interim: document precisely what `OnStall` means.
- **Confidence:** high (reproduced live and by a blinded reviewer independently).

#### P2-8 The resume-clamp loop emits no signals at all while a fence is blocked `[R4]`

- **Evidence:** `feed/server.go:389-404` — only `RawHead`/`wait`/`Head`; `OnPage` fires only after `ReadAllBounded` (line 451) and `probeStall` only in the main loop's empty-page branch (line 486) — both unreachable while the loop spins; `wsstore.go:668-697` (Head reports 0 while blocked).
- **Impact:** a stream that (re)connects while a wedged transaction pins the fence, with a checkpoint above the settled head, probes once per Poll with zero observer output — verified: 400ms in the clamp loop produced no signals of any kind. An idle-in-transaction session wedges subscriptions invisibly.
- **Fix:** rate-limited stall probe inside the clamp loop (or at minimum `OnPage(0,0)` per probe for liveness).
- **Confidence:** high (verified with a fence-blocked fake).

#### P2-9 `Skip` can silently rewind the checkpoint `[R4, lead]` `repro`

- **Evidence:** `feed/server.go:711` (`Reset(..., sr.Position)` with no monotonicity check) vs the doc's "advances".
- **Live repro:** checkpoint 100 → `Skip(Position: 50)` → checkpoint 50, plus a dead-letter row for an event that was not skipped.
- **Fix:** refuse `sr.Position < stored` unless `Force`, or use the monotonic `Save` path; keep `ResetCheckpoint` as the explicit rewind.
- **Confidence:** high (reproduced live).

#### P2-10 The shared conformance suite never exercises `RawHead` or `MatchingHead` `[R4]`

- **Evidence:** `feedtest/conformance.go` — zero matches for either method; `RunLog` covers only `Head` and `ReadAllBounded`.
- **Impact:** the core's fence-aware clamp, stall detection, and `Status` lag all depend on two port methods the "portable statement of what every adapter owes the core" never calls. A future adapter implementing `RawHead` as `Head` (or 0) passes the full suite and silently breaks every blocked-fence resume into a full-log replay.
- **Fix:** portable conformance cases (`RawHead >= Head`; both reach max after settled appends; `MatchingHead` filtering and empty-types→0). Backend-local tests keep the blocked-fence divergence.
- **Confidence:** high.

#### P2-11 SQLite has no `type_url` index for the feed's filtered queries `[R3, lead]`

- **Evidence:** `sqlite/migrations/0001_initial_schema.sql:43-46` vs `postgres/migrations/0003_feed_indexes.sql:19`; queries at `sqlite/feed.go:64-65` (`MatchingHead`) and `sqlite/feed.go:113-115` (`ReadAllBounded` step 1).
- **What:** verified with `EXPLAIN QUERY PLAN`: `MatchingHead` full-scans; the page query walks the rowid range evaluating `type_url` per row. A rare-type subscription's cursor only advances on matches, so each ~250ms poll rescans from the cursor to end-of-log — on the single shared connection, blocking appends behind the scan.
- **Fix:** `CREATE INDEX events_type_pos_idx ON events (type_url, global_position);` — reviewer verified the planner switches to per-type probes; Postgres 0003 already accepted this exact trade.
- **Confidence:** high (planner output reproduced with and without the index).

#### P2-12 `sqlite.NewWithDB` forfeits the package's documented guarantees, and `Close` closes the caller's handle `[R3, lead]`

- **Evidence:** `sqlite/store.go:125-134` (neither `SetMaxOpenConns(1)` nor pragmas) vs `:102-121` (`Open` does both) and the package doc's single-connection claim; `sqlite/store.go:179` (`Close` unconditionally closes) vs `postgres`'s `ownsPool` ownership split.
- **What:** a multi-connection `*sql.DB` yields spurious `SQLITE_BUSY`/`BUSY_SNAPSHOT` surfaced as opaque insert errors; a `mode=memory` DSN without shared cache gives each pool connection a *different empty database*; a non-modernc driver makes `isConstraintViolation` (`:547-555`, modernc-only type check) never match, so `ErrConflict`/`ErrConstraintViolated` silently stop working. Positions remain safe (SQLite serializes write transactions at the file level) — the guarantee loss is liveness and error mapping, plus the `Close` ownership trap.
- **Fix:** enforce or loudly document the single-connection contract in `NewWithDB`, verify the driver, and add `ownsDB` so `Close` mirrors Postgres.
- **Confidence:** high.

#### P2-13 Generated projections/reactions have no upcaster path; `ev` and `state` can disagree `[R5, lead]`

- **Evidence:** dispatch decodes via `deps.<X>Codec` only (`cmd/protoc-gen-es-lite/projection.go:351`); upcasting exists only inside the aggregate fold (`aggregate/runtime.go:465-470`); the generated codec's `Decode` never reads `enc.SchemaVersion` (`gen/counter/v1/counter_es.pb.go:108-126`), so a *future* schema version decodes silently with defaulted fields; `upcast.Registry` is not an `es.Codec` and no glue composes it into Deps.
- **Impact:** with legacy events (see `examples/upcast/upcast_test.go`), a `STATE_AS_OF` projection gets **upcast state** (via the runtime) alongside the **raw legacy event** — the handler sees half-units in `ev` against state computed with doubled units. The nastiest class of projection bug, and it is silent.
- **Fix:** generate an optional `Upcaster` Deps field (or a helper adapting `upcast.Registry` + codec into an `es.Codec`); consider rejecting `SchemaVersion` greater than the variant's current version.
- **Confidence:** high on mechanics.

#### P2-14 `FETCH_MODE_STATE_AS_OF` is quadratic: a full re-fold per consumed event `[R5, lead]`

- **Evidence:** generated dispatch calls `LoadAsOfVersion` per event (`gen/counterview/v1/view_es.pb.go:114,128,142,156`); `aggregate/runtime.go:423` skips snapshot seeding for time-travel loads (`upToVersion != 0`), so every call re-reads and re-folds the stream from version 0.
- **Impact:** event k costs O(k): a 10k-event stream pays a 10k-event fold per live event; a rebuild is ~N²/2 decodes+folds — on exactly the longest-lived, busiest aggregates. The docs sell the default loader as needing "no adapter".
- **Fix:** document the cost beside the Deps field; ship an incremental/cached loader (fold-forward cache keyed by (sid, version), or batch prefetch in the dispatch path).
- **Confidence:** high.

#### P2-15 Poller/Relay: `Run` dies on the first transient error, and a closed `Wake` channel hot-spins `[R5]`

- **Evidence:** `delivery/poller.go:85-107` and `delivery/relay.go:53-76` (any drain error with a live ctx ends `Run`, despite the doc "drains until ctx is cancelled"); `delivery/poller.go:102-105` / `relay.go:70-73` (a closed `Wake` channel makes the `select` loop spin — each iteration a full checkpoint load + `ReadAll`); no reconnecting wrapper exists for the legacy path (unlike feeds' `ReconnectingFeed`), and `NewPoller`/`NewRelay` don't validate `src`/`cp`/`drainer`.
- **Impact:** one network blip permanently stops an unattended consumer; a dead wake-source goroutine that closes its channel turns the relay into a 100%-CPU DB hammer. `delivery/poller.go` has zero package tests.
- **Fix:** backoff-retry inside `Run` (bounded, with the final error preserved), guard/document the `Wake` contract, validate dependencies at construction.
- **Confidence:** high (semantics verified; spin path read, not executed).

#### P2-16 `BufferedMarker`'s "crash window bounded by MaxInterval" is false when traffic stops `[R5, lead]`

- **Evidence:** `projection/buffered_marker.go:22-27` (the claim) vs `:16-18` (the accurate "next Save after it elapses flushes"); flush reachable only from `Save`/`Load`/`Flush`; no timer; `Runner` has no `Flush`/`Close`; `loadMarker` runs once per process. Referenced nowhere outside its own package and tests, so the trap is undiscoverable.
- **Fix:** background timer flush (or a documented shutdown pattern via a new `Runner.Flush`), and fix the type doc. `Load` also resurrects a buffered position over an out-of-band reset, so "reset then rebuild" can rebuild nothing — a `Discard` path is needed.
- **Confidence:** high.

#### P2-17 Snapshots are not lineage-checked; a snapshot newer than the log is served as current state `[R1, lead]`

- **Evidence:** `aggregate/runtime.go:423-430` (seeds state and `from = snap.Version`), `:436-444` (adapters return empty/nil for a nonexistent range — `ErrStreamNotFound` never fired), `:450` (`version = from`), `:474-477` (range check only for `upToVersion > 0`); `snapshot/snapshot.go:4-5` claims the runtime "just folds a longer tail".
- **What:** if the cache is ahead of the log (restore, cell copy, stale shared cache), `load` folds nothing and returns the cached state at `snap.Version` as current — then `Handle` wedges on conflicts at that version forever while `Load`/`LoadStale` keep serving the ghost. The doc's claim is false in exactly this case.
- **Fix:** when `from > 0` and the read returns no rows, discard the seed and refold from 0 (or one `CurrentStreamVersion` check).
- **Confidence:** high on mechanism (traced both adapters); medium on real-world occurrence. Note: **the entire snapshot feature has zero test coverage** — no `snapshot.Cache` implementation exists in the repo.

#### P2-18 A duplicate caller-supplied `EventID` is reported as `ErrConflict` `[R2, R3, lead]` `repro`

- **Evidence:** `postgres/wsstore.go:255-259` (any 23505 during the events INSERT → `ErrConflict`; `pgErr.ConstraintName` never read) and `sqlite/store.go:292-297` (same mapping); `events` carries two unique constraints; live repro: same UUID on two streams → `es-lite: append conflict … errors.Is(ErrConflict)=true`.
- **Adjudication:** safe but misleading — the whole transaction rolls back either way, the runtime never sets `EventID` (only direct store users reach it), and both backends behave identically (parity). Downgraded from the earlier pass's P2 emphasis to P2/P3: fix the classification (`events_pkey` → conflict; `events_event_id_key` → a distinct error) and keep the underlying DB error in the message.
- **Confidence:** high (reproduced live).

#### P2-19 `ErrStreamNotFound` is a dead contract `[R1, lead]`

- **Evidence:** `es/errors.go:17-19` documents it; neither adapter returns it (both return empty results); the only producer is a test fake; `es/verify.go:82` and `aggregate/runtime.go:442` carry dead tolerance branches — validated only against a fake real backends don't imitate. This dead branch is also what makes P1-2/P2-6's empty-stream ambiguity possible.
- **Fix:** implement it in both adapters or rewrite the doc; either way, resolve it together with P1-2/P2-6 so "empty", "not found", and "head truncated" are three distinct, tested outcomes.
- **Confidence:** high (exhaustive grep).

#### P2-20 CI: the coverage gate excludes the production backend; release actions are unpinned with `contents: write` `[R6]`

- **Evidence:** `Taskfile.yml:127-141` (`COVERAGE_FLOOR: 42.7` over the unit suite; measured in the review worktree: `go test -cover ./postgres/` → **0.5%** of statements — every Postgres suite skips without `PG_DSN`); `.github/workflows/release.yml:20-23` (`actions/checkout@v4`, `jdx/mise-action@v2`, `softprops/action-gh-release@v2` unpinned in a job holding `contents: write`).
- **Fix:** pin all actions to SHAs; publish an integration-coverage report (or per-package floors) so the ratchet speaks to `postgres/`.
- **Confidence:** high (coverage measured directly).

### 2.3 P3 — polish (consolidated)

- **Postgres**
  - Requires PG 14+ (`pg_current_xact_id()`, `wsstore.go:179`) but nothing documents or checks it; CI only runs 17 `[R2]`.
  - `DBTX` is satisfied by `*pgxpool.Pool`: `AppendTx` then fails loudly at RLS, but `ReadStreamTx` returns zero rows silently (the USING arm hides everything) `[R2]`.
  - `octet_length(payload)` in the two-step page detoasts *and decompresses* the full payload server-side — the "costs one integer" claim is true on the wire, not in CPU `[R2]`.
  - Clock-source inconsistency: feed tables use `clock_timestamp()` in SQL, but `Drain.published_at` and `Anchor.created_at` use app `time.Now()`; and the wsstore comment "same transaction and therefore the same snapshot" is false under READ COMMITTED (safety actually follows from position immutability) `[R2]`.
- **Core**
  - `StampDecisionTime`'s false return is ignored (`aggregate/runtime.go:229`): a hand-written command implementing `DecisionTime()` but no setter is skew-checked but never normalized — Decide reads client time, exactly what ADR 0034 forbids. Fail the handle or document `[R1]`.
  - `AnchorRoot`: a corrupt-length head hash is silently treated as an unchained head (should substitute only on `len == 0` and reject otherwise); the doc says "sorted by (workspace, stream_id)" but the code sorts by the canonical *string* — a reimplementing auditor can compute a different root; no golden vector exists for the root, unlike the envelope hash `[R1]`.
  - `ChainReport.ChainedFrom` is checker-relative (a chain hashed from v1 reports `ChainedFrom=3` from a v3-seeded checker) but documented as a stream property `[R1]`.
  - `recorded_at` "monotonic" assumes no OS clock step-back; a step can make `ReadStreamAsOf` return a non-prefix (silent wrong time-travel, or a false `ErrChainBroken` under `WithChainVerification`) `[R1]`.
- **SQLite**
  - `scanEnvelope` ignores malformed correlation/causation/command UUIDs (parses to `uuid.Nil`, audit trail lies) while validating `event_id` `[R3]`.
  - Migration runner has no cross-process lock ("no lock needed" comment overstates; check-then-apply race on bare `ALTER TABLE ADD COLUMN`) — loud, self-healing, but fix the comment or use `BEGIN EXCLUSIVE` `[R3]`.
  - Dead columns: `stream_type` (never read, no index for its stated purpose), `checkpoints.updated_at`, `unique_claims.claimed_at`; the Postgres `checkpoints` table is entirely dead *and* is the only es-lite table without RLS `[R3, R6, lead]`.
- **Feed**
  - `Subscribe` can drop a lease-loss diagnosis that races the stream's own error (non-blocking `lost` drain at `server.go:266-270` runs before the renewer can stage); restructure to `cancel(); renewals.Wait()` then drain `[R4]`.
  - The `Waiter` floor means wakeups can never beat the `Poll` cadence — the port cannot distinguish a wakeup from a timeout, so the feature is a load-shedder, not a latency win; extend the port or document `[R4]`.
  - `Definition.Hash` NUL-joins `Consumes`; `validateDefinition` doesn't reject control bytes there (defense-in-depth only — generated type URLs can't contain NUL) `[R4]`.
- **Projections / codegen / cmdsvc**
  - Required generated deps (`Identity`, `Authorize`, `Runtime`, `Counter`, `CounterRouter`) panic at first use instead of at wiring — `Validate` gets a nil default, the required ones get nothing; `New<Name>Handler`/`New<Name>Runner` should validate and name the missing field `[R5]`.
  - `Replay`'s short-page-means-exhausted assumption isn't stated on the `Source` interface — a lagging custom Source silently truncates a rebuild; `Rebuild` returns the log-end position, not the marker position, while the doc invites cutover "at exactly the returned position" `[R5]`.
  - `MetaFrom` silently drops a non-UUID `Correlation-Id`; `MapError`'s doc table omits the `context.Canceled`/`DeadlineExceeded` rows it implements; `resolveMethod` doesn't validate the `result` message's WriteResult shape (fails per-request as `Internal` instead of at generation) `[R5]`.
  - No `FETCH_MODE` for "typed events without aggregate state" — the most common projection shape pays state loads or hand-decodes `env.Payload` `[R5]`.
  - Generator: `commandVariants` dead (`main.go:174-193`); `_ = meta` (`service.go:381`); byte-identical `default:` branches in `writeDispatch`; `specNames` is mutable process-global state reset inconsistently by tests `[R5, lead]`.
  - `NewPoller`'s `drain` reloads the checkpoint every batch iteration (one redundant round trip per batch); Poller/Relay `Run` bodies near-duplicates `[R5, lead]`.
- **CLI / docs / ops**
  - `es-migrate`/`es-verify` silently default `SQLITE_DSN` to `./eslite.db` — a typo migrates/verifies a fresh empty DB and reports success; require it or warn loudly `[R6]`.
  - The 0005 policy arm calls `pg_has_role` without the `CASE WHEN EXISTS` guard the Go check has; if `es_relay` is missing, `pg_has_role` errors and `OR` evaluation order is not a documented contract — mirror the Go guard in a follow-up migration or soften the runbook claim `[R6]`.
  - ADR 0001's cut table and the ADR index still claim es-lite omits multi-tenancy/RLS and the hash chain — both were added back (0004, 0029); add errata `[R6]`.
  - Runbook says the relay-role check happens "once per `Store` at startup" — it is lazy, on the first bypassed call; one line in the runbook contradicts another `[R6]`.
  - Nothing warns that workspaces sharing one database login are mutually trusting under RLS, or covers TLS/DSN hygiene for relay/anchor principals `[R6]`.

---

## 3. Adjudication: corrections to the earlier single-reviewer pass

The blinded panel was run partly to check the earlier pass (`20261107_principal_review.md`). Results:

1. **RETRACTED — "SQLite has the same anchor race, and is arguably worse."** Wrong. SQLite admits one writer at a time, so an uncommitted row's position is necessarily *above* every committed position; `upto = MAX(committed)` is therefore a stable cutoff and the un-transactional `MAX` + `heads(upto)` pair is consistent (all future inserts land above `upto`). The anchor race is **Postgres-only** (concurrent writers can hold lower positions uncommitted while higher ones commit). The SQLite half of that finding should be dropped; the Postgres half stands, reproduced live and independently confirmed by R2.
2. **Downgraded — duplicate `EventID` → `ErrConflict`.** The earlier pass emphasized retry storms; R2/R3's adjudicated view is correct: the transaction rolls back either way, the runtime never supplies `EventID`, and both backends behave identically. It is a diagnostics bug (misclassified permanent error, discarded `pgconn.PgError`), not a liveness hazard. Kept as P2-18.
3. **Confirmed independently (blinded):** anchor torn heads (R2), relay-role latch (R2), `Skip` rewind (R4), filtered-stall false positive (R4, with its own test), sequence `SELECT` privilege (R2+R6), SQLite type index (R3, with planner evidence), `NewWithDB`/`Close` contract (R3), advisory-lock leak (R2, with pgxpool source evidence), snapshot lineage (R1), upcaster gap (R5), quadratic state loading (R5, sharpened: snapshot seeding is *skipped* for time-travel loads, so every call folds from zero), `Idempotency-Key` (R5), `BufferedMarker` window (R5), `global_position` unattested (R6), append-only grants (R6), dead `checkpoints` table (R2/R6), dead generator code (R5).
4. **Not found by the panel (kept from the earlier pass on the lead's own verification):** the fence's idle check burns transaction IDs per *subscriber* whenever any workspace writes (`last_value` is global — ADR 0037's "idle streams burn no transaction ids" only holds when the whole database is idle); the duplicated 20-line token-policy comment in `eventfeed`; the ~10× copy-pasted transaction scaffolding in `postgres/wsstore.go`; `feed.Server.stream`'s ~140-line span (partially addressed by R4's clamp-loop finding).
5. **Sharpened by the panel:** the "no upcaster" finding now has a concrete divergence example (upcast state vs raw event in one handler call); the grants finding now has a full per-principal list; the coverage finding now has a measured number (0.5% for `postgres/`).

---

## 4. What the panel found that the earlier pass missed

The headline new findings, all from blinded reviewers:

| Finding | Reviewer | Why it matters |
|---|---|---|
| **Head truncation is invisible to the default audit** (P1-2) | R1 | Deleting a stream's oldest rows passes `es-verify` and leaves anchors byte-identical — the one tamper with no signal on populated stores. |
| **`eventfeed` retries `ErrDefinitionChanged` forever, silently** (P1-3) | R4 | The fail-closed definition gate the system is proud of is neutered by its own reference consumer; projections stop silently on the most routine deploy. |
| **The `anchors` policy was never role-gated; 0005's comment is false** (P2-3) | R6 | Any principal with `INSERT` on `anchors` can forge anchor rows by setting a GUC; the migration that claims to have closed the bypass left a third arm open. |
| **Sequence `CACHE > 1` silently breaks the fence's lossless proof** (P2-1) | R2 | One reasonable operator tweak reintroduces the P0 the fence was built to prevent; nothing asserts the invariant. |
| **`es-verify` prints `ok`/exit 0 for nonexistent streams** (P2-6) | R6 | An audit tool that reports false-clean on a typo'd stream id or workspace. |
| **`LogFeed` wedges forever, discarding the error** (P1-7) | R5 | The documented single-node live path has no signal path at all for a permanently failing handler. |
| **Poller/Relay exit on the first blip; closed `Wake` hot-spins** (P2-15) | R5 | Unattended legacy consumers stop on any transient error, or peg a CPU. |
| **Conformance suite ignores `RawHead`/`MatchingHead`** (P2-10) | R4 | The port contract the clamp, stall detection, and `Status` depend on is unpinned for third-party adapters. |
| **Resume-clamp loop emits nothing while blocked** (P2-8) | R4 | A wedged fence during reconnect is unobservable — the exact condition `OnStall` exists for. |
| **Required generated deps panic at first use** (P3) | R5 | Wiring mistakes surface as nil-interface panics in production traffic. |
| **PG 14+ requirement undocumented; `DBTX` accepts a pool and `ReadStreamTx` then fails *silently*** (P3) | R2 | Wrong-failure-shape footguns on the caller-owned-transaction path. |
| **Release CI: unpinned actions with `contents: write`; coverage ratchet blind to `postgres/`** (P2-20) | R6 | Supply-chain exposure plus a misleading coverage narrative (0.5% measured). |
| **`AnchorRoot` sort-order ambiguity + corrupt-hash conflation; no golden vector** (P3) | R1 | Anchors exist to be reimplemented off-box; the doc's sort wording can produce a different root. |

---

## 5. Consolidated test gaps

- **No concurrent-append anchor test** (held tx + commit + re-check) — the P1-1 gap; `TestAnchorsPostgres` runs quiescent, the chaos gate never touches anchors.
- **No head-truncation test** (first surviving version > 1) — P1-2; the existing `memStore` harness makes it a five-line test.
- **The entire snapshot feature is untested** — no `snapshot.Cache` implementation exists in the repo, so `WithSnapshots`, `LoadStale`, fold-version guarding, write-through, and the ghost-state path have zero coverage.
- **`cmd/es-verify` and `cmd/es-migrate` have no tests at all** (exit codes, empty store, nonexistent stream, default DSN).
- **No least-privilege end-to-end test** — every Postgres CI test runs as the container superuser; a mis-provisioned role is green in CI and fails in production (P1-5's class).
- **No transient half of `checkRelayRole`** (cancelled ctx / pool blip) — only member/non-member.
- **No advisory-lock release test on a cancelled `Migrate`**; nothing runs with a non-default isolation level.
- **No sequence-cache guard or test** (P2-1).
- **Conformance suite: no `RawHead`/`MatchingHead`** (P2-10); no test for the clamp loop's observability or filtered-subscription stalls.
- **`projection/runner.go` and `LogFeed` have no direct tests**; **`delivery/poller.go` has zero package tests**; **`cmdsvc` has no unit tests** (`MapError`'s full table, `MetaFrom` edges, `DefaultValidator`'s fail-closed mode).
- **No test for `eventfeed` terminal errors** (`ErrDefinitionChanged`, persistent `ErrLeaseHeld`); no test for `Skip` monotonicity; no test for the `lost`-drain race in the reverse interleaving.
- **No duplicate-`event_id` test** (the second UNIQUE constraint on `events` is never exercised); **`LoadCheckpoint`/`SaveCheckpoint` on SQLite are the only completely untested public surface in that package**.
- **Coverage gate:** unit-suite only — `postgres/` measures 0.5% under it. Publish integration coverage or per-package floors.
- Worth adding: fuzz targets for `es.ParseCanonical`, `Definition.Hash` inputs, and codec `Decode` on arbitrary bytes; an `AnchorRoot` golden vector.

---

## 6. Consolidated documentation corrections

- `es/hash.go:45-46` + ADR 0029 §2: "cross-stream ordering is attested by anchors" is not true as implemented — the root never includes `global_position` (P2-4).
- ADR 0037 / `readLastPosition` comments: "idle streams burn no transaction ids" holds only when the whole database is idle; `last_value` is global (lead, §3.4).
- `projection/buffered_marker.go`: the MaxInterval crash-window claim is false when traffic stops (P2-16).
- `feed/server.go` `Skip` doc: "advances" — it can rewind (P2-9).
- `es/errors.go`: `ErrStreamNotFound`'s contract is unimplemented (P2-19); `Meta.OccurredAt` documented as ignored but public; `ErrDecidedAtMismatch` retained-but-never-returned in a pre-1.0 module.
- `cmdsvc.HdrIdempotencyKey`: the name promises dedup the code does not perform (P1-6).
- Migration 0005's header comment ("Both policies that carry the arm…") and ADR 0029's gating implication are inaccurate — three policies carried the arm (P2-3).
- ADR 0001's cut table + the ADR index still list RLS and the hash chain as omitted; add errata (P3).
- `docs/deploy/README.md`: add the per-principal grant list (P1-5), fix the "startup" vs lazy role-check contradiction, and document the sequence `CACHE` invariant (P2-1) and TLS/DSN hygiene for relay/anchor principals.
- `docs/deploy/README.md` / `Open`: document PostgreSQL 14+.
- `postgres/wsstore.go:614-616`: "same transaction and therefore the same snapshot" is false under READ COMMITTED — safety comes from position immutability; pin or correct the isolation-level assumption.

---

## 7. Updated fix order

1. **Anchor commit safety on Postgres** (P1-1) — the audit path; reuse the fence.
2. **`eventfeed` terminal-error escape + observer hook** (P1-3) — the definition gate is currently neutered by its own consumer.
3. **Head-truncation detection in `VerifyStream` + distinct empty/not-found outcome in `es-verify`** (P1-2 + P2-6 + P2-19, one coherent fix).
4. **`checkRelayRole` retry** (P1-4) — one transient error bricks a process.
5. **Runbook grants / sequence privilege** (P1-5) — the feed cannot start on a least-privilege deployment; include the `CACHE 1` assertion (P2-1) and the column-level `UPDATE (published_at)` grant (P2-5) in the same pass.
6. **`anchors` policy role-gating migration + comment corrections** (P2-3).
7. **LogFeed observability seam** (P1-7) and Poller/Relay retry + Wake guard (P2-15).
8. **Idempotency contract decision** (P1-6) — dedup or rename.
9. **Generated upcaster seam** (P2-13) and **state-loader cost** (P2-14).
10. **Feed signals**: filtered stall view (P2-7), clamp-loop probe (P2-8), `Skip` monotonicity (P2-9), conformance `RawHead`/`MatchingHead` (P2-10).
11. **SQLite**: type index (P2-11), `NewWithDB`/`Close` contract (P2-12).
12. **Snapshot lineage check** (P2-17) — with the first real `snapshot.Cache` tests.
13. **Ops hardening**: advisory-lock fix (P2-2), duplicate-`EventID` classification (P2-18), CI pinning + coverage (P2-20), PG14/DBTX footguns, then the P3 tail.

---

## 8. Bottom line

The blinded panel and the earlier pass converge on the same core judgement: **the architecture is right, the fence/lease/definition/RLS design is genuinely strong, and the code is unusually well documented and tested for what is tested.** Nothing found rises to silent data loss under default configurations — the fence holds, the acks are fenced, the conformance discipline is real.

What stands between this library and "trust it in production" is a specific, fixable cluster:

1. **the tamper-evidence story has two blind spots** (torn heads on Postgres, head truncation everywhere) plus one un-gated policy and one false doc claim;
2. **the fail-closed paths have no voice** — terminal errors are swallowed by the consumer adapter, `LogFeed` discards handler errors, the clamp loop and wedged fences emit nothing;
3. **the operational contract is partly oral** — the grants, the sequence privilege, the `CACHE` invariant, and the PG-14 floor all live nowhere, and CI runs as superuser so it cannot notice.

All three clusters have concrete, small fixes with clear test recipes. The single most valuable next step is the one the panel flagged from three different angles: **make every silent failure mode observable, then make the documentation say exactly what the code does.**
