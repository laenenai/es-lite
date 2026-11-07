# es-lite — Principal Engineer Review

> **Corrected 2026-11-07 (see `docs/review/20261107_consolidated_review.md` §2.1):** the claim in §2.1 finding 1 that *SQLite* "has the same race and is arguably worse" for anchors is **wrong** and is retracted. SQLite's single-writer lock means an uncommitted row's position is always above every committed position, so `upto = MAX(committed)` is a stable cutoff and the un-transactional `MAX` + `heads(upto)` pair is consistent. The **Postgres** half of that finding stands (reproduced live and independently confirmed by the blinded panel). This pass was otherwise confirmed, extended and adjudicated by the panel review (`20261107_independent_glm53_panel_review.md`); the single agreed findings register and the remediation plan live in `20261107_consolidated_review.md` and `docs/superpowers/plans/2026-11-07-review-remediation.md`.

- **Review date:** 2026-11-07
- **Commit reviewed:** `main` @ `2fd3a76` (merge of `feat/postgres-production-grade-feed`)
- **Scope:** core module (`es`, `aggregate`, `postgres`, `sqlite`, `feed`, `eventfeed`, `projection`, `delivery`, `reaction`, `upcast`, `migrate`, `snapshot`, `cmd/*`) plus the nested `cmdsvc` module.
- **Method:** full source read; `go vet ./...`; `golangci-lint run ./...`; `go test ./...`; targeted `go test -race` for `feed`, `eventfeed`, `projection`, `aggregate`; and live reproductions against PostgreSQL 17.11.
- **Baseline:** vet clean, lint 0 issues, unit suite green, targeted race suite green. The findings below are the things I would not ship as-is.

---

## 1. Verdict

| Dimension | Score | Notes |
|---|---:|---|
| Core model (`es`, `aggregate`) | 4/5 | Small, coherent, well documented; a few dead compatibility artifacts. |
| Postgres storage | 3/5 | Strong RLS/fence design, but anchor reads are not commit-safe and one privilege assumption is undocumented. |
| SQLite storage | 3.5/5 | Clean single-writer model; missing an index that matters at scale, and `NewWithDB` breaks the package invariant. |
| Feed protocol | 3.5/5 | Lease/ack/definition/skip design is good; `Skip` can rewind and stall detection false-positives. |
| Projections / codegen | 3/5 | Clean generated seams; no upcaster path, quadratic state loading, global generator state. |
| Security / tamper evidence | 2.5/5 | Good keyless chain, but anchors do not attest `global_position`, and schema privileges do not enforce append-only. |
| Tests | 4/5 | Excellent integration/conformance work; several bugs below are untested by construction. |
| Documentation | 3.5/5 | Unusually good, but several claims are no longer true. |

The architecture is sound. The remaining issues are mostly correctness at the edges and operational truthfulness, not structural rework.

---

## 2. Confirmed defects (reproduced)

### 2.1 `Anchor` is not commit-safe — false `ErrAnchorMismatch`

**Files:** `postgres/anchor.go:58-89`, `sqlite/anchor.go:39-67`

Postgres `Anchor`:

1. `SELECT MAX(global_position)` under `READ COMMITTED` sees committed rows only.
2. `heads(...)` runs a separate statement with a new snapshot.
3. An append that drew a lower position and commits between (or after) those statements is omitted, while `upto` still includes higher committed positions.

The recorded anchor then claims to cover a position that is not represented in the root. Later verification fails even though nothing was tampered with.

Reproduced with the real store:

```text
committed A position=1
in-flight B position=2 (tx open)
committed C position=3
anchored upto=3 streams=2
committed in-flight B
CHECK ANCHOR ERROR: anchor root mismatch: ... at global_position 3 (streams 2, recomputed 3)
```

SQLite has the same race and is arguably worse: `sqlite.Anchor` does not wrap the `MAX` + `heads` + `INSERT` in a transaction at all, so a concurrent append can interleave between the three statements (the single connection serializes statements, not the sequence of statements).

**Impact:** the tamper-evidence path produces false positives under normal write concurrency. That trains operators to ignore the alert — the worst failure mode for an audit system.

**Fix:** anchor at a settled bound, reusing the horizon fence (`fenceBound` / `ReadAllBounded` logic), or expose a `Store.AnchorSettled` that waits for the fence before computing heads. For SQLite, wrap the whole operation in a read transaction. Add a test that holds an append open while anchoring, then commits and re-checks.

---

### 2.2 A duplicate caller-supplied `EventID` is reported as `ErrConflict`

**Files:** `postgres/wsstore.go:256-259`, `sqlite/store.go:293-297`

Both adapters map any unique violation during insert to `ErrConflict`. The `events` table has two unique constraints:

- `(workspace_id, stream_id, version)` → genuinely a version conflict
- `(workspace_id, event_id)` / `(event_id)` → a duplicate event id, a permanently invalid input

Reproduced:

```text
duplicate event id error: es-lite: append conflict (expected version mismatch): stream t:dup2 v1
errors.Is(ErrConflict)=true
```

**Impact:** callers, `WithConflictRetry`, and `reaction.RuntimeDispatcher` retry a permanent error. The original `pgconn.PgError` is also discarded here, so the operator cannot see which constraint fired.

**Fix:** inspect `pgErr.ConstraintName` for the primary key/version constraint versus the event-id constraint; return a distinct `ErrDuplicateEventID` for the latter. On SQLite, pre-check or use a dedicated error type/constraint-name mapping. Keep the underlying DB error in the message (`%v` or a joined error).

---

### 2.3 `checkRelayRole` caches transient probe failures forever

**File:** `postgres/drain.go:31-48`

`sync.Once` marks the check complete even when the probe itself failed (context canceled, pool blip, replica unavailable). Every later `Drain` / `Anchor` on that `Store` fails with the cached error for the process lifetime.

Reproduced:

```text
first Drain (cancelled ctx):  postgres: drain: es_relay membership check: context canceled
second Drain (healthy ctx):   postgres: drain: es_relay membership check: context canceled
```

**Impact:** one shutdown-time or transient call permanently disables the relay and anchor path in that process. The runbook says "restart"; that is not acceptable for an availability path.

**Fix:** cache only a definitive verdict (`member` / `not a member`). On a probe error, leave the once un-triggered and return the error so the next call retries. A small `sync.Mutex` plus cached `bool` is enough; `sync.Once` is the wrong primitive here.

---

### 2.4 `Skip` can rewind the checkpoint

**File:** `feed/server.go:676-713`

`Skip` is documented as advancing a cursor, but it calls `Checkpoints.Reset`, which overwrites in both directions:

```text
Skip rewound checkpoint 100 -> 50; a skip must only advance
```

**Impact:** an operator typo (or a stale position) silently replays history and records a dead letter for an event that was not skipped. Not data loss, but an unaudited replay and a misleading dead letter.

**Fix:** use the monotonic `Save` path for `Skip`, or reject `sr.Position < cp.Position` with a clear error. `ResetCheckpoint` remains the explicit rewind operation.

---

### 2.5 Stall detection false-positives for filtered subscriptions

**File:** `feed/server.go:413-424`, `feed/server.go:486`

`probeStall` compares the subscription's cursor `pos` with the unfiltered `RawHead`. If the log contains any later event of a type the projection does not consume, `raw > pos` forever and the stream reports `OnStall` once per episode.

Reproduced:

```text
filtered, caught-up subscription reported 1 stall(s): [{settled:10 raw:11}]
```

with event 10 consumed and already acked, event 11 of a different type.

**Impact:** every filtered projection that is healthy but not at the absolute end of the log emits a false stall signal. That makes the primary "stuck projection" alert untrustworthy.

**Fix:** add a raw matching head (`MatchingRawHead(types)` or make `RawHead` take the type filter). Distinguish "matching backlog above the settled matching head" from "other types' events". Add a filtered-subscription stall test.

---

### 2.6 The feed requires `SELECT` on `events_global_position_seq`, but the runbook does not grant it

**File:** `postgres/wsstore.go:407-414`

`readLastPosition` executes `SELECT last_value FROM events_global_position_seq`. A least-privilege role normally gets `USAGE` (enough for `nextval` via the column default), not `SELECT`. Reproduced with a role granted only `USAGE`:

```text
SELECT last_value FROM events_global_position_seq;
ERROR:  permission denied for sequence events_global_position_seq
```

`pg_sequence_last_value('events_global_position_seq')` does work with `USAGE` only, but returns `NULL` for a never-used sequence, so it needs `COALESCE(..., 0)`.

**Impact:** the entire feed fails at runtime on a correctly locked-down deployment, with an error that looks like a code bug.

**Fix:** use `COALESCE(pg_sequence_last_value('events_global_position_seq'), 0)`, or explicitly document and grant `SELECT` on the sequence in `docs/deploy/README.md`. The runbook currently has no table/sequence grant section at all — that is its own gap.

---

## 3. High-risk design gaps

### 3.1 Snapshots are not lineage-checked

**File:** `aggregate/runtime.go:416-443`

`load` trusts `snap.Version` and calls `ReadStream(sid, from, ...)`. If the cache holds a snapshot newer than the current log (restore, cell copy, stale shared cache, corrupted entry), no tail is read and `version = from` is returned. The caller gets a phantom state that never existed in that log.

**Fix:** after loading the snapshot, verify the first returned envelope is exactly `from+1`, and fall back to a full fold on mismatch; or re-read the event at `from` and compare its hash with a snapshot-side `SourceHash`. At minimum, document that a snapshot cache must be purged on log restore. This is also a cross-tenant hazard if the caller's cache implementation is not workspace-scoped as documented.

---

### 3.2 Generated projections/reactions have no upcaster path

**Files:** `cmd/protoc-gen-es-lite/projection.go` (`writeDispatch`), `gen/counterview/v1/view_es.pb.go:114`

The aggregate runtime applies `es.Upcaster` after `Decode` (`aggregate/runtime.go`), but the generated projection/reaction dispatch decodes the event directly and passes it to the handler:

```go
decoded, err := codec.Decode(es.EncodedEvent{...})
ev, ok := decoded.(*v1.Incremented)
...
h.OnIncremented(ctx, env, ev, state)
```

`state` may be upcast (it came through the runtime) while `ev` is not. A projection deployed after a schema change sees old-shaped events with no migration hook.

**Fix:** add an `Upcaster es.Upcaster[E]` field to generated `Deps` (per consumed aggregate) and apply it after decode; or generate a codec wrapper. Add a generation test that pins the call.

---

### 3.3 `Idempotency-Key` does not provide idempotency

**Files:** `cmdsvc/cmdsvc.go:28`, `cmdsvc/cmdsvc.go:63-83`

`MetaFrom` maps the header to `CommandID` only. There is no uniqueness constraint or dedup table on `command_id`; the example test explicitly shows a second request with the same key appending a second event (`cmdsvc/example/example_test.go:129-137`).

**Impact:** clients will reasonably treat the header as a retry-dedup contract and get duplicate business events — the exact ES-02 failure from the earlier review.

**Fix:** either implement command-id dedup (`UNIQUE (workspace_id, command_id)` with a stored result/version range, or deterministic event IDs derived from `(CommandID, index)` and return the stored envelopes on duplicate), or rename the header to `Command-Id` and document that it is audit correlation only. Given the name, I would implement dedup.

---

### 3.4 Anchors do not attest `global_position`, despite the docs

**Files:** `es/hash.go:45`, `es/anchor.go:61-90`, ADR 0029 §2

The hash comment says:

> `GlobalPosition` is excluded … cross-stream ordering is attested by anchors instead.

But `AnchorRoot` hashes only `(workspace, stream_id, version, head_hash)`. The anchor row records `upto_global_position`, but the root does not bind any event to a position. An operator (or a compromised app connection) can rewrite `global_position` values without changing the anchor root, reordering delivery and potentially making a feed skip or reorder events.

**Fix (choose one):**

- include each stream head's `global_position` in the root, and/or a rolling log-order hash over `(position, event hash)` up to `upto`;
- or correct ADR 0029/README to say anchors attest stream-head content/truncation, not cross-stream ordering.

Separately, the append-only contract is not enforced by privileges: RLS permits `UPDATE` of payload/position on your own workspace's rows, and the relay needs `UPDATE` on `events` for `published_at`. Grant `UPDATE (published_at)` only (and revoke `DELETE`/`TRUNCATE`) to the relay role; consider a trigger rejecting updates to any other column. That would turn "append-only by API contract" into "append-only by database contract" on Postgres.

---

### 3.5 SQLite is missing the feed's type index

**File:** `sqlite/migrations/0001_initial_schema.sql:43-46`

`ReadAllBounded` and `MatchingHead` filter `type_url IN (...)` and order by `global_position`, but SQLite has no `(type_url, global_position)` index. Postgres added exactly this in `0003_feed_indexes.sql`.

**Impact:** a projection consuming a rare type rescans the entire non-matching tail every poll (250 ms by default), because its cursor only advances on matching rows. At even modest log sizes this is a continuous full scan.

**Fix:** add `CREATE INDEX events_type_pos_idx ON events (type_url, global_position);` in a new SQLite migration, mirroring Postgres 0003.

---

### 3.6 `sqlite.NewWithDB` breaks the package invariant and owns the caller's handle

**File:** `sqlite/store.go:110`, `sqlite/store.go:123-136`

`Open` sets `db.SetMaxOpenConns(1)` — the basis of the gap-free, commit-ordered cursor. `NewWithDB` does neither that nor pragmas, and `Close` closes the caller's `*sql.DB` (unlike `postgres.OpenWithPool`, which carefully documents non-ownership).

**Fix:** enforce or document `SetMaxOpenConns(1)` and pragmas in `NewWithDB`; add an `ownsDB` flag so `Close` does not close a borrowed handle, mirroring the Postgres constructor. The current API is a silent correctness trap for anyone who uses `NewWithDB` outside tests.

---

### 3.7 `BufferedMarker`'s documented crash window is not bounded

**Files:** `projection/buffered_marker.go:67-140`, `projection/runner.go:182-191`

The comment promises "a crash window bounded by `MaxInterval`". In reality the interval is only checked on the next `Save`; a quiet stream leaves the pending position buffered indefinitely, and `Runner.Run` never flushes on return. `Load` also resurrects a buffered position over an out-of-band reset, so "reset then rebuild" can rebuild nothing.

**Fix:** add a timer/background flush (or flush from the runner's shutdown path on a detached context), and give the marker a `Discard`/`Reset` path. If this remains deferred, correct the comment — it is currently a trap.

---

### 3.8 Postgres migration advisory lock can leak

**File:** `postgres/store.go:188-191`

`defer conn.Exec(ctx, "SELECT pg_advisory_unlock(...)")` is excluded from errcheck. If the context is canceled after the lock is taken, the unlock fails, the connection returns to the pool still holding a session-level advisory lock, and the next migrator blocks until that session ends.

**Fix:** use `pg_advisory_xact_lock` inside a transaction, or unlock on a detached context with a timeout, or close the connection on unlock failure.

---

## 4. Code smells and simplification opportunities

These are not correctness bugs, but they are where the code could be materially smaller.

1. **Postgres transaction scaffolding is copy-pasted ~10 times.** Every `Workspace` method repeats `Begin` → `setWorkspace` → `defer Rollback` → query. Extract:

   ```go
   func (w *Workspace) withTx(ctx context.Context, fn func(context.Context, DBTX) error) error
   ```

   It would remove hundreds of lines from `postgres/wsstore.go` and make the `Append`/`AppendTx` split clearer.

2. **SQLite `Head` and `RawHead` are identical.** `sqlite/feed.go` — make `Head` delegate to `RawHead` (or vice versa) and keep the semantic comments.

3. **`delivery.Relay.Run` and `delivery.Poller.Run` are the same loop.** Extract a shared `run(ctx, drain func(context.Context) error, cfg loopConfig) error`; the only difference is the drain function.

4. **Payload-cap validation is duplicated verbatim** in `postgres/wsstore.go` and `sqlite/store.go`. Move it to `es.ValidateAppend` or a small internal helper so the two backends cannot drift on error text/version calculation.

5. **Envelope scanning is duplicated** (`postgres/store.go:scanEnvelope`, `sqlite/store.go:scanEnvelope`) with slightly different validation. SQLite validates `event_id`; Postgres ignores parse errors for `event_id`, `correlation_id`, `causation_id`, `command_id`. Corrupt `event_id` silently becomes `uuid.Nil`; the chain then fails only if verification is enabled. Make both scanners return parse errors, or share a scan struct.

6. **The Postgres `checkpoints` table is dead.** `postgres/migrations/0001_initial_schema.sql:71` creates it, but only SQLite implements `delivery.Checkpoints`. Drop it in a migration or explain why it exists.

7. **`events.stream_type` is stored but never used.** `postgres/migrations/0001:18`, `sqlite/migrations/0001`. `stream_id` already contains the type. Either drop it or use it for the "all streams of type X" query the schema comment promises.

8. **Dead compatibility surface in `es`:**
   - `ErrStreamNotFound` is documented as "returned when a read targets a stream that has never been written to", but neither adapter returns it; `VerifyStreamWith` and `aggregate.load` carry dead tolerance branches.
   - `ErrDecidedAtMismatch` is retained "for callers compiled against ADR 0032" in a pre-1.0 module and is never returned.
   - `Meta.OccurredAt` is documented as ignored but remains in the public struct.
   - `commandVariants` in `cmd/protoc-gen-es-lite/main.go:174` is dead code; `newResolver` collects the same set.

   Remove these before 1.0 rather than carrying them forever.

9. **Generator state and dead locals:**
   - `specNames` is a package-level mutable map (`projection.go:38`); tests reset it manually. Pass a generator-scoped map through `run` instead.
   - `_ = meta` at `service.go:381` exists only to silence an unused local; remove the local.
   - `writeDispatch` has an identical `if/else` default branch; collapse it.

10. **`eventfeed` duplicates a 20-line token-policy comment** in both `New` and `newToken` (`eventfeed.go:99-118`, `158-176`). Keep one and reference it.

11. **Constructor style is inconsistent.** `feed.NewServer` returns `(server, error)`; `eventfeed.New` panics; `delivery.NewRelay`/`NewPoller` panic. For a library, prefer errors (or at least document the policy in one place). Panics in constructors are surprising for callers wiring config from flags/env.

12. **`ReconnectingFeed` deliberately swallows inner errors** (`projection/reconnecting.go:69`) and needs an `SA4006` lint exclusion. The spec tracked an observer hook; until it lands, at least make the error available to an optional `OnError` callback so a permanently failing feed is visible without relying on redelivery counts.

13. **Quadratic projection state loading.** Generated `FETCH_MODE_STATE_AS_OF_EVENT_VERSION` dispatch calls `Runtime.LoadAsOfVersion(ctx, env.StreamID, env.Version)` for every event, which re-reads and re-folds the whole stream each time. For a projection consuming every event of a stream, replay is O(n²). Cache folded state per stream in the generated projector (invalidate on version gaps) or expose a `FoldFrom(snapshot)` / incremental loader.

14. **Global idle check burns transaction IDs across workspaces.** `postgres/wsstore.go:474-480`: `last_value` is global, so an idle subscriber in workspace A still draws a fence whenever workspace B writes. ADR 0037's "idle streams burn no transaction ids" is only true when the whole database is idle. Add per-scope high-water tracking or correct the cost model and alerting guidance.

15. **`postgres.Workspace.fenceBound` holds a mutex across DB I/O.** Fine for one `Workspace` per stream, but if a platform caches one `WorkspaceStore` per tenant and calls `Status` while a feed is running, all calls serialize behind network round-trips. Document the one-instance-per-stream requirement prominently, or use a `singleflight`/atomic state.

16. **`feed.Server.stream` is ~140 lines** combining resume clamp, definition check, page loop, poison check, stall probe, and idle wait. Extracting `resumePosition(ctx, log, pos)` and `stallProbe` would make the loop auditable and easier to test. The clamp loop also emits no `OnStall`, so a reconnecting subscriber stuck on a wedged fence is silent — the very case the observer exists for.

---

## 5. Testing gaps

The suite is strong, but several bugs above are invisible to it:

- no concurrent-append anchor test (held transaction + commit, then `CheckAnchor`);
- no duplicate-`EventID` test across streams;
- no filtered-subscription stall test;
- no `Skip` rewind test;
- no transient relay-probe failure test;
- no least-privilege/sequence-grant integration test;
- no snapshot-newer-than-log test;
- no projection upcaster test;
- no SQLite `NewWithDB` multi-connection test;
- the coverage floor (42.7%) is measured with Postgres suites skipped, so the fence, claims, anchor, and feed adapter code are effectively outside the gate. Consider publishing `-coverprofile` from `task test:pg` and ratcheting on the union.

Also worth adding fuzz targets for `es.ParseCanonical`, `Definition.Hash` inputs, and codec `Decode` on arbitrary bytes — these are small, high-leverage parsers.

---

## 6. Documentation corrections

- **ADR 0029 §2 / `es/hash.go:45`:** "cross-stream ordering is attested by anchors" is not true as implemented; the root never includes `global_position` (§3.4).
- **ADR 0037 / `feed/server.go` comments:** "idle streams burn no transaction ids" is only true when the whole database is idle (§4.14).
- **`projection/buffered_marker.go`:** "crash window bounded by `MaxInterval`" is false (§3.7).
- **`feed.Server.Skip`:** "advances a subscription's checkpoint" is false; it can rewind (§2.4).
- **`es/errors.go`:** `ErrStreamNotFound`'s contract is not implemented (§4.8).
- **`cmdsvc.HdrIdempotencyKey`:** the name promises dedup the code does not perform (§3.3).
- **`docs/deploy/README.md`:** add the actual table/sequence grants. In particular, the feed needs `SELECT` on `events_global_position_seq` (or the code must switch to `pg_sequence_last_value`), and the relay should get column-level `UPDATE (published_at)` rather than table-wide `UPDATE`.

---

## 7. Suggested fix order

1. **Anchor commit safety** (§2.1) — it is the audit path.
2. **`checkRelayRole` retry** (§2.3) — one transient error bricks a process.
3. **Sequence privilege / runbook grants** (§2.6) — feed cannot start in a locked-down deployment.
4. **Duplicate event-id error classification** (§2.2) — prevents retry storms.
5. **Generated upcaster** (§3.2) and **snapshot lineage** (§3.1) — both are deferred data-correctness bugs.
6. **`Skip` monotonicity** (§2.4) and **filtered stall** (§2.5) — operational correctness.
7. **SQLite type index** (§3.5), **`NewWithDB` ownership/invariant** (§3.6).
8. **Idempotency contract** (§3.3) — decide dedup vs rename.
9. **Anchor/order documentation and DB immutability hardening** (§3.4).
10. Simplifications and dead-code removal (§4), then the testing gaps (§5).

---

## 8. Bottom line

This is a careful, unusually well-documented library, and the fence/lease/definition design is the right one. I would block a production release on §2.1, §2.3, and §2.6, and treat §2.2, §2.4, §2.5, §3.1, §3.2, §3.3, and §3.5 as the next release's must-fix list.

The code does not need a rewrite; it needs a few narrow correctness fixes and a pass that makes the documentation match the implementation.
