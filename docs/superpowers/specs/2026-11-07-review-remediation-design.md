# Review Remediation — Design

- **Date:** 2026-11-07
- **Status:** approved scope decisions recorded below (maintainer answered the four pivotal questions); design finalized for planning.
- **Inputs:** `docs/review/20261107_consolidated_review.md` (the findings register F1–F38 and its adjudications), Review A, Review B, and the documentation research cited inline.
- **Plan:** `docs/superpowers/plans/2026-11-07-review-remediation.md` implements this design task-by-task.

## 1. Intent and why

Both 2026-11-07 reviews converge: the architecture (fence, leases, definition fingerprints, RLS, keyless chain) is right, but the library is not safe to trust in production until three clusters close:

1. **The audit story has blind spots** — anchors can be recorded over torn heads (F1), head truncation is invisible (F2), cross-stream order is unaudited while docs claim otherwise (F11), and the `anchors` policy was never role-gated (F8).
2. **Fail-closed paths have no voice** — `eventfeed` swallows the terminal `ErrDefinitionChanged` (F3), `LogFeed` discards handler errors (F7), a blocked fence in the resume clamp emits nothing (F15), and stall detection false-positives so operators learn to ignore it (F14).
3. **The operational contract is oral** — required grants, the sequence privilege, the `CACHE 1` invariant and the PG-14 floor live nowhere, and CI runs as superuser so it cannot notice (F5, F9, F28).

Goal: fix every finding F1–F38 (register in the consolidated review), with every behavioral fix landing red-first, and documentation that says exactly what the code does.

## 2. Goals / non-goals / anti-goals

**Goals**
- Every P1/P2 finding fixed; every P3 fixed or explicitly resolved (doc-only where that is the honest fix).
- No new silent failure modes: any path that can wedge emits an observable signal.
- Draft-07-compliant `Idempotency-Key` (maintainer decision).
- Full log-order attestation (maintainer decision) — without serializing appends.
- Both extra capabilities the maintainer elected: `FETCH_MODE_EVENT_ONLY`, per-workspace fence high-water.

**Non-goals**
- Exactly-once delivery; push wakeups (LISTEN/NOTIFY); lease epochs; a running service. All remain out of scope per ADR 0036/0037.
- Removing `events.stream_type` or rebuilding the SQLite log table (doc fix only).
- A dedup GC/expiry daemon — expiry policy stays the operator's (draft §2.3 makes it a resource policy).

**Anti-goals**
- No serialization of appends (ADR 0029's global-chain rejection stands — see D1).
- No payload ever leaves the store in an error, log or metric (ADR 0025).
- No migration may `CREATE INDEX CONCURRENTLY` or edit an applied migration file's semantics (ADR 0010); corrections land as new migrations with errata headers.

## 3. Constraints

- Pre-1.0: breaking API changes are allowed (maintainer decision D9) and each lands with an entry in `docs/migration/2026-11-07-breaking-changes.md`.
- Go 1.27.3, pgx v5, modernc.org/sqlite; no new dependencies.
- Postgres migrations land numbered 0006–0009; SQLite 0005–0007 (see D3).
- New Postgres tables: `log_order` gets the workspace arm + role-gated bypass arm (events parity); `command_dedup` gets the workspace arm only (feed-tables parity, no bypass).
- PG tests run under `task test:pg`; plain `GOWORK=off go test ./...` stays green without `PG_DSN`.
- Generated code is regenerated; `task generate:check` gates drift.
- Conventional commits; every bug fix lands red-first (failing test committed with or before the fix).

## 4. Research that shaped the design

| Question | Source | Consequence |
|---|---|---|
| How should the migration lock be held? | PG docs, *System Administration Functions*: "`pg_advisory_xact_lock` … the lock is automatically released at the end of the current transaction" | D4: xact-scoped lock inside each migration transaction. |
| Is the 0005 policy's `OR` arm safe when `es_relay` is missing? | PG docs, *Logical Operators* §9.1: left-before-right evaluation is "not guaranteed" | D8: `CASE WHEN EXISTS` guard in a policy-hardening migration. |
| Does `last_value` overstate drawn positions? | PG docs, *pg_sequences*: with caching, last_value "can be greater than the last value handed out"; the view masks `last_value` to NULL without `USAGE`/`SELECT` and exposes `cache_size` | D5: read the fence bound through `pg_sequences` and assert `cache_size = 1` per round. |
| What must an Idempotency-Key replay do? | IETF draft-ietf-httpapi-idempotency-key-header-07 §2.6/§2.7/§5: replay stored response, 409 concurrent, 422 key-reuse-with-different-payload, composite key recommended | D6: dedup port + deterministic event IDs + stored response. |
| How should SQLite migrations serialize? | SQLite docs *Transaction Control* §2.1/§2.2 (single write transaction; `BEGIN IMMEDIATE`); modernc supports `_txlock` | D7: the `schema_migrations` INSERT itself is the lock. |

## 5. Approaches considered (per major decision)

### D1 — Log-order attestation (F11, maintainer chose "full")

- **Global append-time chain** (each batch hashes the previous batch's root): rejected — it serializes every append on a single head, exactly what ADR 0029 rejected, and forks silently under concurrent appends (no unique constraint to catch a double-spend of the prev pointer).
- **Anchor-time full scan** (order root over every `(position, event_hash)` at each anchor): rejected — O(N) per anchor and per check on a table designed to keep growing.
- **Chosen: batch-order rows.** Each append transaction hashes only *its own* events — `BatchOrderHash = SHA-256("es-lite.log-order.v1" ‖ u32(count) ‖ per event in position order: u64(position) ‖ 32B hash)` — and inserts one `log_order(from_position, to_position, workspace_id, stream_id, event_count, batch_hash)` row in the same transaction. Zero cross-transaction dependency → no serialization; positions and per-event hashes are already in hand at insert time. `Anchor` gains `OrderRoot` over the sorted `log_order` rows with `to_position <= upto` (O(#batches)), plus `OrderFromPosition` (first attested position — the order analog of `ChainedFrom`, since rows before the migration are unattested). Deep verification (`es-verify order`, wired into `all`) recomputes each batch hash from the events in `(from, to]` — O(N), the same cost class as `VerifyStream` — which is what actually catches a repositioned event; the exported `OrderRoot` catches wholesale rewrites, mirroring the chain/anchor trust split of ADR 0029. Legacy anchors without an order root keep verifying heads-only.

### D2 — Fenced anchors (F1)

- Considered: REPEATABLE READ anchor transaction — insufficient (in-flight lower positions stay invisible to every snapshot; the fence exists precisely because of this).
- Chosen: reuse the horizon fence on the bypass path — clamp `upto` to `last_value`, draw `R`, wait for `pg_snapshot_xmin ≥ R`, then compute heads and `OrderRoot`. After the wait, everything drawn at ≤ `upto` has settled, so the recorded roots are permanent. `CheckAnchor` takes the same fence (a transient false *pass* is as bad as a false alarm). Bounded by ctx; retryable on a wedged tx. SQLite needs nothing (adjudication §2.1).

### D3 — Migration numbering

postgres: `0006_log_order.sql`, `0007_policy_hardening.sql` (role-gate `anchors_admin_only`; add the `EXISTS` guard to all three bypass arms; header carries the errata correcting 0005's "Both policies" comment), `0008_drop_unused_checkpoints.sql`, `0009_command_dedup.sql`. sqlite: `0005_feed_type_index.sql`, `0006_log_order.sql`, `0007_command_dedup.sql`.

### D4 — Migration locking (F10)

Chosen: per-migration transaction that begins with `SELECT pg_advisory_xact_lock(migrateLockKey)`, then re-checks the version, applies, records, commits — auto-released, so a cancelled context cannot leak it onto a pooled session (pgxpool returns clean connections to the pool; that is what made the session-level lock dangerous).

### D5 — Fence bound + cache invariant (F5/F9/F27)

`readLastPosition` becomes `SELECT COALESCE(last_value, 0), cache_size FROM pg_sequences WHERE sequencename = 'events_global_position_seq'` — one statement that (a) works for a `USAGE`-only role (the view masks rather than erroring), (b) returns 0 for a never-used sequence, and (c) asserts `cache_size = 1` every round, converting a silent-loss operator tweak into a loud startup/runtime error. Xid burn (F27/F38): before drawing a fence, consult the workspace's committed max (`RawHead`-shaped read, no xid) — if nothing new is committed beyond `from`, return an empty page without drawing; same shortcut for `Head` on an empty workspace. Safe because `from` is itself a settled position (it only ever advanced under a cleared fence), so no in-flight row can exist at ≤ `from`.

### D6 — Idempotency-Key (F6, maintainer chose "draft-compliant dedup")

Three cooperating pieces:
1. **Deterministic event IDs** (opt-in, `Runtime.WithDeterministicEventIDs()`): when `CommandID` is set, `EventID[i] = uuid.NewSHA1(esNamespace, CommandID ‖ ":" ‖ i)`. A concurrent duplicate command loses on the `(workspace, event_id)` unique constraint → classified by D-F25 as `ErrDuplicateEventID` → mapped to Connect `aborted` (HTTP 409). This closes the draft's concurrent window without an in-flight tracking table.
2. **Dedup port in `cmdsvc`** (structural, so core adapters satisfy it without importing the nested module): `Lookup(ctx, key) (rec DedupRecord, found bool, err error)` / `Save(ctx, key, rec)` with `DedupKey{StreamID, CommandID string}` (workspace scoped by the adapter) and `DedupRecord{Fingerprint string, Response []byte}`.
3. **Generated handler flow** when `Deps.Dedup != nil`: fingerprint = SHA-256 of the marshaled request (draft §2.4 checksum-of-payload option); Lookup hit + matching fingerprint → replay the stored response; hit + mismatch → 422-shaped error (Connect `aborted`, message names the 422 semantics); miss → `HandleAt`; `ErrDuplicateEventID` → 409; success → `Save` the marshaled response. Adapters: `postgres.NewDedupStore` / `sqlite.NewDedupStore` over `command_dedup(workspace_id, stream_id, command_id, fingerprint, response, created_at)`. `Dedup == nil` preserves today's behavior exactly.

### D7 — SQLite migration lock (F34)

Chosen: make the version INSERT the lock. Per pending migration: `BeginTx`; `INSERT INTO schema_migrations (version) VALUES (?)` **first** (the SQLite write lock this takes serializes rivals; the loser sees the PK violation and skips); then apply the DDL; commit. No `_txlock` DSN change, no `BEGIN IMMEDIATE` plumbing through `database/sql`; the "no lock needed" comment is replaced with the actual argument.

### D8 — Policy hardening (F8/F12)

`0007` recreates all three bypass-carrying policies with the role gate **and** the `CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname='es_relay')` guard, and adds a migration-level test asserting the policy text of every table (the feed tables' no-bypass claim is already asserted this way). Grants (F5/F12) land in the runbook as a per-principal block — including `GRANT UPDATE (published_at) ON events TO <relay>` and no `DELETE`/`TRUNCATE` for anyone — plus the least-privilege end-to-end integration test that runs append + feed + drain + anchor under exactly those grants.

### D9 — Breaking changes (maintainer chose "allowed, documented")

Every port/signature change lands with a `docs/migration/2026-11-07-breaking-changes.md` entry: `Waiter.Wait` returns `(woke bool, err error)` (floor applies only when `!woke`), adapters return `ErrStreamNotFound` for never-written streams on `from == 0`, `Log` gains `MatchingRawHead`, `sqlite.NewWithDB` enforces `SetMaxOpenConns(1)` and stops closing borrowed handles, generated constructors panic at wiring (not first request) on nil required deps, `es.Anchor` gains order-root fields.

### D10 — Consumer liveness (F3/F7/F14/F15/F22/F23)

`ReconnectingFeed` gains `IsFatal func(error) bool` (eventfeed marks `*feed.DefinitionChangedError` fatal) and the feed `Observer` gains `OnSubscribeError(scope, projection, instance string, err error)`. `LogFeed` gains `OnRetry(env, err, attempt)` and exponential backoff capped at a `MaxBackoff` default; attempts stay unbounded (at-least-once order is the contract; the fix is signal + cadence, not abandonment). Stall detection compares against `MatchingRawHead`; the clamp loop runs the same rate-limited probe. `BufferedMarker` gets a background flush timer started at construction and stopped by a new `Close`, `Runner.Run` flushes on return via a `Flusher` probe on a detached bounded context, and a `Discard(projection)` method covers the reset-then-rebuild path. `delivery` gets a shared run loop, a `MaxRetries`-with-backoff policy (0 → default 3, negative → fail fast), a closed-`Wake` guard, constructor validation, and its first real test files.

## 6. What is left to the builder

Everything in the plan is decided down to file, signature and test name. The only in-task judgments permitted: internal helper shapes, exact error strings (prefix conventions are pinned), and test-table details that don't change pinned behavior.
