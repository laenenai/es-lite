# Delegated Actor Implementation Plan (v0.16.0)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record, on every event that needs it, the service through which its accountable actor acted — one level, attested by the hash, indexed for the reverse query.

**Architecture:** A value `ActorRef` on `Actor`; a conditional block appended to `CanonicalEnvelope` after the fixed `prev_hash` trailer; three nullable columns plus a partial index in both stores. The zero value means "the actor acted for itself", so every existing event and hash is unchanged.

**Tech Stack:** Go 1.27, `pgx`/Postgres 17, `modernc.org/sqlite`.

**Spec:** `docs/adr/0041-delegated-actor.md` (this plan implements it; read it first). Deferred until the agent-API work starts — nothing here is urgent.

## Global Constraints

- Existing chains must verify byte-identically: `goldenV1Hash` is unchanged and
  an event with a zero `Via` encodes exactly as before.
- The delegation block is appended **after** `prev_hash`, never inserted among
  the existing fields.
- Delegation is an audit fact: strict stored read, unlike ADR 0040's telemetry
  tolerance.
- No new module dependencies; no OpenTelemetry.
- Postgres gets the all-or-nothing `CHECK`; SQLite does not (no second table
  rebuild), and the divergence is documented.
- Additive change; ships as a minor (`v0.16.0`).
- Pre-v0.16 verifiers cannot verify delegated events; the release notes must
  say so before any producer writes one.
- Go 1.27; `GOWORK=off`; conventional commits.

## Review Focus

1. **Old chains verify unchanged** — the existing golden vector is untouched,
   and a new event with a zero `Via` is byte-identical, not just equal-hashed.
   → Task 1 test.
2. **Partial delegation cannot be written** — type without id, id without type,
   or a `Via` with no accountable actor, rejected on both `Append` and
   `AppendTx` before any statement. → Tasks 1–3 tests.
3. **The block is unambiguous** — there is no way for a payload's length prefix
   to be mistaken for the marker; the parser is prefix-deterministic. → Task 1
   test with a payload that begins with the marker's bytes.
4. **A service acting for itself stays ordinary** — `Actor{Type:"service"}` with
   a zero `Via` round-trips as an actor, not as a delegation. → Tasks 2–4.
5. **The reverse query is indexed** — the partial index exists and is used.
   → Tasks 2–3 tests.

---

### Task 1: `ActorRef`, `Actor.Via`, validation, and the hash block

**Files:**
- Modify: `es/actor.go` (`ActorRef`, `Actor.Via`, `Actor.Validate`)
- Modify: `es/errors.go` (`ErrInvalidDelegation`)
- Modify: `es/hash.go` (the conditional block after `prev_hash`)
- Modify: `es/hash_test.go` (three goldens)
- Create: `es/actor_test.go` (or extend the existing test file)

**Interfaces:**
- Consumes: nothing.
- Produces: `ActorRef{Type, ID string}` with `IsZero()` and `Principal()`;
  `Actor.Via ActorRef`; `func (a Actor) Validate() error`;
  `ErrInvalidDelegation`.

- [ ] **Step 1: Write the failing tests**

- `ActorRef`: zero is zero, `Principal()` is `""`; populated gives `type:id`;
  a one-field ref is **not** zero (so validation can catch it).
- `Actor.Validate`: zero actor valid; plain actor valid; complete `Via` with an
  actor valid; partial `Via` invalid; `Via` with a zero actor invalid;
  errors.Is `ErrInvalidDelegation`.
- Hash: `goldenV1Hash` unchanged; a populated-`Via` fixture has pinned bytes
  and hash (generate once, then freeze); a zero-`Via` fixture equals the
  pre-change encoding byte for byte.
- Ambiguity: an event whose payload *starts with* `"es-lite.delegation.v1"`
  encodes and verifies without being read as a delegation block.

- [ ] **Step 2: Run to verify failure**

`GOWORK=off go test ./es/ -run 'TestActorRef|TestActorValidate|TestCanonical'`
Expected: build failure (`undefined: ActorRef`).

- [ ] **Step 3: Implement**

`ActorRef` and `Actor.Via` as in the ADR. `Actor.Validate`:

```go
// zero Actor is valid (system writes); Via must be complete and must
// accompany an accountable actor.
```

`CanonicalEnvelope`: after `appendFixedHash(b, e.PrevHash)`, if
`!e.Actor.Via.IsZero()`, append `appendString(b, delegationDomain)` then
`Via.Type` and `Via.ID`. `delegationDomain = "es-lite.delegation.v1"` beside
the other domain separators. Update the doc comment's field list with the
optional trailer and its version.

- [ ] **Step 4: Run to verify pass**

`GOWORK=off go test ./es/ -v`; the golden vectors must be green and the
pre-existing hash constant untouched.

- [ ] **Step 5: Commit**

```bash
git add es/actor.go es/actor_test.go es/errors.go es/hash.go es/hash_test.go
git commit -m "feat(es): add an optional delegated actor and attest it in the chain"
```

---

### Task 2: Postgres persistence

**Files:**
- Create: `postgres/migrations/0012_delegated_actor.sql`
- Modify: `postgres/wsstore.go` (`validateAppend`, envelope literal, INSERT)
- Modify: `postgres/store.go` (`envelopeCols`, `scanEnvelope`)
- Modify: `postgres/spancontext_test.go` or a new `postgres/actor_test.go`
- Modify: migration-count assertions (`postgres/postgres_test.go`, `postgres/migrations_test.go`)

**Interfaces:**
- Consumes: `es.ActorRef`, `Actor.Via`, `Actor.Validate`.
- Produces: `via_type`, `via_id`, `via_principal` persisted and returned.

- [ ] **Step 1: Write the migration**

```sql
ALTER TABLE events
    ADD COLUMN via_type      text,
    ADD COLUMN via_id        text,
    ADD COLUMN via_principal text;

ALTER TABLE events
    ADD CONSTRAINT events_via_complete CHECK (
        (via_type IS NULL) = (via_id IS NULL)
        AND (via_id IS NULL) = (via_principal IS NULL)
    ) NOT VALID;

ALTER TABLE events VALIDATE CONSTRAINT events_via_complete;

CREATE INDEX CONCURRENTLY IF NOT EXISTS events_via_idx
    ON events (workspace_id, via_principal)
    WHERE via_principal IS NOT NULL;
```

If the migration runner forbids `CONCURRENTLY` inside its transaction, create
the index in a separate migration or drop `CONCURRENTLY` — check
`postgres/migrate` at implementation time.

- [ ] **Step 2: Write the failing tests** (`PG_DSN`-gated)

- Round trip: append with `Actor{Via: ...}` and read it back on `ReadStream`
  and `ReadAll`.
- Absent: a plain actor reads `Via.IsZero()`.
- Rejection: a partial `Via` returns `ErrInvalidDelegation` on `Append` **and**
  on `AppendTx`, and leaves the stream version unchanged.
- CHECK: a raw row with `via_type` only fails with SQLSTATE `23514` and the
  constraint name `events_via_complete`.
- Index: `EXPLAIN` for `WHERE via_principal = $1` uses `events_via_idx`.

- [ ] **Step 3: Implement the write path**

Call `p.Actor.Validate()` in `validateAppend`. Add the three columns to the
INSERT with `viaArgs(p.Actor.Via)` returning `nil, nil, nil` for the zero value
and `Via.Type`, `Via.ID`, `Via.Principal()` otherwise.

- [ ] **Step 4: Implement the read path**

Add the three columns to `envelopeCols`; scan them into `*string` and
reconstruct `Actor.Via`, failing the read when only some are present (an audit
defect, unlike the telemetry tolerance).

- [ ] **Step 5: Run**

`PG_DSN=... GOWORK=off go test ./postgres/ -run 'TestActor|TestDelegat' -v`,
then the full Postgres suite, `GOWORK=off go test ./...`, `golangci-lint run`,
`gofmt`.

- [ ] **Step 6: Commit**

```bash
git add postgres/migrations/0012_delegated_actor.sql postgres/wsstore.go postgres/store.go postgres/actor_test.go
git commit -m "feat(postgres): persist the delegated actor"
```

---

### Task 3: SQLite persistence

**Files:**
- Create: `sqlite/migrations/0010_delegated_actor.sql`
- Modify: `sqlite/store.go` (validation, INSERT, `envelopeCols`, `scanEnvelope`)
- Create: `sqlite/actor_test.go`
- Modify: `sqlite/migrate_test.go` (version 9 → 10) and `sqlite/populated_test.go`

**Interfaces:**
- Consumes: same as Task 2.
- Produces: same behaviour; no `CHECK`.

- [ ] **Step 1: Write the migration**

`ALTER TABLE events ADD COLUMN via_type TEXT;` (and `via_id`,
`via_principal`), then
`CREATE INDEX IF NOT EXISTS events_via_idx ON events (via_principal) WHERE via_principal IS NOT NULL;`.
No table rebuild.

- [ ] **Step 2: Write the failing tests**

Same matrix as Task 2 minus the CHECK case: round trip, absent, partial
rejected on `Append`, index present (`PRAGMA index_list(events)`).

- [ ] **Step 3: Implement** — `Actor.Validate()` inline in `Append`; three
columns in the INSERT and the read; strict reconstruction.

- [ ] **Step 4: Run**

`GOWORK=off go test ./sqlite/ -run 'TestActor|TestDelegat' -v`, then
`./sqlite/`, `./...`, `golangci-lint`, `gofmt`.

- [ ] **Step 5: Commit**

```bash
git add sqlite/migrations/0010_delegated_actor.sql sqlite/store.go sqlite/actor_test.go
git commit -m "feat(sqlite): persist the delegated actor"
```

---

### Task 4: Feed conformance

**Files:**
- Modify: `feedtest/conformance.go` (`Store.Append` signature and `RunLog`)
- Modify: `sqlite/feed_test.go`, `postgres/feed_stores_test.go` (fixtures)

**Interfaces:**
- Consumes: `es.Actor`.
- Produces: `Store.Append(..., sc es.SpanContext, actor es.Actor)`; the round
  trip asserted for every adapter.

- [ ] **Step 1: Extend the signature and the case**

Add `actor es.Actor` after `sc`; thread it into `AppendParams.Actor`. In
`RunLog`, seed one event with `Actor{Via: ...}` and one with a plain service
actor, and assert both round-trip through the feed: the delegated one carries
`Via`, the plain one carries zero `Via`.

- [ ] **Step 2: Run to see the build break**, then update every call site with
`es.Actor{}` except the new cases and both fixtures.

- [ ] **Step 3: Run both conformance suites** (SQLite and `PG_DSN` Postgres).

- [ ] **Step 4: Commit**

```bash
git add feedtest/conformance.go sqlite/feed_test.go postgres/feed_stores_test.go
git commit -m "test(feedtest): assert the delegated-actor round trip"
```

---

### Task 5: Docs and release

**Files:**
- Modify: `README.md` (audit/actor semantics; the verifier-upgrade note)
- Modify: `docs/adr/0041-delegated-actor.md` (status → Accepted (implemented))
- Modify: `docs/adr/README.md` (index row)

- [ ] **Step 1: README**

Add to the audit/actor section: an event records its accountable `Actor` and,
when a service executed on its behalf, the `Via` service; "everything X did"
remains the actor query, and the reverse is the indexed `via_principal`.
State that a pre-v0.16 verifier cannot verify delegated events.

- [ ] **Step 2: Status and index**

`Proposed` → `Accepted (implemented)`; index row the same.

- [ ] **Step 3: Verify** `GOWORK=off go test ./...` and `git diff --check`.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/adr/0041-delegated-actor.md docs/adr/README.md
git commit -m "docs: record the delegated-actor guarantee and mark ADR 0041 implemented"
```

---

## Release notes (for the tag)

- **Auditor upgrade required before the first delegated event.** A pre-v0.16
  verifier reports a valid delegated event as a broken chain; nothing else
  changes for old events.
- **`feedtest.Store.Append`** gained an actor parameter (test-support break,
  same class as v0.15.0's span-context parameter).
- **Migrations**: Postgres `0012` (three columns, `CHECK`, partial index),
  SQLite `0010` (three columns, partial index, no rebuild).
- Ships as `v0.16.0`.

## Out of scope

- The Flowdoc/agent-API mapping of the token's `act` claim to `Via`, and ACL
  evaluation against the accountable `Actor`.
- Multi-hop chains, consent/grant references, backfill.
- A library-level reverse-query method; the column and index are the surface.
