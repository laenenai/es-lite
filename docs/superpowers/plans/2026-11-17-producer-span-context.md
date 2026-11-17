# Producer Span Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry the producer's W3C span context (trace id, span id, flags) on every event, through both stores and the feed, without importing a tracing SDK and without changing the audit hash.

**Architecture:** A fixed-size `es.SpanContext` on `Envelope` and `AppendParams`; nullable columns with an all-or-nothing `CHECK` in Postgres (via `ALTER TABLE`) and in SQLite (via a table rebuild); a tolerant read path that degrades an unparseable value to absent; an app-supplied decorator in the platform maps OpenTelemetry spans into the field. The hash stays byte-for-byte identical; the exclusion is documented and pinned by a golden test.

**Tech Stack:** Go 1.27, `pgx`/Postgres 17, `modernc.org/sqlite`, protobuf (platform side, not this plan).

**Spec:** `docs/adr/0040-producer-span-context.md` and the design at `docs/superpowers/specs/2026-11-17-producer-span-context-design.md`.

## Global Constraints

- Go 1.27.x; no new module dependencies; es-lite must not import OpenTelemetry or any tracing SDK.
- Additive change only: no existing behaviour, hash, or delivery guarantee changes.
- `CanonicalEnvelope` and `EnvelopeHash` stay byte-for-byte identical.
- A partial span context is rejected on write; an unparseable stored value reads as absent, never as a read error.
- Postgres migration is `0011`; SQLite's is `0009` (its series is independent and skips 0005).
- Postgres integration tests are gated on `PG_DSN`; the default suite must stay green without services (`GOWORK=off go test ./...`).
- Conventional commits.
- This plan covers es-lite v0.15.0 only. Contracts, platform and portal changes are separate plans against ADR 0040.

## Review Focus

Input classes and failure modes the ADR implies but no existing test covers. Each gets the test named here, in the task that owns the code.

1. **A SQLite database that already holds events** must migrate to `0009` with every row, position, index and unique constraint intact — the rebuild's real risk is omission, not error. → Task 4 rebuild test.
2. **One batch, many events**: every envelope in a batch carries the same producer context, and absence is per-batch too. → Tasks 3 and 4 round-trip tests.
3. **A rejected append leaves no row**: a partial context must fail before the INSERT, leaving the stream version unchanged. → Tasks 3 and 4 partial-rejection tests.
4. **An event with no context is a normal event**: a legacy/NULL row reads cleanly, `IsValid()` is false, and the feed page still delivers it. → Tasks 3 and 4 absent tests read through `Logs`.
5. **A tampered context still verifies**: the chain attests domain and audit facts only, so a populated envelope must hash identically and `VerifyStream` must pass after the value changes. → Task 2 golden and chain tests.

---

### Task 1: `es.SpanContext` and the parser

**Files:**
- Create: `es/spancontext.go`
- Create: `es/spancontext_test.go`
- Modify: `es/errors.go` (add `ErrMalformedSpanContext`)

**Interfaces:**
- Consumes: nothing.
- Produces: `type SpanContext struct { TraceID [16]byte; SpanID [8]byte; Flags uint8 }`, `func (s SpanContext) IsValid() bool`, `func ParseSpanContext(traceID, spanID []byte, flags uint8) (SpanContext, error)`, and `var ErrMalformedSpanContext error`.

- [ ] **Step 1: Write the failing test**

In `es/spancontext_test.go` (`package es_test`), table-driven:

```go
func TestParseSpanContext(t *testing.T) {
	trace := bytes.Repeat([]byte{0xab}, 16)
	span := bytes.Repeat([]byte{0xcd}, 8)
	zero16 := make([]byte, 16)
	zero8 := make([]byte, 8)

	tests := []struct {
		name          string
		trace, span   []byte
		flags         uint8
		wantErr       bool
		wantValid     bool
	}{
		{"valid", trace, span, 1, false, true},
		{"valid all flags", trace, span, 0xff, false, true},
		{"absent", nil, nil, 0, false, false},
		{"partial span", trace, nil, 0, true, false},
		{"partial trace", nil, span, 0, true, false},
		{"trace too short", trace[:15], span, 0, true, false},
		{"span too short", trace, span[:7], 0, true, false},
		{"zero trace id", zero16, span, 0, true, false},
		{"zero span id", trace, zero8, 0, true, false},
		{"flags without ids", nil, nil, 1, true, false},
	}
	// For each case: call ParseSpanContext, require errors.Is(err, es.ErrMalformedSpanContext)
	// when wantErr, require zero SpanContext on error, and require IsValid() == wantValid.
}

func TestSpanContextIsValid(t *testing.T) {
	// zero value is invalid; a populated value is valid; clearing either id invalidates it.
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `GOWORK=off go test ./es/ -run 'TestParseSpanContext|TestSpanContextIsValid' -v`
Expected: FAIL to build — `undefined: es.SpanContext` / `es.ParseSpanContext`.

- [ ] **Step 3: Implement**

`es/errors.go`: add `ErrMalformedSpanContext = errors.New("es-lite: malformed span context")`, following the file's existing message style.

`es/spancontext.go`: the type; `IsValid` returns `TraceID != [16]byte{} && SpanID != [8]byte{}`. `ParseSpanContext` returns the zero value and nil when both ids are empty **and** `flags == 0`; returns `ErrMalformedSpanContext` when both ids are empty but flags are not, when either id is the wrong length, and when either id is all zeros; otherwise returns the populated value. Quote the parsing rule in the doc comment, because it is the one rule both stores and the platform client call.

- [ ] **Step 4: Run the test to verify it passes**

Run: `GOWORK=off go test ./es/ -run 'TestParseSpanContext|TestSpanContextIsValid' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add es/spancontext.go es/spancontext_test.go es/errors.go
git commit -m "feat(es): add the W3C span context type and parser"
```

---

### Task 2: Envelope field, AppendParams field, and the hash rule

**Files:**
- Modify: `es/envelope.go` (field on `Envelope`)
- Modify: `es/store.go` (field on `AppendParams`, around lines 88-93)
- Modify: `es/hash.go` (the exclusion list, lines 31-49)
- Modify: `es/hash_test.go` (populated-context fixture)
- Modify: `es/verify_test.go` (tamper assertion)

**Interfaces:**
- Consumes: `es.SpanContext` from Task 1.
- Produces: `Envelope.SpanContext SpanContext` and `AppendParams.SpanContext SpanContext`; both are the zero value by default, so every existing literal keeps compiling.

- [ ] **Step 1: Write the failing test**

In `es/hash_test.go`, copy `goldenV1` into a local `withContext`, set `withContext.SpanContext = es.SpanContext{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Flags: 1}`, then assert:

```go
if got, want := es.CanonicalEnvelope(withContext), es.CanonicalEnvelope(goldenV1); !bytes.Equal(got, want) {
	t.Fatalf("span context leaked into the canonical encoding")
}
if es.EnvelopeHash(withContext) != es.EnvelopeHash(goldenV1) {
	t.Fatal("span context changed the envelope hash")
}
```

In `es/verify_test.go`, add a case that hashes a populated envelope, mutates only `SpanContext.TraceID`, and asserts `EnvelopeHash` is still equal to the recorded `Hash` (telemetry is not attested).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `GOWORK=off go test ./es/ -run 'TestCanonicalEnvelopeGolden|TestVerify' -v`
Expected: FAIL to build — `unknown field SpanContext in struct literal`.

- [ ] **Step 3: Implement**

Add the field to both structs with a doc comment stating it is producer telemetry, excluded from the hash, and batch-scoped on `AppendParams`. Add it to the exclusion list in `CanonicalEnvelope`'s comment beside `GlobalPosition` and `published_at`, with one sentence on why (telemetry is not an attested fact).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `GOWORK=off go test ./es/ -v`
Expected: PASS, including the unchanged golden vectors.

- [ ] **Step 5: Commit**

```bash
git add es/envelope.go es/store.go es/hash.go es/hash_test.go es/verify_test.go
git commit -m "feat(es): carry span context on the envelope and append params"
```

---

### Task 3: Postgres persistence

**Files:**
- Create: `postgres/migrations/0011_producer_span_context.sql`
- Create: `postgres/spancontext_test.go`
- Modify: `postgres/wsstore.go` (`validateAppend`, the envelope literal at 260-275, the INSERT at 282-295)
- Modify: `postgres/store.go` (`envelopeCols` at 495-500, `scanEnvelope` at 517-570)

**Interfaces:**
- Consumes: `es.SpanContext`, `es.ParseSpanContext`, `es.ErrMalformedSpanContext`.
- Produces: `AppendParams.SpanContext` is persisted and returned on every read path; `validateAppend` rejects a partial value.

- [ ] **Step 1: Write the migration**

```sql
ALTER TABLE events
    ADD COLUMN trace_id    bytea,
    ADD COLUMN span_id     bytea,
    ADD COLUMN trace_flags smallint;

ALTER TABLE events
    ADD CONSTRAINT events_span_context_complete CHECK (
        (trace_id IS NULL) = (span_id IS NULL)
        AND (span_id IS NULL) = (trace_flags IS NULL)
        AND (trace_id IS NULL OR octet_length(trace_id) = 16)
        AND (span_id  IS NULL OR octet_length(span_id)  = 8)
        AND (trace_flags IS NULL OR trace_flags BETWEEN 0 AND 255)
    ) NOT VALID;

ALTER TABLE events VALIDATE CONSTRAINT events_span_context_complete;
```

`NOT VALID` + `VALIDATE` keeps the adding lock short on a live table; the validation scan is the only long step.

- [ ] **Step 2: Write the failing tests**

`postgres/spancontext_test.go`, `package postgres_test`, skipping with `t.Skip` when `PG_DSN` is empty (follow the existing helper in `postgres/postgres_test.go`):

- `TestSpanContextRoundTrip`: append a two-event batch with a populated context; `ReadStream` and `ReadAll` return both envelopes with the same valid context.
- `TestSpanContextAbsent`: append without a context; the read envelope is the zero value and `IsValid()` is false; `Logs` still pages it.
- `TestSpanContextPartialRejected`: `Append` with `SpanContext{TraceID: set, SpanID: zero}` returns an error wrapping `es.ErrMalformedSpanContext` (`errors.Is`), and `CurrentStreamVersion` is unchanged.
- `TestSpanContextCheckRejectsRawPartial`: a raw `INSERT INTO events (... trace_id ...)` with `trace_id` set and `span_id` NULL fails with SQLSTATE `23514`.

- [ ] **Step 3: Run the tests to verify they fail**

```bash
docker run -d --rm --name es-lite-pg -p 5433:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=eslite postgres:17
PG_DSN='postgres://postgres:pw@127.0.0.1:5433/eslite?sslmode=disable' GOWORK=off go test ./postgres/ -run TestSpanContext -v
```
Expected: FAIL — columns missing / context not returned.

- [ ] **Step 4: Implement the write path**

In `validateAppend`, after the `StreamID` check: if `p.SpanContext != (es.SpanContext{})` and `!p.SpanContext.IsValid()`, return `fmt.Errorf("%w: %s", es.ErrMalformedSpanContext, p.StreamID)`. This covers `Append` and `AppendTx`, since both call it.

Add `SpanContext: p.SpanContext` to the envelope literal, and `trace_id, span_id, trace_flags` to the INSERT's column list. Map the parameters with one helper, `func spanContextArgs(sc es.SpanContext) (traceID, spanID []byte, flags *int16)`: the zero value yields `nil, nil, nil`; a populated value yields `sc.TraceID[:]`, `sc.SpanID[:]`, `int16(sc.Flags)`.

- [ ] **Step 5: Implement the read path**

Add the three columns to `envelopeCols`. In `scanEnvelope`, scan `traceID, spanID []byte` and `flags *int16` (nullable), then:

```go
var f uint8
if flags != nil {
    f = uint8(*flags)
}
if sc, err := es.ParseSpanContext(traceID, spanID, f); err == nil {
    e.SpanContext = sc
}
// A non-NULL value that does not parse is treated as absent (ADR 0040 §4):
// telemetry must never fail a read. The CHECK makes the state unreachable.
```

The scan signature is used by the anchor reads too; adding columns to the constant covers every query.

- [ ] **Step 6: Run the tests to verify they pass**

```bash
PG_DSN='postgres://postgres:pw@127.0.0.1:5433/eslite?sslmode=disable' GOWORK=off go test ./postgres/ -run TestSpanContext -v
GOWORK=off go test ./...
```
Expected: PASS, and the default suite stays green.

- [ ] **Step 7: Commit**

```bash
git add postgres/migrations/0011_producer_span_context.sql postgres/wsstore.go postgres/store.go postgres/spancontext_test.go
git commit -m "feat(postgres): persist producer span context"
```

---

### Task 4: SQLite persistence and the rebuild

**Files:**
- Create: `sqlite/migrations/0009_producer_span_context.sql`
- Create: `sqlite/spancontext_test.go`
- Modify: `sqlite/store.go` (validation in `Append` at 250-257, the INSERT at 336-348, `envelopeCols` at 599-604, `scanEnvelope` at 625-676)
- Modify: `sqlite/migrate_test.go` (expected version 8 → 9)

**Interfaces:**
- Consumes: `es.SpanContext`, `es.ParseSpanContext`, `es.ErrMalformedSpanContext`.
- Produces: the same behaviour as Postgres, including the `CHECK`, so the engines stay at parity.

- [ ] **Step 1: Write the migration**

A rebuild (SQLite cannot `ALTER TABLE ADD CONSTRAINT`). The column set is `0001` plus `0002`'s `prev_hash`/`hash`, plus the three new columns. Write it as:

```sql
CREATE TABLE events_new (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT    NOT NULL,
    stream_type     TEXT    NOT NULL,
    stream_id       TEXT    NOT NULL,
    version         INTEGER NOT NULL,
    type_url        TEXT    NOT NULL,
    schema_version  INTEGER NOT NULL,
    occurred_at     TEXT    NOT NULL,
    recorded_at     TEXT    NOT NULL,
    correlation_id  TEXT    NOT NULL,
    causation_id    TEXT    NOT NULL,
    command_id      TEXT    NOT NULL,
    actor_type      TEXT    NOT NULL,
    actor_id        TEXT    NOT NULL,
    actor_principal TEXT    NOT NULL,
    payload         BLOB    NOT NULL,
    prev_hash       BLOB,
    hash            BLOB,
    trace_id        BLOB,
    span_id         BLOB,
    trace_flags     INTEGER,
    UNIQUE (stream_id, version),
    UNIQUE (event_id),
    CHECK ((trace_id IS NULL) = (span_id IS NULL)
       AND (span_id IS NULL) = (trace_flags IS NULL)
       AND (trace_id IS NULL OR length(trace_id) = 16)
       AND (span_id  IS NULL OR length(span_id)  = 8)
       AND (trace_flags IS NULL OR trace_flags BETWEEN 0 AND 255))
);

INSERT INTO events_new (
    global_position, event_id, stream_type, stream_id, version,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor_type, actor_id, actor_principal, payload, prev_hash, hash
)
SELECT
    global_position, event_id, stream_type, stream_id, version,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor_type, actor_id, actor_principal, payload, prev_hash, hash
FROM events;

DROP TABLE events;
ALTER TABLE events_new RENAME TO events;

CREATE INDEX IF NOT EXISTS events_correlation_idx ON events (correlation_id);
CREATE INDEX IF NOT EXISTS events_command_idx     ON events (command_id);
CREATE INDEX IF NOT EXISTS events_actor_idx       ON events (actor_principal);
CREATE INDEX IF NOT EXISTS events_stream_time_idx ON events (stream_id, recorded_at);
CREATE INDEX IF NOT EXISTS events_type_pos_idx    ON events (type_url, global_position);
```

`global_position` is copied explicitly: it is the log cursor and the basis of `log_order`, and renumbering it rewrites history. The explicit values advance `sqlite_sequence`; `DROP`/`RENAME` carries it over.

- [ ] **Step 2: Write the failing tests**

`sqlite/spancontext_test.go` (`package sqlite_test`), using `sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared")`:

- `TestSpanContextRoundTrip` — two-event batch with a populated context; `ReadStream` returns both with the same valid context.
- `TestSpanContextAbsent` — no context; zero value, `IsValid()` false, `Logs` still pages it.
- `TestSpanContextPartialRejected` — `SpanContext{TraceID: set}` returns `es.ErrMalformedSpanContext`, and the stream version is unchanged.
- `TestSpanContextCheckRejectsRawPartial` — raw INSERT with only `trace_id` fails.
- `TestSpanContextRebuild` — the integrity test: open a store, append three events, read `sqlite/migrations/0009_producer_span_context.sql` with `os.ReadFile` (the package directory is the test's working directory), execute it inside a transaction against the populated database, then assert:
  - three rows remain with the same `global_position` values;
  - the next append gets `max + 1`;
  - `PRAGMA index_list(events)` contains the five index names, and the two `UNIQUE` constraints still reject a duplicate `event_id` and a duplicate `(stream_id, version)`;
  - `PRAGMA table_info(events)` has exactly the 21 expected columns.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `GOWORK=off go test ./sqlite/ -run TestSpanContext -v`
Expected: FAIL — missing columns / no CHECK.

- [ ] **Step 4: Implement**

- `Append`: after the `StreamID` and constraint checks, reject a non-zero-but-invalid `SpanContext` with `ErrMalformedSpanContext`, mirroring Postgres.
- Envelope literal: `SpanContext: p.SpanContext`; map the INSERT parameters with a local `spanContextArgs` identical in shape to Task 3's (`nil, nil, nil` for the zero value).
- INSERT: add the three columns and parameters.
- `envelopeCols` and `scanEnvelope`: add the three columns and the tolerant parse. Keep the audit-UUID parsing strict and unchanged.
- `migrate_test.go`: change the expected `MAX(version)` to 9 and extend the comment with `0009 producer span context`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `GOWORK=off go test ./sqlite/ -v` then `GOWORK=off go test ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add sqlite/migrations/0009_producer_span_context.sql sqlite/store.go sqlite/spancontext_test.go sqlite/migrate_test.go
git commit -m "feat(sqlite): persist producer span context and rebuild events"
```

---

### Task 5: Feed conformance for every adapter

**Files:**
- Modify: `feedtest/conformance.go` (`Store.Append` at 109-116, plus its call sites in `RunLog` and `runEndToEnd`)
- Modify: `sqlite/feed_test.go` (fixture wiring at 150-165)
- Modify: `postgres/feed_stores_test.go` (fixture wiring at 91-105)

**Interfaces:**
- Consumes: `es.SpanContext`.
- Produces: `Store.Append` gains a trailing `sc es.SpanContext` parameter, so every adapter's round trip is asserted by the shared suite.

- [ ] **Step 1: Write the failing assertions**

In `RunLog`, after the existing seeded events, add a case that appends one event with `es.SpanContext{TraceID: …, SpanID: …, Flags: 1}` and one without, then opens the log and subscribes with the suite's existing `ackingSink` (see `newServer`), collecting what it delivers. Assert the first envelope carries a valid context and the second is absent.

- [ ] **Step 2: Change the fixture signature and run to see it fail**

Change the `Append` field to:

```go
Append func(ctx context.Context, t testing.TB, scope string, sid es.StreamID, version uint64, typeURL string, payload []byte, sc es.SpanContext) es.Envelope
```

Run: `GOWORK=off go test ./...`
Expected: FAIL to build at every call site and both fixture implementations.

- [ ] **Step 3: Thread the parameter through**

Update every `st.Append(...)` call in the suite with `es.SpanContext{}` except the new case, and both fixture closures/helpers (`appendFeedEvent`, `feedAppendEvent`) to set `AppendParams.SpanContext`.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
GOWORK=off go test ./sqlite/ -run TestConformance -v
PG_DSN='postgres://postgres:pw@127.0.0.1:5433/eslite?sslmode=disable' GOWORK=off go test ./postgres/ -run TestConformance -v
```
Expected: PASS on both backends.

- [ ] **Step 5: Commit**

```bash
git add feedtest/conformance.go sqlite/feed_test.go postgres/feed_stores_test.go
git commit -m "test(feedtest): assert the span-context round trip in conformance"
```

---

### Task 6: `projection.IsReplaying`

**Files:**
- Modify: `projection/replay.go` (mark the context in `Replay`, add `IsReplaying`)
- Create: `projection/replay_marker_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `func IsReplaying(ctx context.Context) bool`; `Replay` marks the context it passes to `apply`, and `Runner.Rebuild` inherits it because it calls `Replay`.

- [ ] **Step 1: Write the failing test**

```go
func TestIsReplaying(t *testing.T) {
	if IsReplaying(context.Background()) {
		t.Fatal("IsReplaying is true on a plain context")
	}
	var seen bool
	_, err := Replay(context.Background(), stubSource{...}, 0, 10,
		func(ctx context.Context, _ []es.Envelope) error { seen = IsReplaying(ctx); return nil })
	// require err == nil and seen == true
}

func TestIsReplayingFalseOutsideReplay(t *testing.T) {
	// Runner.Apply called directly must see false.
}
```

Add a `Runner.Rebuild` case: a `Runner{Marker: NewMemoryMarker(), Dispatch: …}` over a stub `Source` sees `IsReplaying == true` inside `Dispatch`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `GOWORK=off go test ./projection/ -run TestIsReplaying -v`
Expected: FAIL to build — `undefined: IsReplaying`.

- [ ] **Step 3: Implement**

An unexported key type in `projection/replay.go`, `withReplay(ctx)` applied once at the top of `Replay` before the loop, and `IsReplaying(ctx)` reading it. Do not mark in `Runner.Apply`: a direct call is live.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `GOWORK=off go test ./projection/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add projection/replay.go projection/replay_marker_test.go
git commit -m "feat(projection): mark replay so consumers can suppress span links"
```

---

### Task 7: Docs and the ADR status

**Files:**
- Modify: `README.md` (Delivery guarantees, "What it does not guarantee", after the cross-stream causality bullet at ~198-204)
- Modify: `docs/adr/0040-producer-span-context.md` (status)
- Modify: `docs/adr/README.md` (index row)

**Interfaces:**
- Consumes: everything above.
- Produces: the documented guarantee, and the ADR marked implemented.

- [ ] **Step 1: Add the README bullet**

```markdown
- **Tamper-evident trace context.** The producer's W3C span context (trace id,
  span id, flags) travels with the envelope and survives the store round trip
  and the remote feed, but it is telemetry: it is excluded from
  `CanonicalEnvelope` and `EnvelopeHash`, it may be absent on any event, and a
  rewritten value still verifies. Delivery never depends on it.
```

- [ ] **Step 2: Flip the ADR status**

`docs/adr/0040-producer-span-context.md`: `- **Status:** Proposed` → `- **Status:** Accepted (implemented)`. `docs/adr/README.md`: the 0040 row becomes `Accepted (implemented)`.

- [ ] **Step 3: Verify**

```bash
GOWORK=off go test ./...
git diff --check
```
Expected: green, no whitespace errors.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/adr/0040-producer-span-context.md docs/adr/README.md
git commit -m "docs: record the span-context guarantee and mark ADR 0040 implemented"
```

---

## Self-review notes

- Every ADR acceptance criterion maps to a task: type/parser (1), fields (2), Postgres (3), SQLite parity (4), hash (2), feed/client tolerance (5 and the platform plan), replay marker (6), docs (7).
- `ParseSpanContext` is produced once in Task 1 and consumed by Tasks 3, 4 and the platform client, so the length rule cannot drift.
- The SQLite rebuild is the highest-risk step; its test asserts schema shape, constraints, positions and the sequence, not only the rows.
- Deliberately not in this plan: the contracts `alpha.36` fields, the platform decorators, and the portal wrappers. They are separate plans; the ADR is their contract.
