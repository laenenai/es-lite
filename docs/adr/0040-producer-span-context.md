# ADR 0040: Producer Span Context in the Event Envelope

- **Status:** Proposed
- **Date:** 2026-11-17
- **Related:** ADR 0001 (storage neutrality), ADR 0029 (tamper-evident hash
  chain), ADR 0035 (caller-owned transactions), ADR 0025 (payload
  opaqueness), the 2026-11-17 producer-span-context design
  (`docs/superpowers/specs/2026-11-17-producer-span-context-design.md`), and
  the 2026-11-17 design review that produced §10.

## 1. Context

Cross-service tracing stops at the event append. A command's trace ends when
the event commits; the projection that applies it runs later, in a different
trace, with no causal link back. That makes a class of production questions
unanswerable: which request caused this projection write, how long the effect
took to become visible, and which async work belongs to a failed command.

es-lite already carries `CorrelationID` and `CausationID` on `Meta`,
`AppendParams` and `Envelope`, and the audit hash covers them. What is missing
is the other half of the W3C trace context — the producer's **span** id — so a
consumer can create a real span link rather than only correlate ids. A link
needs the producer's trace id, span id and trace flags.

The library's constraint is that it must stay storage- and SDK-neutral
(ADR 0001): it may store tracing bytes, but it may not import a tracing SDK,
hold its globals, or make an event's delivery depend on telemetry.

## 2. Goals, non-goals, anti-goals

**Goals**

- Every event can carry the producer's W3C span context: trace id (16 bytes),
  span id (8 bytes), trace flags (1 byte).
- The context survives the store round trip and the remote feed, or is
  explicitly absent.
- A projection can start a span with a W3C link to the producer span and join
  it to the business event by correlation id.
- Existing events, hashes and hash-chain verification are unchanged.

**Non-goals**

- Changing the tamper-evident hash domain (§3.4).
- Backfilling trace context for historical events.
- Propagating baggage, trace state or vendor-specific fields.
- Telemetry fields in domain event protos.
- Sampling decisions inside es-lite; the application samples.
- Emitting spans from inside es-lite. The application's append decorator
  measures the same database round trip.

**Anti-goals**

- A hash change that invalidates historical chain verification.
- An event that claims a span context with only half of it present.
- Coupling the library to a tracing SDK, its version, or its globals.
- A feed change that makes an older subscriber misread a newer event.
- Telemetry that can stop delivery: a malformed trace value must never fail a
  read, a page or a projection.

## 3. Decision

### 3.1 The type and the append seam

`es` gains a small file of its own:

```go
type SpanContext struct {
    TraceID [16]byte
    SpanID  [8]byte
    Flags   uint8
}

func (s SpanContext) IsValid() bool                                     // both ids non-zero
func ParseSpanContext(traceID, spanID []byte, flags uint8) (SpanContext, error)
```

`IsValid` is the absence test and never panics. `ParseSpanContext` is the one
validation rule: both ids empty **and** flags zero means absent (the zero
value); otherwise both must be exactly 16 and 8 bytes and non-zero; anything
else is `ErrMalformedSpanContext`. It exists so the stores and the platform's
feed client share one rule instead of three.

`Envelope` gains `SpanContext`; **`AppendParams` gains the same field**.
`AppendParams` is the input to both append entry points — `Store.Append` and
the caller-owned-transaction `Workspace.AppendTx` — so it is the struct every
append already carries, and a store decorator can fill it from the active span
without touching call sites.

`Meta` does **not** gain the field. The context is derived from `ctx`, which is
already correct per append and already carries the caller's span through
reactions; a caller-settable copy would need a precedence rule and would be
stale by default. A caller that needs to override builds `AppendParams` itself.

Two escape hatches are explicitly not intercepted: `Workspace.AppendTx`
(ADR 0035), where the platform fills the field at the one or two places it
builds the params, and `aggregate.WithAppend`, which replaces `store.Append`
outright (`aggregate/runtime.go:398-401`). Both are existing documented
boundaries of the API; this ADR inherits them rather than closing them.

The context is **batch-scoped**, like `CorrelationID`: one append is the events
of one command, and every envelope in the batch carries the same producer span.

### 3.2 Persistence: Postgres

Migration `0011_producer_span_context.sql` adds nullable `trace_id bytea`,
`span_id bytea`, `trace_flags smallint` to `events`. The table is
hash-partitioned, so the columns and the constraint are declared on the parent.

The store writes all three when valid and all three NULL when not. A partial
value is a rejection, not a guessed context. A `CHECK` enforces the same rule
in the database:

```sql
(trace_id IS NULL) = (span_id IS NULL)
AND (span_id IS NULL) = (trace_flags IS NULL)
AND (trace_id IS NULL OR octet_length(trace_id) = 16)
AND (span_id  IS NULL OR octet_length(span_id)  = 8)
AND (trace_flags IS NULL OR trace_flags BETWEEN 0 AND 255)
```

Applications of authoritative length, not just presence, are deliberate: three
nullable `bytea` columns otherwise accept the half-present or wrong-length row
the anti-goals forbid, from any non-library writer (seed scripts, backfills,
tests using raw SQL). For a large table the constraint ships `NOT VALID` and is
validated separately.

Reads reconstruct through `ParseSpanContext`; NULL is the zero value. A
non-NULL value that does not parse is treated as **absent** (§4).

No index. Trace context is not a projection lookup key; the observability
backend indexes it. The columns are nullable with no default, so the `ALTER` is
metadata-only, though it still takes a brief lock on the parent and each
partition.

### 3.3 Persistence: SQLite rebuild

SQLite cannot `ALTER TABLE ADD CONSTRAINT`. Parity between the engines is
required, so migration `0009_producer_span_context.sql` rebuilds `events`:

1. create `events_new` with every existing column, the three new ones, both
   `UNIQUE` constraints and the `CHECK`;
2. `INSERT INTO events_new (global_position, …) SELECT global_position, … FROM
   events`, copying every row and the sequence explicitly;
3. `DROP TABLE events`;
4. `ALTER TABLE events_new RENAME TO events`;
5. recreate all five indexes (`0001`'s four plus `events_type_pos_idx` from
   `0007`).

The rebuild is atomic: the migration runner executes each migration inside a
transaction (`sqlite/store.go:197-217`). There are no foreign keys or triggers
to coordinate, and copying explicit `global_position` values advances
`sqlite_sequence` to the old maximum, which `DROP`/`RENAME` carries over — so
the log cursor and `log_order`'s attestation remain valid.

The risk is omission, not failure. A forgotten index makes feed reads scan; a
forgotten `UNIQUE` silently removes the optimistic-concurrency guarantee. A
test therefore asserts the rebuilt table's full index and constraint set, not
only its columns.

### 3.4 The hash does not change

`CanonicalEnvelope` is a hand-written field sequence (`es/hash.go:50-69`), so a
new `Envelope` field is excluded by construction. `CanonicalEnvelope` and
`EnvelopeHash` stay byte-for-byte identical.

Span context is observability metadata: it is absent on every pre-existing
event, it may legitimately be dropped or re-derived by a caller, and it is not
a fact the audit chain attests. Including it would change the hash of every new
event and force a chain version, breaking verification of historical streams
for telemetry.

The rule is recorded in `CanonicalEnvelope`'s doc comment beside the other
exclusions (`GlobalPosition`, the relay's `published_at`), and a golden test
pins a fixture **with** a populated context to the same bytes and hash as one
without it. The trade-off is explicit: **trace context is not tamper-evident.**
A rewritten `trace_id` verifies as a valid chain. That is acceptable for
telemetry and unacceptable for anything else.

### 3.5 The remote feed contract

In the platform repository, `contracts/platform/eventfeed/v1.Event` gains:

```proto
string correlation_id = 10;
string causation_id   = 11;
bytes  trace_id       = 12;   // 16 bytes when present
bytes  span_id        = 13;   // 8 bytes when present
uint32 trace_flags    = 14;
```

The feed carries none of the three today. Field numbers must be checked against
the live contract; reusing a tag assigned to a removed field is the one mistake
that is unrecoverable on the wire.

The handler fills them from the envelope. The client is tolerant, per §4: a
missing, empty or malformed id drops that value and reports through the feed
observer; the event is always delivered.

`trace_flags` above 255 is masked to the low byte; only bit 0 (sampled) is
defined today and the rest are preserved for W3C.

Optional fields keep old servers and old clients compatible: absent means zero,
which means no link. Contracts tag **alpha.36**.

A second wire path needs checking in the platform: `delivery.Relay` publishes
`[]es.Envelope` through an application-supplied function
(`delivery/relay.go:16`). If that deployment also mirrors events to a broker,
its serializer needs the same three fields or the context is dropped silently
there.

### 3.6 Application mapping (platform only)

The OTel dependency and every span live in the platform, never in es-lite.

- The application wraps its audited stores with a decorator that starts an
  `eventstore.append` span, converts it into `AppendParams.SpanContext`, and
  calls the inner store. The conversion is direct because the types coincide:
  `[16]byte(sc.TraceID())`, `[8]byte(sc.SpanID())`, `uint8(sc.TraceFlags())`.
- On the consume side it wraps `projection.Runner.Dispatch`
  (`projection/runner.go:128-136`) and the dispatch function passed to
  `reaction.NewRunner` (`reaction/reaction.go:104`).
- The link is built with
  `trace.NewSpanContext(trace.SpanContextConfig{…, Remote: true})` and
  `trace.WithLinks`. `Remote` matters: a reconstructed producer context is not
  local to the consuming process.
- Replay and rebuild share `Dispatch` and pass `ctx` unchanged, so an app
  wrapper cannot distinguish them. `projection.Replay` marks the context and
  `projection.IsReplaying(ctx)` reports it; `Runner.Rebuild` goes through
  `Replay`, so both are covered. The wrapper suppresses links while it is set.
- No valid context means no link; the apply span is a normal root. A link may
  point at an unsampled span, which is valid W3C.
- Neither decorator may fail the append or the dispatch.

### 3.7 Not chosen, and why

- **A `traceparent` string column.** Requires parsing and validation at both
  ends, splits into components only after decoding, and invites logging the raw
  header. The structured form is the same information without a codec.
- **Trace id only.** Sufficient for id correlation, insufficient for a W3C
  link, which needs the producer span id.
- **Trace context inside the domain payload.** Per-event contract churn, mixes
  telemetry with business facts, and breaks payload opaqueness (ADR 0025).
- **An OpenTelemetry dependency in es-lite**, whether to accept
  `trace.SpanContext` directly or to emit spans. It buys nothing: the byte
  types are the OTel types, so the application's mapping is a conversion. It
  would couple a storage-neutral library to a tracing API for zero data-model
  benefit. A separate `es-lite/otel` module was considered and rejected until a
  second independent consumer exists; adding it later changes no stored data.
- **A SQLite trigger instead of a rebuild.** Same invariant without the copy,
  but it puts a check on the hot insert path and its abort lands in the store's
  constraint classifier (`sqlite/store.go:350`), which would report it as a
  concurrency conflict.
- **A strict store read for malformed stored values.** See §4.

## 4. Why telemetry cannot stop delivery

Every envelope read goes through one column list and scan (`envelopeCols`,
`scanEnvelope`). A read error fails the store call, `feed.Server` ends the
subscription, and the consumer adapter reconnects onto the same row. On the
consume side a handler error is retried indefinitely
(`eventfeed/eventfeed.go:322-330`). A malformed trace value that failed the
event would therefore wedge a projection permanently — over a value the chain
does not attest.

So the rule is asymmetric, and deliberately so:

- **Audit UUIDs fail loud** (F34): a silently zeroed `correlation_id` forges an
  audit trail.
- **Trace context degrades to absent**: the event is delivered, `IsValid()` is
  false, no link is produced, and the wire reports the drop through the feed
  observer. The `CHECK` makes the bad state unrepresentable in the first place;
  the tolerance is the second line of defence, for a restored dump or a
  constraint bypassed during a migration.

## 5. Costs, accepted knowingly

- Three columns and ~25 bytes per event, plus the `CHECK` evaluation on insert.
- One table rebuild in SQLite, proportional to log size, during migration.
- Five optional wire fields and their handling in the platform feed.
- No index; trace lookups are the observability backend's job.
- A latent mislabel: a `CHECK` violation during the events INSERT would be
  classified as a conflict (`sqlite/store.go:350`). The Go writer cannot produce
  one, so this is unreachable today.
- The library ships a schema with no producer until platform v0.9.0. The
  migration is therefore the only chance to get it right before data exists.

## 6. Security and tenancy

- Trace ids are telemetry metadata, not personal data and not audit content.
  They are, however, stable correlatable identifiers: exporting events to a
  third party links their activity to other traces. That is the point, and it
  belongs in the deployment's data-handling note.
- The values are supplied by the writer and never verified against a remote
  party; they are hints, like the feed wake-ups of ADR 0039, not facts.
- No new grants, no RLS change, no new table. The workspace boundary is
  unchanged: context travels with the event it belongs to.
- The feed fields are exposed under the same subscription authorization as the
  rest of the envelope; no new scope or capability is introduced.
- Logging: a trace id is safe to log (unlike a payload). The library itself
  still never logs a value.

## 7. Consequences

**Positive**

- A projection can link to the producer span, giving true cross-service
  causality for async effects without changing delivery semantics.
- The log stays the single transport: no side table, no second cursor, no
  application-specific storage.
- The library stays dependency-free; the OTel mapping is a type conversion in
  the consumer.
- Existing chains verify unchanged, and the exclusion is documented where a
  maintainer would otherwise "fix" it.

**Negative**

- Trace context is not tamper-evident; a rewritten id verifies.
- One more field on the two hottest structs, carried through every store and
  every envelope in a page.
- A SQLite migration that rewrites the log table, with omission as its failure
  mode.
- The append-decorator approach does not cover `AppendTx` or `WithAppend`; the
  platform must remember those two paths.

## 8. Acceptance criteria

- `es.SpanContext`, `IsValid` and `ParseSpanContext` exist with the documented
  rules; `ErrMalformedSpanContext` is returned for partial, zero-length or
  wrong-length input.
- `Envelope` and `AppendParams` carry the field; `Meta` does not.
- Postgres `0011` and SQLite `0009` add the columns and enforce the same
  `CHECK`; SQLite's rebuild preserves the primary key, both `UNIQUE`
  constraints, all five indexes, the row count and the maximum
  `global_position`.
- `CanonicalEnvelope` is unchanged: the existing golden vector still matches,
  and a populated-context fixture hashes identically to an empty one.
- A pre-existing event (all NULL) reads as absent and produces no link.
- The feed client delivers an event with a malformed trace id, dropping only
  the context, and reports it.
- `projection.IsReplaying` is true inside `Replay` and `Runner.Rebuild`, false
  on live dispatch.
- `feedtest` asserts the round trip and the absent case for every adapter.
- `GOWORK=off go test ./...` is green without services; the Postgres
  integration suite is green; `golangci-lint` and the feedgate build stay clean.

## 9. Implementation outline

1. `es`: the type, the parser, the fields, the `CanonicalEnvelope` comment and
   the hash fixture.
2. Postgres `0011` and the write/read plumbing.
3. SQLite `0009` (rebuild) and the write/read plumbing.
4. `feedtest` conformance assertions.
5. `projection.IsReplaying` and its test.
6. Docs: README delivery guarantees, migration notes.
7. Release v0.15.0. Contracts, platform and portal follow in their own
   repositories against this ADR.

## 10. Questions resolved at design review

1. **Malformed stored value**: degrades to absent, not a read failure (§4); the
   `CHECK` prevents the state rather than the reader punishing it.
2. **`AppendTx`**: not covered by a store decorator. The platform fills
   `SpanContext` at its caller-owned-transaction sites; the ADR says so instead
   of claiming "every append path".
3. **SQLite parity**: a rebuild migration, not behavioural parity only (§3.3).
4. **Feed correlation/causation**: new string fields; empty or malformed
   becomes `uuid.Nil`, and the event is delivered.
5. **Replay detection**: `projection.IsReplaying` is added to v0.15.0; a
   call-site marker was rejected as silently forgettable.
6. **Scope**: the plan in the es-lite repository covers v0.15.0 only; the
   contract, platform and portal work are separate plans against this ADR.
