# Producer span context in the event envelope — design

**Status:** proposed — 2026-11-17, revised after design review

## Intent

Cross-service tracing stops at the event append. A command's trace ends when
the event commits; the projection that applies it runs later, in a different
trace, with no causal link back. That makes a whole class of questions
unanswerable in production: which request caused this projection write, how
long did the effect take to become visible, and which async work belongs to a
failed command.

es-lite already carries `CorrelationID` and `CausationID` on commands and
envelopes. This design adds the missing half of the W3C span context — the
producer's trace id, span id and trace flags — so a projection can create a
real **span link** to the span that appended the event. It is the upstream
piece the observability slice consumes; without it, only id correlation is
possible.

## Goals

- Every event can carry the producer's span context: trace id (16 bytes), span
  id (8 bytes), trace flags (1 byte).
- The context survives the event store round trip and the remote feed, or is
  explicitly absent.
- A projection can start a span with a W3C link to the producer span and join
  it to the business event by correlation id.
- Existing events, hashes and hash-chain verification are unchanged.
- The library keeps no OpenTelemetry dependency: it stores bytes, and the
  application maps them.

## Non-goals

- Changing the tamper-evident hash domain (see Decision 3).
- Backfilling trace context for historical events.
- Propagating baggage, trace state or vendor-specific fields.
- Adding telemetry fields to domain event protos.
- Sampling decisions inside es-lite; the application samples.
- Emitting spans from inside es-lite. The append decorator in the consuming
  application measures the same database round trip without coupling the
  library to a tracing API.

## Anti-goals

- A hash change that invalidates historical chain verification.
- An event that claims a span context with only half of it present.
- Coupling the library to a tracing SDK, its version, or its globals.
- A feed change that makes an older subscriber misread a newer event.
- Telemetry that can stop delivery: a malformed trace value must never fail a
  read, a page or a projection.

## Constraints

- Go 1.27.x; es-lite is a separate library with its own release cadence.
- The remote feed contract lives in the **platform** repository
  (`contracts/platform/eventfeed/v1`), not es-lite. This change spans both.
- Postgres is the production store; SQLite and the test stores must stay
  honest.
- Additive wire changes only; the feed's gRPC/protobuf fields are optional.
- Trace ids are telemetry metadata, not personal data and not audit content.

## Approaches

1. **A structured span context in the envelope (chosen).** `es.SpanContext`
   with fixed-size fields on `AppendParams` and `Envelope`; nullable columns;
   the feed carries the three fields. Explicit, cheap, queryable, and no
   parsing. The field types are the same underlying values OpenTelemetry uses
   (`[16]byte`, `[8]byte`, `byte`), so the application's mapping is a type
   conversion, not a codec.
2. **A W3C `traceparent` string.** Compact and standard, but it needs parsing
   and validation at both ends, splits into three values only after decoding,
   and invites accidental logging of the raw header. Rejected: the structured
   form is simpler and the application already has the components.
3. **Trace id only.** Enough for id correlation, not for a W3C link, which
   requires the producer span id. Rejected: it would not deliver the goal.
4. **Trace context in the domain event payload.** Per-event contract churn,
   mixes telemetry with business facts, and breaks the "payload is the domain
   event" rule. Rejected.

## Decisions

### 1. The type and the append seam

In `es`, a small file of its own:

```go
// SpanContext is the producer's W3C span context, stored as raw bytes so the
// library has no tracing dependency. The zero value means "not recorded".
type SpanContext struct {
    TraceID [16]byte
    SpanID  [8]byte
    Flags   uint8
}

// IsValid reports whether the context is complete: both ids non-zero.
func (s SpanContext) IsValid() bool

// ParseSpanContext is the one validation rule: both ids empty means absent
// (the zero value); otherwise both ids must be exactly 16 and 8 bytes and
// non-zero. Anything else is ErrMalformedSpanContext.
func ParseSpanContext(traceID, spanID []byte, flags uint8) (SpanContext, error)
```

`Envelope` gains `SpanContext SpanContext`; **`AppendParams` gains the same
field**. `AppendParams` is the input to both append entry points — `Store.Append`
and the caller-owned-transaction `Workspace.AppendTx` — so it is the struct
every append already carries, and a store decorator can fill it from the active
span without touching call sites.

That decorator covers `Store.Append` only. Two escape hatches build or pass
`AppendParams` themselves and are not intercepted:

- `Workspace.AppendTx` (ADR 0035), the platform's tenant path that commits the
  event and its audit record in one transaction. The platform fills
  `SpanContext` where it builds those params — one or two call sites, not a
  sweep. This is precisely the path the tenant observability work cares about,
  so it must be on the checklist rather than assumed covered.
- `aggregate.WithAppend`, which replaces `store.Append` outright
  (`aggregate/runtime.go:398-401`).

Both are documented boundaries of the existing API; this design inherits them
rather than closing them.

`Meta` does **not** gain the field. The decorator derives the context from
`ctx`, which is already correct per append and already carries the caller's
span through reactions; a second, caller-settable copy would need a precedence
rule and would be stale by default. A caller that genuinely needs to override
the context builds `AppendParams` itself (or its own append function); it does
not go through `Meta`.

The context is **batch-scoped**, like `CorrelationID`: one append is the
events produced by one command, and every envelope in the batch carries the
same producer span. `Envelope` is per-event because reads are per-event.

Adding a struct field is source-compatible within this repository (keyed
literals throughout) but can break an unkeyed composite literal in a consumer
module. That is why this ships as a minor version, not a patch.

### 2. Persistence

- Postgres migration `0011_producer_span_context.sql` adds nullable
  `trace_id bytea`, `span_id bytea`, `trace_flags smallint` to `events`
  (hash-partitioned, so the columns are declared on the parent).
- SQLite has its own migration series, which is independent and currently ends
  at `0008`: its migration for this change is `0009_producer_span_context.sql`.
  The numbers do not match across backends and are not meant to.
- The store writes all three when valid, all three NULL when not. A partial
  value is an error, not a guessed context.
- A `CHECK` constraint on the parent enforces the same rule in the database:
  `(trace_id IS NULL) = (span_id IS NULL) AND (span_id IS NULL) =
  (trace_flags IS NULL)`, plus `octet_length(trace_id) = 16`,
  `octet_length(span_id) = 8` and `trace_flags BETWEEN 0 AND 255` whenever
  they are non-NULL. Three nullable `bytea` columns otherwise let any
  non-library writer (seed scripts, backfills, tests using raw SQL) create the
  half-present or wrong-length row the anti-goals forbid — and the read path's
  tolerance would then hide it instead of failing. Add it `NOT VALID` and
  `VALIDATE` it if the table is large.
- Reads reconstruct the struct through `ParseSpanContext`; NULL is the zero
  value. A non-NULL value that does not parse — wrong length, a zero id, flags
  above a byte — is treated as **absent**: the envelope reads cleanly,
  `IsValid()` is false, and no link is produced. This deliberately diverges
  from the F34 policy for audit UUIDs (`postgres/store.go:505-510`): those are
  attested facts, and a silently zeroed one forges an audit trail. Trace
  context is not attested, so it must never be able to fail a read, a page or
  a subscription.
- SQLite gets the same constraint by rebuilding `events` in its own migration:
  create `events_new` with the three columns and the `CHECK`, copy every row
  including `global_position` explicitly, drop, rename, and recreate all five
  indexes and both `UNIQUE` constraints. SQLite cannot `ALTER TABLE ADD
  CONSTRAINT`, and parity is a requirement, so the rebuild is the honest way
  to get it. It is atomic — the runner wraps the migration in a transaction
  (`sqlite/store.go:197-217`) — and there are no foreign keys or triggers to
  coordinate. Copying explicit `global_position` values advances
  `sqlite_sequence` to the old maximum and `DROP`/`RENAME` carries it over, so
  the log cursor and `log_order`'s attestation stay valid.
- The rebuild's risk is omission, not failure: a forgotten index makes feed
  reads scan, and a forgotten `UNIQUE` silently removes the concurrency
  guarantee. A test therefore asserts the rebuilt table's full index and
  constraint set, not only its columns. This is the one migration in the
  series where a silent half-success is worse than an error.
- A `CHECK` violation on the events INSERT would be classified as a conflict
  (`sqlite/store.go:350`). The Go writer cannot produce one, so that is a
  latent mislabel rather than a live path; it becomes worth fixing only if the
  store ever accepts caller-supplied stored rows.
- No index: trace context is not a lookup key for a projection, and the
  observability backend indexes it.
- Columns are nullable with no default, so the `ALTER` is metadata-only; it
  still takes a brief lock on the parent and each of the eight partitions.
- Touch points, so none is missed: the Postgres INSERT and its parameter list
  (`postgres/wsstore.go:282-295`), the `envelopeCols` constant and
  `scanEnvelope` (`postgres/store.go:495-516`), and the same write/read pair in
  `sqlite/store.go` (`:336`, `:602`). There is no sqlc; all of this SQL is
  hand-written.

### 3. The hash does not change

`CanonicalEnvelope` is a hand-written field sequence
(`es/hash.go:50-69`), so a new `Envelope` field is excluded by construction.
`CanonicalEnvelope` and `EnvelopeHash` stay byte-for-byte identical. The
span context is **observability metadata**:

- it is absent on every event written before this change,
- it may legitimately be dropped or re-derived by a caller,
- it is not a fact the audit chain is attesting.

Including it would change the hash of every new event and force a chain
version, breaking verification of historical streams for telemetry. A golden
test pins the canonical bytes and hash of a fixture **with** a populated span
context to prove the value is excluded, and the exclusion is added to the
field list in `CanonicalEnvelope`'s doc comment, beside `GlobalPosition` and
the relay's `published_at` marker. That comment is the only thing stopping a
later maintainer from "completing" the encoding.

The trade-off is explicit and belongs in the ADR: **trace context is not
tamper-evident.** A rewritten `trace_id` verifies as a valid chain. That is
acceptable for telemetry and unacceptable for anything else, which is why the
rule is written down.

### 4. The remote feed contract

In the platform repository, `contracts/platform/eventfeed/v1.Event` gains:

```proto
string correlation_id = 10;   // the envelope's correlation id
string causation_id   = 11;   // the envelope's causation id
bytes  trace_id       = 12;   // 16 bytes when present
bytes  span_id        = 13;   // 8 bytes when present
uint32 trace_flags    = 14;
```

The feed carries none of the three today; adding them is what makes either id
correlation or span links possible. Field numbers must be checked against the
live contract before use; reusing a tag already assigned to a removed field is
the one mistake that is unrecoverable on the wire.

The handler fills them from the envelope. The client is **tolerant**: a
missing, empty or malformed `trace_id`/`span_id` drops the context (and
reports it through the feed observer); a missing, empty or malformed
`correlation_id`/`causation_id` becomes `uuid.Nil`; the event is always
delivered. Failing the event would wedge the subscription — a handle error is
retried indefinitely (`eventfeed/eventfeed.go:322-330`), and a failed read
ends the subscription onto the same row — over values that are not attested.
Whether a reaction propagates the consumed ids into its own appended events
stays the application's choice.

`trace_flags` above 255 is masked to the low byte; only bit 0 (sampled) is
defined today and the rest are preserved for W3C's future use.

Optional fields keep old servers and old clients compatible: absent means
zero, which means no link. Contracts tag **alpha.36**.

Second wire path to check in the platform: `delivery.Relay` publishes
`[]es.Envelope` through an application-supplied function
(`delivery/relay.go:16`). If that deployment also mirrors events to a broker,
its serializer needs the same three fields or the span context is dropped
silently on that path.

### 5. Application mapping (platform only)

The OTel dependency and all span creation live in the platform, never in
es-lite.

- The application wraps its audited stores with a decorator that starts an
  `eventstore.append` span, converts it into `AppendParams.SpanContext`, and
  calls the inner store. This is the producer span; it covers `Store.Append`,
  and the `AppendTx` (ADR 0035) and `WithAppend` call sites fill the field
  themselves.
- Conversion is direct because the types coincide:
  `[16]byte(sc.TraceID())`, `[8]byte(sc.SpanID())`,
  `uint8(sc.TraceFlags())`.
- On the consume side the application wraps `projection.Runner.Dispatch`
  (`projection/runner.go:128-136`) and the dispatch function passed to
  `reaction.NewRunner` (`reaction/reaction.go:104`). Both are
  application-supplied; no upstream hook is needed.
- Replay and rebuild share that same `Dispatch`, and the caller's `ctx`
  passes through unchanged, so an app-level wrapper cannot tell a rebuild
  from live delivery. v0.15.0 therefore adds a replay marker:
  `projection.Replay` marks the context and `projection.IsReplaying(ctx)`
  reports it. `Runner.Rebuild` goes through `Replay`, so both paths are
  covered, and the wrapper suppresses links while the marker is set. A
  call-site marker was rejected: every new rebuild entry point would have to
  remember it, and forgetting it is silent noise.
- The apply span links with `trace.WithLinks` using
  `trace.NewSpanContext(trace.SpanContextConfig{... Remote: true})`. The
  `Remote` bit matters: a reconstructed producer context is not local to this
  process, and marking it local changes how the SDK treats it.
- No valid context means no link and the apply span stays a normal root.
  Sampling is independent: a link may point at an unsampled span, which is
  valid W3C.
- Neither decorator may fail the append or the dispatch. A tracing failure is
  dropped, never propagated into the event path.

Correlation and causation ids are already part of the envelope and the audit
hash; this change makes the **feed** carry them. They are not derived from the
trace id, and no append-site sweep is required.

### 6. Releases and order

1. es-lite **v0.15.0** — the type and helper, the append seam, the stores and
   the migrations.
2. Contracts **alpha.36** — the feed fields.
3. Platform **v0.9.0** — bump both, map the feed, add the store and dispatch
   decorators, and ship the telemetry foundation that creates the spans these
   fields describe (the companion observability spec).
4. Portal pins platform v0.9.0 and contracts alpha.36, and wraps its own
   projectors and reaction dispatch.

Until step 3 there is no visible behaviour: the library and the wire carry a
context nobody produces yet. That ordering is deliberate, so the two contracts
change once. It is also load-bearing: platform v0.9.0 does not compile against
`AppendParams.SpanContext` until v0.15.0 is tagged, and that window is the
only chance to get the migration right before anything writes to it.

Scope of the implementation plan in this repository: **step 1 only**. Steps 2–4
land in the platform and portal repositories, each with its own plan; this
document is their contract.

## Testing

Named, because the value of this change rests on these properties:

- **Round trip**: each backend stores and returns all three fields; NULL reads
  back as the zero value and `IsValid()` is false.
- **All-or-nothing**: a partial `SpanContext` is rejected by the store; both
  backends reject a partial, wrong-length or out-of-range row written by raw
  SQL; a row that somehow holds an unparseable value reads as absent rather
  than failing the page.
- **Rebuild integrity (SQLite)**: after migration `0009` the table still has
  all five indexes, both `UNIQUE` constraints, the primary key, the same row
  count and the same maximum `global_position`, and the next append takes
  `max + 1`.
- **Hash stability**: the existing golden vector is unchanged, and a new
  fixture with a populated context produces the same canonical bytes and hash
  as the same fixture without it.
- **Absence is safe**: an event written before this change (all three NULL)
  reads cleanly and produces no link.
- **Replay marker**: `projection.IsReplaying` is true inside `Replay` and
  `Runner.Rebuild`, false on a live `Dispatch`.
- **Conformance**: the `feedtest` suite asserts the round trip and the absent
  case for every adapter, so the platform's future stores inherit the
  contract instead of re-deriving it.

Platform side: a malformed length yields no link and a delivered event; an
empty `correlation_id` from an older server yields `uuid.Nil`; a trace-enabled
append produces a valid context; replay produces no links.

## What is left to the builder

- Exact DDL details: `bytea` vs `bit(128)`, and whether the Postgres `CHECK`
  ships `NOT VALID` for the current table size.
- The SQLite `0009` rebuild statement by statement: column list, copy, index
  and constraint recreation, and the test that compares the rebuilt schema
  against `0001` plus `0007`.
- The golden-vector fixture bytes and its placement beside the existing one.
- The platform-side semconv attribute names and the decorator's exact
  placement around the audited stores.
- Whether the platform testkit store calls `es.ParseSpanContext` or has its own
  fixture helper — it should not reimplement the length rule.
- The precise observer event used to report a dropped malformed context.
- The two platform `AppendTx` call sites that must fill `SpanContext`
  themselves.
