# ADR 0030: Projector and Reaction Codegen from ProjectionSpec / ReactionSpec

- **Status:** Accepted (implemented)
- **Date:** 2026-10-30

## Context

`contracts` (design §9.10, convention 30) declares every read model, index and
executor reaction as a **message option** — `ProjectionSpec` on the read-model
message, `ReactionSpec` on an empty descriptor message — so that the inputs
(`consumes`), the fetch mode and the outputs (`issues`) are lintable promises.
§13.5 asks es-lite to generate the boilerplate from those options: a typed
handler interface, a runner, a state loader seam and the executor loop.

Today those options live in `contracts` as `ont.projection.v1alpha1.spec`
(extension 60010) and `.reaction` (60011) in `ont/projection/v1alpha1/options.proto`.
`ProjectionSpec.ceiling` references `ont.domain.clearance.v1alpha1.Clearance`,
a `contracts`-owned type.

What es-lite already has:

- `cmd/protoc-gen-es-lite/main.go` reads message options through the
  **compiled** Go extension descriptors (`esv1.E_SumType`, `esv1.E_SchemaVersion`
  from `gen/es/v1`) — `proto.HasExtension` / `proto.GetExtension` in
  `sumTypeName` and `schemaVersionOf`. The plugin links against its own
  options package; `Taskfile.yml` even has a `generate:bootstrap` task for the
  cold start that implies.
- `natsjs.Consume` runs one durable consumer with a single `FilterSubject`;
  `natsjs.DefaultSubject` builds `evt.<ws>.<aggregate>.<event>` where
  `<aggregate>` is `StreamID.Type` and `<event>` is the lower-cased last
  segment of the type URL.
- `projection.Replay` + `projection.Marker` (ADR 0005) give rebuild and the
  last-applied `global_position` idempotency marker; `delivery.Handler` and
  `projection.Apply` share one shape.
- `aggregate.Runtime.LoadAsOfVersion(ctx, sid, v) (S, error)` is exactly the
  "state as of the event's stream version" read that `FETCH_MODE_STATE_AS_OF_EVENT_VERSION`
  needs.

## Decision

### 1. The spec options move into es-lite's `es/v1/options.proto`

es-lite owns the runtime contract the options describe (fetch mode, rebuild
mode, checkpoint key, subject derivation), so es-lite owns the descriptors.
Added to `proto/es/v1/options.proto`, in es-lite's 5xxxx range:

```
message Aggregate { string stream_type = 1; }            // on the State message
enum FetchMode    { FETCH_MODE_UNSPECIFIED=0; FETCH_MODE_METADATA_ONLY=1; FETCH_MODE_STATE_AS_OF_EVENT_VERSION=2; }
enum RebuildMode  { REBUILD_MODE_UNSPECIFIED=0; REBUILD_MODE_FROM_LOG=1; }
enum ProjectionKind { …UNSPECIFIED=0; READ_MODEL=1; REACTION=2; INDEX=3; }
message ProjectionSpec { string name=1; ProjectionKind kind=2; repeated string consumes=3;
                         FetchMode fetch=4; RebuildMode rebuild=5; google.protobuf.Duration max_lag=6;
                         string owner_context=7; }
message ReactionSpec   { string name=1; repeated string consumes=2; repeated string issues=3;
                         FetchMode fetch=4; string owner_context=5; }
extend google.protobuf.MessageOptions {
  Aggregate      aggregate  = 50003;
  ProjectionSpec projection = 50010;
  ReactionSpec   reaction   = 50011;
}
```

`ceiling` does **not** move: it is a clearance concern es-lite has no business
knowing. `contracts` keeps `ont/projection/v1alpha1/options.proto` as a thin
companion extension — `message ProjectionPolicy { Clearance ceiling = 1; }`
at 60010 — placed next to `(es.v1.projection)` on the same message. The
`owner_context` string is kept in es-lite's spec because the generated
reaction table (§9.10) is grouped by it and it is opaque to the runtime.

**Why not read `ont.projection.*` dynamically in the plugin.** It is possible:
the plugin can rebuild a `protoregistry.Files` from the `CodeGeneratorRequest`
and re-parse options with `dynamicpb`. Rejected as the primary route because
(a) it makes es-lite's generated runner depend on field numbers and enum
values defined in a downstream repo — the dependency arrow the `contracts`
conventions (rule 25) forbid in the other direction; (b) a drift between the
two would surface only at generation time with no type checking; (c) the
`ceiling` field could not be dropped from a shared message without the
`Clearance` import, so es-lite would import `contracts`' Go package — which
is the "backwards" dependency §13.5 warns about. Importing es-lite's own
`gen/es/v1` is what the plugin already does; extending it is the smallest
change.

**Migration for `contracts`** (cheap now: v1alpha1 may break between commits by
its rule 16, and no read model is in the current slice):

1. Depend on es-lite's options (vendored `third_party/es/v1/options.proto`
   bumped, or the BSR module of ADR 0033).
2. On every State message add `option (es.v1.aggregate) = { stream_type: "space" };`
   (one per aggregate package; §3.4's stream types).
3. Rewrite `(ont.projection.v1alpha1.spec) = {…}` to `(es.v1.projection) = {…}`
   minus `ceiling`, and `(ont.projection.v1alpha1.reaction)` to
   `(es.v1.reaction)`. Add `(ont.projection.v1alpha1.policy) = { ceiling: … }`
   where a ceiling was set.
4. Reduce `ont/projection/v1alpha1/options.proto` to `ProjectionPolicy` +
   extension 60010; retire 60011 (reserve the number).

### 2. What the plugin generates per annotated message

Generated into the annotated message's own Go package (as today), file
`<proto>_es.pb.go`. For `(es.v1.projection)` named `casework.case_summary`
consuming `CaseOpened` and `CaseAccepted`:

```go
// Handler: one method per consumed event, typed by the event's Go type.
type CaseSummaryHandler interface {
    OnCaseOpened(ctx context.Context, env es.Envelope, ev *casev1.CaseOpened, state *casev1.Case) error
    OnCaseAccepted(ctx context.Context, env es.Envelope, ev *casev1.CaseAccepted, state *casev1.Case) error
}
// Loader seam, one per consumed aggregate package; *aggregate.Runtime[...]
// satisfies it with no adapter (LoadAsOfVersion has this exact signature).
type CaseSummaryCaseLoader interface {
    LoadAsOfVersion(ctx context.Context, sid es.StreamID, v uint64) (*casev1.Case, error)
}
type CaseSummaryDeps struct { Case CaseSummaryCaseLoader; Marker projection.Marker }
func NewCaseSummaryRunner(js jetstream.JetStream, stream, workspace string, h CaseSummaryHandler, deps CaseSummaryDeps) *CaseSummaryRunner
func (r *CaseSummaryRunner) Run(ctx context.Context) error            // live, durable consumer
func (r *CaseSummaryRunner) Rebuild(ctx context.Context, src projection.Source, batch int) (uint64, error)
```

- **Event decoding.** The runner decodes `env.Payload` with the consumed
  package's generated `EventCodec` and dispatches on the concrete type.
  Payloads are sealed in `contracts`' deployment, so the runner takes an
  optional `es.Codec[E]`-shaped **unsealer** per package (`Deps.CaseCodec`);
  the default is the plain generated codec. With `FETCH_MODE_METADATA_ONLY`
  the `ev` argument is omitted entirely — `OnCaseOpened(ctx, env)` — and no
  codec is required, matching §13.5.
- **State resolution.** `consumes` entries are full type names. The plugin
  resolves each to its message descriptor (`gen.FilesByPath` / the request's
  descriptor set), takes its proto package, and finds the one message in that
  package carrying `(es.v1.aggregate)`: that message is the state type and its
  `stream_type` the subject segment. Errors at generation time: a consumed
  type that is not a variant of that package's `(es.v1.sum_type) = "Event"`
  container, or a package with zero or two `(es.v1.aggregate)` messages.
- **State injection.** With `FETCH_MODE_STATE_AS_OF_EVENT_VERSION` the runner
  calls `deps.Case.LoadAsOfVersion(ctx, env.StreamID, env.Version)` before the
  handler. In production `contracts` wires the gate client; in tests the
  embedded `aggregate.Runtime` is the loader, so a projector is unit-testable
  against a folded SQLite stream.
- **Subjects.** `natsjs` gains `SubjectFor(workspace, streamType, typeURL string) string`
  (the body of `DefaultSubject`, refactored) and `ConsumerConfig.FilterSubjects
  []string` (nats.go `jetstream.ConsumerConfig.FilterSubjects`, server 2.10+).
  The runner derives one filter subject per `consumes` entry:
  `evt.<workspace>.<stream_type>.<lowercased event>`; `workspace` may be `*`.
- **Checkpoint and idempotency.** Durable name = `spec.name` (sanitized with
  `subjectToken`); `deps.Marker` (ADR 0005) is loaded once at start and events
  with `GlobalPosition <= marker` are acked without handling; after a
  successful handler the marker is saved, then the message acked. Nak on
  handler error. With ADR 0029 headers present, the runner also keeps the last
  `Hash` per stream and nak+logs on a `PrevHash` mismatch (gap detection).
- **Rebuild.** When `rebuild = FROM_LOG`, `Rebuild` wraps the same dispatch
  in a `projection.Apply` and calls `projection.Replay` — one code path for
  live and rebuild, as ADR 0005 requires. `Rebuild` is not generated for
  `REACTION`-kind specs (replaying a reaction re-issues commands; by rule 20
  that is safe but never wanted by default).

### 3. Reactions

For `(es.v1.reaction)` named `casework.carry_in`, consuming `FactCommitted`
and issuing `unit_review.CarryInFact`:

```go
type CarryInHandler interface {
    OnFactCommitted(ctx context.Context, env es.Envelope, ev *urv1.FactCommitted, state *urv1.UnitReview, issue *CarryInIssuer) error
}
// Typed issuer: one method per `issues` entry; collects (stream, command).
func (i *CarryInIssuer) CarryInFact(sid es.StreamID, cmd *urv1.CarryInFact)
type CarryInDeps struct {
    UnitReview       CarryInUnitReviewLoader                     // state loader
    UnitReviewRouter reaction.Dispatcher[urv1.Command]           // one per issuing package
    Marker           projection.Marker
    Actor            es.Actor                                    // the system principal
}
```

A new small package `reaction` holds the executor loop written once:
`Dispatcher[C]` is `Dispatch(ctx, sid, cmd C, meta es.Meta) error`, satisfied by
a one-line adapter over `aggregate.Runtime.Handle`. For each issued command the
runner calls `Dispatch` with `Meta{CausationID: env.EventID, CorrelationID:
env.CorrelationID, Actor: deps.Actor}` and the contract of convention 20:
`es.ErrConflict` is retried with bounded backoff (ADR 0032's
`WithConflictRetry` is the mechanism; the runner sets a default of 5 attempts
when the runtime has none), `es.ErrTerminal` is treated as done, any other
error naks the event. The marker is saved only after every issued command
returned; a crash in between re-delivers, and rule 20 makes the re-dispatch a
decider no-op.

### 4. Lint

The plugin fails generation (not just lint) when a `consumes`/`issues` entry
does not resolve to a variant of an `Event`/`Command` container, when a
`READ_MODEL`/`INDEX` spec lacks `rebuild = FROM_LOG`, or when two specs share
a `name`. A separate `buf lint` plugin (§13.5) is out of scope here; the
generator's checks give `contracts` the same guarantees at `task generate`.

## Consequences

### Positive

- The hand-written part of a projector or reaction is the apply logic only;
  checkpointing, filtering, decoding, state fetching, rebuild and the
  conflict/terminal loop are generated and identical across `contracts`'
  twenty-odd reactions (§9.7).
- The §9.7 reaction table and §9.10 projection catalogue can be rendered from
  descriptors by the same plugin (`--es-lite_opt=docs=path`), as rule 30
  demands.
- The embedded runtime satisfies the loader seam with no glue, so projector
  tests run against SQLite with no NATS.

### Negative

- `es/v1/options.proto` stops being "two options"; the doc comment in it and
  README's "deliberately minimal" claim must change.
- `contracts` migrates its option names and numbers once (§1). Done before the
  first production tag, `buf breaking` never sees it.
- The plugin now resolves cross-file, cross-package descriptors, so it must
  run with every consumed aggregate's file in the same `buf generate` input
  (true in `contracts`, where all of `ont/` is one module).
- `natsjs` depends on NATS 2.10 multi-filter consumers.

## Alternatives Considered

### Keep the options in `contracts` and have the plugin import its Go package

Rejected: es-lite would depend on `contracts`, the opposite of the dependency
the two repos agreed on, and every option change downstream would force an
es-lite release.

### Resolve `ont.projection.*` dynamically in the plugin (no compile-time dep)

Workable and considered above; rejected as primary because it makes the
runtime contract's shape live in a repo that cannot test the runtime.

### Runtime reflection instead of codegen (a generic runner keyed by type URL)

Possible for `METADATA_ONLY`, but the typed handler signature with the
resolved state type is the deliverable `contracts` asked for; generics cannot
express "one method per consumed type".

## Implementation notes

- **Feed abstraction.** The runner constructor does not take a
  `jetstream.JetStream`. `New<Name>Runner(h, deps)` returns a
  `*projection.Runner` (reactions: `*reaction.Runner`) whose `Run(ctx, feed)`
  consumes any `projection.Feed`: `natsjs.NewFeed(js, ConsumerConfig)` over a
  JetStream durable with `FilterSubjects: <Name>Subjects(ws)`, or
  `projection.LogFeed` over the log itself. The in-memory path is what
  `examples/projection` tests on SQLite with no NATS; the JetStream path runs
  under `NATS_URL`. Subjects are emitted as string constants (no `natsjs`
  import in generated code).
- **Runner logic lives in `projection.Runner`** (marker load once, skip at or
  below, dispatch, save, ack; `Apply` for `Replay`; `Rebuild` = `Replay` from 0)
  and `reaction` (`Dispatcher`, `RuntimeDispatcher`, `Issue`, `Execute`,
  `MetaFor`, `Runner` without Rebuild). Generated code is the typed glue
  only. `Deps.Marker == nil` selects an in-memory marker.
- **Gap detection** is a feed concern (`natsjs.ConsumerConfig.Gaps`), not a
  runner concern: a `LogFeed` cannot reorder.
- **Conflict retry in reactions.** `reaction.RuntimeDispatcher` applies
  `DefaultConflictRetries` (5) with `aggregate.DefaultBackoff` only when the
  runtime reports `ConflictRetries() == 0`; a runtime with its own budget is
  used as configured. `Execute` itself does not retry — a conflict that
  escapes the dispatcher naks the event and the feed redelivers it.
- **`strategy: all`.** buf runs local plugins once per directory by default,
  which hides a consumed package's file from the request; `buf.gen.yaml`
  sets `strategy: all` for `protoc-gen-es-lite` and the resolver's error
  says so. `contracts` must do the same (migration doc §2).
- **`kind: PROJECTION_KIND_REACTION` on `(es.v1.projection)` is rejected**;
  a reaction is declared with `(es.v1.reaction)`, so "no Rebuild for
  reactions" falls out of the type (`*reaction.Runner`) rather than a flag.
- **`FETCH_MODE_UNSPECIFIED` is a generation error**, as are a missing
  `name`/`kind`, duplicate names across the run, duplicate consumed event or
  issued command Go names within one spec, and empty `consumes`.
- **Field-number change vs. the `contracts` original:** `rebuild` is 5 and
  `max_lag` 6 in `es.v1.ProjectionSpec` (`ceiling` is gone). Options are
  written by name, so the migration is mechanical
  (`docs/migration/contracts-projection-options.md`).
