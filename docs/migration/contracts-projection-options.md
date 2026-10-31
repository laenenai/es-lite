# contracts migration: projection and reaction options move to es-lite

- **Date:** 2026-10-30
- **es-lite ADR:** 0030 (§1), with 0033 for the dependency
- **Must land:** before `contracts`' first production tag (`buf breaking`
  then freezes the numbers)

es-lite now owns the descriptors the projector runtime acts on:
`(es.v1.aggregate)` (50003), `(es.v1.projection)` (50010) and
`(es.v1.reaction)` (50011) in `es/v1/options.proto`. `contracts` keeps only
the clearance ceiling, which es-lite has no business knowing. These are the
exact edits.

## 1. Depend on the new options

Either bump the vendored copy — `third_party/es/v1/options.proto` from
es-lite commit `feat/contracts-integration` (the file now declares
`Aggregate`, `FetchMode`, `RebuildMode`, `ProjectionKind`, `ProjectionSpec`,
`ReactionSpec`, the three message extensions and the field option
`decided_at`, and imports `google/protobuf/duration.proto`) — or, once the
BSR module is pushed (ADR 0033), replace the vendored module:

```yaml
# buf.yaml
deps:
  - buf.build/bufbuild/protovalidate
  - buf.build/googleapis/googleapis
  - buf.build/laenenai/es-lite            # replaces the third_party module
```

then `buf dep update`, delete `third_party/` and its `modules:` entry. The
`import "es/v1/options.proto";` lines do not change.

## 2. `buf.gen.yaml`: run protoc-gen-es-lite once over all files

The generator resolves `consumes`/`issues` across packages, so it must see
every file in one invocation. buf's default for local plugins is one run per
directory. Add `strategy: all` to the es-lite plugin entry:

```yaml
plugins:
  - local: protoc-gen-es-lite
    out: gen
    opt: paths=source_relative
    strategy: all
```

Optionally add `opt: [paths=source_relative, docs=ont/PROJECTIONS.md]` to
have the §9.7/§9.10 catalogue rendered from the descriptors (convention 30).

## 3. Every aggregate State message: add `(es.v1.aggregate)`

One message per domain package, the State message, names its stream type
(design §3.4). The generator fails if a consumed package has zero or two.

```proto
// ont/domain/party/space/v1alpha1/space.proto
message Space {
  option (es.v1.aggregate) = { stream_type: "space" };
  option (ont.domain.clearance.v1alpha1.default_clearance) = { ... };   // unchanged
  ...
}
```

```proto
// ont/domain/casework/case/v1alpha1/case.proto
message Case {
  option (es.v1.aggregate) = { stream_type: "case" };
  ...
}
```

Do the same for every other package with an `Events`/`Commands` container
(unit review, derogation, …) using the stream type the design assigns.

## 4. Rewrite `ont/projection/v1alpha1/options.proto`

Reduce it to the clearance companion. Retire 60011; reserve both old
numbers so they are never reused.

```proto
syntax = "proto3";

package ont.projection.v1alpha1;

import "google/protobuf/descriptor.proto";
import "ont/domain/clearance/v1alpha1/clearance.proto";

option go_package = "github.com/laenenai/contracts/gen/ont/projection/v1alpha1;projectionv1alpha1";

// ProjectionPolicy is the clearance companion to (es.v1.projection): the
// runtime contract (consumes, fetch, rebuild, checkpoint key) lives in
// es-lite's ProjectionSpec; the ceiling is a clearance concern and stays
// here (design §9.10, convention 30).
message ProjectionPolicy {
  // The read model never holds more than its intended reader may see.
  ont.domain.clearance.v1alpha1.Clearance ceiling = 1;
}

extend google.protobuf.MessageOptions {
  // On a read-model or index message, next to (es.v1.projection).
  ProjectionPolicy policy = 60010;
  // 60011 was `reaction` (ReactionSpec); moved to (es.v1.reaction) = 50011.
}
```

Delete `ProjectionKind`, `FetchMode`, `RebuildMode`, `ProjectionSpec`,
`ReactionSpec` and the `spec`/`reaction` extensions from this file; they
now come from `es/v1/options.proto`. Note `google/protobuf/duration.proto`
is no longer imported here.

## 5. Every read model, index and reaction: rewrite the option

Field for field, `ont.projection.v1alpha1.spec` → `es.v1.projection` minus
`ceiling`; enum values keep their names. Numbers differ (`rebuild` is 5 and
`max_lag` 6 in es-lite, 6 and 7 before) but text-format options are by
name, so the edit is mechanical.

Before:

```proto
message CaseSummary {
  option (ont.projection.v1alpha1.spec) = {
    name: "casework.case_summary"
    kind: PROJECTION_KIND_READ_MODEL
    consumes: ["ont.domain.casework.case.v1alpha1.CaseOpened",
               "ont.domain.casework.case.v1alpha1.CaseAccepted"]
    fetch: FETCH_MODE_STATE_AS_OF_EVENT_VERSION
    ceiling: { level: CLEARANCE_LEVEL_INTERNAL }
    rebuild: REBUILD_MODE_FROM_LOG
    max_lag: { seconds: 30 }
    owner_context: "casework"
  };
  ...
}
```

After:

```proto
message CaseSummary {
  option (es.v1.projection) = {
    name: "casework.case_summary"
    kind: PROJECTION_KIND_READ_MODEL
    consumes: ["ont.domain.casework.case.v1alpha1.CaseOpened",
               "ont.domain.casework.case.v1alpha1.CaseAccepted"]
    fetch: FETCH_MODE_STATE_AS_OF_EVENT_VERSION
    rebuild: REBUILD_MODE_FROM_LOG
    max_lag: { seconds: 30 }
    owner_context: "casework"
  };
  option (ont.projection.v1alpha1.policy) = {
    ceiling: { level: CLEARANCE_LEVEL_INTERNAL }
  };
  ...
}
```

Reactions: `(ont.projection.v1alpha1.reaction)` → `(es.v1.reaction)`, same
fields (`name`, `consumes`, `issues`, `fetch`, `owner_context`). No policy
option (reactions have no ceiling).

Rules the generator enforces (generation fails, not just lint):

- `name` set and unique across the whole `buf generate` run.
- `fetch` set (`METADATA_ONLY` or `STATE_AS_OF_EVENT_VERSION`).
- `kind` set; `READ_MODEL`/`INDEX` require `rebuild = REBUILD_MODE_FROM_LOG`;
  `kind: PROJECTION_KIND_REACTION` on a projection is rejected — declare a
  reaction with `(es.v1.reaction)`.
- every `consumes` entry is a variant of its package's
  `(es.v1.sum_type) = "Event"` container; every `issues` entry a variant of
  a `"Command"` container; each such package has exactly one
  `(es.v1.aggregate)` message with a non-empty `stream_type`.
- consumed event Go names are unique within one spec (they become
  `On<Event>` methods); issued command Go names likewise (issuer methods).

## 6. What the generated Go looks like (for the gate/executor authors)

For `CaseSummary` above, `gen/.../case_summary_es.pb.go` holds
`CaseSummaryName`, `CaseSummarySubjects(workspace)`, `CaseSummaryHandler`
(`OnCaseOpened(ctx, env, ev *casev1.CaseOpened, state *casev1.Case) error`,
…), `CaseSummaryCaseLoader` (satisfied by the embedded `aggregate.Runtime`
and by a gate client), `CaseSummaryDeps{Case, CaseCodec, Marker}` and
`NewCaseSummaryRunner(h, deps) *projection.Runner` with `Run(ctx, feed)` and
`Rebuild(ctx, src, batch)`. The feed is `natsjs.NewFeed(js,
natsjs.ConsumerConfig{Stream, Durable: CaseSummaryName, FilterSubjects:
CaseSummarySubjects("*"), Gaps: natsjs.NewGapDetector()})` in production and
`&projection.LogFeed{Source: store}` in tests. A reaction additionally gets
`<Name>Issuer` with one method per `issues` entry, `<Aggregate>Router
reaction.Dispatcher[...]` and `Actor` in its Deps, and a `*reaction.Runner`
without Rebuild. Sealed payloads: pass the unsealing codec as `<Aggregate>Codec`.

## 7. Verify

`buf lint`, `buf generate` (the es-lite plugin from this branch), `task
generate:check`. `buf breaking` is expected to report the removed
`ont.projection.v1alpha1` messages — allowed under v1alpha1 rule 16 before
the first production tag.
