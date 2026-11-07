# es-lite

[![CI](https://github.com/laenenai/es-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/laenenai/es-lite/actions/workflows/ci.yml)
![Go 1.27](https://img.shields.io/badge/go-1.27-00ADD8?logo=go&logoColor=white)

**A small, embeddable event-sourcing library in Go.** Define aggregates in
protobuf, write a `Decider`, and append the events it produces to an
append-only log. The same `es.Store` contract runs over two backends.

The append-only event log is the source of truth. **History, auditability, and
time-travel** are non-negotiable requirements — which is why the storage model
is event sourcing, not "state + outbox": only an immutable log answers *"what
did aggregate X look like on 3 March, and why?"* from the database alone.

Design decisions live in [`docs/adr`](./docs/adr).

---

## Contents

- [What it is](#what-it-is)
- [Packages](#packages)
- [Architecture](#architecture)
- [Backends](#backends)
- [Delivery guarantees](#delivery-guarantees)
- [Embedding es-lite](#embedding-es-lite)
- [Security model](#security-model)
- [Projections and reactions](#projections-and-reactions)
- [Command services over Connect](#command-services-over-connect)
- [Development](#development)
- [Versioning & releases](#versioning--releases)
- [Documentation](#documentation)

---

## What it is

es-lite is a library, not a service. It owns the durable, append-only log and
the write-side runtime around it; the application owns the process, the
transport, authentication, encryption and business rules.

**es-lite provides:**

| Responsibility | Mechanism |
|---|---|
| Durable, append-only event log | Postgres or SQLite; payload rows are never updated (Postgres adds `published_at` as the only mutable delivery marker; SQLite tails in-process) |
| Optimistic concurrency | `UNIQUE (workspace, stream, version)` → `ErrConflict` |
| Per-workspace data isolation (Postgres) | RLS + hash partitioning; the caller supplies the workspace |
| Cross-stream uniqueness | transactional `unique_claims` → `ErrConstraintViolated` |
| At-least-once delivery, fenced | `feed` core + Postgres adapters; consumer adapter in `eventfeed`; legacy in-process `delivery.Poller`/`Relay`; Postgres feed reads are commit-safe via the horizon fence (ADR 0037) |
| Opaque payloads | stores only the bytes the caller supplies; never encrypts or decrypts |
| Tamper evidence | per-stream SHA-256 hash chain over the sealed bytes, computed in the append transaction; keyless verification (`es-verify`) and exportable anchors (ADR 0029) |
| Schema lifecycle | versioned migrations (`es-migrate`) |
| Aggregate runtime | proto-defined events, the Decider model, generated codecs and sum-types |

**es-lite does not provide:**

| Not its job | Whose job |
|---|---|
| A running eventstore service | the platform's `serve`, or the application embedding the library |
| Authenticating or authorizing callers | the application's transport and identity layer |
| Holding encryption keys / decrypting payloads | **clients** (client-side envelope encryption, KMS/keystore of your choice) |
| PII hashing, erasure or crypto-shredding | **clients**; es-lite has no keystore and no `ForgetWorkspace` |
| Business rules | the **Decider**, which runs in the application |

## Packages

| Package | Role |
|---|---|
| `aggregate` | write-side runtime: load, fold, decide, append; time-travel reads |
| `es` | storage-neutral `Store`, `Envelope`, `StreamID`, decider/codec interfaces, hash-chain verification |
| `postgres` | multi-workspace Postgres backend (RLS, partitioning, claim relay) |
| `sqlite` | single-workspace, pure-Go SQLite backend |
| `projection` | projection runner, rebuild/replay, `LogFeed`, marker |
| `feed` | transport-neutral feed core: checkpoint/lease/log ports and the subscribe loop |
| `eventfeed` | the consumer adapter: binds `feed.Server` to `projection.Feed` — ack strictly after a successful handler, acks coalesced at page boundaries |
| `feedtest` | the shared conformance suite for `Log`/`CheckpointStore`/`LeaseStore` adapters |
| `delivery` | in-process poller and Postgres drain relay |
| `migrate` | versioned schema migration loader |
| `upcast` | event schema upgrade hooks |
| `snapshot` | the `snapshot.Cache` interface; implementations are supplied by the caller |
| `reaction` | reaction execution loop |
| `cmdsvc` | Connect command-service support (nested module) |
| `gen` | generated framework options and example aggregates |

## Architecture

```
 application (Decider + client-side crypto)
        │
        │ Handle(command)
        ▼
    es.Store ───────────────▶ Postgres  (multi-workspace, RLS)
        │                     SQLite    (single-workspace, pure Go)
        ▼
 append-only event log  (source of truth)
        │
        ▼
 feed / projection.LogFeed
        │
        ▼
 projection runner ──────▶ read model
```

- **`es.Store` is the whole storage contract.** The aggregate runtime, decider
  and codec do not change when the backend changes.
- **The log is the outbox.** Delivery tails `global_position`; there is no
  separate outbox table and no broker. On Postgres the feed's tail is
  fence-bounded (ADR 0037) so tailing cannot pass an in-flight lower
  position; the exact guarantee boundary is the
  [Delivery guarantees](#delivery-guarantees) section.
- **Delivery is at-least-once.** Handlers must be idempotent; a last-applied
  position marker makes both delivery and rebuild correct.

## Backends

- **Postgres** (multi-workspace, production): shared tables + `workspace_id` +
  **RLS** + hash partitioning. `Store.Workspace(id)` scopes reads and writes to
  one workspace; the feed reads are commit-safe (the horizon fence, ADR 0037)
  and the `Drain` claim-relay has its own gap-safe cursor. Requires
  **PostgreSQL 14+** — the append xid draw and the horizon fence both use
  `pg_current_xact_id()`, and `Open` probes for it once, failing fast with a
  clear message on older servers. The per-principal table grants are in the
  deployment runbook's
  [Grants section](./docs/deploy/README.md#grants-the-privilege-contract).
- **SQLite** (single-workspace, dev/edge): single-writer, gap-free cursor,
  zero infrastructure — useful for tests and single-tenant nodes.

Both implement the same `es.Store`. Open the backend and pass the store to the
aggregate runtime:

```go
store, err := sqlite.Open(ctx, "file:events.db") // auto-migrates
// or:
// store, err := postgres.Open(ctx, dsn)         // multi-workspace
if err != nil {
    return err
}
defer store.Close()
```

Migrations are versioned (ADR 0010). For a multi-replica Postgres deployment,
run `es-migrate` once and open the store with `postgres.WithoutAutoMigrate()`
so application instances never touch DDL on boot. SQLite and tests use the
default auto-migrate behaviour. Before upgrading a large database to a
version that carries the feed indexes, follow the runbook's
[index build order](./docs/deploy/README.md#index-build-order-migration-0003)
— migration transactions cannot build indexes concurrently.

## Delivery guarantees

The `feed` package is the production delivery path (the design is ADR 0037;
the operational companion is the [deployment runbook](./docs/deploy/README.md)).
What it guarantees:

- **Commit-safe reads.** On Postgres, every feed-facing read (`ReadAllBounded`,
  `Head`, `MatchingHead`) is bounded by the horizon fence: a page never
  advances past a position whose append is still in flight, no matter how
  append transactions interleave — including caller-owned appends
  (ADR 0035). On SQLite the single-writer connection makes the cursor
  gap-free by construction.
- **At-least-once, ordered.** Every event reaches the consumer at least once;
  pages are ordered by `global_position`, and within a stream by version.
  Acks are **fenced**: only the lease holder's receipts move the cursor.
- **Fail-closed on definition drift.** The checkpoint records the
  fingerprint of the projection's consumed type set; resubscribing under a
  changed definition fails with `ErrDefinitionChanged` instead of silently
  skipping the new type's history.
- **Poison quarantined, audited, never silently skipped.** The store's
  payload cap rejects oversized appends up front; an over-cap row that
  enters anyway kills the stream with `*EventTooLargeError` (never
  materialized), and `feed.Server.Skip` is the audited recovery — it
  advances past the position and records a dead-letter row.
- **Stalls are visible, never lossy.** A fence that waits (an in-flight or
  wedged transaction) surfaces via `Observer.OnStall` with the settled and
  raw heads; the failure mode of every interleaving is *delay*, not loss.
- **Workspace isolation.** The Postgres feed tables are RLS-enabled with no
  bypass arm; no non-superuser, non-`BYPASSRLS` session can reach another
  workspace's subscription state.

What it does **not** guarantee:

- **Exactly-once.** Crash windows and lease failover redeliver bounded
  ranges; handlers must be idempotent, and a last-applied marker is the
  standard dedup.
- **Cross-stream causality.** `global_position` is a stable total order, but
  under concurrent appends it is *assignment* order, not real time: never
  infer "A happened before B" from delivery order across two streams.
  Within one stream, version order is the append contract (ADR 0029's
  chain pins it).
- **Push.** No LISTEN/NOTIFY ships — the feed polls (~250 ms by default,
  jittered ±20%). The `Waiter` port is the future hook; ADR 0037 records
  the deferred design question.
- **That every read API is fenced.** On Postgres, commit safety lives on
  the feed-facing reads. The raw `Store.ReadAll` cursor — the in-process
  `projection.LogFeed`/`Replay` tail and the legacy `delivery.Poller` — is
  *not* fenced: under concurrent appenders it can skip, exactly the hazard
  ADR 0001 §5 warned about. Use the feed (or `Drain`, which carries its
  own claim-relay safety) whenever more than one writer appends to a
  Postgres database.

**Settled-head semantics (Postgres).** `Head` and `MatchingHead` report the
*settled* head — never a position whose append is still in flight — so
while a fence is blocked they transiently read 0 and `Status.Lag` can be
transiently negative; `RawHead` is the unfiltered committed maximum, used
for stall detection and the fence-aware resume clamp. A wedged long-lived
transaction anywhere on the primary delays every feed page until it ends:
observable, diagnosable, never a loss.

## Embedding es-lite

Aggregates are defined in `.proto`; `task generate` emits sealed sum-types and
codecs. You hand-write only the `Decider` and its error sentinels.

```go
sid, err := es.NewStreamID("counter", "main")
if err != nil {
    return err
}
rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})
res, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100}), es.Meta{})
if err != nil {
    return err
}
_ = res
```

**Time-travel.** The log is enough to reconstruct state at any point:

```go
past, err := rt.LoadAsOfVersion(ctx, sid, 1)
past, err = rt.LoadAsOfTime(ctx, sid, someTimestamp)

// Bounded-staleness read when a snapshot cache is configured:
state, version, fresh, err := rt.LoadStale(ctx, sid, 5*time.Minute)
```

**Snapshots.** `snapshot.Cache` is a pure, re-derivable optimization: the
runtime seeds folding from a cached state at version N and folds only the tail.
Current loads and `LoadAsOfVersion` calls at or above the cached version seed
from it; `LoadAsOfTime` never seeds, because a snapshot newer than the target
would swallow the requested past. The library ships the interface, not a
storage implementation; implement it over your own database and opt in with:

```go
rt.WithSnapshots(cache, stateCodec, foldVersion)
```

The cache stores opaque state bytes. Seal them client-side if they contain
sensitive data, and purge them when the corresponding key is destroyed.

**Caller-owned transactions (ADR 0035).** When an append must share a
transaction with another write — for example an audit record in a
`tenancy.TenantTx` — use `WorkspaceStore` and the `*Tx` methods. es-lite sets
`app.workspace_id` on the caller's transaction and never commits or rolls it
back; the caller still owns the boundary:

```go
ws := pgStore.WorkspaceStore("ws_123")
_, err := ws.AppendTx(ctx, tx, es.AppendParams{ /* ... */ })
```

`tx` only needs `Exec`, `Query` and `QueryRow` (`postgres.DBTX`), so a
tenant-scoped handle works without exposing a pool. `Append`/`ReadStream` keep
their existing signatures and still open their own transaction.

### Proto options

The framework options your `.proto` files import (`es/v1/options.proto`:
`sum_type`, `schema_version`, `decided_at`, …) are distributed as the Buf
module `buf.build/laenenai/es-lite` (ADR 0033). While BSR publishing is blocked
on the `BUF_TOKEN` secret, each release also ships
`es-lite-proto-<tag>.tar.gz`. Extract it and add the directory as a local
module:

```yaml
# buf.yaml (consumer)
version: v2
modules:
  - path: proto
  - path: third_party/es-lite-proto
```

Or produce the export yourself from a checkout:

```bash
VERSION=dev task buf:export
# dist/es-lite-proto-dev.tar.gz
```

Keep `import "es/v1/options.proto";` unchanged, and run `protoc-gen-es-lite`
from this Go module (`go install
github.com/laenenai/es-lite/cmd/protoc-gen-es-lite@<tag>`). The Go types for
the options stay in `github.com/laenenai/es-lite/gen/es/v1` — do not generate
them from the exported module (in buf v2 dependencies are not generated unless
`include_imports` is set; with `managed` mode add a `disable` override for
`buf.build/laenenai/es-lite`). Once the BSR secret is configured, switch to
`deps: [buf.build/laenenai/es-lite]` and `buf dep update`.

## Security model

es-lite is **keyless**: it persists the opaque bytes callers send. It does not
encrypt, decrypt, hold keys, hash PII, or erase data by destroying keys. Those
are client responsibilities (ADR 0025).

- **Client-side encryption.** Seal payloads before `Append` and open them after
  read. The store sees only the bytes you give it. "Zero-knowledge" holds only
  if you actually encrypt client-side.
- **Erasure is client-owned.** There is no server-side crypto-shredding and no
  `ForgetWorkspace`. If you need key-destruction erasure, keep keys outside
  es-lite and account for snapshots, unique-claim values, read models and
  database backups when you destroy them.
- **Uniqueness values are opaque.** `Claim`/`Release` store the exact value you
  provide. For personal data, pass a keyed hash/MAC you control, never the raw
  value; `LookupClaim` must receive the same value. There is no PII flag.
- **Per-workspace isolation (Postgres).** RLS scopes rows to the workspace the
  caller supplies. That is isolation, not authentication: the application must
  authenticate callers and derive the workspace from trusted identity, never
  from an unverified client field.
- **Append-only by API contract, tamper-evident by hash chain.** The library
  never updates event payloads; on Postgres, `published_at` is the delivery
  path's one mutable column (SQLite tails in-process and has none). Since ADR 0029 every row carries `prev_hash`/`hash`: a
  per-stream SHA-256 chain over the *sealed* bytes, computed inside the append
  transaction, so an auditor with no keys can detect an altered, reordered or
  missing row (`es.VerifyStream`, `es-verify stream|all`). A chain alone cannot
  detect a truncated tail or an operator who rewrites every hash after an
  edit; periodic **anchors** (`es-verify anchor`) fix every stream head under
  one root that you export somewhere the database operator cannot rewrite (a
  signed commit, a transparency log). es-lite does not sign anchors — signing
  is the operator's layer. The chain starts at the first append after
  migration 0002; rows written before it are never back-filled and stay
  unattested (`ChainedFrom` in the report).
- **Metadata is visible.** Stream IDs, actors, type URLs and claim scopes are
  not encrypted by the store. Use TLS and authenticate at the application's API
  boundary; the library does not wire transport security for you.

## Projections and reactions

Read models, indexes and executor reactions can be declared as message options;
`task generate` emits the typed handler interface, state-loader seam, deps and
runner. You write only the apply logic:

```proto
message CounterView {
  option (es.v1.projection) = {
    name: "counter.view"  kind: PROJECTION_KIND_READ_MODEL
    consumes: ["counter.v1.Initialized", "counter.v1.Incremented"]
    fetch: FETCH_MODE_STATE_AS_OF_EVENT_VERSION  rebuild: REBUILD_MODE_FROM_LOG
  };
  ...
}
```

```go
runner := counterviewv1.NewCounterViewRunner(myHandler, counterviewv1.CounterViewDeps{
	Counter: rt,              // *aggregate.Runtime is the state loader (as-of-version)
	Marker:  markerStore,     // ADR 0005 last-applied position
})

// Live, in-process: tail the log directly (tests, single-node SQLite).
err := runner.Run(ctx, &projection.LogFeed{Source: store, From: 0})

// Live, through the feed core: feed.Server streams to a caller-supplied Sink,
// resuming from a durable checkpoint under a lease — at-least-once with
// fenced acks. This module ships the core, the Postgres log/checkpoint/lease
// adapters (feedtest.RunAll is their conformance suite), and the eventfeed
// consumer adapter that binds feed.Server to projection.Feed (ack strictly
// after the handler succeeds). The platform maps a transport to Sink and
// resolves the authorized definition per caller.

// Rebuild from position 0 through the same dispatch path:
pos, err := runner.Rebuild(ctx, store, 500)
```

**State-fetch cost.** `fetch: FETCH_MODE_STATE_AS_OF_EVENT_VERSION` asks the
loader for the state as of each consumed event's version. Without snapshots
every load re-reads and re-folds the whole stream — O(stream length) per
event; a runtime wired with `WithSnapshots` seeds a load covered by a
snapshot and folds only the tail — O(tail). A snapshot newer than the
requested version never seeds, so time travel stays exact.

`projection.LogFeed` polls `Store.ReadAll` and retries a failed handler on the
same envelope, so delivery is at-least-once and ordered per stream. On
Postgres, `LogFeed` is the single-writer/quiescent-log path: its raw cursor is
not commit-safe under concurrent appenders (see
[Delivery guarantees](#delivery-guarantees)); `eventfeed` is the fenced
consumer adapter for that backend. Rebuild reads the database log itself —
not an external retention window — and shares the live apply path (ADR 0005).

Reactions (`(es.v1.reaction)`) get a typed issuer and the executor loop
(`reaction.Execute`: conflicts retried, a terminal target counts as done).
The plugin needs `strategy: all` in `buf.gen.yaml` so `consumes` resolves
across packages; `--es-lite_opt=docs=<path>` renders a catalogue of every
projection and reaction from the descriptors.

## Command services over Connect

Write the service `.proto` by hand, mark it
`(es.v1.command_service) = { aggregate: "counter.v1.Counter" }`, tag the
request's stream-id field `(es.v1.stream_id)` and, on the commands,
`(es.v1.acting_principal)` / `(es.v1.expected_version)` / `(es.v1.decided_at)`
where they apply; `protoc-gen-connect-go` emits the interface and
`protoc-gen-es-lite` emits the handler into a sibling `<pkg>es` package:

```go
handler := countersvcv1es.NewCounterCommandServiceHandler(countersvcv1es.CounterCommandServiceDeps{
	Runtime:   rt,
	Authorize: myAuthorizer,   // one typed Authorize<RPC> per RPC — a missing rule is a compile error
	Identity:  myIdentity,     // Principal(ctx): assigned to the command, never taken from the client
	Errors:    myErrorMapper,  // optional: domain sentinels -> Connect codes
})
mux.Handle(countersvcv1connect.NewCounterCommandServiceHandler(handler))
```

Each RPC runs validate (protovalidate) → stream id → principal injection →
authorise → `Runtime.HandleAt` → `WriteResult{stream_id, version,
global_position}` + state; `es.ErrConflict` becomes `failed_precondition`
with an `es.v1.VersionConflict` detail carrying the current version and
state. The runtime for this lives in the nested module
`github.com/laenenai/es-lite/cmdsvc`, so the core module never imports
connect-go.

See [`examples/`](./examples) — `counter`, `projection` (generated projector
and reaction on SQLite), `upcast` — and [`cmdsvc/example`](./cmdsvc/example)
(generated Connect command service, tested in-process with connect-go).

## Development

Toolchain pinned in [`.mise.toml`](./.mise.toml) (Go 1.27.3, buf, task,
golangci-lint, govulncheck); run `mise install`. The module has no private
dependencies. `cmdsvc/` is a nested Go module (connect-go + protovalidate
stay out of the core graph); every `task` target covers both.

```sh
task                 # list tasks
task generate        # buf lint + codegen (protoc-gen-go + protoc-gen-es-lite)
task test            # unit tests (Postgres integration skips without PG_DSN)
task test:race       # unit tests under the race detector
task test:pg         # Postgres integration suite under -race -shuffle=on
task lint:go         # golangci-lint (errcheck, govet, staticcheck)
task coverage        # unit suite with a coverage floor ratchet
task test:feedgate   # the heavy feed gate: chaos no-loss, plan, fleet (nightly)
task vuln            # govulncheck
task build vet
```

Generated code is checked into `gen/`; `task generate:check` gates drift in CI.

## Versioning & releases

Semver tags are module releases. CI attaches the `es/v1` proto export
(`es-lite-proto-<tag>.tar.gz`) to the GitHub release while BSR publishing is
blocked on `BUF_TOKEN` (ADR 0033). es-lite ships no service image.

```sh
git tag -a v0.2.0 -m "…" && git push origin v0.2.0
```

## Documentation

- **[ADRs](./docs/adr)** — decision records (event-sourcing, workspaces,
  client-owned crypto, migrations, hash chain, codegen, caller-owned
  transactions, no-broker boundary, commit-safe feed reads). Start at
  [the index](./docs/adr/README.md).
- **[Migration notes](./docs/migration)** — what downstream repositories
  change when an ADR moves a contract (currently: projection options).
- **[Backlog](./docs/backlog)** — tracked follow-ups.
- Platform context: [architecture repo](https://github.com/laenenai/architecture).

## Status

Pre-1.0: the API may still shift. Unit tests need no external service; the
Postgres integration suite runs against a real Postgres database. See
[Status in each ADR](./docs/adr/README.md) for what is implemented vs.
designed.

## Licence

MIT — see [LICENSE](LICENSE).
