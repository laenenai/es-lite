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
| At-least-once, in-process delivery | `delivery.Poller` / `delivery.Relay` over the log's global position |
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
  separate outbox table and no broker.
- **Delivery is at-least-once.** Handlers must be idempotent; a last-applied
  position marker makes both delivery and rebuild correct.

## Backends

- **Postgres** (multi-workspace, production): shared tables + `workspace_id` +
  **RLS** + hash partitioning. `Store.Workspace(id)` scopes reads and writes to
  one workspace; the gap-safe `Drain` claim-relay supports delivery.
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
default auto-migrate behaviour.

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
The library ships the interface, not a storage implementation; implement it
over your own database and opt in with:

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
// resuming from a durable checkpoint under a lease. The platform supplies the
// checkpoint, lease and log ports; this module ships the Subscribe loop.

// Rebuild from position 0 through the same dispatch path:
pos, err := runner.Rebuild(ctx, store, 500)
```

`projection.LogFeed` polls `Store.ReadAll` and retries a failed handler on the
same envelope, so delivery is at-least-once and ordered per stream. Rebuild
reads the database log itself — not an external retention window — and shares
the live apply path (ADR 0005).

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
govulncheck); run `mise install`. The module has no private dependencies.
`cmdsvc/` is a nested Go module (connect-go + protovalidate stay out of the
core graph); every `task` target covers both.

```sh
task                 # list tasks
task generate        # buf lint + codegen (protoc-gen-go + protoc-gen-es-lite)
task test            # unit tests (Postgres integration skips without PG_DSN)
task test:race       # unit tests under the race detector
task test:pg         # Postgres integration suite against a throwaway container
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
  transactions, no-broker boundary). Start at [the index](./docs/adr/README.md).
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
