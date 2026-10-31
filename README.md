# es-lite

[![CI](https://github.com/laenenai/es-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/laenenai/es-lite/actions/workflows/ci.yml)
![Go 1.27](https://img.shields.io/badge/go-1.27-00ADD8?logo=go&logoColor=white)

**A zero-knowledge, event-sourcing eventstore** — usable two ways:

- as a **Go library** you embed (proto-defined aggregates, the Decider model, a
  storage-neutral `Store`), and
- as a **NATS service** (`es-lited`) that many services share over NATS, holding
  no keys and storing only the opaque bytes clients send.

The append-only event log is the source of truth. **History, auditability, and
time-travel** are non-negotiable requirements — which is why the storage model
is event sourcing, not "state + outbox": only an immutable log answers *"what
did aggregate X look like on 3 March, and why?"* from the database alone.

Design decisions live in [`docs/adr`](./docs/adr); operations in
[`docs/deploy`](./docs/deploy).

---

## Contents

- [What it is / responsibilities](#what-it-is--responsibilities)
- [Security features](#security-features)
- [Architecture](#architecture)
- [Running the container](#running-the-container)
- [Configuration reference](#configuration-reference)
- [Deployment (Kubernetes)](#deployment-kubernetes)
- [Using es-lite as a library](#using-es-lite-as-a-library)
- [Backends](#backends)
- [Development](#development)
- [Versioning & releases](#versioning--releases)
- [Documentation](#documentation)

---

## What it is / responsibilities

`es-lited` is a **workspace-scoped, keyless eventstore service**. It owns
durability, per-stream versioning, workspace isolation (Postgres), and delivery
of an opaque event log. It delegates encryption, key management, PII handling,
authentication and business rules.

**es-lited IS responsible for:**

| Responsibility | Mechanism |
|---|---|
| Durable, append-only event log | Postgres/SQLite; payload rows are never updated (`published_at` is the only mutable delivery marker) |
| Optimistic concurrency | `UNIQUE (workspace, stream, version)` → `ErrConflict` |
| Per-workspace data isolation | Postgres RLS + hash partitioning; workspace from the subject |
| Cross-stream uniqueness | transactional `unique_claims` → `ErrConstraintViolated` |
| At-least-once delivery | relay/poller + JetStream dedup (see the delivery caveats in ADR 0003/0005) |
| Opaque payloads | stores only the bytes the caller supplies; never encrypts or decrypts |
| Tamper evidence | per-stream SHA-256 hash chain over the sealed bytes, computed in the append transaction; keyless verification (`es-verify`) and exportable anchors (ADR 0029) |
| Schema lifecycle | versioned migrations (`es-migrate`) |

**es-lited is NOT responsible for (by design):**

| Not its job | Whose job |
|---|---|
| Authenticating callers / verifying tokens | the NATS auth callout/ACLs in your deployment |
| Holding encryption keys / decrypting payloads | **clients** (client-side envelope encryption, KMS/keystore of your choice) |
| PII hashing, erasure or crypto-shredding | **clients**; es-lite has no keystore and no `ForgetWorkspace` |
| Authorization decisions | the PDP at the caller/interceptor |
| Business rules | the **Decider**, which runs client-side |

> The client encrypts, decides, hashes PII and owns erasure; the broker
> authenticates and scopes; es-lited durably stores and orders opaque bytes.
> Three concerns, three owners.

## Security model

es-lite is **keyless**: it persists and moves whatever opaque bytes clients send.
It does not encrypt, decrypt, hold keys, hash PII, or erase data by destroying
keys. Those are client responsibilities (ADR 0025).

- **Client-side encryption.** Seal payloads before `Append` and open them after
  read. The service, relay and JetStream see only the bytes you give them.
  "Zero-knowledge" holds only if you actually encrypt client-side.
- **Erasure is client-owned.** There is no server-side crypto-shredding and no
  `ForgetWorkspace`. If you need key-destruction erasure, keep keys outside
  es-lite and account for snapshots, unique-claim values, JetStream retention,
  read models and database backups when you destroy them.
- **Uniqueness values are opaque.** `Claim`/`Release` store the exact value you
  provide. For personal data, pass a keyed hash/MAC you control, never the raw
  value; `LookupClaim` must receive the same value. There is no PII flag.
- **Per-workspace isolation (Postgres).** RLS scopes rows and the workspace is
  read from the NATS subject. That is isolation, not authentication: configure
  per-workspace NATS ACLs/auth callout and never rely on the subject token alone.
- **Append-only by API contract, tamper-evident by hash chain.** The library
  never updates event payloads; `published_at` is the relay's one mutable
  column. Since ADR 0029 every row carries `prev_hash`/`hash`: a per-stream
  SHA-256 chain over the *sealed* bytes, computed inside the append
  transaction, so an auditor with no keys can detect an altered, reordered or
  missing row (`es.VerifyStream`, `es-verify stream|all`). A chain alone cannot
  detect a truncated tail or an operator who rewrites every hash after an
  edit; periodic **anchors** (`es-verify anchor`, or `ANCHOR_INTERVAL` on
  `es-lited`) fix every stream head under one root that you export somewhere
  the database operator cannot rewrite (a signed commit, a transparency log).
  es-lite does not sign anchors — signing is the operator's layer. The chain
  starts at the first append after migration 0002; rows written before it are
  never back-filled and stay unattested (`ChainedFrom` in the report).
- **Metadata is visible.** Stream IDs, actors, type URLs and claim scopes are not
  encrypted by the store. Use TLS and broker ACLs on the client↔service hop; the
  repo does not wire TLS for you.
- **Multi-region / data residency.** Region is in the subject from day 1; a
  region is a self-contained cell (Postgres + NATS + read models). There is no
  transparent cross-region failover for partitioned state.

## Architecture

```
        write path                                       read models
 client (Decider + crypto)                          projection consumers
   │ encrypt payload                                       ▲ decrypt
   │ Handle(cmd)                                           │ (JetStream durable)
   ▼                                                       │
 es.Store over NATS  ──req/reply──▶  es-lited ──▶ Postgres (RLS, partitioned,
   (svc.eslite.<region>.<ws>.*)      (no keys)      append-only, opaque payloads)
                                                          │
                                       es-relayd  ◀───────┘ drain
                                       (no keys)  ──▶ JetStream (evt.<ws>.…, opaque payloads)
```

- **`es.Store` is the whole contract** — the same aggregate runtime/decider/codec
  run embedded or over NATS by swapping the store.
- **The log is the outbox** — no separate table; the relay tails
  `global_position`.
- **Delivery is at-least-once** — handlers must be idempotent; a last-applied
  position marker makes both delivery and rebuild correct.

## Running the container

The image `ghcr.io/laenenai/es-lited` ships **four** static entrypoints:

| Entrypoint | Role | Scale |
|---|---|---|
| `/es-lited` (default) | the eventstore service (`es.Store` over NATS) | N replicas |
| `/es-migrate` | apply schema migrations once, then exit | init container / job |
| `/es-relayd` | drain the log → JetStream (delivery relay) | **singleton** |
| `/es-verify` | audit the hash chain; take / check anchors (ADR 0029) | job / cron |

The image is private — authenticate to GHCR first (`docker login ghcr.io`).

**Migrate, then serve (Postgres):**

```sh
# 1) run migrations once
docker run --rm \
  -e BACKEND=postgres -e PG_DSN='postgres://user:pass@db:5432/eslite?sslmode=verify-full' \
  ghcr.io/laenenai/es-lited:v0.2.0 /es-migrate

# 2) run the service (replicas assume the schema exists)
docker run -d --name es-lited -p 8080:8080 \
  -e BACKEND=postgres -e AUTO_MIGRATE=false \
  -e PG_DSN='postgres://user:pass@db:5432/eslite?sslmode=verify-full' \
  -e NATS_URL='nats://nats:4222' \
  -e NATS_SUBJECT_PREFIX='svc.eslite.eu' \
  ghcr.io/laenenai/es-lited:v0.2.0

# 3) run the delivery relay (singleton)
docker run -d --name es-relayd \
  -e PG_DSN='postgres://...' -e NATS_URL='nats://nats:4222' \
  ghcr.io/laenenai/es-lited:v0.2.0 /es-relayd
```

**Single-node / edge (SQLite, zero-config):** `AUTO_MIGRATE` defaults on, so the
service migrates itself.

```sh
docker run -d -p 8080:8080 \
  -e BACKEND=sqlite -e SQLITE_DSN='file:/data/eslite.db' \
  -e NATS_URL='nats://nats:4222' \
  -v es-data:/data \
  ghcr.io/laenenai/es-lited:v0.2.0
```

**Health.** Each binary serves `GET /healthz` on `HEALTH_ADDR` (default `:8080`)
— 200 when connected to NATS, 503 otherwise (kubelet liveness/readiness). On the
mesh, `es-lited` also registers as a **NATS Micro** service — `nats micro ls` /
`info eslite` / `stats eslite` give health, discovery, and per-endpoint stats.

## Configuration reference

**`es-lited`:**

| Env | Default | Purpose |
|---|---|---|
| `BACKEND` | `postgres` | `postgres` (multi-workspace) or `sqlite` (single-workspace/edge) |
| `PG_DSN` | — | Postgres DSN (required for `BACKEND=postgres`) |
| `SQLITE_DSN` | `file:eslite.db` | SQLite DSN (`BACKEND=sqlite`) |
| `NATS_URL` | `nats://127.0.0.1:4222` | NATS endpoint (the `ESLITE` account) |
| `NATS_SUBJECT_PREFIX` | `svc.eslite.local` | `<prefix>.<ws>.<method>`; region per cell |
| `AUTO_MIGRATE` | `true` | `false` in prod → replicas skip DDL (see `es-migrate`) |
| `HEALTH_ADDR` | `:8080` | kubelet `/healthz` address |
| `ANCHOR_INTERVAL` | — | e.g. `1h`: take a hash-chain anchor at that interval under leader election and log it as JSON (Postgres only) |

**`es-migrate`:** `BACKEND`, `PG_DSN`, `SQLITE_DSN` (as above).
**`es-verify`:** same env; `es-verify stream <ws> <type:id>`, `es-verify all
[--strict]`, `es-verify anchor [--upto N] | --check <anchor.json> | list`.
Exit status is the audit result.
**`es-relayd`:** `PG_DSN`, `NATS_URL`, `ES_STREAM` (default `ES_EVENTS`),
`ES_BATCH` (default 200), `HEALTH_ADDR`.

**Observability:** `es-lited` and `es-relayd` export **OTLP** metrics + traces
when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (→ OpenObserve), inert otherwise — see
[the deploy guide](./docs/deploy/README.md#observability-opentelemetry--openobserve).

Secrets are best injected by your platform's secret manager (for example an
OpenBao/Vault Agent sidecar with dynamic Postgres creds and NATS creds); the pod
then holds no static secrets. TLS is a deployment responsibility. See
[`docs/deploy`](./docs/deploy).

## Deployment (Kubernetes)

Manifests in [`deploy/k8s`](./deploy/k8s): a `Deployment` for `es-lited` (with an
`es-migrate` **init container** and `AUTO_MIGRATE=false`), a singleton
`es-relayd`, and a KEDA `ScaledObject` (autoscale the service on **p95 latency**,
projection consumers on **JetStream lag** — never CPU). Full guide, including the
secret injection, TLS, sharding, and multi-region cells:
**[`docs/deploy/README.md`](./docs/deploy/README.md)**.

## Using es-lite as a library

Aggregates are defined in `.proto`; `task generate` emits sealed sum-types +
codecs. You hand-write only the `Decider` and its error sentinels.

**Proto options from the BSR.** The framework options your `.proto` files
import (`es/v1/options.proto`: `sum_type`, `schema_version`, `decided_at`, …)
are published as the Buf module `buf.build/laenenai/es-lite` (ADR 0033).
Depend on it instead of vendoring the file:

```yaml
# buf.yaml (consumer)
version: v2
modules:
  - path: proto
deps:
  - buf.build/laenenai/es-lite          # pin a label: buf.build/laenenai/es-lite:v0.3.0
```

then `buf dep update`, keep `import "es/v1/options.proto";` unchanged, and
run `protoc-gen-es-lite` from this Go module (`go install
github.com/laenenai/es-lite/cmd/protoc-gen-es-lite@<tag>`). The Go types for
the options stay in `github.com/laenenai/es-lite/gen/es/v1` — do not generate
them from the BSR module (in buf v2 dependencies are not generated unless
`include_imports` is set; with `managed` mode add a `disable` override for
`buf.build/laenenai/es-lite`).

```go
func example(ctx context.Context, nc *nats.Conn, someTimestamp time.Time) error {
// Embedded (direct Postgres/SQLite):
store, err := sqlite.Open(ctx, "file:events.db") // auto-migrates
if err != nil {
	return err
}
defer store.Close()

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

// Over NATS: the store moves opaque bytes. If you need encryption, wrap this
// store with your own client-side sealing codec (es-lite holds no keys).
client := natsstore.NewClient(nc, natsstore.WithClientPrefix("svc.eslite.eu"))
remote := client.Workspace("ws_123")
rt = aggregate.NewRuntime(remote, counter.Decider, counterv1.EventCodec{})

// Time-travel:
past, err := rt.LoadAsOfVersion(ctx, sid, 1)
if err != nil {
	return err
}
_ = past
past, err = rt.LoadAsOfTime(ctx, sid, someTimestamp)
if err != nil {
	return err
}
_ = past

// Bounded-staleness read (snapshot cache; only if you configured WithSnapshots):
state, _, cacheHit, err := rt.LoadStale(ctx, sid, 5*time.Minute)
_, _, _, _ = state, cacheHit, err, store
return nil
}
```

**Projections and reactions from options (ADR 0030).** Declare a read model,
index or executor reaction as a message option and `task generate` emits the
typed handler interface, state-loader seam, deps, subjects and runner; you
write only the apply logic:

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
// live: over JetStream (filter subjects derived from `consumes`) ...
err := runner.Run(ctx, natsjs.NewFeed(js, natsjs.ConsumerConfig{
	Stream: "ES_EVENTS", Durable: counterviewv1.CounterViewName,
	FilterSubjects: counterviewv1.CounterViewSubjects("*"), Gaps: natsjs.NewGapDetector(),
}))
// ... or straight from the log (tests, single-node SQLite):
err = runner.Run(ctx, &projection.LogFeed{Source: store})
// rebuild from position 0 through the same dispatch path:
pos, err := runner.Rebuild(ctx, store, 500)
```

Reactions (`(es.v1.reaction)`) get a typed issuer and the executor loop
(`reaction.Execute`: conflicts retried, a terminal target counts as done).
The plugin needs `strategy: all` in `buf.gen.yaml` so `consumes` resolves
across packages; `--es-lite_opt=docs=<path>` renders a catalogue of every
projection and reaction from the descriptors.

**Command services over Connect (ADR 0031).** Write the service `.proto` by
hand, mark it `(es.v1.command_service) = { aggregate: "counter.v1.Counter" }`,
tag the request's stream-id field `(es.v1.stream_id)` and, on the commands,
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
and reaction on SQLite), `pipeline` (end-to-end), `snapshots`, `upcast` — and
[`cmdsvc/example`](./cmdsvc/example) (generated Connect command service,
tested in-process with connect-go).

## Backends

- **Postgres** (multi-workspace, production): shared tables + `workspace_id` +
  **RLS** + hash partitioning; scales to 100k+ workspaces; the gap-safe `Drain`
  claim-relay for delivery.
- **SQLite** (single-workspace, dev/edge): single-writer, gap-free cursor, zero
  infra — great for tests and single-tenant nodes.

Both implement the same `es.Store`; a service picks its backend without changing
aggregate code.

## Development

Toolchain pinned in [`.mise.toml`](./.mise.toml) (Go 1.27.3, buf, task,
govulncheck); run `mise install`. The module has no private dependencies.
`cmdsvc/` is a nested Go module (connect-go + protovalidate stay out of the
core graph); every `task` target covers both.

```sh
task                 # list tasks
task generate        # buf lint + codegen (protoc-gen-go + protoc-gen-es-lite)
task test            # unit tests (integration suites skip without services)
task test:race       # unit tests under the race detector
task vuln            # govulncheck
task test:pg         # \
task test:nats       #  ┐ docker-gated integration suites (spin up throwaway
task test:pipeline   #  ┘ Postgres / NATS and tear down)
task test:snapshots
task build vet
```

Generated code is checked into `gen/`; `task generate:check` gates drift in CI.

## Versioning & releases

Semver tags publish a multi-arch image to GHCR (`release.yml`); GHCR retention is
org-wide in `laenen-infra`. Cut a release:

```sh
git tag -a v0.2.0 -m "…" && git push origin v0.2.0
```

## Documentation

- **[ADRs](./docs/adr)** — 17 decision records (event-sourcing, workspaces,
  client-owned crypto, NATS service, migrations, hash chain, codegen, …).
  Start at [the index](./docs/adr/README.md).
- **[Migration notes](./docs/migration)** — what downstream repositories
  change when an ADR moves a contract (currently: projection options).
- **[Deployment guide](./docs/deploy/README.md)** — running `es-lited` in
  production.
- **[Backlog](./docs/backlog)** — tracked follow-ups.
- Platform context: [architecture repo](https://github.com/laenenai/architecture)
  (identity, NATS topology, subject addressing).

## Status

Integration-tested against real infrastructure (Postgres 17, NATS 2.10 +
JetStream). The in-repo `natsstore` middleware provides the subject/workspace
pipeline; broker-side authentication and per-workspace ACLs remain deployment
prerequisites (see the security review and ADR 0009). Pre-1.0: the API may
still shift. See [Status in each ADR](./docs/adr/README.md) for what is
implemented vs. designed.

## Licence

MIT — see [LICENSE](LICENSE).
