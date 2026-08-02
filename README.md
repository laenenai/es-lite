# es-lite

[![CI](https://github.com/laenenai/es-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/laenenai/es-lite/actions/workflows/ci.yml)
![Go 1.26](https://img.shields.io/badge/go-1.26-00ADD8?logo=go&logoColor=white)

**A zero-knowledge, event-sourcing eventstore** — usable two ways:

- as a **Go library** you embed (proto-defined aggregates, the Decider model, a
  storage-neutral `Store`), and
- as a **NATS service** (`es-lited`) that many services share over NATS, holding
  no keys and storing only ciphertext.

The append-only event log is the source of truth. **History, auditability, and
time-travel** are non-negotiable requirements — which is why the storage model
is event sourcing, not "state + outbox": only an immutable log answers *"what
did aggregate X look like on 3 March, and why?"* from the database alone.

Design decisions live in [`docs/adr`](./docs/adr) (10 ADRs); operations in
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

`es-lited` is a **workspace-scoped, zero-knowledge eventstore service**. It is
deliberately narrow: it owns durability, ordering, isolation, and delivery of
an opaque, encrypted event log — and delegates everything else.

**es-lited IS responsible for:**

| Responsibility | Mechanism |
|---|---|
| Durable, immutable, append-only event log | Postgres/SQLite; no `UPDATE`/`DELETE` of events |
| Optimistic concurrency | `UNIQUE (workspace, stream, version)` → `ErrConflict` |
| Per-workspace data isolation | Postgres RLS + hash partitioning; workspace from the subject |
| Cross-stream uniqueness | transactional `unique_claims` → `ErrConstraintViolated` |
| Total ordering + at-least-once delivery | `global_position` + relay/poller + JetStream dedup |
| Ciphertext at rest & in flight | stores only what the client encrypted; never decrypts |
| Schema lifecycle | versioned migrations (`es-migrate`) |

**es-lited is NOT responsible for (by design):**

| Not its job | Whose job |
|---|---|
| Authenticating callers / verifying tokens | the NATS auth callout (`natsauthd`) + SPIRE/Zitadel |
| Holding encryption keys / decrypting payloads | **clients** (envelope encryption) + OpenBao (KEKs) |
| Authorization decisions | the PDP (OpenFGA) at the caller/interceptor |
| Business rules | the **Decider**, which runs client-side |

> The client encrypts, decides, and holds keys; the broker authenticates and
> scopes; es-lited durably stores and orders ciphertext. Three concerns, three
> owners — that separation *is* the security model.

## Security features

- **Zero-knowledge server.** `es-lited` and `es-relayd` hold **no keys** and
  **never decrypt**. Clients encrypt event payloads client-side (per-workspace
  envelope encryption); the service stores and moves only ciphertext. Compromise
  the service → you get ciphertext and metadata, never plaintext.
- **Crypto-shredding erasure (GDPR).** Each workspace's data is sealed under a
  per-workspace **OpenBao Transit** KEK. `ForgetWorkspace` deletes the KEK — the
  ciphertext (including in DB backups) becomes permanently unrecoverable.
  Immutability and erasure coexist: destroy a key, not a row.
- **Per-workspace isolation, enforced not asserted.** Postgres **Row-Level
  Security** scopes every access; the workspace comes from the **subject**
  (`svc.eslite.<region>.<ws>.<method>`) and is **cross-checked fail-closed**
  against the authenticated workspace (`subject.ws != auth.ws` ⇒ reject; never
  falls back to a client-supplied value). Defense in depth: broker subject-ACLs
  (via `natsauthd`) **and** the app cross-check **and** RLS — no single gate is
  trusted (architecture ADR 0005).
- **PII-safe uniqueness.** Unique claims on personal data (e.g. email) store an
  **HMAC under the workspace key**, not plaintext — so uniqueness holds while the
  workspace lives, and crypto-shredding unlinks it.
- **Tamper-evident by construction.** Append-only log with full causality/actor
  metadata (`correlation`, `causation`, `command`, `actor`) and two clocks
  (occurred-at vs recorded-at); time-travel reconstructs any past state.
- **Multi-region / data residency.** Region is in the subject from day 1; a
  region is a self-contained cell (Postgres + OpenBao + NATS + read models). No
  transparent cross-region failover for partitioned state.
- **Hardened container.** Distroless, non-root (UID 65532), read-only root
  filesystem, all capabilities dropped, static binary (no shell).
- **mTLS** on the client↔service hop protects the metadata the server does see
  (stream ids, actors, type URLs).

## Architecture

```
        write path                                       read models
 client (Decider + crypto)                          projection consumers
   │ encrypt payload                                       ▲ decrypt
   │ Handle(cmd)                                           │ (JetStream durable)
   ▼                                                       │
 es.Store over NATS  ──req/reply──▶  es-lited ──▶ Postgres (RLS, partitioned,
   (svc.eslite.<region>.<ws>.*)      (no keys)      append-only, ciphertext)
                                                          │
                                       es-relayd  ◀───────┘ drain
                                       (no keys)  ──▶ JetStream (evt.<ws>.…, ciphertext,
                                                            dedup by global_position)
```

- **`es.Store` is the whole contract** — the same aggregate runtime/decider/codec
  run embedded or over NATS by swapping the store.
- **The log is the outbox** — no separate table; the relay tails
  `global_position`.
- **Delivery is at-least-once** — handlers must be idempotent; a last-applied
  position marker makes both delivery and rebuild correct.

## Running the container

The image `ghcr.io/laenenai/es-lited` ships **three** static entrypoints:

| Entrypoint | Role | Scale |
|---|---|---|
| `/es-lited` (default) | the eventstore service (`es.Store` over NATS) | N replicas |
| `/es-migrate` | apply schema migrations once, then exit | init container / job |
| `/es-relayd` | drain the log → JetStream (delivery relay) | **singleton** |

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

**`es-migrate`:** `BACKEND`, `PG_DSN`, `SQLITE_DSN` (as above).
**`es-relayd`:** `PG_DSN`, `NATS_URL`, `ES_STREAM` (default `ES_EVENTS`),
`ES_BATCH` (default 200), `HEALTH_ADDR`.

Secrets are best injected by an **OpenBao/Vault Agent sidecar** (dynamic
Postgres creds via the database secrets engine, NATS creds, mTLS via PKI) — the
pod then holds no static secrets. See [`docs/deploy`](./docs/deploy).

## Deployment (Kubernetes)

Manifests in [`deploy/k8s`](./deploy/k8s): a `Deployment` for `es-lited` (with an
`es-migrate` **init container** and `AUTO_MIGRATE=false`), a singleton
`es-relayd`, and a KEDA `ScaledObject` (autoscale the service on **p95 latency**,
projection consumers on **JetStream lag** — never CPU). Full guide, including the
OpenBao Agent sidecar, mTLS, sharding, and multi-region cells:
**[`docs/deploy/README.md`](./docs/deploy/README.md)**.

## Using es-lite as a library

Aggregates are defined in `.proto`; `task generate` emits sealed sum-types +
codecs. You hand-write only the `Decider` and its error sentinels.

```go
// Embedded (direct Postgres/SQLite):
store, _ := sqlite.Open(ctx, "file:events.db")           // auto-migrates
rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})
res, _ := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100}, es.Meta{})

// Over NATS, zero-knowledge (client encrypts):
client := natsstore.NewClient(nc, natsstore.WithClientPrefix("svc.eslite.eu"))
enc := cryptostore.New(client.Workspace("ws_123"), shredder, "ws_123")
rt := aggregate.NewRuntime(enc, counter.Decider, counterv1.EventCodec{})  // identical runtime

// Time-travel:
past, _ := rt.LoadAsOfVersion(ctx, sid, 1)
past, _  = rt.LoadAsOfTime(ctx, sid, someTimestamp)

// Bounded-staleness read (from the NATS-KV snapshot cache, zero store hops):
state, _, fresh, _ := rt.LoadStale(ctx, sid, 5*time.Minute)
```

See [`examples/`](./examples) — `counter`, `pipeline` (end-to-end),
`zeroknowledge`, `snapshots`, `upcast`.

## Backends

- **Postgres** (multi-workspace, production): shared tables + `workspace_id` +
  **RLS** + hash partitioning; scales to 100k+ workspaces; the gap-safe `Drain`
  claim-relay for delivery.
- **SQLite** (single-workspace, dev/edge): single-writer, gap-free cursor, zero
  infra — great for tests and single-tenant nodes.

Both implement the same `es.Store`; a service picks its backend without changing
aggregate code.

## Development

Toolchain pinned in [`.mise.toml`](./.mise.toml) (Go 1.26+, buf, task); run
`mise install`. es-lite depends on the private `github.com/laenenai/natskit` —
set `GOPRIVATE=github.com/laenenai/*` and authenticate git (see the
[architecture getting-started](https://github.com/laenenai/architecture/blob/main/docs/getting-started.md)).

```sh
task                 # list tasks
task generate        # buf lint + codegen (protoc-gen-go + protoc-gen-es-lite)
task test            # unit tests (integration suites skip without services)
task test:pg         # \
task test:nats       #  ┐ docker-gated integration suites (spin up throwaway
task test:openbao    #  ┘ Postgres / NATS / OpenBao and tear down)
task test:pipeline   # /
task test:zeroknowledge
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

- **[ADRs](./docs/adr)** — 10 decision records (event-sourcing, workspaces &
  crypto-shredding, NATS service, migrations, …). Start at
  [the index](./docs/adr/README.md).
- **[Deployment guide](./docs/deploy/README.md)** — running `es-lited` in
  production.
- **[Backlog](./docs/backlog)** — tracked follow-ups.
- Platform context: [architecture repo](https://github.com/laenenai/architecture)
  (identity, NATS topology, subject addressing).

## Status

Broad and integration-tested against real infrastructure (Postgres 17, NATS 2.10
+ JetStream, OpenBao 2.6.1). Pre-1.0: the API may still shift. Full
authorization *enforcement* depends on the mesh's `natsauthd` callout (the
subject cross-check and RLS scoping are in place and ready). See
[Status in each ADR](./docs/adr/README.md) for what is implemented vs. designed.
