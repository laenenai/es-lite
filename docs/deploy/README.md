# Deploying es-lited

`es-lited` is the es-lite eventstore as a **NATS service** (ADR 0009): it
serves an `es.Store` over NATS request/reply. It is **zero-knowledge** — it
holds no keys and stores only ciphertext; clients encrypt payloads
client-side. It is a stateless service over a shared backend, so it scales
horizontally.

- Image: build with [`../../Dockerfile`](../../Dockerfile) (`task docker:build`).
- Manifests: [`../../deploy/k8s`](../../deploy/k8s).

**Image publishing.** CI publishes `ghcr.io/laenenai/es-lited` **only on version
tags** (`release.yml`, multi-arch amd64/arm64), so GHCR gets one image per
release — not per commit. GHCR **retention is org-wide and central**: a weekly
job in `laenen-infra` (`.github/workflows/ghcr-retention.yml`) prunes untagged
orphans and caps retained versions across all `laenenai` packages — es-lite
carries no cleanup workflow of its own. Manual push:
`IMAGE=ghcr.io/laenenai/es-lited:vX task docker:push`.
- Design: [ADR 0009](../adr/0009-nats-eventstore-service.md),
  [ADR 0002](../adr/0002-deployment-topology-and-delivery.md).

## Configuration (environment)

| Var | Default | Purpose |
|---|---|---|
| `BACKEND` | `postgres` | `postgres` (multi-workspace) or `sqlite` (single-workspace/edge) |
| `PG_DSN` | — | Postgres DSN (required for `BACKEND=postgres`) |
| `SQLITE_DSN` | `file:eslite.db` | SQLite DSN (`BACKEND=sqlite`) |
| `NATS_URL` | `nats://127.0.0.1:4222` | NATS endpoint (the `ESLITE` account) |
| `NATS_SUBJECT_PREFIX` | `svc.eslite` | subject prefix — also the shard/region routing knob |
| `HEALTH_ADDR` | `:8080` | address for the `/healthz` kubelet probe |

**Backends.** Postgres is the multi-workspace production backend (RLS + hash
partitioning, ADR 0004). SQLite is **single-workspace** (no `workspace_id`),
for single-tenant or edge deployments — the server ignores the workspace
argument.

## Secrets: OpenBao provides the *infrastructure* secrets

es-lited is zero-knowledge, so OpenBao's **Transit** (payload) keys live on
the *clients*, not here. What the server gets from OpenBao is its
**infrastructure** secrets, ideally dynamic, via an OpenBao/Vault **Agent
sidecar** (or the CSI provider):

- **Postgres credentials** from the **database secrets engine** — short-lived,
  rotated creds templated into `PG_DSN`, instead of a static password.
- **NATS credentials** for the `ESLITE` account (the `.creds`/JWT).
- **mTLS** material (CA + cert/key) from OpenBao **PKI** for the client↔service
  hop — this protects the *metadata* the server does see (stream ids, actors,
  type URLs, claim scopes; ADR 0009). Payloads are already ciphertext.

The pod ideally holds **no static secret** — the Agent injects all three.

## Health

- **Mesh-native:** the NATS Micro registry — `$SRV.PING` (health), `$SRV.INFO`
  (discovery), `$SRV.STATS` (metrics). `nats micro ls` / `info eslite` /
  `stats eslite`.
- **Kubelet:** `GET /healthz` on `HEALTH_ADDR` — 200 when connected to NATS,
  503 otherwise. Used for liveness/readiness in the manifest.

## Autoscaling

This is an I/O-bound req/reply service — **do not scale on CPU.** Core NATS
request/reply has no persistent queue, so the saturation signal is **latency**,
not queue depth:

- **Primary: p95/p99 request latency** (scale out past your SLO).
- **Secondary: requests/sec per pod** or **in-flight concurrency per pod**.

Feed these from the NATS Micro `$SRV.STATS` (scrape into Prometheus) and drive
**KEDA**'s Prometheus scaler (see `deploy/k8s/keda-scaledobject.yaml`).

Two caveats:

1. **The delivery/projection side scales differently.** JetStream durable
   consumers have a real backlog — autoscale those on **`num_pending`**
   (KEDA's JetStream scaler), not latency.
2. **Postgres is the real ceiling.** Stateless pods scale until the DB
   saturates; past that, more pods worsen latency. Bound `maxReplicas`, front
   Postgres with **PgBouncer**, and when you need more write throughput, scale
   the *data tier* by **sharding workspaces across clusters** (below) — not by
   adding pods. Latency-based HPA is self-correcting: if latency stops
   improving as pods are added, you have hit the DB ceiling.

## Sharding, regions, and data residency

`NATS_SUBJECT_PREFIX` is the routing knob. Run one es-lited (+ its Postgres,
NATS domain, read models) per **shard** — and, for **data residency**, per
**regional cell** — and route by prefix (`svc.eslite.eu`, `svc.eslite.us`).
Clients target the prefix for their workspace's home shard/region. **Shard by
workspace, never by aggregate** (uniqueness and per-workspace ordering are
per-store; ADR 0009). Zero-knowledge narrows residency scope to metadata +
keys, since payloads are ciphertext everywhere.

## Migrations

es-lited applies the schema on `Open` (idempotent `CREATE ... IF NOT EXISTS`).
For **multi-replica Postgres**, prefer running migrations **once** (an init
job / leader) rather than on every replica boot — concurrent `CREATE`/`POLICY`
DDL can race. Tracked in
[`../backlog/2026-08-02_migrations.md`](../backlog/2026-08-02_migrations.md).

## NATS account

Run es-lited in a dedicated **`ESLITE` account** so its subjects, streams, and
KV are isolated with their own quotas. Export the `svc.eslite.*` service and
the `evt.*` projection stream to client accounts. Derive the **workspace from
the caller's JWT** (an identity middleware), never from a client-supplied
field (ADR 0009).
