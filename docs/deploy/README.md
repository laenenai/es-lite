# Deploying es-lited

`es-lited` is the es-lite eventstore as a **NATS service** (ADR 0009): it
serves an `es.Store` over NATS request/reply. It is **keyless** — it holds no
keys and stores only the opaque bytes clients send; encryption, PII hashing and
erasure are client-owned (ADR 0025). It is a stateless service over a shared
backend, so it scales horizontally.

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

## Secrets and TLS

es-lited is keyless, so payload-encryption keys never live here. Keep them in
the client's KMS/keystore; es-lite has no `ForgetWorkspace` and no server-side
crypto-shredding (ADR 0025). The server still needs **infrastructure** secrets,
ideally injected dynamically by your platform's secret manager (for example an
OpenBao/Vault Agent sidecar or CSI provider):

- **Postgres credentials** — short-lived, rotated creds templated into `PG_DSN`.
- **NATS credentials** for the `ESLITE` account (the `.creds`/JWT).
- **TLS material** (CA + cert/key) for the client↔service hop — this protects
  the metadata the server sees (stream ids, actors, type URLs, claim scopes).
  The repo does not wire `NATS_CA`/client certificates yet; configure TLS in
  the NATS connection/deployment.

The pod ideally holds **no static secret**. Payloads are opaque; whether they
are ciphertext depends on the client.

## Health

- **Mesh-native:** the NATS Micro registry — `$SRV.PING` (health), `$SRV.INFO`
  (discovery), `$SRV.STATS` (metrics). `nats micro ls` / `info eslite` /
  `stats eslite`.
- **Kubelet:** `GET /healthz` on `HEALTH_ADDR` — 200 when connected to NATS,
  503 otherwise. Used for liveness/readiness in the manifest.

## Observability (OpenTelemetry → OpenObserve)

`es-lited` and `es-relayd` export **OTLP metrics + traces** to the mesh's
OpenObserve collector — enabled by the standard `OTEL_*` env, and **inert when
unset** (dev/single-node stay zero-config):

| Env | Example |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://openobserve:5081` |
| `OTEL_EXPORTER_OTLP_HEADERS` | `Authorization=Basic …,organization=default,stream-name=default` |
| `OTEL_SERVICE_NAME` | defaults to `es-lited` / `es-relayd` |

**Metrics:** `eslite.requests` (count, labelled `method`+`outcome`) and
`eslite.request.duration` (histogram, seconds) — request rate/latency of
`es-lited`; `eslite.relayed` (count) — events `es-relayd` published to JetStream.
**Traces:** one server span per request, with W3C trace context extracted from
message headers by the in-repo middleware (ADR 0028), so traces span the async
NATS hops. **Logs** go to stdout and are collected by OpenObserve.

## Autoscaling

This is an I/O-bound req/reply service — **do not scale on CPU.** Core NATS
request/reply has no persistent queue, so the saturation signal is **latency**,
not queue depth:

- **Primary: p95/p99 request latency** (scale out past your SLO).
- **Secondary: requests/sec per pod** or **in-flight concurrency per pod**.

Drive these from the OTLP metric `eslite.request.duration` (histogram) exported
to OpenObserve (above) via **KEDA** — query the p95 in OpenObserve/Prometheus
(see `deploy/k8s/keda-scaledobject.yaml`). NATS Micro `$SRV.STATS` is also
available (`nats micro stats eslite`) as an ad-hoc source.

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

## Delivery relay

`es-lited` serves reads/writes; the **relay** publishes the log to JetStream
(ADR 0003). It drains Postgres and publishes each event with
`Nats-Msg-Id = global_position` (dedup). It is **keyless** — no keys, so it
publishes the same opaque bytes it read; projections decrypt if the client
sealed them. There are two ways to run it:

**Folded in (preferred) — `RELAY=true` on es-lited.** Every replica campaigns
for a **NATS KV lease** (`es_leader` bucket, key `relay.<prefix>`); only the
elected leader drains, and if it dies a follower takes over within ~one lease
TTL (≈15 s). No separate Deployment, HA for free. The lease is time-fenced, not
session-fenced, so a brief two-leader overlap during failover is possible — but
harmless: the drain claims rows `FOR UPDATE SKIP LOCKED` and publish dedups on
`global_position`. The election key is **per subject prefix**, so each
region/shard elects its own relay leader independently. Extra config on es-lited:
`ES_STREAM` (default `ES_EVENTS`), `ES_BATCH` (default 200), `ES_ENSURE_STREAM`.

**Stream provisioning.** By default the relay auto-creates the target stream
(`CreateOrUpdateStream`, defaults **R1 / infinite retention**) — fine for
dev/single-node. In prod, **pre-provision the stream** (R3, bounded `MaxAge`)
and set **`ES_ENSURE_STREAM=false`** so es-lited never touches JetStream
topology — the sample manifest does this. Same flag exists on `es-relayd`.

**Dedicated singleton (optional) — `es-relayd`.** Isolates the relay from the
serving path (its own resources, blast radius, rollout cadence). Run it as a
**singleton** (`deploy/k8s/relay.yaml`, `replicas: 1`, `strategy: Recreate`) —
do **not** scale it by raising replicas. Same image, `/es-relayd` entrypoint.
Config: `PG_DSN`, `NATS_URL`, `ES_STREAM`, `ES_BATCH`, `HEALTH_ADDR`,
`ES_ENSURE_STREAM`. Do **not** run this *and* es-lited `RELAY=true` against the
same stream (double drain — wasteful, not unsafe).

Downstream **projection consumers** are the pieces you *do* autoscale — on
JetStream `num_pending` (see the autoscaling section).

## Sharding, regions, and data residency

`NATS_SUBJECT_PREFIX` is the routing knob. Run one es-lited (+ its Postgres,
NATS domain, read models) per **shard** — and, for **data residency**, per
**regional cell** — and route by prefix (`svc.eslite.eu`, `svc.eslite.us`).
Clients target the prefix for their workspace's home shard/region. **Shard by
workspace, never by aggregate** (uniqueness and per-workspace ordering are
per-store; ADR 0009). Zero-knowledge narrows residency scope to metadata +
keys; whether payloads are ciphertext depends on the client.

## Migrations

Schema is **versioned** (numbered files + a `schema_migrations` history table;
[ADR 0010](../adr/0010-schema-migrations.md)). For **multi-replica Postgres**,
run migrations **once** via the `es-migrate` **init container** (same image,
`command: ["/es-migrate"]`) and set **`AUTO_MIGRATE=false`** on the app
container so replicas never touch DDL on boot — see the init container in
[`deploy/k8s/deployment.yaml`](../../deploy/k8s/deployment.yaml). (A Postgres
advisory lock serializes migrators as a backstop.) For SQLite / dev /
single-node, leave `AUTO_MIGRATE` unset — `Open` migrates automatically.
Evolving the schema = a new numbered migration file, never an edit to an
applied one.

## NATS account

Run es-lited in a dedicated **`ESLITE` account** so its subjects, streams, and
KV are isolated with their own quotas. Export the `svc.eslite.*` service and
the `evt.*` projection stream to client accounts. Derive the **workspace from
the caller's JWT** (an identity middleware), never from a client-supplied
field (ADR 0009).

## Tamper evidence: verifying the chain and exporting anchors (ADR 0029)

Every event row carries a per-stream SHA-256 chain computed in the append
transaction. The chain needs no keys to verify, but it is only *evident*
against a database operator if its heads are regularly fixed **outside** the
database. Runbook:

1. **Verify.** `es-verify all` (same env as `es-migrate`) recomputes every
   stream's chain and exits non-zero on the first break; `es-verify all
   --strict` additionally requires every row to be chained (only a store that
   was empty at migration 0002 can satisfy it — `contracts`' case). Run it from
   a job after restores and on a schedule.
2. **Anchor.** `es-verify anchor` prints one JSON line
   (`anchor_id`, `created_at`, `upto_global_position`, `stream_count`,
   `root_hash`). Alternatively set `ANCHOR_INTERVAL=1h` on `es-lited`
   (Postgres only): the elected leader takes an anchor on that cadence and
   logs it as `es-lited: anchor {...}`.
3. **Export.** Ship that line somewhere the database operator cannot rewrite:
   a signed git commit in an ops repository, a transparency log, a
   timestamping authority, or at minimum a different trust domain's log sink.
   es-lite does not sign anchors (it is keyless, ADR 0025); the export is
   where signing belongs.
4. **Check.** `es-verify anchor --check anchor.json` recomputes the root at the
   anchored `global_position` and exits non-zero on mismatch — this is what
   catches a truncated tail or a rewritten-and-rehashed history, which the
   chain alone cannot.

`anchors` rows are store-wide and readable only with `app.bypass_rls`
(admin / the `es-verify` binary), never by a workspace session.
