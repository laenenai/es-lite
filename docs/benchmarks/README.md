# Scale benchmark harness

`cmd/es-bench` seeds a synthetic event log and measures the claims replay path
against it. It backs the matrix in
[the backlog](../backlog/2026-11-21_events-scale-benchmark.md) and the recorded
results in [2026-11-21-results.md](./2026-11-21-results.md).

## What it measures

- **Rebuild and verify** through the real `postgres.Workspace` methods
  (`RebuildClaims`, `VerifyClaims`), including the `events_ops_idx` plan.
- **Append latency** through the real `Workspace.Append` transaction path:
  p50/p99 for a batch of N events, with an optional claim per append.
- **Footprint**: heap, indexes, the op-bearing partial index, and
  `unique_claims`/`log_order` growth.

It does **not** measure chain verification. The bulk seed writes NULL
`prev_hash`/`hash` because it is not the append path, so `es-verify all` would
report the seeded prefix as unchained. The append phase does write real
chained streams. Do not use a seeded store for a chain audit or as a restore
drill.

## Setup

Use a dedicated container; never point this at a shared database.

```bash
docker run -d --name es-lite-bench \
  -e POSTGRES_PASSWORD=pw -p 5435:5432 \
  -v es-lite-bench-data:/var/lib/postgresql/data \
  postgres:17
```

The harness opens through `postgres.Open`, so migrations run on first use.

```bash
GOWORK=off go build -o /tmp/es-bench ./cmd/es-bench
export PG_DSN='postgres://postgres:pw@127.0.0.1:5435/postgres?sslmode=disable'
```

## Run

Seed (bulk insert; resets by default):

```bash
/tmp/es-bench seed -events 10000000 -op-every 100
```

`-op-every 100` means 1% of events carry a claim; `-op-every 10` is 10%.
`-events-per-stream 100` controls the synthetic stream shape, and
`-workspaces N` spreads the events over N workspaces (each hashes to one
partition).

For a cold-ish first pass, restart the container after seeding. This empties
PostgreSQL shared buffers; the host page cache is not dropped, which is a
documented limitation.

```bash
docker restart es-lite-bench
```

Measure (real appends, then a quiesced rebuild and verify):

```bash
/tmp/es-bench measure -appends 2000 -batch 10 -claim-every 1 -json
/tmp/es-bench measure -skip-appends -json   # warm rebuild over the same data
```

Reset between datasets:

```bash
/tmp/es-bench reset
```

## Matrix used

| Run | Events | Op density | Claim-bearing rows | Shape |
|---|---|---|---|---|
| 10M/1% | 10,000,000 | 1% | 100,000 | one workspace, one partition |
| 10M/10% | 10,000,000 | 10% | 1,000,000 | one workspace, one partition |
| 50M/1% | 50,000,000 | 1% | 500,000 | one workspace, one partition |
| 100M/1% | 100,000,000 | 1% | 1,000,000 | one workspace, one partition |

The one-partition shape is the worst case for a rebuild: `workspace_id` prunes
to one of eight hash partitions, so a uniform multi-tenant log is smaller per
tenant, never larger. The 8-partition spread is therefore not a separate run;
100M total over 8 partitions is a 12.5M-per-partition case, which the 10M run
already approximates.

## Caveats

- Local Docker Desktop on macOS. At the time of the recorded run the VM had
  8 GB memory; PostgreSQL used its defaults (`shared_buffers=128MB`,
  `effective_cache_size=4GB`, `synchronous_commit=on`).
- Warm OS page cache except the one container restart before the "cold" run;
  the host cache is not dropped.
- Single writer, no concurrent appends during the rebuild (the documented
  quiescence requirement).
- The seed is synthetic and unchained, and a logical restore drill is a
  separate acceptance item.
