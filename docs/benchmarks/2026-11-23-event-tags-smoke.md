# Event-tags smoke results — 2026-11-23

- **Harness:** [`cmd/es-bench`](../../cmd/es-bench) with the `-tags` scenario
- **Design:** [2026-11-23-event-tags-design.md](../superpowers/specs/2026-11-23-event-tags-design.md)
- **Status:** smoke only. The full 10M and 50M tagged runs are **not** measured here.

## Environment

- Apple M5 Max, 18 CPUs, 128 GB host RAM; Docker Desktop Linux VM.
- PostgreSQL 17, stock settings, dedicated test container.
- One workspace, 100 events per synthetic stream, 32-byte payloads,
  100,000 events, two tags per FlowDoc-shaped event
  (`flowdoc.space` batch tag, `flowdoc.block` per event).
- The seed inserts rows in bulk and projects `event_tags` in the same range;
  the timed appends are real, chained `Workspace.Append` calls with tags.

## Seed

```
es-bench seed -events 100000 -chunk 100000 -tags -tag-space bench-space
seeded 100000 events (1000 op-bearing) over 1 workspace(s) in 1.5s
```

Footprint immediately after seeding:

| Measure | Value |
|---|---|
| events | 100,000 |
| event_tags rows | 200,000 |
| events heap + indexes | 77 MB total (39 MB heap, 38 MB indexes) |
| event_tags total | 29 MB (≈145 bytes/row, heap + PK) |

At 1B tag rows the same ≈145 bytes/row implies ≈145 GB, inside the design's
150–250 GB estimate at 500M events. Treat that as an anchor, not a measurement:
the per-row overhead and index fill factor will move with scale.

## Measure

```
es-bench measure -appends 100 -batch 10 -tags -tag-space bench-space \
  -tag-read-pages 10 -skip-rebuild -skip-verify
```

| Phase | Result |
|---|---|
| Appends (100 × 10 tagged events) | p50 2.7 ms, p99 6.1 ms, mean 2.9 ms |
| `ReadByTags` (10 pages × 1,000, AND on space) | p50 3.1 ms, p99 4.0 ms, mean 3.1 ms |
| event_tags after | 202,000 rows, 29 MB |

The append p50/p99 include the jsonb column, the `event_tags` multi-row
insert, and the per-batch `log_order.tags_hash`. The untagged 2026-11-21
baseline was p50 ~2.5 ms / p99 ~3.9 ms at 10M; the smoke is not directly
comparable (different data size and no claims/replay phase), so the honest
statement is that the tagged append is the same order of magnitude and the
full comparison waits for the 10M/50M run.

## Reproduce

See [README](./README.md) for the container and DSN setup, then run the two
commands above against a database migrated to 0016. `es-bench reset` truncates
`events`, `event_tags`, `log_order` and `unique_claims`.

## What is not measured

- 10M and 50M tagged runs (seed time, append p50/p99, tag-read p99, and the
  `event_tags` footprint at scale).
- The quiesced `RebuildTags` time at scale; the rebuild is a full events scan
  plus one insert per tag row.
- The 500M extrapolation's tag-table share; the 100k smoke only anchors the
  per-row cost.
