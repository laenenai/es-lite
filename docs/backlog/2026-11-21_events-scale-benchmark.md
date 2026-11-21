# Events scale benchmark: rebuild, verify and append at 10M–100M rows

- **Filed:** 2026-11-21 (after the op-bearing replay index, PR #37)
- **Area:** postgres storage / claims replay / capacity
- **Status:** Partially resolved 2026-11-21 — 10M and 50M measured, plus the 8-partition spread; 100M/1% is extrapolated (seeding it is a multi-hour write) and the logical restore drill remains open. Results: [2026-11-21-results.md](../benchmarks/2026-11-21-results.md).

## Problem

The events-only model and the partial `events_ops_idx` are analytically sound,
but the only measurements so far are local, warm-cache probes at 1M rows in a
single partition (1% and 10% op density). They established the plan shape
(index-only, zero heap fetches) and the per-page cost, not the operational
envelope. Neither the release notes nor the runbook may quote a capacity
number as measured until this runs.

The 1M probe also showed that the rebuild's wall time is now dominated by the
`unique_claims` inserts, not the scan: state the matrix in **claim-bearing
batches**, not just events.

## Matrix

Run on production-like storage (SSD, `fsync` on, cold cache for at least the
first pass) and record the hardware and Postgres version:

- **Sizes:** 10M, 50M, 100M events in one partition; if the target is 1B,
  100M events per partition across all eight.
- **Op density:** 0.1%, 1%, 10% of events carrying `constraint_ops` (one
  op-bearing batch per N events).
- **Claim mix:** claim-only batches, release/claim sequences, and the
  one-op-per-batch floor.
- **Operations:**
  - append latency p50/p99 and sustained throughput through the stock
    transaction path (not a bulk `INSERT ... SELECT`);
  - `es-verify claims` and `es-verify claims --rebuild`, quiesced, cold and
    warm;
  - `es-verify all --chains-only` (events-only restore) and `es-verify all`;
  - `setval('events_global_position_seq', max(global_position))` and reopen
    after a restore.
- **Footprint:** heap, each index (`events_ops_idx` specifically), TOAST, WAL
  generated per 1M events, and `unique_claims`/`log_order` growth.

## Acceptance criteria

- A reproducible harness or documented command sequence is committed (for
  example under `docs/benchmarks/`), not just numbers pasted into a PR.
- Results are recorded in the repo with the storage/version caveats, and the
  runbook's scale envelope is updated with measured numbers in place of the
  current extrapolations.
- A pass/fail statement for the target scale, including the partition-count
  decision: eight hash partitions are sufficient through ~10^9 uniform rows;
  a single dominant tenant gets no relief from them, so the target tenant
  shape is stated explicitly.
- If the rebuild exceeds the agreed maintenance window at the target size,
  the next step is named before release (more partitions, an online rebuild,
  or an op-log compaction), not left implicit.

## Not in scope

- Changing the partition count before the measurement.
- Benchmarking SQLite (single-writer, not the scale path).

## Measured (2026-11-21)

Full matrix, hardware and caveats:
[2026-11-21-results.md](../benchmarks/2026-11-21-results.md).

- 10M events / 100k claims: rebuild 13.0 s cold, 11.8 s warm; verify ~11.5 s.
- 10M / 1M claims: rebuild 121 s cold, 129 s warm; verify ~120 s.
- 50M / 500k claims: rebuild 56 s cold, 66 s warm; verify ~64 s.
- 10M over 8 partitions (1.25M/partition, 12.5k claims): rebuild 1.4 s.
- Append p50 ~2.5 ms, p99 ~4–6 ms for a 10-event batch with one claim.
- Footprint ~0.7 KB/event; the op-bearing index is 15–151 MB.
- 100M/1% is extrapolated from the 1M-claim run (~2 min rebuild, ~70 GB), not
  run: the seed's write time grows superlinearly with the index set.

The earlier 1M warm probe established the plan shape: global index + filter →
**index-only scan, zero heap fetches** after `events_ops_idx`; page cost
739 → 93, 2.06 ms → 0.14 ms.
