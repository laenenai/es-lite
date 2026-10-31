# Relay reordering under overlapping drains (review ES-04)

- **Date:** 2026-10-30
- **Status:** Open — mitigated, not fixed

## Problem

Two relay drains claiming rows with `FOR UPDATE SKIP LOCKED` can publish a
stream's versions out of order to JetStream when their batches overlap
(review finding ES-04). JetStream dedup on `global_position` prevents
duplicates but not reorders.

## Mitigation in place (ADR 0029 §6)

Every published message carries `Es-Prev-Hash`/`Es-Hash`; `natsjs.GapDetector`
(`ConsumerConfig.Gaps`) detects a skipped version or a non-linking prev_hash
per stream and naks the message so it is redelivered after the missing one.
A consumer therefore never applies a reordered event, at the cost of a
redelivery.

## Real fix (to do)

Make the relay a single ordered publisher per stream: either a single-leader
drain (already the default under `es-lited` RELAY leader election) with
the claim batch ordered by `(stream_id, version)` and a per-stream high-water
mark, or publish with JetStream `Nats-Expected-Last-Subject-Sequence` per
stream subject. Decide and record in an ADR once a second relay instance is
actually needed.
