# ADR 0038: Log-Order Attestation (Per-Batch `log_order` Rows)

- **Status:** Accepted (implemented)
- **Date:** 2026-11-07
- **Related:** ADR 0029 (the chain/anchor trust split this ADR extends to order),
  ADR 0037 (the horizon fence that settles every bound an order root covers),
  ADR 0010 (the migrations that add the table), review
  `docs/review/20261107_consolidated_review.md` F11 (the finding this closes)

## Context

ADR 0029's chain binds *what each stream contains*: per-stream `prev_hash`/`hash`
linkage, verified without keys, with anchors exporting the heads under one root.
Two things it deliberately did not bind:

1. `global_position` was excluded from the envelope hash (assigned during the
   `INSERT`; including it would need insert-then-update, violating append-only),
   and ADR 0029 §2 claimed "cross-stream ordering is attested by anchors (§4)
   instead." **That claim was false as implemented** (review F11): the anchor
   root of ADR 0029 §4 hashed `(workspace_id, stream_id, head_version,
   head_hash)` — no position appears anywhere in it. An operator who
   repositioned events across the log, or rewrote whole sections wholesale,
   left every chain, every head and every stored anchor intact.
2. Nothing witnessed the order in which events landed in the global log at
   all: a batch's positions lived in the same rows the operator can rewrite.

The 2026-11-07 review (F11, confirmed by two reviewers independently) made
this a P2: "cross-stream reordering is undetectable." The maintainer chose the
full fix (design D1): make the claim true instead of deleting it.

## Decision: per-batch `log_order` rows, hashed in the append transaction

Every append transaction that writes at least one event also writes exactly one
`log_order` row, in the same transaction:

- **The batch hash.** `es.BatchOrderHash` (`es/order.go`) —
  `SHA-256("es-lite.log-order.v1" ‖ u32(count) ‖ per event in ascending
  position order: u64(global_position) ‖ 32-byte stored chain hash)`. It hashes
  only *this transaction's own* events: no cross-transaction reads, no new
  locks, no new round trips — positions and per-event hashes are already in
  hand when the events are inserted. The single extra `INSERT` per non-empty
  batch is the entire append-path cost, and it adds no serialization.
- **The row.** `log_order(from_position PRIMARY KEY, to_position, workspace_id,
  stream_id, event_count, batch_hash, created_at)` — migration `0006` on both
  backends (`postgres/migrations/0006_log_order.sql`,
  `sqlite/migrations/0006_log_order.sql`; SQLite minus `workspace_id` and RLS,
  per 0001's single-workspace rule). The row's range is `[from_position,
  to_position]`: positions are drawn from the shared sequence in event order
  inside one transaction, so the batch's rows span one contiguous per-transaction
  stretch. Postgres indexes `to_position` and carries the same RLS shape as
  `events`: workspace arm plus role-gated bypass arm, `FORCE ROW LEVEL SECURITY`.
- **The order root.** `es.OrderRoot` (`es/order.go`) —
  `SHA-256("es-lite.log-order-root.v1" ‖ u64(row count) ‖ per row sorted by
  ascending `from_position`: u64(from) ‖ u64(to) ‖ u32(count) ‖ 32 batch-hash
  bytes)`. `postgres.Store.Anchor`/`CheckAnchor` and their SQLite mirrors compute
  it over the rows with `to_position <= upto` — O(#batches), not O(#events) —
  inside the same fenced, settled transaction that computes the heads root
  (`postgres/anchor.go`): a row and its events commit in one transaction, so
  every covered row at or below the settled bound is as permanently final as the
  heads are, and `CheckAnchor` agrees forever after.
- **Where the order fields live.** The `anchors` table is unchanged — its five
  legacy columns only. The three order fields travel in the exported anchor
  (`es.Anchor` JSON): `order_root_hash`, `order_from_position`,
  `order_count`. Export is the point, exactly as in ADR 0029 §4: the database
  row is inside the operator's reach; the exported anchor is the off-box
  witness. A new-style anchor *always* carries an order root — a real hash over
  the empty row set when the bound covers no batch — and a nil
  `order_root_hash` is exclusively the legacy marker: anchors recorded before
  this change verify heads-only, order comparison skipped.

### The `OrderFromPosition` rule

`order_from_position` is the first attested position — the `from_position` of
the first covered row, 0 when none are. It is the order analog of the chain's
`ChainedFrom` (ADR 0029 §5): **rows before the migration are unattested.**
Events appended before migration `0006` have no `log_order` row and are never
covered by one — the same cannot-retrofit argument as the chain's (hashing
existing rows after the fact would certify rows that may already have been
altered). An anchor whose bound covers only pre-migration history reports
`order_count` 0 with a real `order_root_hash` over the empty row set; a
verifier reading `order_from_position > 0` knows attestation starts there.

### The `CACHE 1` invariant

The fence's lossless proof (ADR 0037, P2) needs: *every position ≤ L was drawn
before L was read*. A sequence with `CACHE n > 1` breaks exactly that
converse: `last_value` (the value the fence reads as L) advances a full cache
block on the first draw, and the cached positions are handed out *after* it —
so a later append can receive a position at or below an L the fence already
cleared and delivered past. That is a permanently skipped event — the exact
loss class the fence exists to prevent — and on the anchor path it would mean
roots recorded over sections that can still change (false settledness, so
anchors that stop verifying). The default (`CACHE 1`) is what makes
draw-before-read true; nothing in the schema pinned it.

So every fence round now reads its bound through the documented `pg_sequences`
view and asserts `cache_size = 1` (`postgres/wsstore.go`:
`readSequenceState`, `fenceBound`; error
`postgres.ErrSequenceCacheMisconfigured`, whose message names the fix:
`ALTER SEQUENCE events_global_position_seq CACHE 1`). Both feed-facing reads
and the anchor path's `settledBound` round go through the same statement. One
honest asymmetry: an idle workspace short-circuits on its own committed max
before the sequence read, so the assertion fires on its first active round —
the round that would actually deliver or bound something. The runbook carries
the operator-side invariant (`docs/deploy/README.md`).

### The `es-verify order` deep-check contract

The order root alone proves only *that some set of rows hashed to this value*.
The audit that catches real tampering is the deep check, `es-verify order`
(wired into `es-verify all` after the stream chains):

- For every `log_order` row at or below a settled bound, recompute the row's
  `es.BatchOrderHash` from the events themselves — paged by position under the
  role-gated RLS bypass, grouped to each row by its `(workspace, stream)` scope
  and its `[from, to]` range — and compare against the stored `batch_hash`
  (`postgres/anchor.go` `VerifyOrder`, mirrored on SQLite). This is O(N), the
  same cost class as `VerifyStream`.
- **The trust split mirrors ADR 0029's chain/anchor split.** The deep check is
  what actually catches a *repositioned, inserted or deleted event inside an
  attested batch* — a tamper that leaves every per-stream chain, every head and
  the heads root intact. The *exported* order root is the off-box witness that
  catches a wholesale rewrite (an operator who regenerates `log_order` rows
  from the tampered events cannot make them hash to an anchor already
  published outside the database). Exit 1 on the first mismatch, printing the
  tampered batch's `from_position`; 0 when every covered row recomputes.
- Pre-0006 events have no covering row and are skipped by the deep check —
  `order_from_position` delimits what any order verdict can claim.

## Alternatives considered

### Global append-time chain (each batch hashes the previous batch's root)

Rejected: it serializes every append on a single chain head — exactly the
coordinator-shaped write-path cost ADR 0029 rejected for the per-stream chain —
and it forks silently under concurrent appends: two racing appends both claim
the same predecessor and no unique constraint exists to catch the double-spend
of the previous-batch pointer.

### Anchor-time full scan (order root over every `(position, event_hash)` at each anchor)

Rejected: O(N) per anchor and per `CheckAnchor` on a table designed to keep
growing. The batch-row design pays O(#batches) per anchor and defers the O(N)
work to the explicit deep check, where it is the same cost class as the
existing stream verification.

### Store the order root in the `anchors` table

Rejected: the table row is inside the operator's reach, so a root stored there
attests nothing an operator cannot rewrite. The exported anchor — published
off-box — is the witness, so the order fields live there (the DB row keeps the
five legacy columns, and `ListAnchors` serves heads-only roots; re-checking a
stored anchor's order fields is done from the exported JSON, never from the
row).

## Consequences

### Positive

- The claim ADR 0029 §2 made becomes true: cross-stream append order is
  attested, exportable, and independently verifiable without keys — the chain
  binds per-stream content, the heads root binds stream heads, the order root
  binds the global sequence, and `es-verify all` recomputes every layer from
  the rows themselves (`order` recomputes the batch layer alone).
- No append serialization and no new append-path reads or locks: one extra
  single-row `INSERT` per non-empty batch, in a transaction that already
  writes.
- Reused the entire ADR 0029/0037 machinery — the fence for settledness, the
  role-gated bypass for the admin path, `es-verify`'s exit-code contract — so
  the audit story stays one story.

### Negative

- Events appended before migration `0006` are permanently unattested by order
  roots (the `OrderFromPosition` rule); a store that is already populated is
  only partially order-attested forever, exactly like the chain's
  `ChainedFrom`.
- The deep check is a full log read — the same class of cost as
  `VerifyStream`, which is why it is an explicit `es-verify order` run (or part
  of `all`), not something the anchor path does per round.
- The `CACHE 1` assertion turns a previously silent misconfiguration into a
  hard error on every fence round: a deployment that had `ALTER SEQUENCE …
  CACHE n > 1` and "worked" was already inside the loss window this closes;
  the error is the point, and the runbook says so.
