# Breaking changes

es-lite is pre-1.0: the module may break, but it must never surprise.
This file collects every breaking change as it lands — one entry per
change, each stating what changed, who is affected, and what to do.
Entries are grouped by the change date and never rewritten after landing.

## 2026-11-07 — `ReadStream` reports `ErrStreamNotFound` for never-written streams

**What changes.** `es.Store.ReadStream` used to return `(nil, nil)` for a
stream that has never been written, which made "not found"
indistinguishable from "empty". Both adapters (`sqlite.Store` and
`postgres.Workspace`) now return an error wrapping `es.ErrStreamNotFound`,
with the canonical stream id in the message, when a read starts at the
beginning of a never-written stream: `fromVersion == 0` and the stream has
no events at all — whatever `toVersion` is, since from the start of the
stream an empty result already means no rows exist. Empty ranges are
unchanged: reads with `fromVersion > 0`, and bounded reads from 0 of a
stream that exists, still return `(nil, nil)`.

**Who is affected.** Callers of `ReadStream` — directly, through
`aggregate.Runtime`, or through `VerifyStream` — that treat `(nil, nil)`
as "this stream has no events" without inspecting the error. The in-repo
consumers already tolerate the sentinel: `VerifyStream`/`VerifyStreamWith`
and the aggregate runtime fold it as an empty stream, so behavior there is
unchanged. `postgres.Workspace.ReadStreamTx` (the caller-owned-transaction
variant) is deliberately unaffected and keeps returning `(nil, nil)`.

**What to do.** Where "never written" matters, match the sentinel with
`errors.Is(err, es.ErrStreamNotFound)`; otherwise treat the error as an
empty read. Code that asserted `err == nil` on reads of never-written
streams — for example after a rolled-back append — must now accept the
sentinel.

## 2026-11-07 — Postgres `Anchor`/`CheckAnchor` fence on the horizon; the anchor role reads the position sequence

**What changes.** `postgres.Store.Anchor` and `CheckAnchor` now run a
horizon-fence round (spec §5.1.1) before computing stream heads. The
recorded anchor's bound is the position sequence's `last_value` when
`Anchor` is called with `upto == 0` — the highest position drawn,
committed or not, where it used to be the committed maximum — and a
caller's `upto` above `last_value` is clamped down to it. While any
transaction that existed when the round drew its fence xid is still in
flight — an append below the head, in any workspace — both calls wait
(near-idle polling, no locks held, no transaction open) until it settles,
or fail with `postgres: anchor fence still blocked` when their context
ends first. The DB principal running the anchor path now also needs
`SELECT` on `events_global_position_seq` (the fence's first statement
reads its `last_value`), in addition to the existing `events`/`anchors`
grants.

**Who is affected.** Callers of the `es.Anchorer` methods on the Postgres
backend: scheduled anchor/audit jobs now block for as long as an append
is in flight instead of returning immediately, and anchors recorded
under live appends no longer fail `CheckAnchor` with a false
`ErrAnchorMismatch` once those appends commit (the bug this fixes).
Operators of the anchor job's role: without the sequence grant the path
fails with a permission error where it used to work.

**What to do.** Nothing in code for correctness — an anchor recorded now
verifies forever after. Grant the anchor role `SELECT ON SEQUENCE
events_global_position_seq` (a later change reads the bound through
`pg_sequences`, which needs only `USAGE`). Pass `Anchor`/`CheckAnchor` a
context with a deadline where a bounded wait is required; the error
wraps the context's cause. Consumers that read
`Anchor.UptoGlobalPosition` expecting exactly the committed maximum
should expect the drawn horizon instead — at or above it whenever an
append is in flight.
