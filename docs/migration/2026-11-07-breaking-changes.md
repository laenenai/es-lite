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
