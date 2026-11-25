# Read-your-writes example

A runnable, dependency-free example of the read-your-writes flow on SQLite: a
write returns a consistency token, a read waits (bounded) until a projection
has applied that position, and only then reads the projection.

```bash
go run ./examples/readwrite
```

Expected output:

```text
appended 2 events: token position 2, wire "AQAAAAAAAAAAAAI"
wait: reached=true status=live position=2
read model:
  note:demo: first note
  note:demo: second note
```

## How it fits together

- `sqlite.Open` is the event log. `projection.LogFeed` tails it from position
  0, polling every few milliseconds.
- `sqlite.NewProgressStore` is the projection checkpoint: a `(scope,
  projection)` row holding the highest applied position and the projection's
  state (`live`, `rebuilding`, `stalled`).
- `projection.Runner` dispatches each envelope to the in-memory `Notes` read
  model, then saves the checkpoint **after** the read-model update and publishes
  it. `AppendNotes` appends two events and `es.TokenFor` turns their append
  results into a token.
- `projection.Waiter` is assigned to `Runner.Progress`, so the same-process
  runner wakes it directly. `WaitFor` returns `Reached: true` only when the
  checkpoint is `live` and at or above the token's position; a timeout is
  `Reached: false` with a nil error.

`Runner.Scope` is `""`, and so is every `WaitFor` scope: SQLite is a
single-workspace backend and its envelopes carry an empty `Workspace`. The
database file is the tenant boundary.

## eventfeed note

In a deployment, the [`eventfeed`](../../eventfeed) server is the delivery path:
it adapts a fenced feed server into the `projection.Feed` the runner consumes
and owns the delivery cursor (`feed_checkpoints`) and lease. The runner's
progress store (`projection_checkpoints`) is the **applied** checkpoint — it
advances only after the read-model update — and `WaitFor` uses that checkpoint,
never the delivery cursor. This example uses `projection.LogFeed` directly over
SQLite so it runs with no feed server, broker or external service; the waiter
and checkpoint code exercised here are the same.

`LogFeed` is the single-writer path. Over Postgres it pages raw `ReadAll`
without a settlement fence, so run it with a single writer or a quiescent log,
or use `eventfeed` for concurrent appenders. The progress store's checkpoint is
unaffected either way: it describes what the read model applied, not what the
consumer was delivered.

The example has no command runtime, so it does not exercise a no-op command's
token (Task 1 owns that case).
