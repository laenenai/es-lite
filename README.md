# es-lite

A small event-sourcing library for Go. Aggregates, commands, and events
are defined in `.proto`; the append-only event log is the source of
truth; downstream consumers tail the log. Storage runs on **SQLite**
(here today) or **Postgres** (planned, behind the same interface).

es-lite is the deliberately-minimal sibling of the `eventstore`
framework. It keeps the ideas that earn their place — proto-defined
aggregates, the Decider model, a storage-neutral `Store` — and cuts the
heavy machinery (durable command-bus workflows, synchronous projections,
crypto-shredding, Cedar authz, mandatory multi-tenancy, tamper-evident
hash chains). See [docs/adr](./docs/adr) for the reasoning.

## Non-negotiables

History, **auditability**, and **time-travel** are load-bearing
requirements, and they are why the storage model is event sourcing rather
than "state + outbox": only an immutable, append-only log can answer
"what did aggregate X look like on 3 March, and why?" from the database
alone. See [ADR 0001](./docs/adr/0001-event-sourced-lite-architecture.md).

## Architecture in one screen

```
  command ──▶ aggregate.Runtime.Handle
                 │  load stream ─▶ fold via Decider.Evolve ─▶ Decide ─▶ encode
                 ▼
             es.Store.Append   (one tx; UNIQUE(stream_id,version) = optimistic concurrency)
                 │
                 ▼
         append-only events table   ◀── the log IS the outbox (no separate table)
                 │
                 ▼
   delivery.Poller  ── tails global_position from a durable checkpoint ──▶ Handler
     (fallback tick OR Wake signal: Postgres LISTEN/NOTIFY drops in here)
```

- **Storage is neutral.** `Store` moves `Envelope`s (opaque `Payload` +
  audit metadata); it knows nothing about proto or aggregates.
- **Delivery is at-least-once.** Handlers must be idempotent; the
  checkpoint guarantees nothing is skipped. A `Wake` channel lets a push
  source (Postgres `LISTEN/NOTIFY`) eliminate poll latency without
  changing the poller.
- **Deployment.** It's a library you embed. SQLite = single-writer, one
  pod. Postgres = many stateless replicas sharing one DB, with the relay
  run as a singleton (or SKIP LOCKED shards). See
  [ADR 0002](./docs/adr/0002-deployment-topology-and-delivery.md).

## Layout

```
es/                     Core API: Decider, Envelope, StreamID, Store, Codec, Meta, errors
aggregate/              Write-side Runtime: Handle, Load, LoadAsOfVersion, LoadAsOfTime
sqlite/                 SQLite backend (es.Store + delivery.Checkpoints) + embedded schema
delivery/               Poller (log-as-outbox relay) + Signal (wake seam)
cmd/protoc-gen-es-lite/ Codegen plugin: (es.v1.sum_type) → sealed interfaces + Codec
proto/es/v1/            The one framework proto option (sum_type)
examples/counter/       Worked aggregate: proto + Decider + full end-to-end test
gen/                    Generated Go (DO NOT hand-edit) — checked in
docs/adr/               Architecture Decision Records
```

## Defining an aggregate

One `.proto` per aggregate: a State message, one message per command and
event, and `Commands`/`Events` oneof containers annotated with
`(es.v1.sum_type)`. See
[`examples/counter/proto/counter/v1/counter.proto`](./examples/counter/proto/counter/v1/counter.proto).

```protobuf
message Events {
  option (es.v1.sum_type) = "Event";
  oneof variant {
    Initialized initialized = 1;
    Incremented incremented = 2;
  }
}
```

`task generate` emits the sealed `Event` interface, marker methods, and
`EventCodec` into the generated package. You hand-write only the
`Decider` (Initial/Decide/Evolve/IsTerminal) and its error sentinels —
see [`examples/counter/counter.go`](./examples/counter/counter.go).

Wire it up:

```go
store, _ := sqlite.Open(ctx, "file:events.db")
rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})

sid, _ := es.NewStreamID("counter", "main")
res, _ := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5}, es.Meta{})

// Time-travel:
past, _ := rt.LoadAsOfVersion(ctx, sid, 1)          // by version
past, _ = rt.LoadAsOfTime(ctx, sid, someTimestamp)  // by wall-clock

// Delivery:
p := delivery.NewPoller(store, store, handler, delivery.Config{Subscriber: "read-model"})
go p.Run(ctx)
```

## Toolchain

Versions are pinned in [`.mise.toml`](./.mise.toml) (Go 1.26+, buf,
task). Install [mise](https://mise.jdx.dev) and run `mise install`.

```sh
task              # list tasks
task generate     # buf lint + codegen (protoc-gen-go + protoc-gen-es-lite)
task test         # run the suite
task build vet    # compile / vet
```

On a clean checkout where `gen/` was deleted, use
`task generate:bootstrap` — the plugin imports `gen/es/v1`, so the
framework options are generated first (protoc-gen-go only), then the
plugin is built, then the full generate runs.

## Status

Early but broad. Implemented and tested end-to-end:

- Core (`es`, `aggregate`), SQLite backend, delivery poller, `protoc-gen-es-lite`.
- **Postgres backend** — shared tables + `workspace_id` + RLS + hash
  partitioning, workspace-scoped store, and the gap-safe `Drain` claim-relay
  (integration-tested against Postgres 17: `task test:pg`).
- **Crypto-shredding** — `keystore` + `shred` envelope encryption, with an
  **OpenBao** Transit adapter (integration-tested against OpenBao 2.6.1:
  `task test:openbao`). Payloads are encrypted at rest per workspace;
  `ForgetWorkspace` destroys the KEK.

- **NATS/JetStream delivery** (`natsjs`) — a `Publisher` that bridges the
  log to JetStream (metadata in headers, `Nats-Msg-Id = global_position`
  for dedup) as a `delivery.Handler`, plus durable-consumer projections.
  Integration-tested against NATS 2.10 + JetStream (`task test:nats`).

Planned: the `projection.Replay` primitive (ADR 0005) and optional
snapshots. Interfaces are shaped to accept them without breaking changes.
