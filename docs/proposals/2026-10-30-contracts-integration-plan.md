# Implementation plan: es-lite changes for the `contracts` integration

- **Date:** 2026-10-30
- **Scope:** ADR 0029 (hash chain), 0030 (projection/reaction codegen),
  0031 (Connect command service codegen), 0032 (conflict retry, `decided_at`),
  0033 (BSR module).
- **Sizes:** S ≤ 1 day, M 2–4 days, L ≥ 1 week, for one engineer including
  tests.

## Hard sequencing constraints

Must land **before `contracts`' first production event**:

1. **ADR 0029 — hash chain.** A populated store is only partially attested
   forever (ADR 0029 §5). This is the owner's production gate (`contracts`
   design §9.9.6, §13.1).
2. **ADR 0030 §1 — option ownership.** The decision, the new extension
   numbers (`es.v1.aggregate`/`projection`/`reaction`) and `contracts`'
   migration off `ont.projection.v1alpha1.spec/reaction` must be settled while
   `v1alpha1` may still break (its rule 16). After the first production tag,
   `buf breaking` on both sides makes renumbering a real migration. The
   generated runner (ADR 0030 §2–3) can follow later.
3. **ADR 0033 — BSR module**, because item 2 is painful through vendoring.

Can follow: ADR 0030 §2–§4 codegen, ADR 0031, ADR 0032 (ADR 0031 and the
reaction runner depend on it, but hand-written retry loops are a stopgap).

## Dependency order

```
T0 prerequisites ─▶ T1 (0033 BSR) ─▶ T3 (0030 §1 options + contracts migration)
T0 ───────────────▶ T2 (0029 hash chain) ─────────────────────────┐
T3 ─▶ T4 (0032 retry + decided_at) ─▶ T5 (0030 codegen) ─▶ T6 (0031 cmdsvc)
                                                     T2 ◀── T5 gap detection headers
T7 docs/CI closes every task.
```

## Tasks

### T0 — Prerequisites and review items the plan leans on (S)

- Confirm the open review findings this plan touches: ES-03 (`recorded_at`
  sampling — fixed inside T2), ES-04 (relay reordering — mitigated by T2's
  gap-detection headers, not fixed), ES-02 (ambiguous retries — out of scope,
  flagged in ADR 0032). File a backlog item for ES-04's real fix.
- Pin `nats-server` 2.10+ in `Taskfile.yml` (`test:nats`, `test:pipeline`)
  for multi-filter consumers (already `nats:2.10`).
- Files: `docs/backlog/2026-10-30_relay-ordering.md`.

### T1 — ADR 0033: name and push the buf module (S)

- `buf.yaml`: `name: buf.build/laenenai/es-lite` on the `proto` module.
- `.github/workflows/release.yml`: `buf push proto --label ${{ github.ref_name }}`;
  `ci.yml` on `main`: `buf push proto --label main`. Secret `BUF_TOKEN`.
- README "Using es-lite as a library": consumer `buf.yaml` snippet.
- Tests: none in-repo; `buf push` has no dry run, so validate the workflow
  once with a throwaway label from a PR branch, then delete the label.
- `contracts` side: `deps: [buf.build/laenenai/es-lite]`, delete
  `third_party/`, `buf dep update`, `task generate:check`.

### T2 — ADR 0029: hash chain (L)

**es core**
- `es/hash.go`: `EnvelopeHash(Envelope) [32]byte`, `ZeroHash`,
  `UnchainedHeadHash(sid, version)`, canonical encoder (length-prefixed,
  big-endian, µs timestamps). `es/envelope.go`: `PrevHash`, `Hash []byte`.
- `es/verify.go`: `VerifyStream(ctx, Store, StreamID) (ChainReport, error)`;
  `ChainReport{ChainedFrom, Head, HeadHash, BrokenAt, Err}`; `ErrChainBroken`.
- Tests: `es/hash_test.go` with **golden vectors** (fixed envelope → fixed
  hex) so an external auditor can validate a reimplementation; `verify_test.go`
  over an in-memory fake store with a mutated payload, a swapped order, and a
  truncated tail (anchor-detectable only — assert the report says so).

**storage**
- `sqlite/migrations/0002_hash_chain.sql`, `postgres/migrations/0002_hash_chain.sql`:
  nullable `prev_hash`/`hash`, `anchors` table, Postgres partial index on
  `hash IS NULL`, RLS policy on `anchors` (admin/bypass only).
- `sqlite/store.go Append`: head query returns `(version, hash)`; sample
  `recorded` after it; truncate timestamps to µs; compute chain; insert both
  columns. `queryEnvelopes` scans them.
- `postgres/wsstore.go Append`: same; `SELECT clock_timestamp()` after the
  head read; `INSERT` passes `recorded_at` explicitly. `postgres/store.go
  envelopeCols`/`scanEnvelope` scan the two columns (fold the review's CLN-02
  duplication while here if cheap).
- `postgres/anchor.go`, `sqlite/anchor.go`: `Anchor(ctx, uptoGP) (Anchor, error)`
  and `ListAnchors`; Postgres under `app.bypass_rls`.
- Tests: `sqlite/chain_test.go` (chain across batches, race loser leaves no
  fork, `recorded_at` monotonic across two interleaved transactions — the
  ES-03 regression), `postgres/chain_test.go` (same via `task test:pg`),
  `sqlite/migrate_test.go` extended to assert version 2 and the
  populated-store `ChainedFrom` behaviour (write rows under 0001, migrate,
  append, verify).

**transport**
- `natsjs/natsjs.go`: `HdrHash`, `HdrPrevHash`; `toMsg`/`DecodeEnvelope`.
  `natsjs/consumer.go`: optional `GapDetector` (last hash per stream) with
  `Nak` on mismatch. Test in `natsjs_test.go` by publishing v1, v3.
- `natsstore/wire.go`: `envelopeWire.PrevHash/Hash`; `conv.go` round-trip.
  `server_test.go`: client `VerifyStream` over the NATS store passes.

**tooling**
- `cmd/es-verify/main.go`: `stream`, `all`, `anchor`, `anchor --check`;
  env parity with `es-migrate`. Dockerfile: fourth entrypoint. `es-lited`:
  optional `ANCHOR_INTERVAL` under the `leader` election (reuse the relay
  wiring).
- `aggregate/runtime.go`: `WithChainVerification()`; `load` recomputes hashes.
  `runtime_test.go`: tampered row fails the load.
- Docs: README security section (remove "no hash chain"), `docs/deploy`
  (anchor export runbook), ADR index.

### T3 — ADR 0030 §1: options move and `contracts` migration (S es-lite / S contracts)

- `proto/es/v1/options.proto`: `Aggregate`, `FetchMode`, `RebuildMode`,
  `ProjectionKind`, `ProjectionSpec`, `ReactionSpec`; extensions 50003, 50010,
  50011. Update the file's "two options" comment. `task generate` (bootstrap
  path still works: options compile with `protoc-gen-go` alone).
- `examples/counter/proto/counter/v1/counter.proto`: `option (es.v1.aggregate)
  = { stream_type: "counter" }` on `Counter`; a `CounterView` message with
  `(es.v1.projection)` and a `ResetOnClosed` descriptor with `(es.v1.reaction)`
  — the fixtures T5 generates against.
- Tests: `buf lint`, `buf breaking` clean; `task generate:check`.
- `contracts`: ADR 0030 §1 steps 2–4; `buf lint`; regenerate.

### T4 — ADR 0032: conflict retry and `decided_at` (S)

- `aggregate/runtime.go`: `WithConflictRetry`, loop in `Handle`, `DecidedAt`
  handling; `es/errors.go`: `ErrDecidedAtMismatch`, `ErrDecidedAtMissing`;
  `es/decided_at.go`: the interface.
- `proto/es/v1/options.proto`: `extend FieldOptions { bool decided_at = 50023; }`.
- `cmd/protoc-gen-es-lite/main.go`: emit `DecidedAt()` for tagged variants;
  generation error on wrong field type / non-command.
- Counter example: add `decided_at` to `Close`.
- Tests: `aggregate/runtime_test.go` — two runtimes on one SQLite stream,
  injected conflict succeeds on retry within `n`, fails past `n`, `ctx` cancel
  stops backoff; `OccurredAt` taken from the command; mismatch and missing
  sentinels. Plugin golden test: regenerated `gen/counter/v1/counter_es.pb.go`.

### T5 — ADR 0030 §2–§4: projector and reaction codegen (L)

- `natsjs`: `SubjectFor(ws, streamType, typeURL)`; `ConsumerConfig.FilterSubjects`.
- New package `reaction`: `Dispatcher[C]`, `RuntimeDispatcher(rt)` adapter,
  `Issue{StreamID, Command}` collector, the executor loop (conflict retry via
  T4, `ErrTerminal` = done).
- New package `projection` additions: `Runner` helper the generated code
  calls (marker load/skip/save, ack/nak, gap detector, rebuild via `Replay`).
  Keep generated code thin; logic lives here and is unit-tested once.
- `cmd/protoc-gen-es-lite/main.go`: split into `main.go`, `sumtype.go`,
  `projection.go`, `reaction.go`, `resolve.go` (descriptor lookup across
  files: `consumes`/`issues` → variant check → package → `(es.v1.aggregate)`
  message). Generation errors listed in ADR 0030 §2/§4. Optional
  `docs=<path>` plugin parameter rendering the §9.7-style table.
- Tests: plugin golden files for the counter fixtures (`gen/counter/v1/…`);
  `examples/projection/` end-to-end on SQLite + embedded runtime as loader
  (no NATS) and, under `task test:pipeline`, the generated runner against
  JetStream with two filter subjects; rebuild equals live (same read model
  after `Rebuild` from 0 and after live consumption); reaction runner test
  where the issued command conflicts once and where the target stream is
  terminal.
- Docs: README "Using es-lite as a library" projection section; ADR 0005
  errata pointing at the generated runner.

### T6 — ADR 0031: Connect command service codegen (M)

- `proto/es/v1/options.proto`: `CommandService`, extensions 50020–50022;
  `proto/es/v1/cmdsvc.proto`: `VersionConflict`.
- New nested module `cmdsvc/` (`go.mod` with `connectrpc.com/connect`,
  `protovalidate-go`): `IdentitySource`, `Validator`, `ErrorMapper`,
  `MapError(err, reload func) *connect.Error`, `WriteResult` setter by field
  name via protoreflect.
- Plugin: `service.go` — validate RPC shape, emit handler struct, per-RPC
  methods, `Authorizer` and typed `IdentitySource` interfaces.
- Fixture: `examples/counter/proto/counter/v1/counter_service.proto` (+
  `protoc-gen-connect-go` added to `buf.gen.yaml` for the examples module
  only). Tests: `examples/cmdsvc/` — httptest server, happy path returns
  `WriteResult` + state; conflict returns `FailedPrecondition` with the
  detail and current state; principal field overwritten even when the client
  sets it; validation failure maps to `InvalidArgument`.
- Docs: README; `contracts` side writes its first `ont/api/...` service.

### T7 — Docs, index, CI (S)

- `docs/adr/README.md`: rows 0029–0033 (status Proposed → Accepted as each
  lands); the "Relationship to `eventstore`" paragraph no longer lists the
  hash chain as cut.
- `README.md`: security model, library section, ADR count.
- `ci.yml`: `es-verify` smoke in the pipeline job (`es-verify all` exits 0
  after the pipeline test).

## Summary table

| Task | ADR | Size | Blocks | Before first prod event |
|---|---|---|---|---|
| T0 prerequisites | — | S | T2 | — |
| T1 BSR module | 0033 | S | T3 | yes (enables T3) |
| T2 hash chain | 0029 | L | T5 (headers) | **yes** |
| T3 options move + contracts migration | 0030 §1 | S + S | T4, T5 | **yes (decision + descriptors)** |
| T4 retry + decided_at | 0032 | S | T5, T6 | no |
| T5 projection/reaction codegen | 0030 §2–4 | L | T6 (shared plugin refactor) | no |
| T6 command service codegen | 0031 | M | — | no |
| T7 docs/CI | all | S | — | with T2 |

Critical path to production: T0 → T1 → T3 and T0 → T2 in parallel; about
two to three engineer-weeks, dominated by T2.
