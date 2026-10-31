# ADR 0032: Bounded Conflict Retry and `OccurredAt` from `decided_at`

- **Status:** Accepted (implemented)
- **Date:** 2026-10-30

## Context

Two small gaps between `aggregate.Runtime.Handle` and what `contracts` needs
from every machine caller.

**Conflict retry.** `Handle` (`aggregate/runtime.go`) loads, decides and
appends once; on `es.ErrConflict` it returns the error and the doc comment
says "the caller reloads and retries". `contracts` §3.7 and convention 20 make
every executor, scheduler and rule implement that loop, and §13.3 asks for it
as a runtime option. Since `Decide` is pure and `load` re-reads the stream,
re-running the whole `Handle` body is safe by construction.

**`OccurredAt`.** `Envelope.OccurredAt` is "domain time, supplied by the
command" (`es/envelope.go`), but in code it comes from `es.Meta.OccurredAt`,
which the *caller* fills and `Handle` defaults to `time.Now()` when zero.
`contracts` convention 7 and §3.5 require commands that depend on time to
carry `decided_at`, the handler to set `Meta.OccurredAt = decided_at`, and
`Decide` never to read a clock. Today nothing stops a handler from forgetting
that, in which case the envelope says "now" while the decider checked a
mandate window against `decided_at` — exactly the envelope/payload
disagreement §3.5 forbids.

## Decision

### 1. `Runtime.WithConflictRetry(n int, backoff func(attempt int) time.Duration)`

A chaining option in the style of `WithUpcaster`/`WithSnapshots`. With it set,
`Handle` repeats load → `IsTerminal` → `Decide` → `Append` up to `n` additional
times when `Append` returns `es.ErrConflict`, sleeping `backoff(attempt)`
(honouring `ctx`) between attempts. `backoff == nil` means a default
full-jitter exponential schedule starting at 10 ms and capped at 500 ms.
Only `ErrConflict` is retried: `ErrTerminal`, `ErrConstraintViolated`,
decider rejections and store errors return immediately, as now. The final
error wraps the last `ErrConflict` so `errors.Is` still matches. `CommandID`
is minted once before the loop so every attempt carries the same id.

The default stays **no retry** (`n = 0`): a human-issued command through a UI
should surface the conflict with the current state (§9.11), not silently win.
ADR 0030's reaction runner and ADR 0031's generated handlers set it
explicitly.

### 2. `(es.v1.decided_at)` field option and the `DecidedAt` interface

A field option (`bool decided_at = 50023`, see ADR 0031's block) on a
`google.protobuf.Timestamp` field of a command message. For each command
variant carrying it, `protoc-gen-es-lite` emits, next to the `isCommand()`
marker it already adds:

```go
func (c *AddMember) DecidedAt() time.Time { return c.GetDecidedAt().AsTime() }
```

and `es` declares `type DecidedAt interface { DecidedAt() time.Time }`.
`Handle` then applies, before the `meta.OccurredAt.IsZero()` default:

- if `cmd` implements `es.DecidedAt` and the value is non-zero, and
  `meta.OccurredAt` is zero → `meta.OccurredAt = cmd.DecidedAt()`;
- if both are set and differ → return `es.ErrDecidedAtMismatch` (new sentinel)
  without appending. A caller that sets both is asserting two domain times;
  refusing is safer than picking one;
- if the field is tagged but zero → `es.ErrDecidedAtMissing`. `contracts`
  already marks the field `(buf.validate.field).required`, so this is a
  belt-and-braces check for callers that skip validation.

The option is rejected at generation time on a non-`Timestamp` field or on a
message that is not a `Command` variant.

## Consequences

- Convention 20's loop exists once, in es-lite, and convention 7's rule is
  enforced by the runtime for any command that declares `decided_at`; neither
  can be forgotten by a handler.
- `aggregate.Runtime` grows two fields and `Handle` a loop; `Result` is
  unchanged. Two new sentinels in `es/errors.go` and the NATS wire error
  mapping (`natsstore/wire.go` `errKind`) is untouched since both are
  runtime-side.
- Retrying re-runs `Decide` with fresh state, so a command that was a no-op
  on the first attempt may produce events on the second — correct, and the
  reason retry must not be combined with caching the first decision.
- The review's ES-02 (ambiguous NATS timeouts duplicating appends) is **not**
  addressed here: retry on `ErrConflict` is unrelated to retry on timeout.
  Noted so the two are not conflated.

## Alternatives Considered

### A free function `aggregate.HandleWithRetry(rt, …)` instead of an option

Workable, but the generated reaction runner and command handlers would each
need to know about it, and an option keeps `Handle` the single entry point
for metrics and tracing.

### Deriving `OccurredAt` by field name (`decided_at`) without an option

Rejected: convention 29 (tooling rides on options), and a payload field that
happens to be named `decided_at` in an event or a value type must not be
interpreted.

### Making `Meta.OccurredAt` always win over the command

Rejected: it keeps the forgetting failure mode (`Meta` zero → `now`) and
contradicts convention 7, which names the command as the source.

## Implementation notes

- **Method name.** The generated accessor is `DecisionTime() time.Time`, and
  `es.DecidedAt` declares that method, not `DecidedAt()`. protoc-gen-go
  already emits a struct field named `DecidedAt` for a proto field called
  `decided_at` — the name `contracts` uses on every such command — and Go
  forbids a field and a method of the same name on one type. The interface
  keeps the name `es.DecidedAt` so the option, the interface and the ADR
  still line up.
- **Unset field.** `timestamppb.(*Timestamp).AsTime()` on a nil pointer
  returns the Unix epoch, not the zero time. The generated method returns
  `time.Time{}` for an unset field so `Handle` can tell "missing" from 1970.
- **Order of checks.** `Handle` applies the `decided_at` rule before any I/O
  (not "before the `OccurredAt` default" after `Decide`, as the text has it):
  a mismatch or a missing value never costs a load and never appends. The
  observable behaviour is the one the ADR specifies.
- **Retry placement.** Audit defaults (`CommandID`, `CorrelationID`,
  `OccurredAt`) are filled once before the loop; `handleOnce` is the body
  the loop repeats. `DefaultBackoff` is exported so callers can compose it.
- **Plugin checks.** Besides the two the ADR lists, generation also fails
  when a message tags two fields or tags a repeated/map field.
