# ADR 0034: The Server Stamps Decision Time; `decided_at` Is Only Intent

- **Status:** Accepted (implemented)
- **Date:** 2026-11-02
- **Supersedes:** ADR 0032 §2 (`OccurredAt` from `decided_at`)
- **Related:** contracts ADR-003 / CR-09 / HI-24; es-lite issue #10

## Context

`aggregate.Runtime.Handle` let a command that implements `es.DecidedAt` supply
the envelope's domain time: a zero `Meta.OccurredAt` was replaced with the
client's `decided_at`, and a differing non-zero value was rejected with
`es.ErrDecidedAtMismatch` (ADR 0032). Contracts rule 7 and contracts ADR-003
now require the opposite trust boundary. `decided_at` is the caller's claim
about when a decision happened, not the authority for the event time or for
an expiry-window check.

The security consequence is concrete. `AcceptCase` checks a derogation
window and `AcceptInvitation` checks an invitation expiry. If those deciders
compare the window against a client-supplied timestamp, a caller can backdate
an acceptance into a valid window. If the envelope's `OccurredAt` comes from
the same value, audit and time-travel ordering are also client-controlled.

## Decision

1. **The runtime, not the command or caller, sets the event time.**
   `aggregate.Runtime.Handle`/`HandleAt` always stamps `Meta.OccurredAt` from
   the server clock immediately before deciding. A caller-supplied
   `Meta.OccurredAt` is ignored. `OccurredAt` is never derived from
   `decided_at`, including when the command value is inside the allowed skew.
2. **A bounded skew check rejects implausible intent.** For a command that
   implements `es.DecidedAt`, an unset (`zero`) value still returns
   `es.ErrDecidedAtMissing` (unchanged contract). A value further than
   `es.MaxDecisionSkew` (five minutes) from server time returns a typed
   `*es.DecidedAtSkewError`, which matches `es.ErrDecidedAtSkew` with
   `errors.Is`; nothing is loaded or appended. `es.ErrDecidedAtMismatch` is
   retained for source compatibility but is no longer returned.
3. **The decider sees server time.** After the skew check, the runtime replaces
   the command's tagged `(es.v1.decided_at)` field with the same server
   reading before calling `Decide`. Generated commands are stamped through
   proto reflection; hand-written commands may implement
   `es.DecisionTimeSetter`. Therefore a decider that gates an expiry window
   must read `cmd.DecisionTime()` (not a separately supplied client value):
   it now observes server time. `Decide` remains a pure function of state and
   the already-normalized command; it still never reads a clock.
4. **Envelope and command cannot disagree about the decision instant.**
   `OccurredAt` and the normalized command decision time are taken from one
   clock reading per `Handle` call, so retries cannot move them and audit
   consumers see the same instant the decider validated.
5. **Legally relevant times remain payload facts.** es-lite does not add a
   field to an event or copy `decided_at` into one. Contracts must carry the
   legal instant explicitly (for example an `accepted_at` on an acceptance
   event), stamped from the server decision time exposed to the decider by
   this ADR.

## Exact mechanism for expiry-window deciders

A decider for an expiry-window command (today `AcceptCase`,
`AcceptInvitation`) must:

- take the window comparison instant from `cmd.DecisionTime()`; and
- never read the original client value from elsewhere or trust
  `Meta.OccurredAt`.

The runtime has already checked that value against the server clock and
replaced it with server time. A decider must not call `time.Now()` itself:
that would make replay non-deterministic and is already forbidden by the
`Decider` contract.

## Consequences

- The generated Connect command handlers and `cmdsvc.MetaFrom` need no special
  clock code: they leave `Meta.OccurredAt` zero and the runtime stamps it.
  `cmdsvc.MapError` maps the new skew sentinel to `invalid_argument`.
- The `(es.v1.decided_at)` option comment in `proto/es/v1/options.proto` and
  the generated counter fixture now describe this boundary.
- Existing callers that supplied `Meta.OccurredAt` to steer the event time
  lose that ability; they should fix their clock source or use a test clock
  at the runtime boundary. The event log's `RecordedAt` remains the
  storage-commit clock.
- `ErrDecidedAtMismatch` is dead in the runtime but kept exported so existing
  callers still compile. Removing it is a later breaking-release decision.
- Contracts must re-vendor `options.proto` and add the legally relevant
  server-stamped payload fields; those are contract changes, not runtime
  changes.

## Alternatives considered

- **Keep sourcing `OccurredAt` from `decided_at` after skew validation.**
  Rejected: within five minutes the client still chooses event ordering, and
  the envelope remains client-influenced rather than server-stamped.
- **Add a `now` argument to `Decider.Decide`.** Rejected for now: it breaks
  every existing decider and generated fixture, while normalizing the command
  gives the same guarantee for the only commands that need a decision time.
- **Trust `Meta.OccurredAt` as an internal-caller escape hatch.** Rejected:
  the runtime cannot distinguish an internal caller from a generated handler
  that copied a client value, and ADR-003 says the runtime stamps the clock.
- **Use a generated `SetDecisionTime` method only.** Workable, but every
  already-generated contracts package would need regeneration before the
  runtime could protect an expiry window. Reflection stamps tagged proto
  fields now; the setter interface covers non-proto commands.
