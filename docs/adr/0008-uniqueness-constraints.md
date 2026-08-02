# ADR 0008: Uniqueness Constraints

- **Status:** Accepted (amends ADR 0003 — Decider signature)
- **Date:** 2026-08-02

## Context

Some invariants are not expressible inside a single aggregate: "email is
unique across all users", "this slug is taken". A `Decider.Decide` sees
only its own stream's state, so it cannot know whether another stream has
already claimed a value. Enforcing such a constraint needs a single
serialization point across streams.

ADR 0001 deliberately left cross-aggregate uniqueness out of es-lite (the
parent framework's `unique_claims` was cut). This ADR reintroduces it —
the smallest correct mechanism — because it is a genuine capability many
domains need and cannot safely emulate in application code.

The natural question is whether to solve it by **exposing the storage
transaction** so callers do their own check-and-insert, or by making
uniqueness a **first-class store capability**. This ADR chooses the
latter and says why.

## Decision

### Do not expose the storage transaction

`Store` stays transaction-neutral. Exposing a `Tx` would:

- **Break storage neutrality.** SQLite's transaction is
  `database/sql.Tx`; Postgres's is `pgx.Tx`. Handing either back through
  the `Store` interface leaks the backend and makes code un-writable
  against `es.Store`.
- **Leak a correctness-critical invariant into every caller.** "Run your
  uniqueness check inside my transaction" invites check-then-insert
  races and forgotten releases in user code.

The parent framework made the same call. Uniqueness is modelled as data
the store understands, not as a transaction it lends out.

### Uniqueness is a first-class constraint applied in the append transaction

A `unique_claims` table with a `UNIQUE` index is the serialization point.
`Decide` emits **constraint operations** alongside events; `Store.Append`
applies them **in the same transaction** as the event insert. A unique
violation rolls the whole transaction back — no event is persisted — and
returns `es.ErrConstraintViolated`.

```go
type ConstraintOp struct {
    Op    ConstraintOpKind // Claim | Release
    Scope string           // namespace, e.g. "user.email"
    Value string           // the value being made unique
    PII   bool             // true => store a keyed hash, not plaintext
}
```

Table (Postgres shown; SQLite mirrors without RLS/partitioning):

```sql
CREATE TABLE unique_claims (
    workspace_id text NOT NULL,
    scope        text NOT NULL,
    value_key    bytea NOT NULL,   -- plaintext bytes, or HMAC for PII
    stream_id    text NOT NULL,
    PRIMARY KEY (workspace_id, scope, value_key)
);
```

Renaming a unique value is one command emitting `Release{old}` +
`Claim{new}`, applied atomically with the event.

### The Decider signature changes

`Decide` returns constraint operations together with events, so a single
decision commits atomically (amends ADR 0003):

```go
Decide func(state S, cmd C) (events []E, constraints []ConstraintOp, err error)
```

Aggregates with no uniqueness return `nil` constraints — a mechanical,
one-line change. es-lite is pre-release, so the break is acceptable and
preferable to splitting one atomic decision across two functions.

### Scope is per-workspace by default

`value_key` is unique within `(workspace_id, scope)`. At 100k workspaces
(ADR 0004) uniqueness is almost always intra-workspace ("email unique
within this workspace"), and it falls out of `workspace_id` leading the
key plus RLS. Cross-workspace/global uniqueness is possible (omit the
workspace from the key) but is the rare case and opt-in.

### PII values are HMAC'd under the workspace key

Storing a raw email in `unique_claims` would reintroduce PII that
crypto-shredding (ADR 0004) is meant to erase — the claim row would
survive a workspace shred. So for `PII: true` ops, `value_key` is
`HMAC(workspaceUniquenessKey, scope || value)`, where the key is derived
from the workspace's DEK (via the `shred` layer). Uniqueness still holds
(the HMAC is deterministic within the workspace); after the workspace is
shredded the key is gone and the stored hashes are unlinkable to any
value — **erasure stays complete**. Non-PII scopes (a public slug) store
plaintext bytes.

`unique_claims` therefore joins the erasure surfaces of ADR 0004 §4: for
PII claims, shredding the workspace key is sufficient; the rows may remain
as inert hashes.

## Consequences

### Positive

- **Strongly consistent uniqueness, atomically** with the event that
  needs it. No saga, no eventual-consistency window, reusing the
  transaction Append already opens.
- **Storage stays neutral.** No Tx leaks; the same `Decider`/`Store`
  contract works on both backends.
- **Erasure remains complete** for PII uniqueness via the HMAC design.

### Negative

- **Decider signature break.** Every `Decide` gains a `[]ConstraintOp`
  return (usually `nil`). One-time, mechanical.
- **`unique_claims` is another derived surface** to reason about at shred
  time (mitigated: HMAC makes PII rows inert; plaintext rows for non-PII
  are non-sensitive).
- **The uniqueness HMAC key must be stable for the workspace's life.**
  Collision detection compares new HMACs against stored ones, so the key
  cannot rotate without re-hashing all of a workspace's claims. es-lite's
  DEKs are generated once (not auto-rotated), so this holds today; a
  future DEK-rotation feature must rewrap claims or derive the uniqueness
  key from a non-rotating per-workspace secret. Documented, not solved
  here.
- **Single-database only.** The claim table must share the transaction
  with the events, so it lives in the same database. Genuinely cross-
  database uniqueness needs the reservation-aggregate pattern below.

## Alternatives Considered

### Expose the storage transaction

Rejected — breaks storage neutrality and pushes a correctness-critical
check into user code. See Decision.

### Uniqueness as its own reservation aggregate + process manager

Model the unique value as a stream ("user.email:a@b.com") that is claimed
in a two-phase reservation, coordinated by a saga. This is the right
answer when the constraint spans databases or services (no shared
transaction). It is heavier — eventual consistency, compensation on
failure — and unnecessary when events and claims share one database, so
it is out of scope for es-lite's single-DB backends. Noted as the
escalation path.

### Check a read-model unique index before accepting the command

Rejected as unsound: an async projection is eventually consistent, so two
concurrent commands both pass the check and both succeed. Making the
read-model write synchronous and in-transaction just re-derives the
claim-table design with more moving parts.
