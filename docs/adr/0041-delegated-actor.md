# ADR 0041: Delegated Actor (One Level)

- **Status:** Proposed — deferred to the agent-API work; no implementation
  scheduled
- **Date:** 2026-11-18
- **Related:** ADR 0001 (auditability is non-negotiable), ADR 0029 (the
  tamper-evident hash and its canonical encoding), ADR 0040 (the optional-hash
  block precedent), RFC 8693 (`act` — the actor a subject acts through).

## 1. Context

A service acting on a user's behalf is now a normal production shape: an API
gateway forwards a request, an agent runtime executes a task, a background
worker applies a user's earlier decision. Today the log can record the user
(`Actor{Type: "user", ID: ...}`) or the service, never both, so the audit trail
loses the execution path. The questions that become unanswerable are concrete:
which service performed this action on Alice's behalf, and what did service S
do across all users?

Delegation is an audit fact about who caused an event, which is `Actor`'s job.
Putting it anywhere else splits the trail from the log:

- a `delegated_by` field on every command and event proto makes the audit truth
  depend on each event author remembering to set it, and churns every payload
  contract;
- a header or side table separates the fact from the event it belongs to, and
  cannot be verified with the chain.

This ADR records the shape for a future release. It is deliberately not
implemented yet: it belongs with the agent API, not the MVP identity work.

## 2. Goals, non-goals, anti-goals

**Goals**

- One event can record the service through which its actor acted.
- `Actor` stays the accountable subject: `Principal()` and the
  `actor_principal` index keep answering "everything Alice did" unchanged.
- The reverse query — what did service S do — is indexed and cheap.
- Existing events and their chain verification are byte-identical.
- The store round-trips the fact on both backends, or it is explicitly absent.

**Non-goals**

- Multi-hop delegation chains (user → service A → service B).
- Recording the consent grant, token id or claim that authorised the
  delegation. `Via` records the execution path, not the authority.
- Evaluating authorization. That is the caller's policy engine; the log only
  records who acted.
- Backfilling delegation onto historical events.

**Anti-goals**

- A hash change that invalidates any existing chain.
- A partially recorded delegation (a type without an id, or a `Via` with no
  actor).
- Nesting: a delegated actor cannot itself delegate.
- Excluding delegation from the hash as if it were telemetry. It is an audit
  fact; ADR 0040's exclusion applies to trace context only.
- A stored shape that only one backend can read.

## 3. Decision

### 3.1 The type

```go
// ActorRef identifies a principal without granting it the accountability of
// an Actor. It is a value type: the zero value means "not set", and no
// nesting is representable.
type ActorRef struct {
    Type string
    ID   string
}

func (r ActorRef) IsZero() bool        // both fields empty
func (r ActorRef) Principal() string   // canonical "type:id", "" when zero

type Actor struct {
    Type string
    ID   string

    // Via is the service through which this actor acted. Zero when the
    // actor acted for itself. One level only: an ActorRef cannot delegate.
    Via ActorRef
}
```

`Actor.IsZero` keeps its current meaning — no type and no id — and is
unaffected by `Via`. `Actor.Principal()` is unchanged, which is the point.

A value type, not `*Actor`, for three reasons: `Actor` is compared with `==`
in existing call sites, and a pointer would make two equal actors unequal; a
pointer would make nesting representable and turn "one level" into a rule
someone must remember; and a pointer aliases the caller's struct into the
envelope.

**Naming.** `DelegatedBy` would mean the delegator (the user), not the service.
`Via` reads as "the actor, via this service", matches RFC 8693's `act` shape
(subject = user, actor = service), and stays short in storage
(`via_principal`).

A service acting for itself is simply `Actor{Type: "service", ID: ref}` with a
zero `Via` — no special case in the log.

### 3.2 The hash

`CanonicalEnvelope` (`es/hash.go:50-69`) is a hand-written, length-prefixed
field sequence ending in the fixed 32-byte `prev_hash`. The delegation block is
appended **after `prev_hash`**, and only when `Via` is non-zero:

```text
canonical := <every existing field, unchanged> ‖ prev_hash(32)

if !Via.IsZero():
    canonical ‖= lenPrefixed("es-lite.delegation.v1")
              ‖  lenPrefixed(Via.Type)
              ‖  lenPrefixed(Via.ID)
```

Why after `prev_hash`: the old prefix is length-deterministic, so a verifier
knows exactly where the 32-byte trailer ends; anything after it is the optional
block. No peek-ahead heuristics, no collision between a payload's length prefix
and a marker, and an event with a zero `Via` produces exactly the bytes it
produces today.

The marker is domain separation and carries a version, so a future chain block
can follow it without ambiguity.

**Forward compatibility, stated plainly.** An event written with delegation
cannot be verified by a pre-v0.16 verifier: it recomputes the old preimage and
reports a broken chain. That is unavoidable when the fact is attested, and it
is a release-ordering constraint — every auditor, in-tree or reimplemented,
must be upgraded before the first delegated event is written. The ADR records
it so it is a decision, not a surprise.

**Golden vectors** (acceptance criteria):

1. The existing fixture, unchanged, still hashes to `goldenV1Hash`.
2. A fixture with a populated `Via` has pinned canonical bytes and a pinned
   hash.
3. A new event with a zero `Via` is byte-identical to the old encoding, not
   merely equal in hash.

### 3.3 Persistence

Postgres migration `0012` and SQLite `0010` add:

- `via_type text` (SQLite `TEXT`), `via_id text`, `via_principal text`;
- a partial index on `(workspace_id, via_principal)` in Postgres and
  `(via_principal)` in SQLite, `WHERE via_principal IS NOT NULL` — the index
  stays small while delegation is rare.

All three are NULL together or set together. Postgres gets the `CHECK`
(`(via_type IS NULL) = (via_id IS NULL) AND ...`). SQLite gets the columns via
`ALTER TABLE ADD COLUMN` and **no** `CHECK`: enforcing it there needs another
full `events` rebuild, one release after `0009` already rewrote the table.
All-or-nothing is enforced by the writer and its tests, and the divergence is
recorded here and wherever `feedtest` documents normalizations. A future
migration that touches `events` for another reason can add the constraint then.

Reads are strict, unlike ADR 0040's telemetry tolerance: a stored delegation
that is partial or unparseable is an audit-fact defect and fails the read,
matching the F34 policy for the audit UUIDs. `via_principal` is written from
`Via.Principal()` at append time, the same way `actor_principal` already is.

No backfill: historical events read as `Via` zero, which is true.

### 3.4 Validation and the append seam

`AppendParams` and `Meta` carry `Actor` by value, so they gain `Via`
automatically; no call site changes. The rule lives in `es` — the zero `Actor`
is valid, `Via` must be complete, and a `Via` with no accountable actor is
invalid — as `Actor.Validate`, so the two stores cannot drift. It returns the
new sentinel `ErrInvalidDelegation`. Postgres calls it from `validateAppend`
(covering `Append` and `AppendTx`); SQLite calls it inline in `Append`, as it
does today.

Nesting is unrepresentable in the type, so no runtime rule is needed.

### 3.5 Not chosen, and why

- **`DelegatedBy *Actor`.** Inverted name (see §3.1), pointer equality
  surprise, representable nesting, aliasing. Rejected.
- **A `delegated_by` field on the affected command/event protos.** Repetition
  per contract, audit truth depends on each author, and it churns when this
  field arrives anyway. Acceptable only if an es-lite release is impossible;
  it is not a design, it is a deferral.
- **A separate delegation event in the log.** Attests the grant, not the
  execution, and forces every reader to join two records to answer "who did
  this".
- **A slice of actors (a chain) now.** One level covers the present need; a
  future optional block can widen it without touching existing bytes.

## 4. Why one level

RFC 8693 allows nested `act` claims, so the wire can express a chain. The log
does not need to carry it yet: one executor is the current shape, and a chain
type would make every reader handle a list for a case that does not exist. A
future widening appends another optional hash block, so the format stays open;
only the Go field changes.

## 5. Costs, accepted knowingly

- Three nullable columns and a partial index on the log table.
- One conditional block in the canonical encoding, and the auditor-upgrade
  ordering that comes with it.
- A second definition of "principal" (`ActorRef.Principal`) that must stay
  byte-identical to `Actor.Principal`; a shared test pins both.
- SQLite carries no database-level all-or-nothing constraint for this field.

## 6. Consequences

**Positive**

- The log records who is accountable **and** who executed, without a second
  store or a per-contract field.
- "Everything Alice did" and "everything service S did" are both indexed
  answers.
- The fact is attested by the chain, unlike trace context.

**Negative**

- Pre-v0.16 verifiers cannot verify delegated events; upgrades must precede
  producers.
- One more optional path in the canonical encoder, and a golden vector that
  must be regenerated deliberately if the block ever changes.

## 7. Acceptance criteria

- `ActorRef` and `Actor.Via` exist with the documented zero and principal
  behaviour; partial `Via` is rejected with `ErrInvalidDelegation` by both
  stores.
- The three golden vectors above hold; `goldenV1Hash` is unchanged.
- An event appended with a `Via` reads back with it on both backends; an event
  without one reads back zero.
- The partial indexes exist and are used for the reverse lookup (asserted by
  the query plan in Postgres; by index name in SQLite).
- `feedtest` asserts the round trip and the absent case for every adapter.
- `GOWORK=off go test ./...` is green without services; the Postgres suite and
  `golangci-lint` stay green.

## 8. What is left to the builder

- The encoder's capacity and append mechanics for the new trailer (the ADR
  fixes its placement after `prev_hash`), and the delegated golden fixture's
  bytes.
- The migration numbers (`0012` Postgres, `0010` SQLite) against the heads at
  implementation time.
- The reverse-query surface: the column and index are in scope; a library
  method is not. A caller queries the column directly.
- The Flowdoc mapping, which is out of scope here: the token's `act` claim maps
  to `Via`, and ACLs evaluate the accountable `Actor`.
