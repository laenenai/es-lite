# Breaking changes

es-lite is pre-1.0: the module may break, but it must never surprise.
This file collects every breaking change as it lands — one entry per
change, each stating what changed, who is affected, and what to do.
Entries are grouped by the change date and never rewritten after landing.

## 2026-11-07 — `feed.Server.Skip` refuses to rewind the checkpoint without `force`

**What changes.** `Skip` used to accept any position: a skip below the
current checkpoint silently rewound the subscription (Reset semantics)
and recorded the dead letter. It now refuses such a request with
`feed.ErrInvalidRequest` — "skip position %d is behind the checkpoint %d;
pass Force to rewind" — before recording anything, so both the cursor and
the audit trail stay untouched by the refused call. `force` keeps the
deliberate rewind semantics exactly.

**Who is affected.** Callers that used a below-checkpoint `Skip` as a
replay trigger: those calls now fail until they pass `force`.
Poison-path skips (at or above the checkpoint) are unaffected, and
re-skipping the event at the cursor stays idempotent.

**What to do.** Pass `force: true` where the rewind is genuinely wanted;
for deliberate replays prefer `ResetCheckpoint`, which re-identifies the
checkpoint under its definition hash.

## 2026-11-07 — A duplicate caller-supplied `EventID` is `ErrDuplicateEventID`, not `ErrConflict`; corrupt audit UUIDs fail reads

**What changes.** An append whose `EventID` already exists in the log
used to surface as `es.ErrConflict`, discarding the underlying database
error. Both adapters now classify it as `es.ErrDuplicateEventID` —
Postgres by constraint name (`events_workspace_id_event_id_key` /
`events_event_id_key`), SQLite by the driver's constraint message — so a
permanent duplicate is no longer mistaken for a retryable
optimistic-concurrency clash (which stays `ErrConflict` via the version
constraint). In the same change both row scanners reject malformed audit
UUIDs: a row whose `event_id`, `correlation_id`, `causation_id` or
`command_id` is not a UUID now fails the read with the column named and
the value's length (never its bytes) instead of silently decoding to a
zero UUID.

**Who is affected.** Direct store callers that branched on
`errors.Is(err, es.ErrConflict)` for duplicate event ids: they now see
the new sentinel (the aggregate runtime never sets `EventID` itself, and
the deterministic-event-ids option collides on this constraint by
design). Readers of stores seeded with corrupt audit columns: rows that
used to scan as zero UUIDs now error.

**What to do.** Match `errors.Is(err, es.ErrDuplicateEventID)` for the
duplicate case; keep `ErrConflict` for version races. Fix or clean rows
with malformed audit UUIDs — they were never valid audit facts.

## 2026-11-07 — The fence requires sequence `CACHE 1` and fails loudly otherwise

**What changes.** The position sequence's `cache_size` is asserted on
every fence round: a sequence configured with `CACHE n > 1` — which
silently breaks the fence's lossless proof by handing out positions after
`last_value` has already advanced past them — now fails the feed-facing
reads and the anchor path with
`postgres.ErrSequenceCacheMisconfigured`, whose message names the fix
(`ALTER SEQUENCE events_global_position_seq CACHE 1`). The fence bound is
read through the `pg_sequences` view, which also means the feed and
anchor roles need only `USAGE` on the sequence, never `SELECT`.

**Who is affected.** Deployments that had `ALTER SEQUENCE … CACHE n > 1`
in place: feed pages and anchor calls now error instead of running
inside the loss window. A deployment that appeared to work with a cached
sequence was already exposed to permanently skipped events; the error is
the fix, not a regression.

**What to do.** Keep the sequence at `CACHE 1` — the database default.
ADR 0038 records why; the deployment runbook carries the invariant.

## 2026-11-07 — `AppendTx`/`ReadStreamTx` reject a connection pool

**What changes.** The Postgres caller-owned-transaction methods
(`WorkspaceStore`'s `AppendTx` and `ReadStreamTx`) now refuse a
`*pgxpool.Pool` passed as the `DBTX` argument — "requires an active
transaction, not a pool" — instead of silently running every statement
in its own autocommit transaction, which scattered the append's xid
draw, its inserts and the workspace setting across transactions: no
error, all guarantees gone.

**Who is affected.** Callers that passed a pool where a transaction was
required: they now get an immediate, named error instead of a
compromised append. Genuine transaction handles are unchanged.

**What to do.** Pass the caller's `pgx.Tx` (or another transactional
`DBTX`); the error names exactly what is missing.

## 2026-11-07 — Feed definitions reject control bytes in consumed types

**What changes.** `Subscribe`'s definition validation now rejects any
`consumes` entry containing an ASCII control byte with
`feed.ErrInvalidRequest`, naming the offending type. A control byte is
never legitimate in a type URL, and adapters join subscription keys and
type filters with byte-level concatenation (SQLite joins key parts with
NUL), so such an entry could make distinct definitions or key triples
collide into one.

**Who is affected.** Callers that subscribed with control bytes inside
consumed type names — a shape that was already ambiguous — now get an
immediate validation error instead of a silently colliding subscription.

**What to do.** Nothing for legitimate proto type URLs; the refusal is
the same byte-level hygiene the subscription key already had.

## 2026-11-07 — `es.ErrDecidedAtMismatch` is removed

**What changes.** `es.ErrDecidedAtMismatch` — exported "for callers
compiled against ADR 0032" but returned by nothing since ADR 0034 made
the server clock the only authority for event time — is gone, together
with the `cmdsvc.MapError` arm that mapped it to `invalid_argument`. The
decided_at family keeps its live rejection sentinels —
`es.ErrDecidedAtMissing` (field unset), `es.ErrDecidedAtSkew` /
`*es.DecidedAtSkewError` (outside the trust window) — and gains
`es.ErrDecidedAtUnstampable` (its own entry follows).

**Who is affected.** Code that references `es.ErrDecidedAtMismatch` — a
compile break; nothing returned it, so no runtime behavior changes.
Callers of `cmdsvc.MapError`: the `invalid_argument` arm for it is gone
(no error can carry it anyway).

**What to do.** Delete references to the removed sentinel; use
`errors.Is(err, es.ErrDecidedAtSkew)` for the trust-boundary rejection
and `es.ErrDecidedAtMissing` for the unset case.

## 2026-11-07 — An unstampable `decided_at` command is rejected loudly

**What changes.** `es.ErrDecidedAtUnstampable` is new, and
`aggregate.Runtime.Handle`/`HandleAt` now return it — the error names the
command type — when a command implements `es.DecidedAt` but
`es.StampDecisionTime` cannot replace its decision time with the server
clock: the command is neither an `es.DecisionTimeSetter` nor a proto
message with a tagged `(es.v1.decided_at)` field. Such a command was
previously accepted silently and ran Decide against the client's clock
value; generated proto commands are unaffected (their tagged Timestamp
field is always settable).

**Who is affected.** Hand-written command types that implement
`DecisionTime()` without a `SetDecisionTime` method (and are not proto
messages): a `Handle` that silently succeeded now fails loudly —
`errors.Is(err, es.ErrDecidedAtUnstampable)` — before anything is loaded
or appended.

**What to do.** Give a hand-written `es.DecidedAt` command a
`SetDecisionTime(time.Time)` method so the runtime can stamp it — a
command that only reports a decision time it can never have replaced is a
broken command, and the runtime now says so.

## 2026-11-07 — `es.Meta.OccurredAt` is removed

**What changes.** `es.Meta.OccurredAt` — documented as "ignored by
Runtime" since ADR 0034 made the server clock the only source of event
time — is gone; `es.Meta` carries identity and audit fields only
(`CommandID`, `CorrelationID`, `CausationID`, `Actor`). The runtime takes
its one server-clock reading straight onto each event's
`Envelope.OccurredAt` (`es.AppendParams.OccurredAt` is unchanged), so
the field was a dead end: a caller could set it, and the runtime
discarded it.

**Who is affected.** Code that sets or reads `meta.OccurredAt` — a
compile break, and the only behavior change is that the code now matches
the truth: the value was already ignored. No recorded event changes,
because the runtime never took the field's value.

**What to do.** Delete the field references. There is nothing to migrate
to: event time is the server's by design (ADR 0034) — a caller setting
`Meta.OccurredAt` to steer event time was already having no effect.

## 2026-11-07 — Generated command-service handlers gain draft-compliant `Idempotency-Key` dedup

**What changes.** The `<Name>Deps` struct protoc-gen-es-lite emits for an
`(es.v1.command_service)` grows `Dedup es.Deduplicator` — the storage port
behind draft-ietf-httpapi-idempotency-key-header-07's stored-response
flow (design D6). `Dedup` is nil by default, and a nil `Dedup` — or a
request carrying no `Idempotency-Key` — keeps today's handler behavior
byte for byte: no lookup, no fingerprint, no save. Wired, a key-carrying
request runs the draft flow after `Authorize` (an unauthorized replay
must leak nothing: no stored response, no fingerprint oracle): the
handler fingerprints the request (`cmdsvc.Fingerprint`, SHA-256 hex of
the marshaled request — the draft §2.4 checksum-of-payload option) and
looks the command up under `es.DedupKey{StreamID, CommandID}` (the
header's UUID as `cmdsvc.MetaFrom` maps it). A hit with a matching
fingerprint returns the stored response without handling (draft §2.6); a
hit with a different fingerprint is key reuse, rejected with
`cmdsvc.IdempotencyKeyReused()` — Connect `aborted`, the message naming
HTTP 422 semantics (draft §2.7); a miss handles the command and saves
the marshaled response for the next retry. `cmdsvc.MapError` now maps
`es.ErrDuplicateEventID` to `cmdsvc.IdempotencyConcurrent()` — Connect
`aborted`, the message naming HTTP 409 semantics — so the concurrent
duplicate that loses on the event-id constraint is reported per draft
§2.6's concurrent window; that arm fires only when the runtime derives
event ids from the command id, so wire
`Runtime.WithDeterministicEventIDs()` alongside `Dedup`. The composite
key of draft §5 is complete in the adapter: the record is scoped to the
workspace (the client-attribute component — sqlite by being
single-workspace, postgres through the `command_dedup` table's RLS)
plus the stream plus the key's UUID. A `Dedup.Save` failure after a
committed command returns `internal`: the response was produced but not
stored, so the client's retry with the same key re-handles the command,
collides on the derived event ids and is reported `aborted` until it
picks a fresh key or the record is recovered by hand — never a silent
success and never a second append.

**Who is affected.** Applications wiring a generated handler's `Dedup`:
requests carrying an `Idempotency-Key` stop double-appending on retry,
and a concurrent duplicate is reported `aborted` — a service that wires
no conflict retry can still surface a bare version conflict for a
duplicate whose load interleaved the winner's commit (the runtime's
optimistic check fires before the event-id insert), which is one more
reason to wire `WithConflictRetry` on a machine-facing edge (ADR 0032):
the retry re-loads and then loses on the event-id collision. Callers of
`cmdsvc.MapError` that classified `es.ErrDuplicateEventID` themselves —
it used to fall through to the app's mapper or `failed_precondition`:
the new arm claims it first.

**What to do.** Nothing without a wired `Dedup`. To adopt: wire
`sqlite.NewDedupStore(store)` (or `postgres.NewDedupStore` on the
workspace), construct the runtime with `.WithDeterministicEventIDs()`
and its conflict-retry budget, and treat an `aborted` error naming HTTP
409 as "this key's command already committed — use a fresh key", never
as a retry cue.

## 2026-11-07 — `aggregate.Runtime` gains opt-in deterministic event ids derived from the command id

**What changes.** `aggregate.Runtime` gains `WithDeterministicEventIDs()`,
an opt-in builder. When set, the runtime fills every appended event's
`EventID` by deriving it from the command's id instead of leaving it zero
for the store adapter to mint a fresh UUIDv7:
`uuid.NewSHA1(esLiteCommandNamespace, []byte(meta.CommandID.String() + ":" + strconv.Itoa(i)))`
— command id, colon, 0-based index in the command's batch, hashed in a
fixed namespace. A nil command id never derives (the runtime keeps minting
one per Handle, and those events behave exactly as before). The option is
off by default and today's behavior is unchanged without it: the same
command handled twice still appends twice, under two distinct minted ids.
With it, the retry derives the same ids as the first run and the second
append collides on the store's `events.event_id` unique constraint,
surfacing `es.ErrDuplicateEventID` instead of silently duplicating events.
`esLiteCommandNamespace` is a fixed package-level UUID, pinned forever
once shipped — changing it would silently re-key every derivation and
break dedup against already-stored ids.

**Who is affected.** No one by default — the option is additive and off.
Callers that retry commands (`WithConflictRetry` users, reaction runners,
generated command services) and anyone needing at-most-once appends per
command id benefit: enabling the option turns a duplicated re-append into
a surfaceable `es.ErrDuplicateEventID`. Note the ids are unique across
the whole log, not per stream: the same command id deriving ids on a
different stream collides too, by intent — one command id appends one
batch of events, ever.

**What to do.** Nothing, unless you want the dedup signal: construct the
runtime with `.WithDeterministicEventIDs()` and treat a returned
`errors.Is(err, es.ErrDuplicateEventID)` as "this command already ran" —
an idempotency signal, never a retry cue. Never re-derive ids under a
different namespace: `esLiteCommandNamespace` is pinned and must never
change.

## 2026-11-07 — `ReadStream` reports `ErrStreamNotFound` for never-written streams

**What changes.** `es.Store.ReadStream` used to return `(nil, nil)` for a
stream that has never been written, which made "not found"
indistinguishable from "empty". Both adapters (`sqlite.Store` and
`postgres.Workspace`) now return an error wrapping `es.ErrStreamNotFound`,
with the canonical stream id in the message, when a read starts at the
beginning (`fromVersion == 0`) of a stream that has no events at all. How
the adapters know that depends on the bound's shape: an unbounded read
(`toVersion == 0`) proves it from the empty result itself — from the
start of the stream with no upper bound, zero rows already means no row
exists — while a bounded empty result is ambiguous (the stream's first
rows could sit above the bound), so there the adapters run an existence
probe and keep `(nil, nil)` when the stream exists. Empty ranges are
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

## 2026-11-07 — `es.Anchorer` gains `OrderRows`; `es.Anchor` gains order fields

**What changes.** The `es.Anchorer` interface gains
`OrderRows(ctx context.Context, upto uint64) ([]es.OrderRow, error)`,
which returns the log_order rows with `to_position <= upto` (0 = the
current log head, as for `Anchor`) — the rows an anchor at `upto`
attests with its order root, which `es-verify order` recomputes each
batch hash from. `es.Anchor` gains three fields: `OrderRootHash
[]byte` (`order_root_hash`, omitted when nil — the anchor's order root
over those rows), `OrderFromPosition uint64` (`order_from_position`,
the first attested position, 0 when none are) and `OrderCount uint64`
(`order_count`, how many rows the root aggregates). The accompanying
types and functions — `es.OrderEntry`, `es.OrderRow`,
`es.BatchOrderHash`, `es.OrderRoot` — are new and additive. The
in-repo `sqlite.Store` and `postgres.Store` carry temporary
`OrderRows` stubs that return an error; the real implementations land
with log-order attestation (design D1), and nothing in this module
calls `OrderRows` yet.

**Who is affected.** Third-party `es.Anchorer` implementations: adding
a method to an interface is a compile-time break — their types no
longer satisfy `es.Anchorer` until they grow an `OrderRows` method
(the in-repo adapters hit exactly this). Consumers that serialize
`es.Anchor` as JSON see the new fields: `order_root_hash` is omitted on
legacy anchors (recorded before log-order attestation; such anchors
verify heads-only — order comparison skips the order fields when it is
nil), while `order_from_position`/`order_count` always serialize, 0 on
legacy anchors.

**What to do.** Implement `OrderRows` on every custom `Anchorer`. A
backend without log-order rows yet can mirror the in-repo stubs and
return an error; one that adopts log-order attestation returns the
rows with `to_position <= upto` in any order — `es.OrderRoot` sorts.
JSON consumers must tolerate the three new anchor fields; ones that
decode into their own structs ignore them by default.

## 2026-11-07 — Postgres appends write `log_order`; the anchor/relay role reads it

**What changes.** Migration `0006` adds the `log_order` table, and from
the moment it lands every Postgres append transaction also writes one
row to it: the batch's position range, workspace/stream scope, event
count, and `batch_hash` — a SHA-256 over exactly that transaction's own
events (`es.BatchOrderHash`, domain `es-lite.log-order.v1`) — computed
and inserted inside the append transaction itself, with no new reads,
locks or serialization (the single extra INSERT per non-empty batch is
the accepted cost). `postgres.Store.Anchor`/`CheckAnchor` now also
compute and compare an order root over the rows at or below the fenced,
settled bound, so every anchor recorded from now on carries
`order_root_hash` (never nil — a legacy anchor's nil is what marks it
heads-only). `es-verify` grows the `order` subcommand, and `all` runs
it after the stream chains: it recomputes every covered batch hash from
the events and exits 1 on the first mismatch, 2 when zero rows are
attested or the backend cannot verify order yet (SQLite's log-order
attestation lands in a later task; `order`/`all` exit 2 with its stub
error until then).

**Who is affected.** The DB principals running the anchor path and
`es-verify`: they now need `SELECT` on `log_order` (the anchor's order
root and the deep verify read it under the role-gated RLS bypass), in
addition to their existing grants — without it the path fails with a
permission error where it used to work. Append-heavy workloads see one
more single-row INSERT per batch. Events appended before migration
`0006` have no `log_order` row and stay unattested by the order root —
an anchor covering only those rows reports `order_count` 0 with a real
`order_root_hash` over the empty row set.

**What to do.** Add `GRANT SELECT ON log_order TO <the anchor job's
role>` (and to the relay principal's, if it runs `es-verify`) before
deploying this version. No code changes for `es.Anchorer` consumers:
`OrderRows` is already on the interface, and the three anchor fields
already serialize (the entry above). Expect `es-verify all` on SQLite
to exit 2 transitionally until its log-order attestation lands.

## 2026-11-07 — SQLite appends write `log_order`; the `es-verify` sqlite transition ends

**What changes.** SQLite migration `0006` — the mirror of Postgres's;
SQLite has no `0005` (Postgres's is RLS-role-specific) — adds the same
`log_order` table minus `workspace_id` (SQLite is single-workspace,
0001's rule) and minus RLS. From the moment it lands, every SQLite
append transaction also writes one row to it: the batch's position
range, stream scope, event count, and `batch_hash`
(`es.BatchOrderHash`) — computed and inserted inside the append
transaction itself, no new reads, no new locks. `sqlite.Store`'s
`Anchor`/`CheckAnchor` compute and compare the order root exactly as
Postgres does (without a fence round — the single-writer connection
keeps committed state stable), and `sqlite.Store.VerifyOrder`
deep-verifies the order, so `es-verify order`/`all` now work on
SQLite: the transitional exit 2 the previous entry promised is over —
on an attested log they exit 0, and on a repositioned event `order`
exits 1 with the tampered batch's `from_position`. The deep-verify
report type moved to the core as `es.OrderVerifyReport` (with
`es.OrderRowMismatch`) so both backends share one shape;
`postgres.OrderVerifyReport`/`postgres.OrderRowMismatch` remain as
transparent type aliases, so existing references compile unchanged.

**Who is affected.** SQLite deployments: the database migrates to
version 6 on open, and appends cost one more single-row INSERT per
batch. `es-verify` users on SQLite: `order`/`all` no longer exit 2 on
attested logs. Consumers of the report type: new code should import
`es.OrderVerifyReport`; existing `postgres.OrderVerifyReport`
references keep working through the alias.

**What to do.** Nothing for SQLite operators (no roles, no grants) and
no code changes for `es.Anchorer` consumers. Prefer the `es` type in
new code; the postgres aliases are kept for compatibility.

## 2026-11-07 — `feed.Log` gains `MatchingRawHead`; `OnStall` compares against the subscription's own backlog

**What changes.** The `feed.Log` interface grows
`MatchingRawHead(ctx context.Context, types []string) (uint64, error)`:
the highest raw committed position among events of `types` — the
subscription's own backlog, with no settle bound. The feed's stall
probe now compares the stream's position against `MatchingRawHead` over
the request's consumed types instead of the all-type `RawHead`, so a
filtered subscription that is caught up on its own types no longer
reports a stall when unrelated event types land above it (the misfire
the review reproduced live). `Observer.OnStall`'s second parameter is
now that matching raw head — the subscription's own backlog — rather
than the all-type raw head.

**Who is affected.** Third-party `feed.Log` implementers: adding a
method to an interface is a compile-time break — their types no longer
satisfy `feed.Log` until they grow a `MatchingRawHead` method (the
in-repo adapters and test fakes hit exactly this). Consumers that read
`OnStall`'s second parameter as the all-type raw head now see the
matching raw head instead.

**What to do.** Implement `MatchingRawHead` on every custom `feed.Log`.
A backend without a settle fence returns the same value as
`MatchingHead` (the SQLite adapter's body is identical); a
fence-bounded backend returns the filtered committed maximum in its
own scope-pinned transaction without the fence round — the
`MatchingHead` query, unfenced. Skipping the fence is sound because
stall detection is a liveness signal, not a correctness boundary — a
torn read can only mis-time a stall report, never corrupt data.
`OnStall` consumers must treat the raw parameter as the subscription's
own backlog, not the log's.

## 2026-11-07 — `feed.Waiter.Wait` reports whether the stream was genuinely woken

**What changes.** `feed.Waiter.Wait` now returns
`Wait(ctx context.Context, timeout time.Duration) (woke bool, err error)`.
`woke` is true only when the waiter observed a genuine wakeup — an
append notification or another wake source firing — and is the signal
the feed uses to let the woken stream read its next page immediately:
the idle poll floor now applies only when `woke` is false, so a
genuine wake no longer pays up to `Config.Poll` on top of the wait,
while a timed-out wait is still floored (a Waiter that returns
instantly must not spin the read loop against the log). The error
result is unchanged: a timeout that ends without a wakeup returns
`(false, nil)` — the same nil the method returned before — and ctx
ending or a real failure returns `(false, err)` with the same error
the method returned before. Two accompanying caller-visible fixes land
with it: `Subscribe` now stops and joins its renewer before assembling
the returned error, so a renewal failure observed just before the
stream ended is joined into it instead of silently dropped; and a
woken idle wait reaches the next page read without waiting out the
poll.

**Who is affected.** Third-party `feed.Waiter` implementations:
changing the method's result list is a compile-time break — their
types no longer satisfy `feed.Waiter` until `Wait` grows the `bool`
result (the in-repo test fakes hit exactly this). `Subscribe` callers
that match the returned error exactly (rather than with `errors.Is`/`As`)
may see additional joined causes where a racing lease loss used to be
dropped, and subscribers with a wakeup source now see pages arrive
sooner.

**What to do.** Return whether the call is a genuine wake: `true` from
the wake path (`(true, nil)`), `false` from the timeout path
(`(false, nil)`), and `(false, err)` when ctx ends or the wakeup source
fails. An implementation that cannot tell a wake from a timeout — a
pure sleep or poll-style waiter — must return `false` on every
non-error path and it keeps exactly its old pacing, since only
non-wakes are floored. Prefer `errors.Is`/`errors.As` over exact error
comparisons when inspecting `Subscribe`'s result.

## 2026-11-07 — `feed.Observer` gains `OnSubscribeError`; `eventfeed` surfaces terminal Subscribe errors

**What changes.** The `feed.Observer` interface grows
`OnSubscribeError(scope, projection, instance string, err error)`, and
the reference consumer adapter reports through it: every `eventfeed`
attempt whose `Subscribe` call ends in an error — poison included,
fatal or not — is reported while its caller context is still live,
before the reconnect loop backs off or gives up. Attempts ended by the
caller's own cancellation report nothing. Alongside the signal, terminal
Subscribe errors stopped being retried: `projection.ReconnectingFeed`
gains `IsFatal func(error) bool` (nil means no error is fatal — the old
retry-forever behavior), and `eventfeed` classifies
`*feed.DefinitionChangedError` as fatal, so `Feed` returns the error to
its caller — the runner exits loudly — instead of spinning behind the
backoff forever with zero signal and a flapping lease (review F3).

**Who is affected.** Third-party `feed.Observer` implementers: adding
a method to an interface is a compile-time break — their types no
longer satisfy `feed.Observer` until they grow an `OnSubscribeError`
method (the in-repo fakes hit exactly this). Callers of
`eventfeed.Feed` and `projection.ReconnectingFeed`: `Feed` can now
return a terminal error before its context ends, so a caller that
treated any early non-`ctx.Err()` return as impossible must now handle
it; a deployment whose checkpoint's definition hash drifted stops
retrying and surfaces the error instead of silently delivering nothing.

**What to do.** Implement `OnSubscribeError` on every custom
`feed.Observer` — or embed `feed.NoopObserver`, which discards it — and
wire it to logging/metrics so failed subscribe attempts become
visible. Callers of `eventfeed.Feed` should treat an
`errors.Is(err, feed.ErrDefinitionChanged)` return as fatal for the
subscription: the recovery is an operator's `ResetCheckpoint` under the
new definition, not a restart.

## 2026-11-07 — `sqlite.NewWithDB` enforces the single-connection contract on the caller's handle

**What changes.** `sqlite.NewWithDB` now calls `db.SetMaxOpenConns(1)` on
the handle it is given, holding borrowed handles to the same
single-connection contract `Open` has always enforced. The single Go
connection is what buys `busy_timeout`/`BUSY_SNAPSHOT` immunity and keeps
an in-memory (`mode=memory`) database alive for the Store's lifetime;
file-level single-writer is what keeps positions gap-free — a borrowed
handle configured for more connections silently forfeited the first two
guarantees. `NewWithDB` still does not touch the DSN's pragmas: WAL,
`busy_timeout` and `foreign_keys` remain the caller's, and the driver
must be `modernc.org/sqlite` (`isConstraintViolation` matches its error
type only).

**Who is affected.** Callers of `sqlite.NewWithDB` whose handle was
configured with a higher connection limit: the limit is lowered to 1 as a
side effect of construction — code that reads the handle's
`Stats().MaxOpenConnections` after wrapping sees 1, and concurrent users
of the same handle outside the Store serialize behind it.

**What to do.** Nothing for correctness — the enforced limit is the
contract the package already documented. A caller that genuinely needs
more connections for its own work must use a separate handle, not share
the Store's.

## 2026-11-07 — `sqlite.Store.Close` no longer closes a borrowed handle

**What changes.** `sqlite.Store.Close` now closes the underlying
`*sql.DB` only when the Store created it (`Open`). A Store built with
`NewWithDB` borrows the caller's handle, and `Close` leaves it open —
mirroring `postgres.OpenWithPool`, which has always worked this way.
Closing is the caller's job.

**Who is affected.** Callers of `sqlite.NewWithDB` that relied on
`Store.Close` to close their handle: the handle now stays open after
`Close`, so a program that counted on that side effect holds the handle
until its own cleanup runs.

**What to do.** Close the handle yourself when you are done with it — the
code that built the `*sql.DB` closes it, exactly as the `OpenWithPool`
contract has always required on the Postgres side.

## 2026-11-07 — Generated constructors panic on a nil required dep; generation rejects unwritable `result` shapes

**What changes.** The constructors `protoc-gen-es-lite` emits —
`New<Name>Handler` for a command service, `New<Name>Runner` for a
projection or reaction — now validate their required deps at
construction and panic, naming the dep, where they previously accepted
anything and misbehaved on first use: `Deps.Runtime`, `Deps.Authorize`
and `Deps.Identity` for a service handler; each per-aggregate state
loader (`Deps.<Aggregate>`) on a state-fetching spec; each per-aggregate
issuing router (`Deps.<Aggregate>Router`) on a reaction. The optional
seams keep their nil defaults: `Validate` (protovalidate), `Filter`,
`Errors`, `Marker`, `Codec`, `Upcaster`, `Actor`. In the same change,
generation now fails — instead of emitting a handler that cannot report
a write — when a command RPC's response `result` message lacks a
singular `stream_id` (string), `version` (uint64) or `global_position`
(uint64), or when the `result` or state response field is repeated; the
error names the offending field.

**Who is affected.** Applications that construct a generated handler or
runner without one of the required deps: the constructor call now
panics immediately instead of the first request or consumed event
failing mysteriously. Protos whose command-service responses carry a
`result` message of the wrong shape previously generated code that did
not compile or could not carry a write result; `buf generate` now
rejects them with a field-naming error.

**What to do.** Wire every required dep at construction; a panic there
is the loud form of a failure that already existed at first use. Give
every command RPC's response the `WriteResult` shape (singular
`stream_id`, `version`, `global_position`) and a singular state field —
the `counter_service.proto` fixture is the reference.

## 2026-11-07 — `delivery` loops retry transient drain errors; `Poller`/`Relay` constructors validate their deps

**What changes.** `Poller.Run` and `Relay.Run` no longer exit on the first
drain error: a failing drain is retried with backoff — `Config.MaxRetries`
/ `RelayConfig.MaxRetries` (0 → 3 retries, positive → exactly that many,
negative → fail fast: the first drain error is returned immediately with
no retries, design D10) and `Config.RetryBackoff` / `RelayConfig.RetryBackoff`
(default 100ms per attempt, capped at 5s) — and only an exhausted budget
is returned. A closed `Wake` channel no longer spins the loop hot: its
buffered signal, if any, is drained once and the channel is then ignored.
`NewPoller` now panics on a nil `src` or `cp` and `NewRelay` on a nil
`drainer`, naming the parameter (alongside the existing `Subscriber`/
handler checks). `Poller` also caches its checkpoint position after each
successful save and reloads it from the store only on the first pass and
after errors, instead of once per batch.

**Who is affected.** Callers that treated any `Run` return as a
process-fatal store blip: transient errors are now absorbed up to the
retry budget (a negative `MaxRetries` fails fast instead), so `Run`
returns only for exhausted budgets or cancellation. Constructions wiring
nil for `src`/`cp`/`drainer` now panic at wiring time instead of failing
on first use. Operators repointing a live subscriber's checkpoint
externally: a running poller no longer rereads the checkpoint each pass,
so the change surfaces after the process restarts or a drain error, not
on the next tick.

**What to do.** Pick the retry policy per deployment — the defaults
(3 retries, linear backoff) ride out short blips; a negative `MaxRetries`
is the fail-fast posture: `Run` surfaces the first drain error
immediately. Wire real deps at construction. Repoint checkpoints with
the poller stopped.

## 2026-11-07 — `cmdsvc.MetaFrom` keeps non-UUID `Correlation-Id` values as UUIDv5 instead of dropping them

**What changes.** `cmdsvc.MetaFrom` used to read a `Correlation-Id` header
only when it parsed as a UUID and silently dropped any other value, leaving
`es.Meta.CorrelationID` zero — the runtime then defaulted it to the
command's own `CommandID`, so events from one client-reported flow were
indistinguishable from unrelated ones. A non-UUID value now hashes to a
stable UUIDv5 in the same fixed namespace non-UUID `Idempotency-Key`
values already use (`3b5a0c2e-7f5d-4b1a-9c3e-2d6f8a1b4c7e`), so the
caller's free-text correlation id reaches the audit trail and stays stable
across retries. `Correlation-Id` values that parse as a UUID pass through
unchanged, and every `Idempotency-Key` path is unchanged.

**Who is affected.** Clients that send a non-UUID `Correlation-Id`, and
anything reading `correlation_id` off recorded events: events from such
commands now carry the stable hash of the header instead of the command's
own id. Correlation chains that previously collapsed onto per-command ids
become traceable. Already-recorded rows are not rewritten.

**What to do.** Nothing required. A consumer that matched events on
`correlation_id == command_id` (the old fallback shape) for non-UUID
correlations must match on the header's hash instead — or, better, send a
UUID `Correlation-Id`, which still passes through byte-for-byte.
