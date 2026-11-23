# Event tags and multi-stream append — design

**Status:** proposed — 2026-11-23

## Intent

es-lite can already answer "what happened in this stream" and "what happened
after position P across all streams". It cannot answer "which events carry this
label", because an event's only queryable dimensions are its type, stream,
actor, correlation, time and position. FlowDoc needs a third axis: every event
belongs to a document, a space and a block, and the document and space views
are change feeds, not only history.

This design makes a small, bounded key/value tag set **first-class on the
event**, keeps `event_tags` a **rebuildable projection** so an events-only
restore keeps every tag, and adds a **multi-stream append** that writes several
streams in one caller-owned transaction. Tags are attested by a per-batch hash
and a separate anchor root, so an indexed tag is not an unsigned tag.

The v0.21/v0.22 events-only model is unchanged: `events` remains the only
authoritative table. `event_tags` joins `unique_claims` as a projection of the
log, rebuilt and verified from the log.

## Goals

- Store a bounded flat tag set (`string → string`) on every event.
- Query events by tags with **AND** semantics, ascending global position,
  position-bounded and paginated.
- Make `event_tags` a synchronous projection of `events.tags`, never a backup
  input; an events-only restore rebuilds it.
- Make tags tamper-evident at the same split as log order: a per-batch
  `log_order.tags_hash` and an exported anchor `tags_root_hash`.
- Append several streams in one caller-owned transaction with one command id,
  per-stream expected versions, and all-or-nothing semantics.
- Keep FlowDoc's document, space and block dimensions without making the
  document a tag or the space a second stream.
- Keep SQLite honest: the same API and semantics, one workspace per database.

## Non-goals

- Full-text search, nested/structured tag values, or multiple values per key.
- Tags as state. `ReadByTags` returns events; the caller folds them.
- Attesting tags in `CanonicalEnvelope`. Tags are excluded from the event hash;
  the batch/anchor split is the attestation (Decision 6).
- A tag-aware remote feed contract. The local feed carries `Envelope.Tags`
  automatically; changing the platform protobuf is a separate change.
- Deleting or rewriting a tag. The log is immutable; erasure is opaque values
  and key destruction, not tag mutation (Decision 10).
- Backfilling tags for historical events.

## Anti-goals

- A second authoritative table. If `event_tags` were the only copy, an
  events-only restore would lose every tag and `es-verify` would need a new
  backup set.
- A GIN-only jsonb design. The dominant read is AND + position range +
  `LIMIT`, which wants a tag-first btree, not a bitmap and sort.
- A stream/type partition key. It breaks workspace pruning and forces itself
  into the unique keys (ADR 0004 errata).
- Making `ReadByTags` a method on `es.Store`. That breaks third-party stores;
  it is a capability interface.

## Constraints

- Go 1.27.x. Additive public API; ship as a minor version (`v0.23.0`).
- Postgres is the production store; SQLite and the test stores stay honest.
- `events` and `event_tags` are `PARTITION BY HASH (workspace_id)` with the
  same modulus (8).
- The append path is synchronous; no new cross-transaction reads or locks.
- Tag values are non-secret, non-personal identifiers. Personal data uses the
  existing keyed-MAC rule (ADR 0025), because a tag is immutable and indexed.
- Existing event hashes, order roots and anchors verify unchanged.

## Approaches

The design forks were settled one at a time; this records the losing options.

1. **Tags on the event; `event_tags` a projection (chosen).** Preserves the
   events-only restore and gives one query surface.
2. **`event_tags` as an append-only side table.** The user's first proposal.
   Rejected: an events-only restore loses tags, and the side table becomes a
   second authoritative copy.
3. **`events.tags` jsonb only, GIN-indexed.** Simpler, but AND + ordered range
   + `LIMIT` becomes a bitmap scan and sort; a common tag scans most of a
   document. Rejected for the primary read path.
4. **Versioned binary tags on the event.** Smallest and strictest, but the log
   becomes unreadable to operators. Rejected in favour of jsonb; the projection
   carries the strictness.
5. **A separate `stream_tags` table for the space.** Normalized and small, but
   a space change feed fans out over documents. Rejected.
6. **Folding `tags_hash` into the order root.** Fewer anchor fields, but the
   order root starts attesting content and needs a root version. Rejected in
   favour of a sibling tags root.

## Decisions

### 1. The event is the source; `event_tags` is a projection

`events` gains a nullable `tags` column holding a **flat JSON object** of
string keys to string values. `NULL` means "no tags"; the empty object is
normalized to `NULL`. The column is excluded from `CanonicalEnvelope`, exactly
like `span_context` and `constraint_ops`, so no existing hash changes.

`event_tags` is written in the **same append transaction** from the same
in-memory tag set that produced the jsonb. It is the query index, not a second
source:

- an events-only restore loads `events` and rebuilds `event_tags`;
- `event_tags` is not in the backup set;
- `es-verify tags` recomputes it from `events.tags` and refuses divergence.

A malformed `events.tags` row (not an object, or a non-string value) is
corruption, not "no tags": reads and rebuilds fail loudly rather than dropping
the row. The Go append path cannot produce one; the check exists for raw SQL
and third-party writers.

### 2. The FlowDoc tag model

- **A document is a stream**: `stream_id = flowdoc.document:<id>`. The document
  is therefore **not** a tag. Its stream is the per-document change timeline: it
  carries the document's own events (tree, access, leases, title) and one
  content-free `BlockChanges` notice per content command. Block payloads stay on
  per-block streams.
- **A space is immutable for a document's life**, but it is tagged on **every
  event**, alongside the block. Two tags per FlowDoc event:
  - `flowdoc.space` — batch-scoped; the whole append is one document in one
    space;
  - `flowdoc.block` — per event.
- The space change feed is one tag query:
  `ReadByTags({flowdoc.space: S}, FromPosition: cursor)`.
- **A per-document change feed is a stream read, not a tag query.** With
  `document = stream`, `GetDocumentChanges` is
  `ReadStream(document, fromVersion, 0)` — a **stream version** cursor, not a
  global position. Block payloads stay on per-block streams; every content
  command appends one content-free `BlockChanges` notice to the document stream
  in the same transaction, so the stream is a complete change timeline. The
  notice carries `{block_id, content_revision, stream_version}` per changed
  block. This needs no per-event document tag. Global positions remain the
  delivery positions for the live hub; the client's change cursor is the stream
  version. If the application later needs a uniform tag cursor across
  documents, adding `flowdoc.document` is a one-line model change at the cost of
  a third tag row per event; it is not in this version.

The batch/per-event split is the cost lever: space is repeated per event to
make it one ordered scan, and the document is free because it is the stream.

### 3. Schema, key, partitions and limits

Postgres:

```sql
ALTER TABLE events ADD COLUMN tags jsonb;
ALTER TABLE events ADD CONSTRAINT events_tags_object
    CHECK (tags IS NULL OR jsonb_typeof(tags) = 'object') NOT VALID;
-- VALIDATE later: every existing row is NULL, so it is instant.

CREATE TABLE IF NOT EXISTS event_tags (
    workspace_id    text   NOT NULL,
    tag_key         text   NOT NULL,
    tag_value       text   NOT NULL,
    global_position bigint NOT NULL,
    PRIMARY KEY (workspace_id, tag_key, tag_value, global_position)
) PARTITION BY HASH (workspace_id);  -- p0..p7, MODULUS 8
```

- **Tag-first key.** `(workspace_id, tag_key, tag_value, global_position)` is
  the PK and the only index. It serves each tag's position range directly; the
  join back to `events` uses the existing workspace/position index.
- **No secondary position index.** "Tags of this event" is `events.tags`; a
  position-first index on `event_tags` would duplicate the event row's own
  column and add write amplification on ~1B rows.
- **Eight hash partitions**, same modulus as `events`. Partition assignment is
  by `workspace_id`; the tables are not physically co-located, and do not need
  to be.
- **RLS** mirrors `events` exactly: workspace arm plus the role-gated
  `app.bypass_rls = 'on' AND pg_has_role(current_user, 'es_relay', 'member')`
  arm, `ENABLE` + `FORCE`, and the same policy on each partition if partitions
  are ever granted directly.
- **No grants in the migration.** Deployment grants the appender `SELECT,
  INSERT`, the rebuild principal `SELECT, INSERT, DELETE`, and readers
  `SELECT` (Decision 9).

Limits, enforced in Go before any statement runs (so `AppendTx` is covered
too):

| Rule | Value | Error |
|---|---|---|
| key | 1–128 bytes, `^[a-z0-9._:-]+$` | `ErrInvalidTag` |
| value | 0–512 bytes, valid UTF-8, no NUL | `ErrInvalidTag` |
| tags per merged event | ≤ 32 | `ErrTooManyTags` |
| duplicate key | impossible in the Go map API; a raw-SQL jsonb duplicate is collapsed by jsonb (last wins) | `ErrInvalidTag` (API) |
| batch tag vs event tag | same key, different value rejected | `ErrConflictingTag` |

The key charset is ASCII, so bytes and runes coincide. The value cap is well
inside a btree text entry and keeps a malformed insert from wedging the log.

SQLite (one database is one workspace, so no `workspace_id` column):

```sql
ALTER TABLE events ADD COLUMN tags TEXT;  -- JSON object, NULL = none

CREATE TABLE IF NOT EXISTS event_tags (
    tag_key         TEXT    NOT NULL,
    tag_value       TEXT    NOT NULL,
    global_position INTEGER NOT NULL,
    PRIMARY KEY (tag_key, tag_value, global_position)
);

ALTER TABLE log_order ADD COLUMN tags_hash BLOB;
```

SQLite stores the same flat object with `encoding/json` (which sorts map keys)
and validates on read. `WITHOUT ROWID` is a builder choice; the PK already
covers every read.

### 4. Read API

A capability interface, not a method on `es.Store`:

```go
// TagQuery selects events that carry every tag in Tags.
type TagQuery struct {
    Tags         map[string]string // AND semantics; must be non-empty
    FromPosition uint64            // exclusive; 0 starts at the beginning
    ToPosition   uint64            // inclusive; 0 means no upper bound
    Limit        int               // rows to return; 0 = DefaultTagReadLimit
}

type TagReader interface {
    ReadByTags(ctx context.Context, q TagQuery) ([]Envelope, error)
}
```

- **AND semantics**: an event must carry every `(key, value)` pair.
- **Ascending global position**, regardless of query order.
- **From exclusive, To inclusive.** Pagination passes the last returned
  `GlobalPosition` as `FromPosition`; no duplicates, no gaps.
- **Limit**: `0` uses `DefaultTagReadLimit` (500); negative or above
  `MaxTagReadLimit` (1000) is `ErrInvalidTagQuery`.
- **Empty `Tags`** is `ErrInvalidTagQuery`; "everything" is `ReadAll`, and
  conflating them hides a caller bug. `ToPosition != 0 && ToPosition <
  FromPosition` is also `ErrInvalidTagQuery`.
- Returns fully populated `Envelope`s, including `Tags`.

The reference Postgres query is one `EXISTS` probe per tag, driven by the
events position index and stopped by `LIMIT`:

```sql
SELECT e.*
FROM events e
WHERE e.workspace_id = $1
  AND e.global_position > $2
  AND ($3::bigint = 0 OR e.global_position <= $3)
  AND EXISTS (SELECT 1 FROM event_tags t
              WHERE t.workspace_id = e.workspace_id
                AND t.tag_key = $4 AND t.tag_value = $5
                AND t.global_position = e.global_position)
  -- one EXISTS per additional tag
ORDER BY e.global_position
LIMIT $n;
```

For a rare tag the planner may prefer to intersect `event_tags` first and join
`events`; the adapter may use either plan as long as the semantics hold. The
builder EXPLAINs both shapes at 10M and 50M.

**As-of.** `ToPosition = P` is "as of **global position** P", not "document
version P" and not `RecordedAt`. It returns events, not state; the caller folds
them. A document at version V is `ReadStream(document, 0, V)`. The two are
different clocks and the API names them differently.

### 5. The projection is synchronous

Every append inserts its tag rows in **one multi-row statement per append**,
inside the append transaction, after the event inserts return their positions:

```sql
INSERT INTO event_tags (workspace_id, tag_key, tag_value, global_position)
SELECT $1, k, v, p
FROM unnest($2::text[], $3::text[], $4::bigint[]) AS t(k, v, p);
```

Consequences:

- read-your-writes: a committed append is immediately visible to `ReadByTags`;
- all-or-nothing: a tag insert failure rolls back the event;
- no feed lag between a document change and its tag query;
- the append pays two rows per FlowDoc event and one statement per append.

A zero-tag append skips the statement entirely. The append hint
(`pg_notify`) still runs last in the single-append path.

### 6. Attestation: a per-batch hash and a sibling anchor root

Tags are excluded from the event hash, so the chain does not attest them. They
are attested at the same trust split as log order:

- **`log_order.tags_hash bytea NULL`** — one value per non-empty batch,
  computed in the same transaction. `NULL` means "no event in this batch has
  any tag"; a tagged batch always carries the hash, including a batch whose
  tag set happens to be empty after merge (that case is normalized to NULL).
- **`anchor.tags_root_hash bytea NULL`** — the exported aggregate over the
  `log_order` rows at or below the bound that have a non-NULL `tags_hash`.
  `NULL` is a legacy anchor recorded before this migration: it verifies
  heads-only and order-only, exactly as `OrderRootHash == nil` does today. A
  new anchor always carries a real root, even over zero rows.
- **`anchor.tags_from_position` / `anchor.tags_count`** — the first covered
  position and the number of covered batches, mirroring
  `OrderFromPosition`/`OrderCount`.

The canonical encodings live in `es` beside `BatchOrderHash`/`OrderRoot`:

```go
type TagEntry struct {
    Position uint64
    Key      string
    Value    string
}

// TagsHash: "es-lite.tags.v1" (len-prefixed), u32 entry count,
// then per entry, sorted by (Position, Key, Value):
//   u64 Position ‖ len-prefixed Key ‖ len-prefixed Value.
func TagsHash(entries []TagEntry) [HashSize]byte

type TagsRow struct {
    From, To uint64
    TagsHash []byte
}

// TagsRoot: "es-lite.tags-root.v1" (len-prefixed), u64 row count,
// then per row, sorted by ascending From:
//   u64 From ‖ u64 To ‖ 32 TagsHash bytes.
func TagsRoot(rows []TagsRow) [HashSize]byte
```

Sorting is inside the hash, so map iteration order and query order do not
matter. A malformed hash length panics, as `BatchOrderHash` does. The existing
order root is unchanged: `OrderRoot` ignores the new field, so old anchors and
their order roots verify byte-for-byte.

`es.OrderRow` gains `TagsHash []byte`; `logOrderRow` and the `OrderRows` query
select it. `CheckAnchor` compares the recomputed tags root when
`TagsRootHash != nil`; a legacy anchor skips the comparison.

### 7. `AppendMultiTx`

The existing `AppendTx` already sets the workspace and shares the append body;
the multi form is a tested entry point over it.

```go
type MultiAppend struct {
    Appends []AppendParams // one per stream, executed in input order
}

// On postgres.Workspace, like AppendTx.
func (w *Workspace) AppendMultiTx(
    ctx context.Context, db DBTX, m es.MultiAppend,
) ([]es.AppendResult, error)
```

SQLite has no caller-owned append transaction today, so it exposes
`(*Store).AppendMulti(ctx, m)` instead: one store-owned transaction, same
contract, same results. The shared `es.ValidateMultiAppend` is the contract
both adapters call.

Contract:

- **Caller owns the transaction.** The store never commits or rolls back; it
  sets `app.workspace_id` once. On error the caller must roll back.
- **One workspace.** Every append is on the same `Workspace`; no mixed-tenant
  transaction.
- **Distinct streams.** Two appends with the same `StreamID` are
  `ErrDuplicateMultiStream`; the second `ExpectedVersion` would be stale.
- **Non-empty.** `len(Appends) == 0` is `ErrEmptyMultiAppend`; each element
  must have at least one event.
- **Shared command metadata.** Every append must carry the same `CommandID`,
  `CorrelationID`, `CausationID`, `Actor` and `SpanContext`; a mismatch is
  `ErrConflictingMultiAppend`. A zero `OccurredAt` resolves to one server time
  for the whole multi-append; non-zero values must agree.
- **Input-order execution and positions.** Appends run in the order given and
  positions follow that order. This is the caller's contract; the store does
  not sort.
- **Per-stream optimistic concurrency.** Each append checks its own
  `ExpectedVersion` in the shared transaction; any conflict aborts the whole
  transaction.
- **Event ids.** Caller-supplied `EventID`s are preserved; a zero mints a
  UUIDv7. Deterministic derivation stays the caller's job and **must use a
  global event index across all appends** — the runtime's
  `CommandID:index` derivation restarts per append and would collide. The
  store does not derive ids and does not inspect the index.
- **Deadlocks.** Overlapping multi-appends in different orders can deadlock.
  The store surfaces the Postgres error; the caller retries the whole
  transaction with identical params. The recommended convention is to sort
  appends by canonical stream id before calling; positions then follow that
  sorted order.
- **Result.** One `AppendResult` per append, in input order, each with its own
  `FromVersion`, `ToVersion` and fully populated envelopes.
- **One `log_order` row per append**, so the order root covers every batch.
  Each append's tag rows are inserted by its own append body.
- **Append hint.** Each append would otherwise emit its own `pg_notify`. The
  implementation collects the hints and emits one per distinct workspace (or
  one per append) **after the last append**, so ADR 0039's last-statement
  property still holds for the transaction as a whole. See "What is left to
  the builder".

### 8. How tags reach `AppendParams`

There are two seams, and neither breaks `Decider.Decide`:

- **Batch tags**: `es.Meta` gains `Tags map[string]string`, copied by the
  runtime onto `AppendParams.Tags`. FlowDoc puts `flowdoc.space` here.
- **Per-event tags**: `es.Decider` gains an optional function field:
  ```go
  // Tags returns the event's per-event tag set; nil means none.
  // It MUST be pure, like Decide and Evolve.
  Tags func(event E) map[string]string
  ```
  The runtime calls it for each decided event and sets `EventData.Tags`.
  FlowDoc puts `flowdoc.block` here.

Merge rule: start with batch tags, overlay the event tags; a key present in
both with different values is `ErrConflictingTag`; the same value dedupes. The
merged set is what `events.tags` stores and what `event_tags` projects, and
the `≤32` cap applies to the merged set.

Callers using `aggregate.WithAppend` bypass the runtime and set both fields
themselves. Direct `Store.Append`/`AppendTx` callers likewise. This mirrors the
existing `SpanContext` boundary: the store never guesses tags from the payload.

### 9. Restore, rebuild and verification

`es.TagMaintainer`, like `ClaimsMaintainer`, on `*Workspace`:

```go
type TagsRebuildReport struct { Events, Tags int64 }

type TagsVerifyReport struct {
    OK              bool
    Events, Tags    int64
    FirstDivergence string
}

type TagMaintainer interface {
    RebuildTags(context.Context) (TagsRebuildReport, error)
    VerifyTags(context.Context) (TagsVerifyReport, error)
}
```

- **`RebuildTags`** deletes the workspace's `event_tags` and re-projects
  `events.tags`:
  ```sql
  INSERT INTO event_tags (workspace_id, tag_key, tag_value, global_position)
  SELECT e.workspace_id, kv.key, kv.value, e.global_position
  FROM events e, jsonb_each_text(e.tags) AS kv
  WHERE e.workspace_id = $1 AND e.tags IS NOT NULL;
  ```
  A pre-pass counts non-string values and refuses (`jsonb_each_text` would
  silently stringify them). Requires quiescence, as the claims rebuild does.
- **`VerifyTags`** rebuilds into a temp table and compares both ways (the
  `EXCEPT` pattern of `VerifyClaims`), reporting the first divergence without
  touching the live index.
- **Attestation verify** recomputes each covered batch's `TagsHash` from
  `events.tags` (not from `event_tags`) and compares to
  `log_order.tags_hash`; a NULL hash over tagged events is a mismatch. It
  feeds the same anchor comparison as order verification.

CLI:

```
es-verify tags [--rebuild] [--upto N] [<workspace>]
  verify the event_tags projection against events.tags (and, with --upto,
  the per-batch tags hashes); --rebuild re-projects the workspace
es-verify all
  gains the tag-attestation stage (not the projection rebuild); --chains-only
  still stops before it, preserving the events-only restore path
```

`es-verify tags --rebuild` is the restore path: load `events`, run it, done.

### 10. Erasure and the trust boundary

- **Tags are immutable and indexed.** A tag cannot be deleted or rewritten
  without breaking the log/projection contract. Therefore a tag value must be
  a non-personal identifier. Personal data follows the existing `LookupClaim`
  rule: the caller supplies a keyed MAC or an opaque id, and es-lite stores it
  verbatim.
- **Tags are attested, not chained.** A database writer can change
  `events.tags` and `event_tags` together, but cannot change the off-box
  anchor's `tags_root_hash`; `es-verify tags` recomputes from the event rows.
  This is the same trust boundary as `unique_claims` and log order, and it is
  narrower than "tags are unsigned".
- **RLS** scopes both tables by workspace. The bypass arm is role-gated to
  `es_relay`, as for `events`; there is no unscoped read.

### 11. Migrations and rollout

- **Postgres `0016_event_tags.sql`**:
  - `ALTER TABLE events ADD COLUMN tags jsonb` (metadata-only);
  - the object `CHECK ... NOT VALID`, then `VALIDATE CONSTRAINT`;
  - `CREATE TABLE event_tags ... PARTITION BY HASH (workspace_id)` and
    `event_tags_p0..p7` with `MODULUS 8`;
  - RLS enable/force/policy on the parent (mirroring `events`);
  - `ALTER TABLE log_order ADD COLUMN tags_hash bytea`.
- **SQLite `0014_event_tags.sql`**:
  - `ALTER TABLE events ADD COLUMN tags TEXT`;
  - `CREATE TABLE event_tags (tag_key, tag_value, global_position, PK ...)`;
  - `ALTER TABLE log_order ADD COLUMN tags_hash BLOB`.
- **No data migration.** Existing events have NULL tags; existing log_order
  rows have NULL `tags_hash`; existing anchors have no tags root. All three
  are the legacy/untagged case and verify as before.
- **Deployment grants** (runbook):
  - appender: `SELECT, INSERT` on `event_tags` (plus its existing `events`
    grant);
  - rebuild principal: `SELECT, INSERT, DELETE` on `event_tags`;
  - readers: `SELECT`.
- **Partition build on a large store**: create the eight partitions and their
  policies outside the migration window if the table is already large; the
  table starts empty, so the cost is catalog-only.
- **Release**: `v0.23.0` (additive API, new capability interfaces, new
  migrations).

## Testing

Named, because the value rests on these properties:

- **Validation**: key charset/length, value length/UTF-8/NUL, 32-tag cap,
  duplicate keys, batch/event conflict; all rejected before any statement and
  on both backends; `AppendTx` is covered.
- **Round trip**: `events.tags` NULL/object, `Envelope.Tags`, local feed
  delivery; a malformed raw-SQL row fails the read loudly.
- **Projection**: read-your-writes; a rolled-back append leaves no tag rows;
  one multi-row statement per append; rebuild reproduces the live index
  exactly; `VerifyTags` catches a deleted, added and changed tag row.
- **ReadByTags**: AND semantics, From exclusive / To inclusive, ascending
  order, pagination across a boundary, `Limit` default/cap, empty query,
  inverted range, as-of `ToPosition`, isolated workspace under RLS.
- **Attestation**: golden vectors for `TagsHash` and `TagsRoot`; NULL
  `tags_hash` for untagged batches; a tagged batch's hash covers
  position/key/value; an anchor's tags root covers only non-NULL rows;
  a legacy anchor with no tags root still verifies; tampered tags, a tampered
  `tags_hash`, and a repositioned event are each caught.
- **Multi-append**: distinct streams, per-stream versions, input-order
  positions, one result per append, duplicate stream rejected, conflicting
  metadata rejected, a conflict on one stream rolls back all, caller-supplied
  ids preserved, zero ids minted, deterministic-id restart collision is
  documented and demonstrated, deadlock retry convention documented.
- **Restore**: an events-only dump (no `event_tags`, no `log_order`) restores,
  `RebuildTags` runs, `RebuildClaims` still runs, and `es-verify tags` +
  `es-verify order --chains-only` pass by their documented scopes.
- **SQLite parity**: the same read/append/rebuild contracts, no RLS/partition
  assumptions, one-workspace semantics.
- **Conformance**: add `TagReader`/`TagMaintainer` to the adapter test suite so
  a future store inherits the contract.
- **Benchmark**: `cmd/es-bench` gains tags on/off (2 tags per event) and
  measures append p50/p99 and `ReadByTags` at 10M/50M; the runbook records the
  envelope.

## Capacity

At 500M events and two tags per event, `event_tags` is ~1B rows. With a
space id (~36 B), a block id (~36 B), the key names and the 8-byte position,
the table plus its btree is estimated at roughly 150–250 GB; measure at 10M
and 50M before quoting a number. The rebuild is a full scan plus ~1B index
inserts, comparable to the claims rebuild — tens of minutes at 500M, measured
at the lower tiers. The append gains one multi-row statement and two rows per
FlowDoc event; the benchmark records the p50/p99 shift.

## Alternatives considered

| Option | Why not |
|---|---|
| Side-table-only tags | Events-only restore loses tags; second authoritative table. |
| jsonb + GIN only | AND + ordered range + `LIMIT` becomes bitmap + sort. |
| Binary codec on the event | Compact, but operator-hostile; jsonb chosen. |
| Fold tags into `CanonicalEnvelope` | Changes every new event hash and branches the chain verify. |
| `flowdoc.document` as a tag | Redundant with the stream plus the `BlockChanges` notice; one more row per event. |
| `stream_tags` for the space | Space feed becomes a multi-stream merge. |
| Asynchronous projection | Tag reads and the change feed lag; needs a worker. |
| Document as the space stream | Document-level queries fan out; loses the document stream's ordering. |

## Resolved questions

- **jsonb vs binary**: jsonb. The projection supplies the strict query path;
  the log stays readable.
- **Projection synchronous vs async**: synchronous, one statement per append.
- **Tag-first vs position-first**: tag-first, no secondary index.
- **Attestation**: per-batch `tags_hash` + sibling anchor `tags_root_hash`;
  the order root is unchanged.
- **Multi-append identity**: caller owns ids; a global index is the caller's
  job; distinct streams; input order; deadlocks surface.
- **Tags from commands**: batch via `es.Meta.Tags`, per-event via the optional
  `Decider.Tags` function.
- **Document change feed**: a stream read, not a tag query (Decision 2), with
  the content-free `BlockChanges` notice on the document stream.

## Open questions for the builder

- The exact `DefaultTagReadLimit`/`MaxTagReadLimit` (proposed 500/1000) after
  measuring a realistic page.
- Whether `event_tags` is `WITHOUT ROWID` in SQLite.
- The `tags_hash` treatment of an event whose merged tag set is empty — this
  design normalizes it to no contribution and NULL for the whole batch.
- Whether `es-verify all` should also verify the projection per workspace
  (expensive) or only the attestation (proposed).
- The `pg_notify` collection shape in `AppendMultiTx`: one hint per stream,
  one per workspace, or one per multi-append.
- The EXPLAIN winner for `ReadByTags` at 10M/50M and whether the adapter needs
  a candidate-first branch for rare tags.
- The platform feed contract's tag fields (out of scope here, but the
  `Envelope.Tags` seam should be ready).

## What is left to the builder

- The `es/tags.go` types, errors, validation, `TagsHash`/`TagsRoot` and their
  golden vectors.
- `es.Envelope.Tags`, `es.AppendParams.Tags`, `es.EventData.Tags`,
  `es.Decider.Tags`, `aggregate.Meta.Tags`, and the runtime merge.
- Postgres migration 0016 (DDL, partitions, RLS, `log_order.tags_hash`) and
  SQLite migration 0014, plus the schema-parity tests.
- The Postgres append body: jsonb encode, merged-set validation, the
  multi-row `event_tags` insert, `log_order.tags_hash`, and the multi-append
  loop with shared metadata checks and hint collection.
- The SQLite append body and `ReadByTags`.
- `es.OrderRow.TagsHash`, `es.Anchor.TagsRootHash`/`TagsFromPosition`/
  `TagsCount`, `CheckAnchor`, and the `OrderRows` query.
- `TagReader` and `TagMaintainer` on both adapters, the conformance suite, and
  `es-verify tags` plus the `all` stage.
- The runbook grants, the ADR, the benchmark scenario/results, and the
  release note.
