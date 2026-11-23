# ADR 0044: Event Tags and Multi-Stream Append

- **Status:** Accepted (implemented)
- **Date:** 2026-11-23
- **Related:** ADR 0004 (workspace partitioning), ADR 0025 (client-owned crypto
  and erasure), ADR 0029 (the chain attests the log), ADR 0035 (caller-owned
  transactions), ADR 0038 (`log_order`), ADR 0040 (span context excluded from
  the chain), ADR 0042 (derived state is re-derivable), ADR 0043 (constraint
  ops on the event log)

## 1. Context

es-lite could read a stream, a workspace or the whole log, but not **"the
events carrying this fact"** — the shape a cross-cutting change feed needs.
FlowDoc is the motivating consumer: a space feed ("everything that changed in
space S") and a block filter span documents, while each document is its own
stream. Reading every document and filtering in the client does not scale; a
projection table without a restore story is invisible state.

The requirements:

- tags are metadata, not a business fact: adding one must not change an
  existing event hash, order root or anchor;
- an **events-only restore** must preserve tags, as ADR 0043 made it preserve
  constraints;
- the read is `AND` over several tags, ordered by global position, paginated by
  position, and stopped by `LIMIT` — not a bitmap scan and sort;
- FlowDoc also needs to write several streams in one transaction (a document
  notice plus block events), which the existing single-append API cannot do;
- tag writes must not silently diverge from the log, and the log cannot attest
  them through the chain.

## 2. Decision

### 2.1 The event is the source; `event_tags` is a projection

`events` gains a nullable `tags` column holding a **flat JSON object** of
string keys to string values. `NULL` means "no tags"; an empty object is
normalized to `NULL`. The column is excluded from `CanonicalEnvelope`, exactly
like `span_context` and `constraint_ops`, so no existing event hash changes.

`event_tags` is the query index, written **in the same append transaction**
from the same merged set that produced the jsonb. It is not a second source:

- an events-only restore loads `events` and rebuilds `event_tags`;
- `event_tags` is not in the backup set;
- `RebuildTags` and `VerifyTags` treat `events.tags` as the truth.

A stored `tags` value that is not the flat string map the append wrote — a
non-object, a non-string member, an invalid key or an over-cap set — is
corruption. Reads, rebuilds and verifies fail loudly and name the stream and
version, never the bytes. Silently dropping a tag a change feed depends on is
the failure this rule exists to prevent.

The append validates the whole merged set before any statement runs, so
`AppendTx` is covered too: key `1–128` bytes matching `^[a-z0-9._:-]+$`, value
`0–512` bytes of valid UTF-8 with no NUL, at most 32 tags per merged event,
and no key whose batch and per-event values differ. A batch that carries tags
but no events is rejected (`ErrInvalidTag`) rather than dropping the tags, the
same rule ADR 0043 applied to constraint ops.

Tags reach the append through two seams:

- **batch tags** — `es.Meta.Tags` → `AppendParams.Tags` (the whole append);
- **per-event tags** — the optional, pure `es.Decider.Tags(event)` →
  `EventData.Tags` (one event).

The merge is batch tags overlaid with event tags; the same key and value
dedupes, a different value is `ErrConflictingTag`. The merged set is what
`events.tags` stores and what `event_tags` projects.

### 2.2 The FlowDoc model

- **A document is a stream**: `stream_id = flowdoc.document:<id>`. The document
  is therefore **not** a tag.
- **A space is immutable for a document's life**, but is tagged on every event
  as the batch tag `flowdoc.space`.
- **A block is tagged per event** as `flowdoc.block`.

Two tag rows per FlowDoc event. The space feed is one tag query
(`flowdoc.space = S`); a document's change feed is a stream read, not a tag
query, using its stream version as the cursor. Block payloads stay on per-block
streams; the document stream carries the content-free change notices.

The repetition is deliberate: a space feed is one ordered index scan rather
than a fan-out over documents, and the document costs no row because its
identity is already the stream id.

### 2.3 Schema, key, partitions and limits

Postgres (migration `0016_event_tags.sql`):

```sql
ALTER TABLE events ADD COLUMN tags jsonb;
ALTER TABLE events ADD CONSTRAINT events_tags_object
    CHECK (tags IS NULL OR jsonb_typeof(tags) = 'object') NOT VALID;

CREATE TABLE event_tags (
    workspace_id    text   NOT NULL,
    tag_key         text   NOT NULL,
    tag_value       text   NOT NULL,
    global_position bigint NOT NULL,
    PRIMARY KEY (workspace_id, tag_key, tag_value, global_position)
) PARTITION BY HASH (workspace_id);   -- p0..p7, MODULUS 8

ALTER TABLE log_order ADD COLUMN tags_hash bytea;
```

- **Tag-first primary key**, and the only index. It serves each tag's position
  range directly; the join back to `events` uses the existing workspace and
  position index.
- **No secondary position index.** "The tags of this event" is `events.tags`,
  already on the event row; a position-first index would duplicate it and add
  write amplification on a very large table.
- **Eight hash partitions**, the same modulus as `events`. Partitioning is by
  `workspace_id`, so a workspace's tag rows are pruned like its events; the
  tables are not physically co-located and need not be.
- **RLS mirrors `events`**: `ENABLE` + `FORCE`, the workspace arm and the
  role-gated `app.bypass_rls = 'on'` + `es_relay` membership arm. No grants in
  the migration; deployment issues them (runbook).
- **SQLite** (migration `0014_event_tags.sql`) is one workspace per database:
  `events.tags TEXT`, an `event_tags` table keyed
  `(tag_key, tag_value, global_position)`, and `log_order.tags_hash BLOB`.

Limits:

| Rule | Value | Error |
|---|---|---|
| key | 1–128 bytes, `^[a-z0-9._:-]+$` | `ErrInvalidTag` |
| value | 0–512 bytes, valid UTF-8, no NUL | `ErrInvalidTag` |
| tags per merged event | ≤ 32 | `ErrTooManyTags` |
| batch vs event value | same key, different value rejected | `ErrConflictingTag` |
| tags with zero events | rejected | `ErrInvalidTag` |
| `ReadByTags` limit | 0 → 500 default; 1–1000 accepted | `ErrInvalidTagQuery` |

The key charset is ASCII, so bytes and runes coincide. The value cap fits a
btree text entry and keeps a malformed insert from wedging the log.

### 2.4 `ReadByTags`

A capability interface, not a method on `es.Store`, so a third-party store is
not broken by the addition:

```go
type TagQuery struct {
    Tags         map[string]string // AND; must be non-empty
    FromPosition uint64            // exclusive
    ToPosition   uint64            // inclusive; 0 = no bound
    Limit        int               // 0 = DefaultTagReadLimit
}

type TagReader interface {
    ReadByTags(context.Context, TagQuery) ([]Envelope, error)
}
```

- **AND semantics**: an event carries every `(key, value)` pair.
- **Ascending global position**, independent of query order.
- **From exclusive, To inclusive.** Pagination passes the last returned
  `GlobalPosition`; no duplicates, no gaps.
- **As-of is a global position**, not a document version and not `RecordedAt`:
  it returns events, and the caller folds them.
- An empty tag set and an inverted range are `ErrInvalidTagQuery`; "everything"
  is `ReadAll`, not a tag query.
- An empty result is `(nil, nil)` — "no tag matches" is a normal answer, unlike
  `ReadStream`'s missing stream.

The reference Postgres query drives events in position order and probes the
tag-first key once per tag, so `LIMIT` stops the scan instead of materializing
the whole intersection:

```sql
SELECT e.* FROM events e
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

The planner may instead intersect `event_tags` first for a rare tag; both plans
satisfy the contract.

### 2.5 The projection is synchronous

Each append inserts its tag rows in **one multi-row statement per append**,
inside the append transaction, after the event inserts return their positions.
A zero-tag append skips the statement entirely.

Consequences: a committed append is immediately visible to `ReadByTags`; a tag
insert failure rolls the events back, so an event is never committed without
its tags; there is no feed lag between a document change and its tag query; and
the append pays two rows per FlowDoc event plus one statement.

`RebuildTags` deletes the workspace's rows and re-projects `events.tags` in one
Repeatable Read transaction (Postgres; SQLite gets the same from its single
connection), so a failure leaves the live index unchanged. It pre-checks every
stored tag with the strict decoder — a non-object, an invalid key or value, or
an over-cap set is refused, because `jsonb_each_text` would stringify a
non-string member and `json_each` would expand a string array into synthetic
keys. `VerifyTags` rebuilds into a temporary table and diffs both directions
without touching the live index; it applies the same guard. **Both require
quiescence**: a rebuild is a restore operation, and its report is
snapshot-bound. An append committed during the rebuild keeps its own tag rows
(the delete and re-insert see only the snapshot), so the projection is not
lost; a racing *rebuild* may surface as a serialization error, loud and
retryable. Stop writers for a predictable report and to keep rebuilds from
overlapping.

### 2.6 Attestation: a per-batch hash and a sibling anchor root

Tags are excluded from the event hash, so the chain does not attest them. They
are attested at the same trust split as log order:

- **`log_order.tags_hash bytea NULL`** — the batch's `es.TagsHash`, computed in
  the same transaction from exactly its own tag rows. `NULL` means no event in
  the batch carried a tag; an untagged batch is not an empty tag set.
- **`anchor.tags_root_hash bytea NULL`** — the exported `es.TagsRoot` over the
  covered `log_order` rows with a non-NULL `tags_hash`, plus
  `tags_from_position` and `tags_count`. `NULL` is a legacy anchor recorded
  before this migration (including a v0.22-style order-only anchor): it skips
  the tags comparison exactly as `OrderRootHash == nil` skips order. A new
  anchor always records a real root, even over zero tagged rows.
- **`OrderRoot` is unchanged** and ignores `TagsHash`, so old order roots and
  anchors verify byte-for-byte.

The canonical encodings live beside `BatchOrderHash`/`OrderRoot`:

```text
es-lite.tags.v1        (len-prefixed), u32 count,
                       per entry sorted by (Position, Key, Value):
                         u64 Position ‖ len(Key) ‖ Key ‖ len(Value) ‖ Value

es-lite.tags-root.v1   (len-prefixed), u64 row count,
                       per row sorted by ascending From:
                         u64 From ‖ u64 To ‖ 32-byte TagsHash
```

Sorting is inside the hash, so map iteration and query order do not matter.
The collapse of duplicate `(position, key)` entries happens in the stored JSON
object; the Go API cannot produce one. Goldens pin both encodings
(`es/tags_hash_test.go`).

`CheckAnchor` verifies the stored `log_order.tags_hash` through the exported
root — it detects a changed batch hash or a changed covered row set. The deeper
audit is `VerifyTagHashes`/`es-verify tags`: it recomputes each batch's hash
from `events.tags`, so a database writer who changes the event tags and the
projection together still fails unless they also forge every anchor off-box.
This is the same trust boundary as `unique_claims` and order, and it is
narrower than "tags are unsigned".

### 2.7 Multi-stream append

FlowDoc needs several streams in one transaction: one content-free document
notice plus block events, with one command id and one outcome. The contract is
`es.MultiAppend`; Postgres exposes the caller-owned
`(*Workspace).AppendMultiTx(ctx, db, m)`, SQLite the store-owned
`(*Store).AppendMulti(ctx, m)`.

- **Caller owns the transaction** on Postgres: the store never commits or rolls
  back and sets `app.workspace_id` once. On error the caller must roll back.
- **One workspace**; every append is on the same `Workspace`.
- **Distinct streams.** A duplicate stream is `ErrDuplicateMultiStream`: the
  second `ExpectedVersion` would be stale.
- **Non-empty.** No appends, or an append with no events, is
  `ErrEmptyMultiAppend`.
- **Shared command identity.** `CommandID`, `CorrelationID`, `CausationID`,
  `Actor` and `SpanContext` must agree; a mismatch is
  `ErrConflictingMultiAppend`. A zero `OccurredAt` resolves to one server time
  for the whole transaction; non-zero values must agree.
- **Input order.** Appends execute in the given order and positions follow it.
  The store does not sort.
- **Per-stream optimistic concurrency.** Each append checks its own
  `ExpectedVersion`; any conflict aborts the whole transaction.
- **Caller-owned ids.** A supplied `EventID` is preserved; zero mints a UUIDv7.
  Deterministic derivation stays the caller's job and **must use a global
  event index across all appends** — the runtime's `CommandID:index`
  derivation restarts at zero and would collide on the second stream.
- **All-or-nothing.** Every append writes its own `log_order` row and its own
  tag rows; any failure rolls back the events, the tags, the claims, the order
  rows and the hint together.
- **Deadlocks.** Overlapping multi-appends in different orders can deadlock.
  The store surfaces the Postgres error and the caller retries the whole
  transaction; sorting appends by canonical stream id is the recommended
  convention.
- **One append hint.** `pg_notify` is emitted after the last append, preserving
  ADR 0039's last-statement property for the transaction as a whole.

### 2.8 Erasure and the trust boundary

Tags are immutable and indexed: deleting an `event_tags` row is a cache wipe,
and the next rebuild resurrects it from `events.tags`. A tag value must
therefore be a **non-personal identifier**. Personal data follows ADR 0025:
the caller supplies a keyed MAC or an opaque id and destroys the key; es-lite
stores the bytes verbatim, and the same key lets a clone match them.

The database remains the write trust boundary, as for claims and order. The
off-box anchor's `tags_root_hash` is the evidence that tag content did not
change after the fact; `es-verify tags` recomputes from the event rows.

## 3. Consequences

### 3.1 Append and footprint

- A tagged append pays one extra statement, the jsonb column, and two tag rows
  per FlowDoc event. The 100k-event smoke (2026-11-23) measured p50 2.7 ms /
  p99 6.1 ms for 10-event tagged appends; the untagged 2026-11-21 10M baseline
  was p50 ~2.5 ms / p99 ~3.9 ms. The runs are not directly comparable, but the
  tagged append is the same order of magnitude. See
  [the smoke results](../benchmarks/2026-11-23-event-tags-smoke.md).
- `event_tags` cost ≈ 145 bytes/row (heap + PK) in the smoke: 200k rows / 29 MB
  for 100k events. At 500M events and two tags each, ~1B rows implies roughly
  150 GB — inside the design's 150–250 GB estimate. This is an anchor, not a
  measurement; index fill factor and text overhead move with scale. The full
  10M and 50M tagged runs were **not** measured.
- `ReadByTags` paged 10 × 1,000 events at p50 3.1 ms / p99 4.0 ms in the same
  smoke.

### 3.2 Rebuild and verification

`RebuildTags` is linear in tag rows: a full events scan plus one insert per
tag row, in one transaction. On a large store, budget it as a quiesced restore
step, not an online job. `VerifyTags` doubles the scan (live vs temporary) but
never mutates the live index. `VerifyTagHashes` streams each covered batch and
its tagged events through one join, ordered by `from_position`: the audit is one
round trip, not one query per batch, and the database's work is proportional to
the covered rows and the tagged events it finds.

`es-verify all` gained a tag-attestation stage. It treats **zero tagged
batches as intact**, because an untagged log is valid; the explicit
`es-verify tags` command still reports `EMPTY` (exit 2) when invoked on its own.
`--chains-only` still stops before the stage, preserving the events-only restore
path.

### 3.3 RLS and grants

`event_tags` has its own policy and grants. The appender needs `SELECT, INSERT`;
the rebuild principal needs `SELECT, INSERT, DELETE`; readers need `SELECT`.
The anchor/verify principal already has `es_relay` plus `SELECT` on `events`
and `log_order`, which is what `es-verify tags` needs. No existing grant grows.

### 3.4 No legacy path, and nothing to backfill

The change is additive. Existing events have `NULL` tags, existing `log_order`
rows have `NULL` tags_hash, and existing anchors have no tags root — all three
are the untagged/legacy case and verify exactly as before. There is no backfill
and no dual write, because there is no earlier tag information to preserve. A
store that never tags pays nothing after the migration.

Tags are **not** chain-attested and never will be in this envelope version:
doing so would change every event hash or require a conditional canonical
format. Their attestation is the per-batch hash and the sibling root.

## 4. Alternatives considered

- **`event_tags` as an authoritative append-only side table.** Rejected: an
  events-only restore loses tags, and the table becomes a second source.
- **`events.tags` jsonb with a GIN index, no projection.** Simpler, but `AND` +
  ordered range + `LIMIT` becomes a bitmap scan and sort, and a common tag
  scans most of a document. Rejected for the primary read path.
- **A versioned binary tag blob on the event.** Smallest and strictest, but the
  log becomes unreadable to operators. Rejected in favour of jsonb; the
  projection carries the strictness.
- **A separate `stream_tags` table for spaces.** Normalized and small, but a
  space feed fans out over documents and loses position ordering. Rejected.
- **Folding `tags_hash` into the order root.** Fewer anchor fields, but the
  order root would start attesting content and need a root version. Rejected
  for a sibling root.
- **Attesting tags in the chain.** Breaks historical hashes or needs a new
  envelope version. Rejected for this release.
- **Tag-only appends** (tags with zero events). Rejected: tags ride on events
  like constraint ops, and an invisible tag write is the failure mode ADR 0043
  removed.
- **A `ReadByTags` method on `es.Store`.** Breaks third-party stores. Rejected
  for the capability interface.

## 5. Rollout

- **Migrations:** Postgres `0016_event_tags.sql` (column, check, partitioned
  table and eight partitions, RLS, `log_order.tags_hash`); SQLite
  `0014_event_tags.sql` (column, table, `log_order.tags_hash`). The new table
  starts empty, so its creation is catalog-only; there is no concurrent index
  build and no backfill (ADR 0043's 0015 index section does not apply).
- **Store and runtime:** tags are written, projected and decoded on every
  append and read; `es.Meta.Tags` and the optional `es.Decider.Tags` are the
  seams.
- **Maintenance and audit:** `RebuildTags`/`VerifyTags` on the workspace,
  `VerifyTagHashes` on the store, `es-verify tags [--rebuild] [--upto N]
  [<workspace>]`, and the new `es-verify all` stage.
- **Multi-append:** `(*Workspace).AppendMultiTx` and `(*Store).AppendMulti`
  with `es.ValidateMultiAppend`.
- **Grants and restore:** runbook updated for the three `event_tags` grants,
  the `events` → `RebuildTags` → `RebuildClaims` → `es-verify tags` restore
  order, and the quiescence requirement.
- **Validation:** a 100k tagged smoke; the 10M and 50M tagged runs are recorded
  as not measured in
  [the smoke results](../benchmarks/2026-11-23-event-tags-smoke.md).
- **Release note (v0.23.0):** event tags on the event log and their
  `event_tags` projection, `ReadByTags`, caller-owned multi-stream append,
  `es-verify tags` and the tag-attestation stage; migrations Postgres 0016 /
  SQLite 0014. Tags add ~2 rows per FlowDoc event and are **not** chain-attested
  — the per-batch `log_order.tags_hash` and the sibling anchor
  `tags_root_hash` attest them. Additive public API: new capability interfaces
  and fields, no break. Release notes are generated from commit subjects by
  `.github/workflows/release.yml`; there is no `CHANGELOG.md` to edit.
- **No API break** beyond new fields, errors and interfaces; a tag-bearing
  append that previously could not exist now validates, and an invalid tag is a
  typed rejection.

## 6. Resolved

1. **Where tags live:** on the event (`events.tags` jsonb), with `event_tags` a
   synchronous, rebuildable projection.
2. **Chain:** tags are excluded from `CanonicalEnvelope`; the per-batch
   `tags_hash` and the sibling anchor `tags_root_hash` attest them.
3. **FlowDoc model:** document = stream, immutable space as a batch tag, block
   as a per-event tag; two tag rows per event.
4. **Read contract:** `TagQuery` AND semantics, ascending position, `From`
   exclusive / `To` inclusive, position pagination, 500/1000 limits.
5. **Projection:** one multi-row insert in the append transaction; rebuild and
   verify require quiescence; non-string values are refused, not stringified.
6. **Multi-append:** distinct streams, one command identity, input order,
   per-stream expected versions, caller-owned ids with a global index,
   all-or-nothing; deadlocks surface and the caller retries.
7. **Restore:** `events` (and optionally `log_order`) → `RebuildTags` →
   `RebuildClaims` → `es-verify tags` / `es-verify all --chains-only`.
8. **Erasure:** tags are immutable, non-personal identifiers; personal data is
   a keyed MAC or opaque id under ADR 0025.
