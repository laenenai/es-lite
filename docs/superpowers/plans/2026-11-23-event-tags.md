# Event Tags and Multi-Stream Append Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add first-class, attested, queryable event tags and a caller-owned multi-stream append to es-lite without breaking the events-only restore model.

**Architecture:** `events.tags jsonb` is the source; `event_tags` is a synchronous, partition-aligned projection rebuilt from the log. Tags are queried through a tag-first btree by a `TagReader` capability. A per-batch `log_order.tags_hash` and a sibling anchor `tags_root_hash` attest them outside the event hash. `AppendMultiTx` reuses the single-append body inside one caller-owned transaction.

**Tech Stack:** Go 1.27.x, PostgreSQL 17 (pgx v5), SQLite (modernc), `encoding/json`, the in-repo `migrate` loader, `cmd/es-verify`, `cmd/es-bench`.

**Spec:** `docs/superpowers/specs/2026-11-23-event-tags-design.md` — read it before Task 1. It argues every choice this plan assumes.

## Global Constraints

- Go 1.27.x. Ship as `v0.23.0` (additive API, new migrations).
- Tag key: 1–128 bytes matching `^[a-z0-9._:-]+$`.
- Tag value: 0–512 bytes, valid UTF-8, no NUL.
- Maximum 32 tags per **merged** event (batch tags overlaid with event tags).
- `event_tags` Postgres PK: `(workspace_id, tag_key, tag_value, global_position)`; no secondary index.
- `event_tags` SQLite PK: `(tag_key, tag_value, global_position)` (one database is one workspace).
- Postgres partitions: `HASH (workspace_id)`, `MODULUS 8`, `p0`–`p7`, matching `events`.
- Read defaults: `DefaultTagReadLimit = 500`, `MaxTagReadLimit = 1000`.
- Attestation domains: `es-lite.tags.v1`, `es-lite.tags-root.v1`, length-prefixed (u32) like `es-lite.log-order.v1`.
- Tags are excluded from `CanonicalEnvelope`; no existing event hash, order root, or legacy anchor may change.
- Migrations: Postgres `0016_event_tags.sql`, SQLite `0014_event_tags.sql`.
- SQLite has no caller-owned append transaction; it gets `AppendMulti`, not `AppendMultiTx`.
- Do not add tags to `es.Store`. Add capability interfaces.
- Postgres commands below show `PG_DSN=...` as shorthand for the DSN exported by
  `task test:pg` (`postgres://postgres:pw@127.0.0.1:5433/eslite?sslmode=disable`)
  or your own test database.

## Review Focus

These are the input classes the spec implies but no single task's happy-path test covers. Each line has a test in the task that owns the code.

1. **An append with no tags at all** — `events.tags` must be `NULL` (not `{}`), `event_tags` must gain zero rows, and `log_order.tags_hash` must be `NULL`; a batch-level tag must not leak into an event that overrides it to the same key.
2. **Pagination boundaries** — `FromPosition` is exclusive and `ToPosition` inclusive; `ReadByTags(From=last)` must not return `last`, and `To=last` must return it. A page at `ToPosition` must be stable.
3. **Workspace isolation on the projection** — an unscoped `event_tags` query returns no rows; the bypass arm requires `app.bypass_rls='on'` **and** `es_relay` membership; a workspace cannot read another workspace's tags.
4. **Multi-append atomicity and identity** — a conflict or invalid tag in the second append leaves zero rows from the first; two appends for the same stream are rejected; a zero `EventID` mints, a supplied one survives; deterministic ids restarted per stream collide (`ErrDuplicateEventID`) and that is documented, not silently fixed.
5. **Legacy and hash stability** — a pre-0016 anchor JSON (`tags_root_hash` absent) still verifies; an event with tags hashes exactly as the same event without tags; the order root is byte-identical to the pre-change root.

---

## File Map

| File | Responsibility |
|---|---|
| `es/tags.go` (new) | `TagQuery`, `TagReader`, `TagMaintainer`, validation, merge, JSON codec, limits |
| `es/tags_hash.go` (new) | `TagEntry`, `TagsHash`, `TagsRow`, `TagsRoot` |
| `es/multi.go` (new) | `MultiAppend`, `ValidateMultiAppend` |
| `es/store.go` (modify) | `AppendParams.Tags`, `EventData.Tags` |
| `es/envelope.go` (modify) | `Envelope.Tags` |
| `es/meta.go` (modify) | `Meta.Tags` |
| `es/decider.go` (modify) | optional `Tags func(E) map[string]string` |
| `es/errors.go` (modify) | tag/multi typed errors |
| `es/order.go` (modify) | `OrderRow.TagsHash` (ignored by `OrderRoot`) |
| `es/anchor.go` (modify) | `Anchor.TagsRootHash`, `TagsFromPosition`, `TagsCount` |
| `aggregate/runtime.go` (modify) | copy `Meta.Tags`, call `Decider.Tags` |
| `postgres/migrations/0016_event_tags.sql` (new) | tags column, projection, partitions, RLS, `log_order.tags_hash` |
| `postgres/store.go` (modify) | `envelopeCols`/`scanEnvelope` |
| `postgres/wsstore.go` (modify) | append tags, projection, `tags_hash`, multi append |
| `postgres/tags.go` (new) | `ReadByTags` |
| `postgres/tags_maintenance.go` (new) | rebuild/verify projection, verify tag hashes |
| `postgres/anchor.go` (modify) | tags root in `Anchor`/`CheckAnchor`/`OrderRows` |
| `sqlite/migrations/0014_event_tags.sql` (new) | tags column, projection |
| `sqlite/store.go` (modify) | `envelopeCols`/`scanEnvelope`, append tags, projection, multi |
| `sqlite/tags.go` (new) | `ReadByTags` |
| `sqlite/tags_maintenance.go` (new) | rebuild/verify projection, verify tag hashes |
| `sqlite/anchor.go` (modify) | tags root |
| `cmd/es-verify/main.go` (modify) | `tags` command, `all` stage |
| `cmd/es-bench/main.go` (modify) | tags-on benchmark scenario |
| `docs/adr/0044-event-tags.md` (new) | the ADR this design becomes |
| `docs/deploy/README.md` (modify) | grants, restore/rebuild/verify |

---

## Preflight (execution setup, not a task)

- Create an isolated worktree on `feat/event-tags` with `superpowers:using-git-worktrees`.
- Run the baseline: `task test` and, with Docker available, `task test:pg`.
- Keep the worktree clean between tasks; every task ends with one commit.

---

### Task 1: Core tag vocabulary, validation, and JSON codec

**Files:**
- Create: `es/tags.go`
- Modify: `es/errors.go`
- Test: `es/tags_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  ```go
  const (
      MaxTagKeyBytes   = 128
      MaxTagValueBytes = 512
      MaxTagsPerEvent  = 32
      DefaultTagReadLimit = 500
      MaxTagReadLimit  = 1000
  )

  func ValidateTag(key, value string) error
  func MergedTags(batch map[string]string, perEvent []map[string]string) ([]map[string]string, error)
  func EncodeTags(tags map[string]string) ([]byte, error)
  func DecodeTags(raw []byte) (map[string]string, error)

  type TagQuery struct {
      Tags         map[string]string
      FromPosition uint64
      ToPosition   uint64
      Limit        int
  }
  func (q TagQuery) Validate() error
  func (q TagQuery) EffectiveLimit() int
  ```

- [ ] **Step 1: Write the failing tests**

In `es/tags_test.go` (package `es_test`), add a table test `TestValidateTag` covering: valid `flowdoc.space`/`space-1`; empty key; 129-byte key; `Flowdoc.Space` (uppercase rejected); `space/1` (slash rejected); 513-byte value; value with `\x00`; invalid UTF-8 (`string([]byte{0xff})`). Each bad case must satisfy `errors.Is(err, es.ErrInvalidTag)`.

Add `TestMergedTags`:
```go
got, err := es.MergedTags(
    map[string]string{"flowdoc.space": "s1", "k": "batch"},
    []map[string]string{
        {"flowdoc.block": "b1"},
        {"k": "batch"},
    },
)
// got[0] == {space:s1, k:batch, block:b1}; got[1] == {space:s1, k:batch}
```
Add conflict case `{"k":"batch"}` + event `{"k":"event"}` → `ErrConflictingTag`; 33 tags → `ErrTooManyTags`; batch-less/empty → `nil` maps.

Add `TestTagsJSONRoundTrip`: `EncodeTags(nil)` and `EncodeTags(map[string]string{})` are `nil`; a populated map encodes to a JSON object whose keys are sorted; `DecodeTags(nil)`, `DecodeTags([]byte("{}"))` return nil; a non-object (`[]`) and a non-string value (`{"k":1}`) return an error.

Add `TestTagQueryValidate`: empty `Tags` → `ErrInvalidTagQuery`; `Limit=-1` and `Limit=1001` → error; `From=5,To=4` → error; `Limit=0` → `EffectiveLimit()==500`; `Limit=1000` → 1000.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./es -run 'TestValidateTag|TestMergedTags|TestTagsJSONRoundTrip|TestTagQueryValidate' -v`
Expected: FAIL — `undefined: es.MergedTags`, `es.ErrInvalidTag`, etc.

- [ ] **Step 3: Implement**

Add to `es/errors.go`:
```go
ErrInvalidTag             = errors.New("es-lite: invalid tag")
ErrTooManyTags            = errors.New("es-lite: too many tags")
ErrConflictingTag         = errors.New("es-lite: conflicting tag value")
ErrInvalidTagQuery        = errors.New("es-lite: invalid tag query")
```
Add to `es/tags.go` the constants, `TagReader`, `TagMaintainer`, `TagsRebuildReport`, `TagsVerifyReport`, `TagsHashVerifyReport`, `TagRowMismatch` (signatures below), and the functions above.

Validation details:
- key length in bytes, regex `^[a-z0-9._:-]+$` compiled once at package level;
- value length in bytes, `utf8.ValidString`, no `\x00`;
- `MergedTags` starts each event's set from `batch` (copy), overlays that event's map; a key already present with a different value is `ErrConflictingTag` naming the key only; it validates the batch once, each event's tags, and the merged cap; it returns `nil` for an event with no merged tags. It does not depend on `EventData`, so this task compiles before the fields land in Task 3.

Codec details:
- `EncodeTags` validates, returns `nil` for `len==0`, else `json.Marshal` (Go sorts map keys);
- `DecodeTags` returns `nil` for `len(raw)==0`, `"null"`, or `{}`; `json.Unmarshal` into `map[string]string` so a non-string value fails; wrap the error with `ErrInvalidTag`.

Interface skeletons (implement bodies now; the adapters use them later):
```go
type TagReader interface {
    ReadByTags(ctx context.Context, q TagQuery) ([]Envelope, error)
}
type TagsRebuildReport struct{ Events, Tags int64 }
type TagsVerifyReport struct {
    OK              bool
    Events, Tags    int64
    FirstDivergence string
}
type TagRowMismatch struct {
    From              uint64
    ExpectedTagsHash  []byte
    ActualTagsHash    []byte
}
type TagsHashVerifyReport struct {
    OK          bool
    RowsChecked uint64
    Mismatch    *TagRowMismatch
}
type TagMaintainer interface {
    RebuildTags(ctx context.Context) (TagsRebuildReport, error)
    VerifyTags(ctx context.Context) (TagsVerifyReport, error)
}
type TagAttester interface {
    VerifyTagHashes(ctx context.Context, upto uint64) (*TagsHashVerifyReport, error)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./es -run 'TestValidateTag|TestMergedTags|TestTagsJSONRoundTrip|TestTagQueryValidate' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add es/tags.go es/errors.go es/tags_test.go
git commit -m "feat(es): add event tag vocabulary, validation, and JSON codec"
```

---

### Task 2: Attestation encodings

**Files:**
- Create: `es/tags_hash.go`
- Test: `es/tags_hash_test.go`

**Interfaces:**
- Consumes: `HashSize` from `es`.
- Produces:
  ```go
  type TagEntry struct {
      Position uint64
      Key      string
      Value    string
  }
  func TagsHash(entries []TagEntry) [HashSize]byte

  type TagsRow struct {
      From, To uint64
      TagsHash []byte
  }
  func TagsRoot(rows []TagsRow) [HashSize]byte
  ```

- [ ] **Step 1: Write the failing golden tests**

`TestTagsHashGolden` pins the fixture (deliberately scrambled input order):
```go
entries := []es.TagEntry{
    {Position: 42, Key: "flowdoc.space", Value: "space-1"},
    {Position: 42, Key: "flowdoc.block", Value: "block-1"},
    {Position: 43, Key: "flowdoc.space", Value: "space-1"},
}
// expected pre-image hex:
// 0000000f65732d6c6974652e746167732e763100000003
// 000000000000002a0000000d666c6f77646f632e626c6f636b00000007626c6f636b2d31
// 000000000000002a0000000d666c6f77646f632e73706163650000000773706163652d31
// 000000000000002b0000000d666c6f77646f632e73706163650000000773706163652d31
// expected hash: 5d2d8d31f7add6d5f3165c573d8bbb0f620035eca871773d0c37dedc5c255abe
```
The test reimplements the pre-image locally (as `es/order_test.go` does) and asserts both bytes and hash.

`TestTagsRootGolden` pins rows given out of order:
```go
rows := []es.TagsRow{
    {From: 5, To: 7, TagsHash: mustHex("9f72ea0cf49536e3c66c787f705186df9a4378083753ae9536d65b3ad7fcddc4")},
    {From: 1, To: 4, TagsHash: mustHex("5d2d8d31f7add6d5f3165c573d8bbb0f620035eca871773d0c37dedc5c255abe")},
}
// sorted by From; expected pre-image:
// 0000001465732d6c6974652e746167732d726f6f742e7631
// 0000000000000002
// 0000000000000001 0000000000000004 5d2d...5abe
// 0000000000000005 0000000000000007 9f72...ddc4
// expected hash: 1059292641a7da566f0db0326a4fc348669a792f739fa36178d904ea2070b253
```
Add `TestTagsHashDeterministic` (shuffled input yields the same hash) and `TestTagsRootPanicsOnBadHash` (`defer recover`, 31-byte hash).

- [ ] **Step 2: Run to verify failure**

Run: `go test ./es -run 'TestTagsHash|TestTagsRoot' -v`
Expected: FAIL — undefined `es.TagsHash`, `es.TagsRoot`.

- [ ] **Step 3: Implement**

Domain constants `tagsDomain = "es-lite.tags.v1"` and `tagsRootDomain = "es-lite.tags-root.v1"`. Use the existing `appendString` helper for length prefixes (u32).
- `TagsHash`: stable sort by `(Position, Key, Value)`; `u32` entry count; per entry `u64 Position`, len-prefixed key, len-prefixed value.
- `TagsRoot`: stable sort by `From`; `u64` row count; per row `u64 From`, `u64 To`, 32 hash bytes; panic on a hash that is not `HashSize` (mirror `OrderRoot`).
- Document both encodings in the Go doc comments, including the empty-set contract.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./es -run 'TestTagsHash|TestTagsRoot' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add es/tags_hash.go es/tags_hash_test.go
git commit -m "feat(es): add versioned tags and tags-root attestation encodings"
```

---

### Task 3: Envelope, append, meta, decider, and runtime wiring

**Files:**
- Modify: `es/envelope.go`, `es/store.go`, `es/meta.go`, `es/decider.go`
- Modify: `aggregate/runtime.go`
- Test: `es/tags_test.go` (round trip), `aggregate/tags_test.go` (new)

**Interfaces:**
- Consumes: Task 1.
- Produces: `Envelope.Tags`, `AppendParams.Tags`, `EventData.Tags`, `Meta.Tags`, `Decider.Tags func(E) map[string]string` (optional). The runtime fills `AppendParams.Tags` from `meta.Tags` and each `EventData.Tags` from `decider.Tags(ev)`.

FlowDoc lives outside this repository. This task supplies its seams: put `flowdoc.space` in `Meta.Tags` and return `flowdoc.block` from `Decider.Tags`. Do not add FlowDoc code here.

- [ ] **Step 1: Write the failing tests**

In `es/tags_test.go`, `TestEnvelopeCarriesTags` builds an envelope with `Tags: map[string]string{"k":"v"}` and asserts it survives a `CanonicalEnvelope`/`EnvelopeHash` call unchanged — and that removing `Tags` yields the **same** hash (tags are excluded). Use the existing golden event fixture pattern from `es/hash_test.go`.

In `aggregate/tags_test.go` (package `aggregate_test`), use a fake `es.Store` that records the last `AppendParams` (the package already has append-fake patterns; mirror them):
- a decider with a counter in `Tags` (`n++; return {"flowdoc.block": fmt.Sprintf("b%d", n)}`), producing two events;
- `Handle(ctx, sid, cmd, es.Meta{Tags: map[string]string{"flowdoc.space":"s1"}})` produces `AppendParams.Tags == {space:s1}` and `EventData.Tags == {block:b1}` then `{block:b2}`;
- a decider with `Tags: nil` produces nil event tags.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./es ./aggregate -run 'TestEnvelopeCarriesTags|TestRuntimeCopiesTags' -v`
Expected: FAIL — unknown fields / no recorded tags.

- [ ] **Step 3: Implement**

- `es/envelope.go`: add `Tags map[string]string` after `ConstraintOps` (or beside `SpanContext`), with a comment that tags are not chain-attested.
- `es/store.go`: add `Tags map[string]string` to `AppendParams` (batch-scoped) and `EventData` (per event), documented with the merge rule and the `≤32` cap.
- `es/meta.go`: add `Tags map[string]string` with a comment that it is batch-scoped and validated by the adapter.
- `es/decider.go`: add `Tags func(event E) map[string]string` with a purity comment; nil means none.
- `aggregate/runtime.go` in `handleOnce`: after encoding each event, set `encoded[i].Tags = r.decider.Tags(events[i])` when non-nil; set `Tags: meta.Tags` on the `AppendParams`.
- Do not call `MergedTags` in the runtime; the store owns validation and merging.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./es ./aggregate -run 'TestEnvelopeCarriesTags|TestRuntimeCopiesTags' -v`
Expected: PASS. Then `go test ./es ./aggregate` to catch hash regressions.

- [ ] **Step 5: Commit**

```bash
git add es/envelope.go es/store.go es/meta.go es/decider.go aggregate/runtime.go es/tags_test.go aggregate/tags_test.go
git commit -m "feat: carry batch and per-event tags through the append path"
```

---

### Task 4: Multi-append contract

**Files:**
- Create: `es/multi.go`
- Modify: `es/errors.go`
- Test: `es/multi_test.go`

**Interfaces:**
- Consumes: `AppendParams` (Task 3).
- Produces:
  ```go
  type MultiAppend struct {
      Appends []AppendParams
  }
  func ValidateMultiAppend(m MultiAppend) error
  ```

- [ ] **Step 1: Write the failing tests**

`TestValidateMultiAppend` table:
- empty `Appends` → `ErrEmptyMultiAppend`;
- an append with zero `Events` → `ErrEmptyMultiAppend`;
- two appends with the same canonical `StreamID` → `ErrDuplicateMultiStream`;
- different `CommandID` → `ErrConflictingMultiAppend`;
- different `Actor` or `SpanContext` → `ErrConflictingMultiAppend`;
- different non-zero `OccurredAt` → `ErrConflictingMultiAppend`;
- two distinct streams with identical metadata and zero `OccurredAt` → nil.

Build valid stream ids with `es.NewStreamID("counter", "a")` / `"b"`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./es -run TestValidateMultiAppend -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement**

Add errors to `es/errors.go`: `ErrEmptyMultiAppend`, `ErrDuplicateMultiStream`, `ErrConflictingMultiAppend`.
Implement `ValidateMultiAppend` exactly as tested. Compare `Actor` and `SpanContext` with `==` (both are comparable value types). Compare `OccurredAt` only when both are non-zero, after `TruncateMicros`, with `time.Time.Equal`.
Document that the store does not sort appends or derive event ids.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./es -run TestValidateMultiAppend -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add es/multi.go es/errors.go es/multi_test.go
git commit -m "feat(es): define the multi-append contract and its errors"
```

---

### Task 5: Postgres schema

**Files:**
- Create: `postgres/migrations/0016_event_tags.sql`
- Modify: `postgres/postgres_test.go` and `postgres/wsstore_test.go` (truncate lists)
- Test: `postgres/migrations_test.go` (add tests)

**Interfaces:**
- Consumes: nothing.
- Produces: `events.tags jsonb`, `event_tags` + eight partitions + RLS, `log_order.tags_hash bytea`.

- [ ] **Step 1: Write the failing schema tests**

In `postgres/migrations_test.go` add:
- `TestEventTagsSchema`: query `pg_partitioned_table`/`pg_inherits` or `pg_class` for `event_tags` and `event_tags_p0..p7`; each partition must exist. Query `pg_indexes` for the PK columns `(workspace_id, tag_key, tag_value, global_position)` on every partition; assert no other index on `event_tags` (only the PK).
- `TestEventTagsRLS`: `pg_class.relrowsecurity` and `relforcerowsecurity` true for `event_tags`; a policy named `event_tags_workspace_isolation` exists.
- `TestEventsTagsColumn`: `information_schema.columns` has `events.tags` of type `jsonb`, nullable.
- `TestLogOrderTagsHashColumn`: `log_order.tags_hash` is `bytea`, nullable.

- [ ] **Step 2: Run to verify failure**

Run: `PG_DSN=... go test ./postgres -run 'TestEventTagsSchema|TestEventTagsRLS|TestEventsTagsColumn|TestLogOrderTagsHashColumn' -v`
Expected: FAIL — table/column absent.

- [ ] **Step 3: Write the migration**

`postgres/migrations/0016_event_tags.sql`, with a leading comment tying it to the spec:
```sql
ALTER TABLE events ADD COLUMN tags jsonb;
ALTER TABLE events ADD CONSTRAINT events_tags_object
    CHECK (tags IS NULL OR jsonb_typeof(tags) = 'object') NOT VALID;

CREATE TABLE IF NOT EXISTS event_tags (
    workspace_id    text   NOT NULL,
    tag_key         text   NOT NULL,
    tag_value       text   NOT NULL,
    global_position bigint NOT NULL,
    PRIMARY KEY (workspace_id, tag_key, tag_value, global_position)
) PARTITION BY HASH (workspace_id);

CREATE TABLE IF NOT EXISTS event_tags_p0 PARTITION OF event_tags
    FOR VALUES WITH (MODULUS 8, REMAINDER 0);
-- repeat p1..p7, matching events 0001

ALTER TABLE events VALIDATE CONSTRAINT events_tags_object;

ALTER TABLE event_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_tags FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS event_tags_workspace_isolation ON event_tags;
-- Copy the CURRENT events policy (0007_policy_hardening.sql), not 0001's
-- pre-role-gate arm:
CREATE POLICY event_tags_workspace_isolation ON event_tags
    USING (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay')
                        THEN pg_has_role(current_user, 'es_relay', 'member')
                        ELSE false END))
    WITH CHECK (workspace_id = current_setting('app.workspace_id', true)
           OR (current_setting('app.bypass_rls', true) = 'on'
               AND CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay')
                        THEN pg_has_role(current_user, 'es_relay', 'member')
                        ELSE false END));

ALTER TABLE log_order ADD COLUMN tags_hash bytea;
```
Add `event_tags` to both `TRUNCATE` lists in `postgres/postgres_test.go` and `postgres/wsstore_test.go`.

- [ ] **Step 4: Run to verify pass**

Run: `PG_DSN=... go test ./postgres -run 'TestEventTagsSchema|TestEventTagsRLS|TestEventsTagsColumn|TestLogOrderTagsHashColumn' -v`
Expected: PASS. Then run the whole Postgres suite once: `task test:pg` (expect existing tests green).

- [ ] **Step 5: Commit**

```bash
git add postgres/migrations/0016_event_tags.sql postgres/migrations_test.go postgres/postgres_test.go postgres/wsstore_test.go
git commit -m "feat(postgres): add event tags projection, partitions, RLS, and log_order.tags_hash"
```

---

### Task 6: Postgres append, projection, and envelope round trip

**Files:**
- Modify: `es/order.go` (add `OrderRow.TagsHash`), `postgres/store.go`, `postgres/wsstore.go`, `postgres/anchor.go` (the `readLogOrderRows` scan)
- Test: `postgres/tags_test.go` (new, package `postgres_test`)

**Interfaces:**
- Consumes: Tasks 1, 3; Task 5 schema.
- Produces: tags written on append; `Envelope.Tags` on every read; `event_tags` rows; `log_order.tags_hash`.

- [ ] **Step 1: Write the failing tests**

`postgres/tags_test.go`:
- `TestAppendPersistsTagsAndEnvelope`: append two events with batch `{flowdoc.space:s1}` and per-event `{flowdoc.block:b1/b2}`; read back via `ReadStream`; assert each `Envelope.Tags` has both keys and the right block.
- `TestAppendWithoutTagsWritesNullAndNoRows`: append one event with no tags; `SELECT tags FROM events ...` is NULL; `SELECT count(*) FROM event_tags WHERE workspace_id='ws_a'` is 0; `SELECT tags_hash FROM log_order ...` is NULL.
- `TestAppendRollbackWritesNoTags`: inside `AppendTx`, append then roll back; `event_tags` count is 0.
- `TestAppendRejectsInvalidTags`: table of invalid batch/event tags (bad key, 513-byte value, NUL, 33 tags, conflict) → `Append` returns the Task 1 error and writes nothing.
- `TestReadCorruptTagsFailsLoudly`: raw-SQL update a row's `tags` to `'"not-an-object"'` or `'{"k":1}'`; `ReadStream` must error, not silently return no tags.
- `TestTagsDoNotChangeEventHash`: append an event with no tags and one with tags; recompute `es.EnvelopeHash` from the returned envelopes; both match the stored `hash`.

- [ ] **Step 2: Run to verify failure**

Run: `PG_DSN=... go test ./postgres -run 'TestAppendPersistsTags|TestAppendWithoutTags|TestAppendRollbackWritesNoTags|TestAppendRejectsInvalidTags|TestReadCorruptTags|TestTagsDoNotChangeEventHash' -v`
Expected: FAIL — tags not stored/returned.

- [ ] **Step 3: Implement**

`postgres/store.go`:
- append `tags` to `envelopeCols` (last column);
- `scanEnvelope`: scan into `tagsRaw []byte`; `e.Tags, err = es.DecodeTags(tagsRaw)`; wrap a failure with the stream and version, like `constraint_ops`.

`postgres/wsstore.go`:
- Add an unexported adapter helper:
  ```go
  func mergedTags(p es.AppendParams) ([]map[string]string, error) {
      per := make([]map[string]string, len(p.Events))
      for i := range p.Events { per[i] = p.Events[i].Tags }
      return es.MergedTags(p.Tags, per)
  }
  ```
- `validateAppend`: call `mergedTags(p)` and return the error (this covers `Append`, `AppendTx`, and later multi).
- `append`: call `merged, err := mergedTags(p)` once; for each event, `tagsArg` = `string(encoded)` or nil via `es.EncodeTags(merged[i])`; add `tags` to the `INSERT INTO events` column/value lists (after `constraint_ops`); set `env.Tags = merged[i]`.
- After the event loop and before/with the `log_order` insert: build parallel arrays `keys, values, positions` from `merged`; if non-empty, one statement:
  ```sql
  INSERT INTO event_tags (workspace_id, tag_key, tag_value, global_position)
  SELECT $1, k, v, p
  FROM unnest($2::text[], $3::text[], $4::bigint[]) AS t(k, v, p)
  ```
- Build `[]es.TagEntry` from the same arrays; `tagsHash := es.TagsHash(entries)`; pass `tagsHash[:]` or nil to `log_order.tags_hash` (nil if no entries).
- Add `TagsHash []byte` to `es.OrderRow` (additive; `OrderRoot` ignores it). Add `tags_hash` to the `INSERT INTO log_order` and to `readLogOrderRows`' SELECT/scan (after `batch_hash`), scanning into the embedded `OrderRow.TagsHash`.

Do not add a statement when there are no tags.

- [ ] **Step 4: Run to verify pass**

Run: `PG_DSN=... go test ./postgres -run 'TestAppendPersistsTags|TestAppendWithoutTags|TestAppendRollbackWritesNoTags|TestAppendRejectsInvalidTags|TestReadCorruptTags|TestTagsDoNotChangeEventHash' -v`
Expected: PASS. Then `task test:pg` for regressions.

- [ ] **Step 5: Commit**

```bash
git add postgres/store.go postgres/wsstore.go postgres/tags_test.go
git commit -m "feat(postgres): write and project event tags in the append transaction"
```

---

### Task 7: Postgres `ReadByTags`

**Files:**
- Create: `postgres/tags.go`
- Test: `postgres/tags_test.go` (append tests)

**Interfaces:**
- Consumes: Task 6.
- Produces: `func (w *Workspace) ReadByTags(ctx context.Context, q es.TagQuery) ([]es.Envelope, error)`; compile assertion `var _ es.TagReader = (*Workspace)(nil)`.

- [ ] **Step 1: Write the failing tests**

- `TestReadByTagsAND`: three events tagged `{space:s1,block:b1}`, `{space:s1,block:b2}`, `{space:s2,block:b1}`; query `{space:s1,block:b1}` returns exactly the first.
- `TestReadByTagsRangeAndOrder`: query with `FromPosition` = first position (exclusive) and `ToPosition` = third (inclusive) returns positions 2 and 3 in ascending order; a query with `ToPosition` = second excludes the third.
- `TestReadByTagsPagination`: `Limit=1`; page with `From=0` returns the first; page with `From=first.GlobalPosition` returns the second; no duplicates.
- `TestReadByTagsLimit`: `Limit=0` returns at most 500 (seed a small set); `Limit=1001` and `Limit=-1` return `ErrInvalidTagQuery`.
- `TestReadByTagsRejectsEmptyQuery` and `TestReadByTagsRejectsInvertedRange`.
- `TestReadByTagsWorkspaceIsolation`: append to `ws_a` and `ws_b`; a query on `ws_a` never returns `ws_b`; an unscoped raw query (no `app.workspace_id`) returns zero rows.
- `TestEventTagsBypassRequiresRelay`: follow the `leastpriv_test.go` / `es_rls_probe` pattern; a non-member session with `app.bypass_rls='on'` sees zero `event_tags` rows, and an `es_relay` member sees them across workspaces.
- `TestReadByTagsAsOf`: query `ToPosition = second.GlobalPosition` returns only events at or before it.

- [ ] **Step 2: Run to verify failure**

Run: `PG_DSN=... go test ./postgres -run 'TestReadByTags' -v`
Expected: FAIL — undefined method.

- [ ] **Step 3: Implement**

Open a read transaction, `setWorkspace`, then build:
```sql
SELECT <envelopeCols>
FROM events e
WHERE e.workspace_id = $1
  AND e.global_position > $2
  AND ($3::bigint = 0 OR e.global_position <= $3)
  AND EXISTS (SELECT 1 FROM event_tags t
              WHERE t.workspace_id = e.workspace_id
                AND t.tag_key = $4 AND t.tag_value = $5
                AND t.global_position = e.global_position)
  -- one EXISTS per additional tag, with increasing parameter numbers
ORDER BY e.global_position
LIMIT $n
```
Use deterministic ordering of the tag map for parameter construction (sort keys) so tests and plans are stable. Resolve `q.EffectiveLimit()`.
Add `var _ es.TagReader = (*Workspace)(nil)`.

- [ ] **Step 4: Run to verify pass**

Run: `PG_DSN=... go test ./postgres -run 'TestReadByTags' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add postgres/tags.go postgres/tags_test.go
git commit -m "feat(postgres): add tag-first ReadByTags"
```

---

### Task 8: Postgres rebuild, verify, and tag-hash attestation

**Files:**
- Create: `postgres/tags_maintenance.go`
- Test: `postgres/tags_maintenance_test.go` (new)

**Interfaces:**
- Consumes: Tasks 2, 6.
- Produces:
  ```go
  var _ es.TagMaintainer = (*Workspace)(nil)
  func (w *Workspace) RebuildTags(ctx context.Context) (es.TagsRebuildReport, error)
  func (w *Workspace) VerifyTags(ctx context.Context) (es.TagsVerifyReport, error)
  func (s *Store) VerifyTagHashes(ctx context.Context, upto uint64) (*es.TagsHashVerifyReport, error)
  var _ es.TagAttester = (*Store)(nil)
  ```

- [ ] **Step 1: Write the failing tests**

- `TestRebuildTagsFromEventLog`: append tagged events; `DELETE FROM event_tags`; `RebuildTags` restores the exact rows and reports `Events`/`Tags`.
- `TestVerifyTagsOK` then `DELETE` one tag row → `VerifyTags` reports `OK=false` and a `FirstDivergence` naming the key/value/position.
- `TestVerifyTagsDetectsExtraRow`: insert a bogus tag row → divergence.
- `TestRebuildRefusesNonStringValue`: raw-SQL set `tags='{"k":1}'`; `RebuildTags` returns an error and does not delete the live index.
- `TestVerifyTagHashes`: append a tagged batch; `VerifyTagHashes(0)` reports `OK`; raw-SQL change `events.tags` (without `log_order`) → `OK=false` with the batch's `From` and both hashes; change `log_order.tags_hash` → also caught. An untagged batch has a NULL hash and is skipped.

- [ ] **Step 2: Run to verify failure**

Run: `PG_DSN=... go test ./postgres -run 'TestRebuildTags|TestVerifyTags|TestVerifyTagHashes' -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement**

`TagAttester` is already defined in Task 1; this task implements it.

`RebuildTags`: one transaction with `setWorkspace`; first a pre-pass:
```sql
SELECT count(*) FROM events e, jsonb_each(e.tags) kv
WHERE e.workspace_id=$1 AND e.tags IS NOT NULL
  AND jsonb_typeof(kv.value) <> 'string'
```
non-zero → error naming the count. Then `DELETE FROM event_tags WHERE workspace_id=$1`; `INSERT ... SELECT ... FROM events e, jsonb_each_text(e.tags) kv WHERE e.workspace_id=$1 AND e.tags IS NOT NULL`; report `count(events with tags)` and `count(event_tags)`.

`VerifyTags`: begin tx, `setWorkspace`, `CREATE TEMP TABLE tags_rebuild (LIKE event_tags INCLUDING ALL) ON COMMIT DROP`; run the same projection into `tags_rebuild`; compare both ways with `EXCEPT` (live minus rebuild, rebuild minus live), `LIMIT 1`; report `OK`, counts, and `FirstDivergence` as `key=value@position` (never the values? tag values are non-secret, but keep the existing claims style: quote key/value; they are allowed).

`VerifyTagHashes`: read `log_order` rows ≤ bound with the unexported `readLogOrderRows` (it exposes the embedded `OrderRow.TagsHash` added in Task 6); for each row, page the events in its `(workspace, stream, from..to)` range with `SELECT global_position, tags ... ORDER BY global_position`; recompute `es.TagsHash` from non-null tags; compare to the stored `tags_hash` (NULL must correspond to no entries). Return the first mismatch. No new reads/locks in the append path.

- [ ] **Step 4: Run to verify pass**

Run: `PG_DSN=... go test ./postgres -run 'TestRebuildTags|TestVerifyTags|TestVerifyTagHashes' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add postgres/tags_maintenance.go postgres/tags_maintenance_test.go es/tags.go
git commit -m "feat(postgres): rebuild and verify the tags projection and hashes"
```

---

### Task 9: Anchor tags root

**Files:**
- Modify: `es/anchor.go`, `postgres/anchor.go`
- Test: `es/anchor_test.go`, `postgres/anchor_tags_test.go` (new)

**Interfaces:**
- Consumes: Tasks 2, 6, 8.
- Produces:
  ```go
  // es.Anchor gains:
  TagsRootHash     []byte `json:"tags_root_hash,omitempty"`
  TagsFromPosition uint64 `json:"tags_from_position"`
  TagsCount        uint64 `json:"tags_count"`
  ```
  `Anchor`/`CheckAnchor` compute and compare the tags root; `OrderRows` already returns `TagsHash` (Task 6).

- [ ] **Step 1: Write the failing tests**

`es/anchor_test.go`:
- `TestOrderRowTagsHashDoesNotChangeOrderRoot`: build `OrderRow`s with and without `TagsHash`; `OrderRoot` returns the same hash.
- `TestAnchorJSONLegacyTagsRoot`: unmarshal an anchor JSON without `tags_root_hash`; fields are nil/0; this is the legacy marker.

`postgres/anchor_tags_test.go`:
- `TestAnchorCarriesTagsRoot`: append a tagged batch; `Anchor(0)` returns non-nil `TagsRootHash`, `TagsCount` equal to the number of tagged batches, `TagsFromPosition` equal to the first tagged batch's from; `CheckAnchor(a)` passes.
- `TestCheckAnchorDetectsTamperedTag`: at anchor, raw-SQL change `events.tags` and `event_tags` consistently; `CheckAnchor` fails (the stored anchor's tags root no longer matches the recomputed one).
- `TestLegacyAnchorWithoutTagsRootVerifies`: construct an `es.Anchor` from a pre-change JSON (no tags fields) with the current root/order values; `CheckAnchor` verifies heads/order and skips tags.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./es -run 'TestOrderRowTagsHash|TestAnchorJSONLegacy' -v` and `PG_DSN=... go test ./postgres -run 'TestAnchorCarriesTagsRoot|TestCheckAnchorDetectsTamperedTag|TestLegacyAnchorWithoutTagsRootVerifies' -v`
Expected: FAIL — fields absent.

- [ ] **Step 3: Implement**

- `es/anchor.go`: add the three fields; document `TagsRootHash == nil` as the legacy marker and "new anchors always carry a real root, even over zero tagged rows". Add a helper `tagsRows(rows []OrderRow) []TagsRow` or build it in the adapters. Do **not** touch `OrderRoot` (Task 6's `OrderRow.TagsHash` is deliberately ignored).
- `postgres/anchor.go`: in `Anchor`, after `rows` are read, filter `rows` with `TagsHash != nil`, build `[]es.TagsRow`, `tagsRoot := es.TagsRoot(...)`, set `TagsRootHash`, `TagsCount`, and `TagsFromPosition` (lowest From or 0). In `CheckAnchor`, recompute the same and compare when `a.TagsRootHash != nil`; skip when nil. Include the fields in the JSON export/import (no DB `anchors` change; the root travels with the exported anchor, as `OrderRootHash` does).
- `readLogOrderRows` already selects and scans `tags_hash` from Task 6; confirm `OrderRows` carries it, and do not add a second read.
- Mirror the `es.OrderRow.TagsHash` scan in SQLite's `readLogOrderRows` now if it already selects `log_order`; the SQLite anchor fields are wired in Task 13.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./es -run 'TestOrderRowTagsHash|TestAnchorJSONLegacy' -v` and `PG_DSN=... go test ./postgres -run 'TestAnchorCarriesTagsRoot|TestCheckAnchorDetectsTamperedTag|TestLegacyAnchorWithoutTagsRootVerifies' -v`
Expected: PASS. Then `task test:pg`.

- [ ] **Step 5: Commit**

```bash
git add es/order.go es/anchor.go es/anchor_test.go postgres/anchor.go postgres/anchor_tags_test.go
git commit -m "feat: attest tags in a sibling anchor root"
```

---

### Task 10: Postgres `AppendMultiTx`

**Files:**
- Modify: `postgres/wsstore.go`
- Test: `postgres/multi_append_test.go` (new)

**Interfaces:**
- Consumes: Tasks 4, 6.
- Produces: `func (w *Workspace) AppendMultiTx(ctx context.Context, db DBTX, m es.MultiAppend) ([]es.AppendResult, error)`.

- [ ] **Step 1: Write the failing tests**

`postgres/multi_append_test.go` (external package):
- `TestAppendMultiTxCommitsBothStreams`: two appends, distinct streams, one command id; begin a tx, call, commit; read both streams and assert versions/positions/envelopes and one result per append in input order.
- `TestAppendMultiTxRollsBackOnConflict`: first append succeeds, second has a stale `ExpectedVersion`; the call returns `ErrConflict`; after rollback, neither stream has events.
- `TestAppendMultiTxRejectsDuplicateStream`: `ErrDuplicateMultiStream`.
- `TestAppendMultiTxRejectsEmpty` and a zero-event append: `ErrEmptyMultiAppend`.
- `TestAppendMultiTxSharedMetadata`: different `CommandID` → `ErrConflictingMultiAppend`; no rows.
- `TestAppendMultiTxEventIDs`: supplied id survives; zero mints a UUIDv7 (non-nil); deterministic ids derived with a per-append index (`CommandID:0`) collide on the second stream → `ErrDuplicateEventID` (this pins the documented caveat).
- `TestAppendMultiTxInputOrderPositions`: two appends; the first stream's first position is lower than the second's.
- `TestAppendMultiTxTags`: tags on both streams project and read back; a rollback leaves no tag rows.

- [ ] **Step 2: Run to verify failure**

Run: `PG_DSN=... go test ./postgres -run 'TestAppendMultiTx' -v`
Expected: FAIL — undefined method.

- [ ] **Step 3: Implement**

- `AppendMultiTx`: `requireTx`; `es.ValidateMultiAppend`; resolve one `occurred` (first non-zero, else `time.Now()`, truncated); for each append: `validateAppend`, set `p.OccurredAt = occurred`, call the append body with notification suppressed, append the result.
- Refactor `w.append` into `w.appendInternal(ctx, db, p, notify bool)`; `w.append` calls it with `true`. For multi, call with `false`, then after the loop call a new `w.notifyAppendOnce(ctx, db)` if any append had events, so the hint is the transaction's last statement.
- Do not commit or roll back; return the results in input order.
- `setWorkspace` once at entry, before the loop.

- [ ] **Step 4: Run to verify pass**

Run: `PG_DSN=... go test ./postgres -run 'TestAppendMultiTx' -v`
Expected: PASS. Then `task test:pg`.

- [ ] **Step 5: Commit**

```bash
git add postgres/wsstore.go postgres/multi_append_test.go
git commit -m "feat(postgres): add caller-owned multi-stream append"
```

---

### Task 11: SQLite schema, append, projection, and envelope

**Files:**
- Create: `sqlite/migrations/0014_event_tags.sql`
- Modify: `sqlite/store.go`
- Test: `sqlite/tags_test.go` (new)

**Interfaces:**
- Consumes: Tasks 1, 3.
- Produces: `events.tags` TEXT, `event_tags` table, tag-aware scans, append/projection.

- [ ] **Step 1: Write the failing tests**

- `TestSQLiteEventTagsSchema`: `PRAGMA table_info(events)` has `tags`; `PRAGMA table_info(event_tags)` has the four columns; `PRAGMA table_info(log_order)` has `tags_hash`; `PRAGMA index_list(event_tags)` shows the PK; reopen/migrate is idempotent.
- `TestSQLiteAppendPersistsTags`: same round trip as Postgres; `Envelope.Tags` present.
- `TestSQLiteAppendWithoutTagsWritesNull`: `tags` is NULL; `event_tags` count 0.
- `TestSQLiteAppendRejectsInvalidTags`: same table as Postgres.
- `TestSQLiteReadCorruptTagsFailsLoudly`: raw-SQL set `tags='{"k":1}'`; read errors.
- `TestSQLiteTagsDoNotChangeEventHash`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./sqlite -run 'TestSQLiteEventTags|TestSQLiteAppendS|TestSQLiteAppendW|TestSQLiteAppendRejects|TestSQLiteReadCorrupt|TestSQLiteTagsDoNot' -v`
Expected: FAIL — column/table absent.

- [ ] **Step 3: Implement**

Migration `sqlite/migrations/0014_event_tags.sql`:
```sql
ALTER TABLE events ADD COLUMN tags TEXT;
CREATE TABLE IF NOT EXISTS event_tags (
    tag_key         TEXT    NOT NULL,
    tag_value       TEXT    NOT NULL,
    global_position INTEGER NOT NULL,
    PRIMARY KEY (tag_key, tag_value, global_position)
);

ALTER TABLE log_order ADD COLUMN tags_hash BLOB;
```
`sqlite/store.go`:
- append `tags` to `envelopeCols`; scan `sql.Null[string]` and `es.DecodeTags([]byte(v))`.
- `validateAppend`: the same unexported `mergedTags(p)` helper as Postgres (build `[]map[string]string` from `p.Events`, call `es.MergedTags`).
- append: encode tags per event; add `tags` to the events INSERT; after the event loop insert the tag rows in one statement:
  ```sql
  INSERT INTO event_tags (tag_key, tag_value, global_position) VALUES (?,?,?), ...
  ```
  Build the placeholders dynamically; skip when empty.
- SQLite writes `log_order.tags_hash` in the same transaction:
  ```sql
  INSERT INTO log_order (from_position, to_position, stream_id, event_count, batch_hash, tags_hash) ...
  ```
  (SQLite's `log_order` has no `workspace_id`; match its actual columns.)
- `es.TagsHash` over the batch entries; nil when untagged.
- Add `tags_hash` to SQLite's `readLogOrderRows` SELECT/scan into the embedded `OrderRow.TagsHash`, so Task 13's anchor and `VerifyTagHashes` can read it.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./sqlite -run 'TestSQLiteEventTags|TestSQLiteAppendS|TestSQLiteAppendW|TestSQLiteAppendRejects|TestSQLiteReadCorrupt|TestSQLiteTagsDoNot' -v`
Expected: PASS. Then `go test ./sqlite`.

- [ ] **Step 5: Commit**

```bash
git add sqlite/migrations/0014_event_tags.sql sqlite/store.go sqlite/tags_test.go
git commit -m "feat(sqlite): add event tags and the synchronous projection"
```

---

### Task 12: SQLite `ReadByTags`, rebuild, and verify

**Files:**
- Create: `sqlite/tags.go`, `sqlite/tags_maintenance.go`
- Test: `sqlite/tags_test.go` (reads), `sqlite/tags_maintenance_test.go` (new)

**Interfaces:**
- Consumes: Tasks 7–8's contracts (without workspace/RLS).
- Produces:
  ```go
  func (s *Store) ReadByTags(ctx context.Context, q es.TagQuery) ([]es.Envelope, error)
  func (s *Store) RebuildTags(ctx context.Context) (es.TagsRebuildReport, error)
  func (s *Store) VerifyTags(ctx context.Context) (es.TagsVerifyReport, error)
  func (s *Store) VerifyTagHashes(ctx context.Context, upto uint64) (*es.TagsHashVerifyReport, error)
  var _ es.TagReader = (*Store)(nil)
  var _ es.TagMaintainer = (*Store)(nil)
  var _ es.TagAttester = (*Store)(nil)
  ```

- [ ] **Step 1: Write the failing tests**

Mirror the Postgres cases without workspace/RLS: AND, range, pagination, limit, invalid query, as-of; rebuild/verify divergence (including non-string `tags`); tag-hash verify (tamper `events.tags`, then tamper `log_order.tags_hash`).

- [ ] **Step 2: Run to verify failure**

Run: `go test ./sqlite -run 'TestSQLiteReadByTags|TestSQLiteRebuildTags|TestSQLiteVerifyTags|TestSQLiteVerifyTagHashes' -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement**

- `ReadByTags`: the reference query without `workspace_id`; one `EXISTS` per tag; `ORDER BY global_position LIMIT`; `EffectiveLimit`.
- Rebuild/verify: use `json_each(events.tags)`; `json_each` exposes a `type` column, so refuse `type <> 'string'` before deleting the live index. Compare live vs a temporary rebuild with `EXCEPT` both ways. If the pinned `modernc.org/sqlite` build lacks JSON1 (verify with a one-line test first), do the rebuild in Go: scan `(global_position, tags)`, `DecodeTags`, and insert the rows; the contract is unchanged.
- `VerifyTagHashes`: read `log_order` rows ≤ bound; page each row's events by `(stream_id, global_position BETWEEN from AND to)`; recompute `es.TagsHash`; compare to the stored hash (NULL = no entries). Return the first mismatch.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./sqlite -run 'TestSQLiteReadByTags|TestSQLiteRebuildTags|TestSQLiteVerifyTags|TestSQLiteVerifyTagHashes' -v`
Expected: PASS. Then `go test ./sqlite`.

- [ ] **Step 5: Commit**

```bash
git add sqlite/tags.go sqlite/tags_maintenance.go sqlite/tags_test.go sqlite/tags_maintenance_test.go
git commit -m "feat(sqlite): read, rebuild, and verify event tags"
```

---

### Task 13: SQLite anchor tags root and multi-append

**Files:**
- Modify: `sqlite/anchor.go`, `sqlite/store.go`
- Test: `sqlite/anchor_tags_test.go` (new), `sqlite/multi_append_test.go` (new)

**Interfaces:**
- Consumes: Tasks 9, 10, 12.
- Produces:
  ```go
  func (s *Store) AppendMulti(ctx context.Context, m es.MultiAppend) ([]es.AppendResult, error)
  ```

- [ ] **Step 1: Write the failing tests**

- `TestSQLiteAnchorTagsRoot`: append a tagged batch; `Anchor(0)` returns non-nil `TagsRootHash` and correct counts/from; `CheckAnchor` passes; a tampered `events.tags` fails.
- `TestSQLiteLegacyAnchorWithoutTagsRoot`: an anchor with nil `TagsRootHash` verifies heads only.
- `TestSQLiteAppendMulti`: commit both streams; rollback on a second-stream conflict; duplicate stream rejected; shared-metadata mismatch rejected; supplied and zero ids; per-stream tags project and read back.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./sqlite -run 'TestSQLiteAnchorTags|TestSQLiteLegacyAnchor|TestSQLiteAppendMulti' -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement**

- Anchor: read `log_order.tags_hash`, build `[]es.TagsRow`, `es.TagsRoot`, set the three fields; `CheckAnchor` skips when `TagsRootHash == nil`.
- `AppendMulti`: refactor `Append` into `appendTx(ctx, tx, p)`; `AppendMulti` opens one `*sql.Tx`, calls `es.ValidateMultiAppend`, resolves one `occurred`, calls `validateAppend` + `appendTx` per append, commits; on error rolls back. SQLite has no notify path to defer.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./sqlite -run 'TestSQLiteAnchorTags|TestSQLiteLegacyAnchor|TestSQLiteAppendMulti' -v`
Expected: PASS. Then `go test ./sqlite`.

- [ ] **Step 5: Commit**

```bash
git add sqlite/anchor.go sqlite/store.go sqlite/anchor_tags_test.go sqlite/multi_append_test.go
git commit -m "feat(sqlite): attest tags and add multi-stream append"
```

---

### Task 14: `es-verify tags` and the `all` stage

**Files:**
- Modify: `cmd/es-verify/main.go`
- Test: `cmd/es-verify/main_test.go`

**Interfaces:**
- Consumes: Tasks 8, 12.
- Produces:
  ```
  es-verify tags [--rebuild] [--upto N] [<workspace>]
  es-verify all   # adds the tag-hash attestation stage
  ```

- [ ] **Step 1: Write the failing CLI tests**

Extend `cmd/es-verify/main_test.go`:
- `tags` with no args on a Postgres store runs the attestation and prints `ok tags: N batches verified` (or `EMPTY` when zero tagged batches).
- `tags --rebuild <workspace>` rebuilds the projection; a deleted tag row is restored.
- `tags <workspace>` verifies the projection; a divergence prints `BROKEN` and exits 1.
- `all` on a log with a tampered `tags_hash` exits 1; `all --chains-only` still passes (the restore path).
- Usage: `--rebuild` without a workspace is exit 2.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/es-verify -run 'TestTags|TestAllTags' -v`
Expected: FAIL — unknown subcommand.

- [ ] **Step 3: Implement**

- Add `verifyTagHashes func(context.Context, uint64) (*es.TagsHashVerifyReport, error)` and `tags func(workspace string) es.TagMaintainer` to `backend`; wire both backends in `openBackend`.
- Add `cmdTags`: parse `--rebuild`, `--upto`, optional workspace. With `--rebuild`, require a workspace, call `RebuildTags`, print counts. With a workspace, call `VerifyTags`; without, call `VerifyTagHashes`. Exit codes follow the existing convention: 0 intact, 1 broken, 2 usage/error/empty.
- In `cmdAll`, after `cmdOrder`, call `cmdTags` with nil when not `--chains-only`; merge exit codes with `mergeExit` so a broken stream/order dominates.
- Update the usage text.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./cmd/es-verify -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/es-verify/main.go cmd/es-verify/main_test.go
git commit -m "feat(es-verify): verify and rebuild event tags"
```

---

### Task 15: Benchmark the tag path

**Files:**
- Modify: `cmd/es-bench/main.go`
- Modify: `docs/benchmarks/2026-11-21-results.md` or add a dated results file
- Test: manual run (no unit test for the harness)

**Interfaces:**
- Consumes: Tasks 6–8.
- Produces: a `-tags` mode on `seed`/`measure` that writes `flowdoc.space`/`flowdoc.block` and measures append p50/p99 plus `ReadByTags` latency at 10M and 50M.

- [ ] **Step 1: Add the failing check**

Add a small unit test for the flag plumbing (e.g. `TestTagScenarioParses`) that constructs the scenario from argv and asserts the tags are set. Run it: expected FAIL — flag absent.

- [ ] **Step 2: Implement**

- Add `-tags` (bool) and `-tag-space`/`-tag-block-prefix` flags; use `es.AppendParams.Tags` + `EventData.Tags`.
- Add a `measure` stage that pages `ReadByTags({flowdoc.space: s})` and reports p50/p99.
- Keep the default path unchanged (no tags → no tag rows), so the existing baseline stays comparable.

- [ ] **Step 3: Run**

Run the harness at 10M and 50M per `docs/benchmarks/README.md` (or the runbook); record append p50/p99 and `ReadByTags` in the results file, including the `event_tags` row count and size.

- [ ] **Step 4: Commit**

```bash
git add cmd/es-bench/main.go docs/benchmarks/
git commit -m "bench: measure tagged appends and tag reads"
```

---

### Task 16: ADR, runbook, and release notes

**Files:**
- Create: `docs/adr/0044-event-tags.md`
- Modify: `docs/deploy/README.md`
- Modify: `CHANGELOG.md` or the release notes file
- Test: link/format check

**Interfaces:**
- Consumes: the whole feature.
- Produces: the operator-facing record.

- [ ] **Step 1: Write the ADR**

Follow the ADR 0043 shape: Context, Decision (event source + projection, FlowDoc model, key/partitions/limits, synchronous projection, attestation split, multi-append identity/locking, erasure), Consequences (append cost, ~1B rows at 500M, rebuild time, RLS/grants, no legacy path), Alternatives, Rollout.

- [ ] **Step 2: Update the runbook**

Add:
- the appender/rebuild/reader grants for `event_tags`;
- the restore order: load `events` → `RebuildTags` → `RebuildClaims` → `es-verify tags` / `order --chains-only`;
- the quiescence requirement for `RebuildTags`;
- the benchmark envelope and the tags-on/off comparison.

- [ ] **Step 3: Release note**

`v0.23.0`: event tags, `ReadByTags`, multi-append, `es-verify tags`; migrations 0016/0014; note that tags add ~2 rows per FlowDoc event and are not chain-attested (anchored separately).

- [ ] **Step 4: Verify**

Run the link check used earlier and `git diff --check`; run `task test` one final time.

- [ ] **Step 5: Commit**

```bash
git add docs/adr/0044-event-tags.md docs/deploy/README.md CHANGELOG.md
git commit -m "docs: record event tags and multi-append"
```

---

## Definition of Done

- `task test` passes.
- `task test:pg` passes (including the new Postgres tag/multi/anchor tests).
- `go test -race -shuffle=on ./...` passes for the affected packages.
- An events-only restore drill succeeds: dump `events` only, restore, `RebuildTags`, `RebuildClaims`, `es-verify tags`, `es-verify order --chains-only`.
- The golden vectors in `es/tags_hash_test.go` match the values in Task 2.
- `git log` shows one commit per task and no unrelated changes.

## Execution Handoff

After the plan is approved, choose an execution method:

- **Subagent-driven** — a fresh subagent per task plus a fresh reviewer; best for this plan because the Postgres/SQLite tasks share interfaces and a wrong signature in Task 1 or 6 propagates.
- **Native** — one agent executes all tasks in this session, then a final review; faster, but the cross-adapter contract is checked only at the end.
