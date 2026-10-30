# ADR 0029: Tamper-Evident Event Log (Per-Stream Hash Chain)

- **Status:** Proposed (reverses the "tamper-evident hash chain" cut in ADR 0001)
- **Date:** 2026-10-30

## Context

ADR 0001 cut the parent framework's tamper-evident hash chain with the
rationale "immutability + DB backups covers audit for non-adversarial threat
models; re-add per parent ADR 0028 if required". `es/envelope.go` says so in
its doc comment ("no tamper-evidence hash chain"), and the README's security
section states plainly that "there is no hash chain or signature, so the log is
not tamper-evident against a database operator".

The first production consumer — the `contracts` repository's casework domain
(design document §3.14, §9.9.6, §13.1) — has an **owner decision that tamper
evidence is required before the first production event**. Its threat model is
precisely the one ADR 0001 waved off: records are a council's legal record,
and a database operator (or a restored backup) must not be able to alter or
truncate history without that being detectable by an auditor who holds no keys.

Two properties of the current code shape the design:

1. **The store never sees plaintext** (ADR 0025). `postgres/wsstore.go`
   `Append` and `sqlite/store.go` `Append` insert `ev.Payload` verbatim; the
   relay (`postgres/drain.go`, `natsjs/publisher.go`) moves the same bytes. So
   the chain must be computed over the **sealed** payload bytes; that is also
   what makes it verifiable without keys.
2. **Both `Append` implementations already read the stream head inside the
   append transaction** (`SELECT COALESCE(MAX(version),0) ...`) to enforce
   `ExpectedVersion`. The previous event's hash can be fetched by the same
   query, so chaining costs no extra round trip.

The parent framework's ADR 0028 (not in this repository; the reference ADR
0001 points to) is the model: a per-stream `prev_hash`/`hash` pair on each
row. This ADR follows its shape and adapts it to lite's keyless, sealed-payload
model.

## Decision

### 1. Per-stream chain, stored on the row, computed in the append transaction

Each `events` row gains two columns: `prev_hash` and `hash` (32 bytes,
SHA-256). The chain is **per stream** — keyed by `(workspace_id, stream_id)`
on Postgres and `stream_id` on SQLite — because per-stream order (`version`)
is the invariant the store already enforces with `UNIQUE (…, stream_id,
version)`. A global chain would serialize all appends on one head row, which
ADR 0002's coordinator-free write path forbids.

- `prev_hash` of version 1 is 32 zero bytes. For every later version it is the
  `hash` of version−1 in the same stream.
- `hash = SHA-256(canonical(envelope) ‖ prev_hash)` with the canonical
  encoding in §2.
- The adapters compute it in `Append`, between the version check and the
  `INSERT`, in the same transaction. The head lookup replaces
  `SELECT COALESCE(MAX(version),0)` with
  `SELECT version, hash FROM events WHERE … ORDER BY version DESC LIMIT 1`
  (`current = 0`, `prev = zero32` when no row). Within one batch, event *i*
  chains to event *i−1* in memory.
- **A fork is structurally impossible**: two writers racing on the same head
  both compute from the same `prev_hash`, and the loser fails on the existing
  `UNIQUE (stream_id, version)` constraint (mapped to `es.ErrConflict`). No new
  locking.

### 2. Canonical encoding (the thing an auditor reimplements)

`es/hash.go` adds `func EnvelopeHash(e Envelope) [32]byte` (reads
`e.PrevHash`). Encoding, in this order, with every variable-length field
prefixed by a big-endian `uint32` length and fixed-width integers big-endian:

```
"es-lite.envelope.v1"            domain separator (len-prefixed)
workspace, stream_id (canonical "type:id")
version u64, event_id 16 bytes
type_url, schema_version u32
occurred_at i64 unix µs, recorded_at i64 unix µs
correlation_id, causation_id, command_id   16 bytes each (Nil = zeros)
actor_type, actor_id
payload (the sealed bytes exactly as stored)
prev_hash 32 bytes
```

**What is deliberately excluded and why.**

- `global_position` is excluded. It is assigned by `nextval()` /
  `AUTOINCREMENT` *during* the `INSERT` (`RETURNING global_position`,
  `LastInsertId`), so including it would need insert-then-update — forbidden
  by the append-only contract. It is a delivery cursor, not an audit fact;
  cross-stream ordering is attested by anchors (§4) instead.
- `published_at` is excluded: it is the relay's mutable delivery marker.

**What is deliberately included, and the one behavioural change it forces.**
`recorded_at` is the time-travel basis (`ReadStreamAsOf`), so it must be
attested. Today Postgres fills it with `DEFAULT now()` (transaction start) and
SQLite samples `time.Now()` *before* `BeginTx` — the review's ES-03 finding.
Both adapters change to sample the commit clock **after the version check,
inside the transaction** (`SELECT clock_timestamp()` on Postgres; `time.Now()`
after the head read on SQLite), pass it explicitly in the `INSERT`, and use the
same value for the hash. Because the loser of a version race never commits,
this also makes `recorded_at` monotonic per stream, closing ES-03 as a side
effect. Timestamps are truncated to **microseconds** before hashing and
inserting — `timestamptz` has µs precision, and a verifier must recompute from
what the database returns. `OccurredAt` from the caller is truncated the same
way, and the returned `Envelope` reflects the truncated values.

### 3. Verification without keys

- `es.VerifyStream(ctx, store Store, sid StreamID) (ChainReport, error)` is a
  free function over any `es.Store`: it pages `ReadStream`, recomputes each
  hash from the returned envelope, and checks `prev_hash` linkage and
  contiguous versions. It therefore works over the NATS client too — the
  server returns `prev_hash`/`hash` and the client recomputes from the sealed
  bytes it holds. No new `Store` method.
- `cmd/es-verify` wraps it for operators: `es-verify stream <ws> <type:id>`,
  `es-verify all` (Postgres under `app.bypass_rls`, like `Drain`), and
  `es-verify anchor …` (§4). Exit status is the audit result.
- `ChainReport` carries `ChainedFrom` (the first version with a hash — see §5),
  `Head`, `HeadHash`, and the first broken version, if any.
- Optional read-path verification: `aggregate.Runtime.WithChainVerification()`
  recomputes the hash for every envelope `load` folds and fails the load with
  `es.ErrChainBroken`. Off by default — it doubles hashing on the hot read path
  and `contracts` reads through a gate that can verify once per fetch.

### 4. Global anchors (what makes a keyless chain tamper-*evident*)

A chain without keys proves internal consistency, not authenticity: an
operator who can edit rows can recompute every hash after the edit. A chain
also cannot detect deleting the tail of a stream. Both are caught by
periodically fixing the heads **outside** the database:

- `anchors(anchor_id, created_at, upto_global_position, stream_count,
  root_hash)` table. `root_hash = SHA-256` over the sorted list of
  `(workspace_id, stream_id, head_version, head_hash)` for every stream with
  `global_position <= upto_global_position`. (A flat hash, not a Merkle tree:
  a membership proof for one stream is not a requirement yet; a tree can
  replace the root later without changing the row shape.)
- Produced by `es-verify anchor` (cron/job) or by `es-lited` on
  `ANCHOR_INTERVAL` under the existing `leader` election, exactly as the
  in-process relay runs. Computation runs under `app.bypass_rls`.
- **Export is the point**: the anchor row is printed/JSON-exported and the
  operator publishes it somewhere the database operator cannot rewrite (a
  signed git commit, a transparency log, a timestamping authority). es-lite
  does not sign anchors — it is keyless by ADR 0025; signing is the operator's
  layer. `es-verify anchor --check <exported>` recomputes and compares.

### 5. Why it cannot be retrofitted, and what the migration does

A chain attests what the store **observed at append time**. Hashing existing
rows after the fact would (a) require `UPDATE` on `events`, violating the
append-only contract every ADR rests on, and (b) certify rows that may already
have been altered — a back-computed chain over tampered history is internally
consistent and proves nothing. So the chain starts at the first append after
the migration, never earlier.

Migration `0002_hash_chain.sql` (both backends, via ADR 0010's runner):

- `ALTER TABLE events ADD COLUMN prev_hash <bytes>; ADD COLUMN hash <bytes>;`
  — **nullable**, and `CREATE TABLE anchors (…)`. Postgres: a partial index
  `events_unchained_idx ON events (global_position) WHERE hash IS NULL`.
- **Empty store** (the `contracts` case): every row ever written has a hash;
  `ChainedFrom = 1` for every stream; `es-verify` can require that with
  `--strict`. This is the only configuration that is tamper-evident end to end.
- **Populated store**: rows written before 0002 stay `NULL`. For a stream whose
  head predates the chain, the first chained event uses
  `prev_hash = SHA-256("es-lite.unchained-head.v1" ‖ stream_id ‖ head_version)`
  so the chain still binds to *where* it started; `VerifyStream` reports
  `ChainedFrom = N` and treats earlier rows as unattested. Any `NULL` hash on a
  row whose `recorded_at` is later than `schema_migrations.applied_at` for
  version 2 is reported as broken — the store writes hashes unconditionally
  after the migration.

### 6. Wire and transport changes

- `es.Envelope` gains `PrevHash []byte` and `Hash []byte` (nil before 0002).
  `es.Store` is unchanged; `AppendParams`/`EventData` are unchanged — hashes
  are store-assigned like `GlobalPosition` and `RecordedAt`.
- `natsstore/wire.go` `envelopeWire` gains `prev_hash`/`hash` (base64).
- `natsjs` adds headers `Es-Hash` and `Es-Prev-Hash` (hex) in `toMsg` and
  reads them in `DecodeEnvelope`. A consumer can then **detect a per-stream
  gap or reorder on the relay**: keep the last `Hash` seen per stream; if an
  incoming event's `Es-Prev-Hash` does not match it (and the version is not
  `last+1`), the consumer has missed or reordered an event — `Nak` and alert,
  or re-fetch the stream from the gate. This is the first concrete mitigation
  for the review's ES-04 (relay reordering under overlapping drains).

## Consequences

### Positive

- Meets the `contracts` owner's production gate with no change to the
  `es.Store` interface, the decider model, or the zero-knowledge posture:
  hashes cover ciphertext, so the auditor needs no keys.
- Head lookup folds into the existing version check; the only new cost is one
  SHA-256 per event (a few µs for typical sealed payloads) and 64 bytes per
  row. Verification is a sequential stream read.
- Fixes ES-03 (`recorded_at` monotonicity) as a prerequisite rather than a
  separate project.
- Gives relay consumers a per-stream gap detector for free.

### Negative

- **Must land before the first production event.** A store that is already
  populated is only partially attested forever (§5).
- A keyless chain is only as strong as its anchors' export discipline. If
  anchors are never published off-box, the chain detects corruption and
  accidental truncation but not a determined operator. Documented, not solved.
- `recorded_at` becomes an explicit insert value instead of a column default;
  the Postgres `DEFAULT now()` stays for safety but is never relied on.
- `Envelope` grows; all three row scanners (`postgres/store.go
  scanEnvelope`, `sqlite/store.go queryEnvelopes`, `natsstore` conversions)
  and the review's CLN-02 duplication get touched at once.

## Alternatives Considered

### Global chain (one `prev_hash` across all streams)

Rejected: it serializes every append on a single head and reintroduces the
coordinator ADR 0002 removed. The anchor root gives cross-stream attestation at
a period the operator chooses instead of on every write.

### Signing (HMAC/Ed25519) instead of, or on top of, hashing

Rejected for es-lite: it is keyless by ADR 0025, and a key held by the server
is a key the database operator's colleague holds. Signing belongs where the
anchor is exported. A client-side `Meta`-level signature over the sealed
payload remains possible on top of this design.

### Compute the hash client-side and send it in `EventData`

Rejected: the client does not know `recorded_at` (DB clock) before the append,
and a client-supplied hash certifies nothing the server did not check anyway.
Server-side computation in the transaction is the only placement where
"version N links to N−1 as committed" is guaranteed.

### Trigger-maintained hash columns in the database

Rejected: it splits the canonical encoding across Go and two SQL dialects
(SHA-256 over µs timestamps and UUID bytes is awkward in SQLite), and the
verifier must be in Go anyway. One encoding, one implementation.
