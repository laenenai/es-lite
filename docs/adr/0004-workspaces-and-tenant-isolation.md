# ADR 0004: Workspaces and Tenant Isolation

- **Status:** Accepted (amends ADR 0001 — reinstates crypto-shredding as
  the workspace erasure mechanism)
- **Date:** 2026-08-01

## Context

A **workspace** is es-lite's tenant boundary — the unit of isolation and
of the "right to erasure". The target deployment has **100k+ workspaces**
on Postgres. Two questions follow: how are workspaces isolated at that
scale, and how is a workspace erased given ADR 0001 makes the event log
immutable and append-only?

ADR 0001 deliberately cut crypto-shredding to stay lite, and left a
loud warning that erasure vs. an immutable log is the one hard tension,
and that crypto-shredding is expensive to add late. At 100k workspaces on
shared tables, that tension is now unavoidable, so this ADR reinstates
crypto-shredding — scoped to the workspace, which is far simpler than the
parent framework's per-field, per-subject model.

Physical isolation was considered and rejected for this scale (see
*Alternatives*): a single Postgres cluster cannot carry 100k databases or
100k schemas without catalog and connection-pool collapse.

## Decision

### 1. Isolation: shared tables + `workspace_id` + RLS + hash partitioning

On Postgres, all workspaces share one schema. Every row carries a
`workspace_id`; every primary key and index leads with it. Isolation is
enforced by **Row-Level Security**, not by a `WHERE` clause callers must
remember:

```sql
CREATE TABLE events (
    workspace_id    TEXT   NOT NULL,
    global_position BIGINT GENERATED ALWAYS AS IDENTITY,
    stream_id       TEXT   NOT NULL,
    version         BIGINT NOT NULL,
    ...
    PRIMARY KEY (workspace_id, stream_id, version)
) PARTITION BY HASH (workspace_id);
-- a FIXED number of hash partitions (e.g. 64–256), NOT one per workspace:
-- 100k partitions would cripple query planning and autovacuum.
```

RLS policies key off a per-transaction session variable the store sets:

```sql
SET LOCAL app.workspace_id = 'ws_123';
-- policy: USING (workspace_id = current_setting('app.workspace_id'))
```

A query that forgets to filter still cannot cross a workspace. The relay
and other admin-scope readers run under a role that bypasses RLS and read
across all workspaces (mirroring the parent's `ReadAll` vs.
`ReadAllForTenant` split).

Hash-partitioning by `workspace_id` (fixed count) keeps the single giant
`events` table's indexes and vacuum tractable without creating
per-workspace objects.

**SQLite** stays single-workspace per file — the dev/edge backend. The
two backends deliberately diverge on isolation (ADR 0002): SQLite =
one-file-one-workspace, Postgres = shared-tables-many-workspaces.

### 2. Workspace scopes the Store, not the identity

Core `StreamID` stays `Type:ID` (ADR 0001) — workspace does **not** enter
stream identity. Instead a workspace-scoped store view carries it:

```go
ws := pgStore.Workspace("ws_123") // returns an es.Store
rt := aggregate.NewRuntime(ws, counter.Decider, counterv1.EventCodec{})
```

Every call on `ws` sets `app.workspace_id` (RLS) and stamps
`workspace_id` on writes. The `es.Store` interface is unchanged and
`aggregate.Runtime` stays workspace-agnostic; workspace re-enters at
exactly two edges — the store factory and the NATS subject
(`evt.<workspace>.<aggregate>.<event>`, ADR 0003).

### 3. Erasure: per-workspace crypto-shredding (default)

Every event payload is encrypted at rest under a **per-workspace data
encryption key (DEK)**. The DEK is wrapped by a key-encryption key (KEK)
held in a `KeyStore` (KMS/HSM in production, a local key file in dev).
Encryption happens at the storage-adapter boundary — transparent to the
Decider, which only ever sees plaintext domain events.

```
append:  event → proto bytes → AEAD-encrypt with workspace DEK → payload column
read:    payload column → AEAD-decrypt with workspace DEK → proto bytes → event
erase:   ForgetWorkspace(ws) → destroy the workspace DEK
```

**Erasing a workspace destroys its DEK.** The ciphertext stays in the
append-only log — so immutability and the audit *structure* are preserved
(row count, versions, timestamps, actor principals, causality are intact)
— but the payloads are cryptographically unrecoverable. This is GDPR
erasure that does not `DELETE` from an immutable log.

Encryption is **envelope-level** (the whole payload), not per-field as in
the parent framework. At workspace-isolation granularity that is the
right trade: everything in a workspace is that workspace's data, so one
key erases all of it, and there is no per-field classification machinery
to carry. Non-PII audit metadata (timestamps, versions, actor principal,
correlation/causation ids) lives in dedicated columns and stays plaintext
so the audit trail survives a shred.

### 4. Erasure must extend beyond the log

Destroying the DEK erases the log, but derived data holds decrypted
copies. Erasure is therefore a **three-part** operation, and all three
are required for it to be complete:

1. **Log** — destroy the workspace DEK (crypto-shred).
2. **Projections / read models** — purge the workspace's rows
   (`DELETE ... WHERE workspace_id = ?`). Read models are disposable and
   rebuildable (ADR on projection rebuild), so deleting them does not
   violate the immutability that applies only to the event log.
3. **NATS/JetStream** — the relay publishes **ciphertext** payloads (or
   only non-PII/derived fields), so a shredded workspace's PII is not
   left readable in a JetStream stream that outlives the erasure. If
   plaintext must be published for a consumer, that stream's retention
   must be shorter than the erasure SLA and purged on erasure.

A `Shredder`/`WorkspaceKeys` component owns the DEK lifecycle and the
`ForgetWorkspace` operation; the projection purge and NATS handling are
wired by the application, which knows its own read models and consumers.

## Consequences

### Positive

- **Scales to 100k+ workspaces.** One schema, one migration, normal
  connection pooling; RLS makes isolation structural.
- **Erasure without breaking immutability.** Drop a key, not a row. The
  append-only invariant of ADR 0001 holds; the audit skeleton survives.
- **Simple key model.** One DEK per workspace, envelope-level encryption.
  No per-field classification, no subject bookkeeping — a fraction of the
  parent's crypto surface.
- **Core untouched.** `StreamID`, `es.Store`, and `aggregate.Runtime` do
  not change; workspace and encryption live at the adapter edge.

### Negative

- **Everything is encrypted.** Envelope-level encryption means no
  plaintext columns for ad-hoc SQL analytics over payloads; querying is
  via projections (which hold decrypted derived state) or via decrypt-on-
  read. Acceptable given projections are the intended query surface.
- **A KeyStore is now a hard dependency.** Losing a workspace's DEK
  without intending to is indistinguishable from erasure — key durability
  and backup become operationally critical.
- **Erasure is multi-surface.** Log + projections + NATS must all be
  handled; missing one leaks. This is inherent to any system with derived
  copies and is documented as a required checklist, not automated away.
- **Encrypt/decrypt on the hot path.** AEAD per event adds CPU and
  latency. Negligible next to I/O, but non-zero; a per-workspace DEK
  cache avoids re-unwrapping the KEK on every operation.

## Alternatives Considered

### Database-per-workspace / schema-per-workspace

Rejected at 100k. A single Postgres cluster cannot carry 100k databases
(catalog bloat; PgBouncer pools are per-database, so 100k pools) or 100k
schemas (pg_class/pg_attribute explosion, planner slowdown, 100k-way
migrations). Both are fine at hundreds–low-thousands; neither survives
100k. DB-per-workspace remains the natural SQLite shape for small
deployments, which is why SQLite keeps it.

### Shared tables, erasure by `DELETE WHERE workspace_id = ?`

The pragmatic fallback, and offered as an option, but rejected as the
*default*. It deletes from the append-only log, breaking the immutability
ADR 0001 rests on, and a bulk delete across a 100k-workspace partitioned
table is heavy and hard to audit as "complete." Crypto-shredding erases
by destroying one key — cheaper, provably complete, and immutability-
preserving.

### Per-field crypto-shredding (parent framework model)

Rejected as too much for lite. Per-field classification, per-subject DEKs,
and DSAR field-level export are powerful for a regulated multi-tenant SaaS
but are exactly the surface lite exists to shed. Workspace-granular
envelope encryption gives the erasure guarantee that matters here with a
fraction of the machinery. A workspace that later needs per-field control
is a signal to graduate to the parent framework.
