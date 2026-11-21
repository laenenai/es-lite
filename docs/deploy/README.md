# Deployment

es-lite ships **no daemon and no image**. It is a Go library with Postgres and
SQLite backends, an in-process delivery path (`delivery`) and a
transport-neutral feed core (`feed`) whose Postgres reads are commit-safe
(ADR 0037). A runnable eventstore service is the platform's `serve`;
deployment, workload identity, authorization and multi-workspace routing are
the platform's concern (ADR 0036).

The hash-chain anchors described in ADR 0029 are still available, but the
library no longer schedules them: run `es-verify anchor` from your own CronJob
or scheduler and export the JSON line off-box. `es-verify stream|all` verifies
a stream or the whole store.

For the decisions behind the library boundary, see
[`docs/adr`](../adr).

## Index build order (migration 0003)

**Skip this section entirely if migration 0003 has already run** on the
database in question — the indexes exist, the migration is a no-op, and
re-running the manual steps only builds 8 redundant child indexes per
index (16 across both) and then fails at `ATTACH PARTITION` (the
partition indexes are already attached). The steps below are for
databases upgrading from a pre-0003 schema where `events` already holds
production data.

Migrations run in one transaction (ADR 0010), so they cannot use
`CREATE INDEX CONCURRENTLY`. A plain index build on a partitioned table with
≥10M rows blocks appends for the whole build — and with auto-migrate on
`Open`, the first upgraded replica would take that lock at boot. So for
databases of that size, build both feed indexes without blocking appends
**before** upgrading to a version that includes `0003_feed_indexes.sql`.

Postgres cannot `CREATE INDEX CONCURRENTLY` on a partitioned parent at all
(all versions through 18), so the build is per partition: build the index on
each partition with `CONCURRENTLY`, then create the parent index with `ONLY`
and attach the partition indexes — the last two steps are metadata-only and
instant. Run this through `psql` (`\gexec` runs one statement per row;
`CREATE INDEX CONCURRENTLY` cannot run inside a transaction block):

```sql
-- 1. Build both indexes on every events partition, without blocking appends.
SELECT format('CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I %s',
              p.relname || regexp_replace(i.name, '^events', ''), p.relname, i.cols)
FROM (VALUES ('events_ws_pos_idx',      '(workspace_id, global_position)'),
             ('events_ws_type_pos_idx', '(workspace_id, type_url, global_position)'))
         AS i (name, cols)
CROSS JOIN (SELECT c.relname FROM pg_inherits g
            JOIN pg_class c ON c.oid = g.inhrelid
            WHERE g.inhparent = 'events'::regclass) AS p
\gexec

-- 2. Create the parent indexes (metadata only, instant; marked invalid
--    until every partition index is attached in step 3).
CREATE INDEX IF NOT EXISTS events_ws_pos_idx
    ON ONLY events (workspace_id, global_position);
CREATE INDEX IF NOT EXISTS events_ws_type_pos_idx
    ON ONLY events (workspace_id, type_url, global_position);

-- 3. Attach the partition indexes (metadata only, instant). Once all are
--    attached, the parent indexes become valid on their own.
SELECT format('ALTER INDEX %I ATTACH PARTITION %I',
              i.name, p.relname || regexp_replace(i.name, '^events', ''))
FROM (VALUES ('events_ws_pos_idx',      '(workspace_id, global_position)'),
             ('events_ws_type_pos_idx', '(workspace_id, type_url, global_position)'))
         AS i (name, cols)
CROSS JOIN (SELECT c.relname FROM pg_inherits g
            JOIN pg_class c ON c.oid = g.inhrelid
            WHERE g.inhparent = 'events'::regclass) AS p
\gexec
```

A `CONCURRENTLY` build that fails or is cancelled leaves an `INVALID` index
behind; drop it (`DROP INDEX CONCURRENTLY <index name>;`) and re-run step 1.
Before upgrading, confirm both parent indexes are valid — the expected
result is **two rows, both `t`**. Zero rows means the manual build was
skipped: expect migration 0003 to take the blocking-build path instead
(acceptable only on small databases). An `f` row means the parent is not
fully attached yet: a partition's child index is missing or itself invalid —
find it (`SELECT ... FROM pg_index WHERE NOT indisvalid`, or re-run the
step-3 attach), fix or rebuild that child, and attach:

```sql
SELECT c.relname, i.indisvalid FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname IN ('events_ws_pos_idx', 'events_ws_type_pos_idx');
```

Once this runbook step is done, migration 0003 is a **no-op**: its
`CREATE INDEX IF NOT EXISTS` statements find the parent indexes already in
place. Databases that skip the manual step pay a blocking build inside the
migration — acceptable only for small databases.

## Index build order (migration 0012)

Migration 0012 adds one more partial index, `events_via_idx` on
`(workspace_id, via_principal) WHERE via_principal IS NOT NULL` — the
reverse audit query's index (ADR 0041: "everything service S did, across
users"). The same lock arithmetic as above applies, and this section is
deliberately its own: **do not follow the migration 0003 section for this
index.** That section is keyed to 0003 — its "skip this section entirely if
migration 0003 has already run" rule says nothing about 0012, and its
two-index lists and child-name formulas would build the wrong indexes
here. Like the 0003 runbook, this one is needed only for databases where
`events` holds **≥10M rows**; on smaller databases migration 0012's plain
`CREATE INDEX IF NOT EXISTS` finishes fast enough to run inside the
migration.

Migrations run in one transaction (ADR 0010), so they cannot use
`CREATE INDEX CONCURRENTLY`, and Postgres cannot `CREATE INDEX CONCURRENTLY`
on a partitioned parent at all (all versions through 18) — so the build is
per partition: build the index on each partition with `CONCURRENTLY`, then
create the parent index with `ONLY` and attach the partition indexes —
the last two steps are metadata-only and instant. The partial predicate
must be repeated in **both** the child build and the parent statement:
`ATTACH PARTITION` requires identical definitions, and a parent built
without the predicate cannot adopt children built with it. Run this
through `psql` (`\gexec` runs one statement per row; `CREATE INDEX
CONCURRENTLY` cannot run inside a transaction block):

```sql
-- 1. Build the index on every events partition, without blocking appends.
SELECT format('CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I %s',
              p.relname || '_workspace_id_via_principal_idx', p.relname,
              '(workspace_id, via_principal) WHERE via_principal IS NOT NULL')
FROM (SELECT c.relname FROM pg_inherits g
      JOIN pg_class c ON c.oid = g.inhrelid
      WHERE g.inhparent = 'events'::regclass) AS p
\gexec

-- 2. Create the parent index (metadata only, instant; marked invalid
--    until every partition index is attached in step 3). The predicate
--    must match step 1 exactly.
CREATE INDEX IF NOT EXISTS events_via_idx
    ON ONLY events (workspace_id, via_principal)
    WHERE via_principal IS NOT NULL;

-- 3. Attach the partition indexes (metadata only, instant). Once all are
--    attached, the parent index becomes valid on its own.
SELECT format('ALTER INDEX events_via_idx ATTACH PARTITION %I',
              p.relname || '_workspace_id_via_principal_idx')
FROM (SELECT c.relname FROM pg_inherits g
      JOIN pg_class c ON c.oid = g.inhrelid
      WHERE g.inhparent = 'events'::regclass) AS p
\gexec
```

A `CONCURRENTLY` build that fails or is cancelled leaves an `INVALID` index
behind; drop it (`DROP INDEX CONCURRENTLY <index name>;`) and re-run step 1.
Before upgrading, confirm the parent index is valid — the expected result
is **one row, `t`**. Zero rows means the manual build was skipped: expect
migration 0012 to take the blocking-build path instead (acceptable only on
small databases). An `f` row means the parent is not fully attached yet: a
partition's child index is missing or itself invalid — find it
(`SELECT ... FROM pg_index WHERE NOT indisvalid`, or re-run the step-3
attach), fix or rebuild that child, and attach:

```sql
SELECT c.relname, i.indisvalid FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'events_via_idx';
```

Once this runbook step is done, migration 0012 is a **no-op** for the
index: its `CREATE INDEX IF NOT EXISTS` finds the parent index already in
place. Databases that skip the manual step pay a blocking build inside the
migration — acceptable only for small databases.

## Relay role and RLS bypass (migration 0005)

`Drain` — the delivery relay — and the anchor/verify path (`Anchor`,
`CheckAnchor`, `ListStreams`, `ListAnchors`, `ClaimWorkspaces`) are the only
readers that work across workspaces, through the `app.bypass_rls` arm of the
`events` and `unique_claims` policies. Migration 0005
role-gates that arm: it
additionally requires `pg_has_role(current_user, 'es_relay', 'member')`,
so the GUC alone — settable by any session — no longer lifts workspace
isolation. Both paths verify the membership lazily, on the first bypassed
call, and cache a definitive verdict for the `Store`'s lifetime, failing
fast with `postgres.ErrRelayRoleMissing` instead of silently reading
nothing; a transient probe failure — a cancelled context, a connection
blip — is returned but not cached and re-probes on the next call, so one
blip cannot refuse a process forever. The silence would look like an idle
backlog for `Drain`
— and be worse on the anchor path: `Anchor` would compute a root over
zero heads and record it as a real anchor, and `CheckAnchor` would
report a spurious mismatch.

**Create the role and grant it to every bypass principal before
upgrading** — the create-role-then-migrate ordering is part of the
migration. The moment 0005 applies, the bypass stops working for every
session outside `es_relay`: with the grants already in place the relay
keeps draining and `es-verify` keeps anchoring; without them, both fail
fast on their next bypassed call. The principals are the relay's login
role and whatever login role the `es-verify anchor` CronJob runs as:

```sql
-- before the upgrade that carries migration 0005:
CREATE ROLE es_relay NOLOGIN NOSUPERUSER NOBYPASSRLS;
GRANT es_relay TO <the relay's login role>, <the anchor scheduler's login role>;
```

The membership verdict is probed on the first bypassed call, and a
definitive one stands for the `Store`'s lifetime: after granting a missed
role, restart the refused processes —
the relay, and the `es-verify` anchor job (a freshly opened `Store`
re-verifies). The reverse direction is quieter: **revoking `es_relay` from a
running relay is silent until restart.** The policy re-evaluates
`pg_has_role` per query and simply admits no rows — a revoked-but-running
relay drains nothing and looks like an idle backlog, and only the next
restart fails fast with `postgres.ErrRelayRoleMissing`. To revoke safely,
stop the process first, revoke, and let the restart surface the refusal.
Superusers are implicitly members of every
role, so migrations and psql/admin tooling are unaffected; and ordinary
workspace-pinned traffic takes the policies' workspace arm and never
depends on `es_relay` existing.

## Grants: the privilege contract

Every es-lite principal connects as an ordinary non-superuser,
non-`BYPASSRLS` login role, and the grants below are the complete set it
needs — nothing more. Two invariants hold across the whole matrix: **no
role gets `DELETE` or `TRUNCATE` on `events`** (the log is append-only in
the privilege model too, and `published_at` is the one mutable column,
which only the relay may write), and no principal holds any `UPDATE` on
`events` except the relay's `published_at`. The `GRANT es_relay` of the
section above is role *membership* — the RLS bypass — while the table
grants below are the other half of the contract.

`TestLeastPrivilegeEndToEnd`, in the Postgres integration suite, drives
the whole append → feed subscribe/ack → `Drain` → `Anchor`/`CheckAnchor`
flow through probe roles holding **exactly** these grants, so the runbook
and the code cannot drift apart silently: a grant the code needs but the
runbook omits fails the test, and so does a code path reaching for an
undocumented privilege.

es-lite requires **PostgreSQL 14 or newer**. The floor is the support
policy, not a single feature gate: `pg_current_xact_id()` — the first
statement of both the append xid draw and the horizon fence — and the
`pg_current_snapshot`/`pg_snapshot_xmin` pair the fence reads all exist
from PostgreSQL 13, so the probe mechanically rejects only PostgreSQL 12
and older. Both constructors, `Open` and `OpenWithPool`, run the probe
once, before any migration, failing fast with a clear message on an older
server instead of a confusing error deep inside the first append or fence
round.

Every principal needs schema access to name the objects at all:

```sql
GRANT USAGE ON SCHEMA public TO <role>;
```

**Migration role and the shared ledger** — `Migrate` writes
`migrations.goose_migrations`. On the first run after upgrading, it creates
`migrations` and the table when they are absent. A least-privilege deployment
pre-creates them with a maintenance role and grants the migration role DML
only; the migrator then performs no DDL (`to_regclass` proves the table exists
before anything is created):

```sql
CREATE SCHEMA IF NOT EXISTS migrations;

CREATE TABLE IF NOT EXISTS migrations.goose_migrations (
    module     text        NOT NULL,
    version    bigint      NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (module, version)
);

GRANT USAGE ON SCHEMA migrations TO <migration role>;
GRANT SELECT, INSERT, UPDATE, DELETE ON migrations.goose_migrations TO <migration role>;
```

The table belongs to the database the DSN names. A module pointed at another
database gets its own ledger under the same module name. SQLite keeps its
private `schema_migrations`.

**Appender** — the process calling `Workspace.Append` (or `AppendTx`): it
draws positions, reads the stream head for optimistic concurrency,
inserts the events and their `log_order` attestation (design D1),
applies uniqueness constraints — their ops ride on the first event row
(ADR 0043) — reads the event log to rebuild or verify the claims index
after a restore. It is also the process running `aggregate.Runtime`, so with a
snapshot cache configured (ADR 0042) it both reads and writes `snapshots`
— the same role as for `events`. `Release` is the only `DELETE` on the
append path, and `Purge` on the snapshot cache is the other off-path
`DELETE`:

```sql
GRANT USAGE ON SEQUENCE events_global_position_seq TO <appender>;
GRANT SELECT, INSERT ON events, unique_claims TO <appender>;
GRANT DELETE ON unique_claims TO <appender>;
GRANT INSERT ON log_order TO <appender>;
GRANT SELECT, INSERT, UPDATE, DELETE ON snapshots TO <appender>;
```

**Feed process** — `feed.Server` over the Postgres adapters: the
checkpoint/lease/dead-letter state is the feed's own, and the horizon
fence's first statement reads the position sequence through
`pg_sequences`, which masks `last_value` for roles without `USAGE` on the
sequence — the feed never needs `SELECT` on it.

```sql
GRANT USAGE ON SEQUENCE events_global_position_seq TO <feed process>;
GRANT SELECT, INSERT, UPDATE, DELETE ON feed_checkpoints, feed_leases, feed_dead_letters TO <feed process>;
GRANT SELECT ON events TO <feed process>;
```

**Relay** — the `Drain` delivery relay: it claims unpublished rows and
stamps `published_at` (the database's own clock), the one column it may
touch:

```sql
GRANT SELECT, UPDATE (published_at) ON events TO <relay>;
GRANT es_relay TO <relay>;
GRANT SELECT ON unique_claims, log_order TO <relay>;
```

**Anchor job** — the `es-verify anchor` CronJob (`Anchor`,
`CheckAnchor`, `ListAnchors`): it reads heads and the per-batch `log_order`
rows across workspaces under the role-gated bypass, records anchors, and
reads the shared migration ledger (`migrations.goose_migrations`) for the
chain's `ChainedSince`. The `SELECT` on
`anchors` serves the job's read-back of recorded rows — `es-verify
anchor list` (`ListAnchors`) — and `Anchor`'s `RETURNING created_at`
(the database-clock stamp it reports); `es-verify anchor --check`
verifies one exported anchor JSON, never a stored row. The `USAGE` on
the sequence is not optional: the anchor path
fences on the same horizon the feed does (`settledBound`), whose first
statement reads the position sequence through `pg_sequences` — and the
view masks `last_value` for roles without `USAGE`, which would fold the
bound to 0 over a non-empty log. `Anchor` refuses that shape loudly with
`postgres.ErrAnchorSequenceMasked` (the message names this grant)
instead of recording a root over zero heads as a real anchor:

```sql
GRANT USAGE ON SCHEMA migrations TO <anchor job>;
GRANT SELECT ON events, log_order, migrations.goose_migrations TO <anchor job>;
GRANT SELECT, INSERT ON anchors TO <anchor job>;
GRANT USAGE ON SEQUENCE events_global_position_seq TO <anchor job>;
GRANT es_relay TO <anchor job>;
```

### The `CACHE 1` invariant

The position sequence `events_global_position_seq` must stay at
`CACHE 1` (the database default — nothing in the schema pins it, so the
fence asserts it instead). The horizon fence's lossless proof needs every
position at or below its bound to have been *drawn* before the bound was
read; a sequence with `CACHE n > 1` hands cached positions out only after
`last_value` has already advanced past them, so a fence can clear and
deliver past a position that commits later — a permanently skipped event
on the feed, and on the anchor path a root recorded over a section that
can still change. Every fence round — feed pages and the anchor path's
settled bound alike — reads the bound through the documented
`pg_sequences` view and asserts `cache_size = 1`, so a misconfigured
sequence fails loudly:

```
position sequence events_global_position_seq must use CACHE 1;
run ALTER SEQUENCE events_global_position_seq CACHE 1
```

An idle workspace short-circuits on its own committed maximum before the
sequence read, so the assertion fires on its first active round.
`ALTER SEQUENCE … CACHE 32` is a reasonable-looking throughput tweak; it
is exactly the misconfiguration this invariant exists to catch, and it
was already inside the loss window before the assertion existed. ADR 0038
records the full argument.

### Shared logins are mutually trusting; DSNs are trust documents

RLS isolates workspaces by the `app.workspace_id` the *application* pins
on its own session — the database cannot tell two workspaces driven by the
same login role apart. **Workspaces that share one database login are
mutually trusting:** any process holding that role's credentials can pin
any workspace id. Give each tenant — or each group of genuinely mutually
trusting tenants — its own login role; the grant blocks above are per
`<role>` precisely so that wall can exist.

The relay and anchor/verify principals sit above every workspace: their
login roles hold `es_relay`, and the anchor job reads across all
workspaces under the bypass. Treat their DSNs accordingly:

- Connect over TLS and keep verification on — `sslmode=verify-full` with a
  pinned root. es-lite does not enforce or parse TLS settings itself; the
  DSN is the only place transport security is configured, so
  `sslmode=disable` or verification-less `require` must never leave a
  local dev network.
- The DSN names the host, the login role and (if used) the password: keep
  it out of process dumps, shared logs and shell history, and prefer
  secret injection over environment files.
- One login role per principal (relay, anchor job, each appender, each
  feed process) — a leaked credential then reaches exactly the grants its
  owner needs and nothing wider.

## Snapshot cache (ADR 0042)

Migration 0013 adds a `snapshots` table: the folded state of long streams,
one row per (workspace, stream), written by `aggregate.Runtime` outside the
append transaction and read back on the next load. It is pure cache state —
always re-derivable from the log — and it is workspace-private: its RLS
policy has no bypass arm, so no relay, anchor or feed principal gets any
privilege on it, and the appender grant above is the whole contract.

**The name is reserved.** An adopter may already own a table called
`snapshots`; if its shape differs, rename it before upgrading — the
migration refuses an incompatible shape loudly instead of recording itself
as applied, because the runtime ignores `Load`/`Save` errors and a silently
skipped migration would degrade every load to a permanent full fold with no
signal. (SQLite's 0011 is a plain `CREATE TABLE` and refuses a foreign
table the same way.)

**Backups.** Snapshots are derived state, so a logical backup may exclude
them when size matters: `pg_dump --exclude-table=snapshots …`. A physical
restore that leaves the table empty or missing needs no recovery step:
missing snapshots rebuild lazily — the next append past the runtime's gap
rewrites the row, and `Runtime.WarmSnapshot` repopulates a stream
explicitly, archived aggregates nothing appends to included.

**Erasure.** Both adapters expose `Purge(ctx)` — delete every snapshot the
cache holds, workspace-scoped on Postgres. It is best-effort and
application-coordinated, like the projection purge (ADR 0004 §4): a command
in flight, or one starting immediately after, can re-insert a row. Quiesce
the workspace before running it — or, for sealed state, destroy the key
first, which makes any racing re-insert inert.

## Backup, restore and clones (ADR 0043)

The append path is the authority. From v0.21.0 the functional source is the
event log:

- `events` (with every partition) — stream history, versions, the chain,
  global positions, audit metadata, and the append's constraint ops, which
  ride on the first event row of a constraint-bearing batch. Aggregates, the
  chain, positions and the claims index all rebuild from it.
- `log_order` — one row per event-bearing append, the order root's input. It
  is the order witness, not state: stream it only if you need to re-verify the
  global append order on the restored database.

Everything else is derived or optional. `unique_claims` is a derived index —
the ops on the events replay into an empty one. `snapshots` is a cache.
`anchors` recompute (the exported JSON is the durable evidence).
`feed_checkpoints` and `feed_dead_letters` are optional; `feed_leases` is
ephemeral. `command_dedup` is a post-commit response cache: exclude it, and read
the dedup note below before deciding otherwise. There is no second state table
and no second sequence: one table carries the events and the ops.

**PITR.** If arbitrary point-in-time recovery is the requirement, use physical
backups plus WAL archiving (`pgBackRest`, `WAL-G`, or the managed equivalent).
The whole database returns — the events *and* the index — and nothing needs
rebuilding. Restore the base backup, replay WAL to the target, restore cluster
globals (roles, memberships) if they are not in the base backup, then run the
checks below and resume. PITR is cluster-wide and all-or-nothing; it cannot
restore one workspace, and restoring to before a client's ack is data loss by
definition. The WAL archive and the logical stream below are complementary:
WAL for time travel, the stream for portability, clones and continuous
verification.

### Logical restore

The stream restores to a **consistent cut**, not to an arbitrary timestamp. Cut
the events (and `log_order` if you carry it) at one commit boundary — one LSN
from a CDC stream, or one export epoch. Never mix epochs: the ops and their
event are one row, so an event cut is internally consistent; loading
`unique_claims` from another cut is what breaks it.

1. Provision an empty database at the same major version and run `es-migrate`
   to head (schema, policies, triggers).
2. Create the roles and grants (`pg_dumpall --globals` or IaC). Confirm
   `es_relay` and the appender's grants. The appender's existing `SELECT,
   INSERT` on `events` already covers `constraint_ops`: there is no new table
   and no new grant.
3. Load `events` (all partitions) into that cut, and `log_order` too if you
   want order verification on the restored database. There are no foreign
   keys; one transaction is still preferred. For a workspace-scoped restore,
   filter by `workspace_id` (or run through the relay path) — a workspace
   session sees only its own rows under RLS.
4. Advance the position sequence, and confirm `CACHE 1` (§ *The `CACHE 1`
   invariant*):
   ```sql
   SELECT setval('events_global_position_seq',
                 (SELECT max(global_position) FROM events));
   ```
   Skip it only when the restored log is empty. There is no second sequence.
   SQLite's `AUTOINCREMENT` tracks explicit-id inserts itself, so this step is
   Postgres-only.
5. **Stop writers.** Verify the index first: `es-verify claims` is
   non-destructive and must report `claims OK` (exit 0; 1 names the first
   divergence, 2 is a refusal or error). Rebuild only if you intend to replace
   the index, after exporting any divergent claims: `es-verify claims
   --rebuild`, then `es-verify claims` again (the second command must report
   `claims OK`). `--rebuild`
   replaces the index with what the log can prove, and an op committed after
   its snapshot cannot be replayed; Postgres runs it in one Repeatable Read
   transaction, so a writer racing the snapshot may surface as a
   serialization error — loud and retryable, never a silent divergence. A
   write that races the rebuild must retry after it commits. Keep writers
   stopped until step 8.
   The whole-database form (no
   workspace argument) needs the owner/superuser, or a role with `es_relay`
   plus `SELECT` on `events` and `unique_claims` and `DELETE, INSERT` on
   `unique_claims`: `ClaimWorkspaces` takes the role-gated bypass, which the
   appender lacks, while the relay has `es_relay` but not the `unique_claims`
   DML. The appender role is enough only for `es-verify claims <workspace>`.
   Deleting and reinserting `unique_claims` is the one place a restore writes
   that table. The `constraint_ops` on the events are the only input. A
   restored database whose live claims have no ops in the event log is
   inconsistent — a partial restore or out-of-band SQL — and both commands
   refuse it (`es.ErrClaimsWithoutDecisions`, exit 2) rather than guess or
   delete. Do not load `unique_claims` from a different cut and do not invent
   a backfill. Re-cut from the events, or start fresh.

   The guard is deliberately narrow: it fires only when there are **zero**
   op-bearing events. A partially inconsistent store — some claims backed by
   ops, some not — passes it, and `--rebuild` deletes every live claim the log
   does not license. Run `es-verify claims` first; on a mixed store, export
   the divergent claims through the new append path before rebuilding. The
   rollback window in
   [`docs/migration/2026-11-07-breaking-changes.md`](../migration/2026-11-07-breaking-changes.md)
   is one way such a store arises.
6. Run the chain audit. If you loaded `log_order`, run `es-verify all`: it
   verifies every stream chain and then the global log order (every batch hash
   recomputed from the events). An events-only restore has no order witness,
   so run `es-verify all --chains-only` instead: it verifies every stream and
   skips the order stage, which would fail `EMPTY` by design (exit 2). Keep the
   exported anchor JSON as the order evidence (ADR 0043 §2.3).
7. Optionally restore `anchors` and `feed_checkpoints` (or replay the feed
   from zero). Do not expect a restored database without `log_order` to
   recompute the order root: roots need the witness. Keep the source's
   exported anchor JSON as the evidence, or record new anchors only after
   `log_order` is present. Leaving `published_at` unset causes projection
   re-delivery — at-least-once, which projections already handle.
8. Resume writers. Snapshots are absent and rebuild lazily; warm explicitly
   with `Runtime.WarmSnapshot` if the first reads matter.

An `es-verify claims` diff after an unreconciled cut is expected — it means the
cut was mixed, not that data was lost. Re-cut and redo the load.

`es-verify claims` exits 0 when the index matches the ops on the event log
(`claims OK: N op records`), 1 on a divergence (`DIVERGED:`, naming the first
divergent `(scope, value_key, stream_id)`), and 2 on a connection, usage or
refusal error — the errored workspace was not audited, and a 2 is never a
pass, even when earlier workspaces completed (a confirmed divergence
dominates a later error). The mutating mode (`--rebuild`) exits 0 or 2 only: a
rebuild that cannot replay the log refuses instead of leaving a partial index.

### Clones

The events table (plus `log_order` if you need order verification) is a
logical replication feed. Two shapes, different rules:

- **Follow (read-only clone).** Apply later cuts as they arrive. Apply **per
  source transaction**, not per table: the ops ride on the event row, so an
  event and its claim are never split.

  The clone never draws new positions, but it must still advance the position
  sequence after seeding and after each applied cut:
  ```sql
  SELECT setval('events_global_position_seq',
                (SELECT max(global_position) FROM events));
  ```
  Logical replication copies explicit `global_position` values without
  advancing the sequence, while the verifier's `settledBound(0)` reads
  `pg_sequences.last_value`. An unadvanced sequence makes order verification
  recompute every batch from an empty event page and report `BROKEN` (exit 1)
  — a false failure; `EMPTY` is the no-`log_order` case. `CheckAnchor` clamps
  its bound. Grant the principal that advances the sequence `UPDATE` on it for
  `setval` (`GRANT UPDATE ON SEQUENCE events_global_position_seq TO <clone
  applier>`), or run the `setval` as owner/superuser; grant the verifier
  `USAGE` to read `last_value` — without it the value is masked to 0.

  The clone also maintains `unique_claims`: apply each event's ops per source
  transaction, or run `es-verify claims --rebuild` after a cut (pausing the
  apply while it runs). A fresh events-only clone that loads events and leaves
  the index empty diverges against the log; never load `unique_claims` from a
  different cut.

  Audit on a schedule: `es-verify all --chains-only` when `log_order` is not
  loaded; `es-verify all` (or `es-verify order`) only when it is and the
  sequence has been advanced. That is the useful property: the DR copy proves
  itself continuously instead of at incident time.
- **Fork (writable staging).** Seed at a cut, `setval` the one position
  sequence, then stop applying and let it diverge. It is a fork: never merge
  it back and never point it at the backup prefix. Promotion is a handover —
  quiesce, take a final cut, repoint — not a merge.

Apply idempotently, because CDC is at-least-once: dedupe events on
`(workspace, stream, version)` or `event_id`; a duplicate event skips its ops
with it. `log_order` dedupes on `from_position`. On a replica a duplicate
`Claim` is a **no-op**, not `ErrConstraintViolated` — the source already
validated it. Order by `global_position` and allow gaps: concurrent appends
can commit out of draw order. A per-workspace clone is an export, not
replication — filter by `workspace_id`; claims are per-workspace, so such a
clone is self-consistent. `log_order` is optional for a functional clone and
required to re-verify order; `command_dedup` does not travel.

A clone moves production payloads and claim identifiers into a lower-trust
environment. Do not share the production sealing key. Claim values are MACs:
the same key lets the clone match and correlate them; a different key keeps
uniqueness working but makes them opaque. Choose deliberately.

### Dedup on restore

`command_dedup` is not restored. A retry carrying a pre-restore
Idempotency-Key can therefore miss the stored response and re-handle. With
`Runtime.WithDeterministicEventIDs()` the event insert collides and the client
gets `aborted`/409 — the documented "do not reuse this key" outcome; without
deterministic ids, a retry with no expected-version check can duplicate. This
is the same window that already exists between a commit and a failed dedup
`Save`; a restore widens it, it does not create it. Clients must treat a 409
after a restore as a duplicate, not a retryable error. If response replay
across a restore matters, include `command_dedup` in the backup and accept its
retention/PII obligations.

### Erasing claims (ADR 0043)

The ops are on the immutable event log, so a **plaintext claim value is
permanent**. Deleting `unique_claims` is a cache wipe, not erasure: the next
`es-verify claims --rebuild` resurrects every claim from its event. There is
no row-level erasure path for a claim, and the appender's `DELETE` on
`unique_claims` exists for `Release` and a rebuild's clear, not for erasure.

Per-value erasure is therefore the same contract as event payloads (ADR 0025):
for personal data, pass a keyed MAC or an opaque token you control and destroy
the key. The stored value is inert without it. The same key lets a clone match
and correlate the values; a different key keeps uniqueness working but makes
them opaque. Do not pass a plaintext value you may later need to erase: the
event log keeps exactly the bytes you supplied, and no rebuild, purge or
`DELETE` can remove them.

Deleting `unique_claims` by itself is a repair action, not a privacy one. A
rebuild restores the index from the log; a store whose live claims have no ops
in the log is the inconsistent case both commands refuse. Purge or repair the
index only when you are rebuilding it from the events.

## Feed operations (ADR 0037)

The feed's failure modes are *delay*, never *loss* — but only if the
signals are wired. This section is the operator's map from each signal to
its runbook path. All of it applies to the Postgres feed path
(`feed.Server` over the Postgres adapters, driven by `eventfeed` or the
platform's transport).

### Poison events and Skip

Oversized events are stopped twice: the store's payload cap (default 8 MiB,
`postgres.WithMaxPayloadBytes` / `sqlite.WithMaxPayloadBytes`) rejects an
over-cap append with `es.ErrPayloadTooLarge` before anything enters the
log, and a legacy over-cap row that is inserted anyway is never
materialized by a feed page — the stream dies with
`*feed.EventTooLargeError` (position, type, size, limit; no payload) and
`Observer.OnPoison` fires. The event stays in the immutable log; the
subscription stops there deliberately rather than silently skipping it.

Recovery is `feed.Server.Skip` with a `feed.SkipRequest`
(scope, projection, instance, position, actor, reason, force):

- It advances the checkpoint **past** the position (resume reads strictly
  after it) and records a dead-letter row in `feed_dead_letters` **before**
  advancing — a crash in between leaves an audit row and an unadvanced
  cursor, so the retry is idempotent and no skip is ever unaudited.
- `Skip` preserves the checkpoint's definition fingerprint: it is a
  position advance, not a redefinition. A skip *below* the current
  checkpoint is refused — it would silently rewind the subscription and
  re-deliver history the consumer already acknowledged — unless the
  request carries `force`, which keeps the deliberate rewind (Reset
  semantics) and records the dead letter; that is sometimes wanted for
  replay, but not the normal poison path.
- Unless `force` is set it refuses while a live lease holds the key. The
  normal flow needs no force: the poison error already killed the stream
  and released its lease.

**Triage** a dead letter by `(workspace, projection, reason)`:

1. **Fix the cause** — raise the payload cap, add an upcast for an
   unparseable schema version, or fix the handler the reason names.
2. Then either **accept the gap** — a recorded, audited omission; the row
   is the audit trail — or **replay it**: `ResetCheckpoint` to a position
   before the skip re-delivers the dead-lettered event like any other.
3. Rows are tiny pointers, not copies (the event itself never leaves the
   log); retention is an ops policy, not a mechanism. Alert on
   `OnPoison` and on `feed_dead_letters` depth.

A handler that always fails is *not* this DLQ: the feed fails the stream
stop-first, and the bounded-retry budget for handler-level poison lands
with the projection-plan companion work (review A7).

### Stall alerting

`Observer.OnStall(settledHead, matchingRawHead)` fires — exactly once per
stall episode, probes rate-limited — when a stream's pages come back empty
while committed backlog sits above the settled position. The second
parameter is the *matching* raw head — the backlog of the subscription's
own consumed types, not the log-wide maximum — so a caught-up filtered
subscription does not report a stall when unrelated event types land above
it. Two regimes produce a genuine stall:

- **A long caller-owned append** — correct waiting; it self-clears on
  commit and needs nothing.
- **A wedged long-lived transaction** anywhere on the primary — it pins
  the commit horizon and stalls *every* feed page on the database until it
  ends. This is the one to alert on: page `OnStall` into metrics and alert
  on sustained stalls, not single blips.

When a stall persists, find the horizon-pinning transaction on the
primary (`pg_stat_activity`, `state = 'idle in transaction'` / long
`xact_start`), but run anything heavier on a replica — diagnosing with
big analytics queries on the primary adds to exactly the problem being
diagnosed. The same horizon discipline means wraparound maintenance is a
live concern at feed-fleet scale (the fence burns transaction ids; ADR 0037
records the numbers): alert on `age(datfrozenxid)` per database and keep
autovacuum's freeze schedule unimpeded.

### Checkpoints, resets and lease failover

- **Lease failover bounds the feed's RTO at `LeaseTTL`** (30 s default): a
  dead feed process's lease expires within the TTL and another process's
  subscriber acquires the key, resuming from the last acknowledged page.
  Crash windows redeliver at most one page when acks are healthy — that
  is the at-least-once contract; the consumer's marker deduplicates.
- **`ResetCheckpoint` re-identifies, it does not clear.** A reset writes
  the reset request's definition hash as the checkpoint's new identity and
  moves the position; `DeleteSubscription` is the operation that removes
  the identity entirely.
- **A forced reset can be flipped back by the stream it displaced.** A
  live old-definition subscriber still holds the lease, and its next ack —
  still owner-valid — re-records the old fingerprint and its own position.
  A forced reset sticks only after the old stream ends; stop it first
  when the reset must hold. (Without force, the reset simply refuses while
  any lease is held, so this only applies to the forced path.)

### Pool sizing

pgx's default pool (`max(4, NumCPU)`) starves a feed process at roughly
50+ concurrent streams: each stream polls on its own cadence and every
fence round needs a connection. Size the pool explicitly per feed process
at **≥ 2× the expected concurrent streams**, bounded by Postgres
`max_connections` across *all* feed processes plus the app's own
connections. The measured envelope for 100 idle subscribers: ~400
polls/s, < 5% of one core, < 200 KB/s network, pages bounded by the
byte budget (16 MiB default) — the pool, not the CPU, is the binding
constraint. The server's idle-poll jitter (±20%) and wait floor already
keep a fleet from stampeding the pool in lockstep; at ~1000 subscribers
per process, the per-poll fence round wants to be shared per workspace
(ADR 0037's future-work note).

### Feed tables and RLS

The feed's own tables — `feed_checkpoints`, `feed_leases`,
`feed_dead_letters` — carry **no bypass arm**: their policies admit only
the workspace arm, with `FORCE ROW LEVEL SECURITY`. Not even a session
holding `app.bypass_rls` **and** `es_relay` can reach another workspace's
subscription state; the feed never reads across tenants. Connect feed
processes as ordinary non-superuser, non-`BYPASSRLS` roles and grant them
exactly the schema access they need — the `es_relay` grant above exists
for `Drain` and the anchor/verify path, not for the feed.

## Feed wake-ups (ADR 0039)

The feed's idle path is a poll — a committed append reaches an idle
subscriber on average one half-poll after commit. ADR 0039 adds an optional
wake seam on top of it: a *hint* that new events may exist, which lets a
subscriber read sooner. The horizon fence, checkpoints, leases and delivery
semantics are unchanged — a missed, late or duplicated wake costs at most one
poll, never an event. Two sources exist; both are optional, and with neither
configured behaviour is exactly the polling described above.

- **C1 — application hint (default, recommended).** The process that appends
  calls `hub.Wake(scope)` after a successful append — in-process, or relayed
  over the application's own bus cross-process. es-lite ships no broker: the
  bus, its subjects and its authorization are the application's concern, on
  both backends (SQLite has no C2).
- **C2 — Postgres `LISTEN/NOTIFY` (opt-in, off by default).** The Postgres
  adapter's `WithAppendNotify(channel)` option makes each non-empty append
  transaction run `pg_notify` with the workspace ID as payload; a
  `NotifyListener` in the feed process receives it and wakes the hub. NOTIFY
  is transactional, so the hint commits with the append — including
  caller-owned transactions, which C1 cannot cover without touching every
  call site. `WithCheckpointNotify(channel)` is the same hint for checkpoint
  saves: `CheckpointStore.Save` emits it in the save transaction, so a
  committed advance wakes the readers waiting on it. Use a second channel;
  the append and checkpoint wakes mean different things.

### C2's trust precondition

**C2 is enabled only when every role that can `CONNECT` to the database is
trusted, or the database is dedicated to the append/feed path.** `LISTEN` and
`pg_notify` are not RLS-filtered — notifications are visible to all users —
and any role that can `CONNECT` can both `LISTEN` on a nameable channel and
spoof a notification on it. The precondition is therefore about *all*
connect-capable roles, not just the role the listener runs under: this is
stricter than "run the listener under a trusted role". Before enabling,
enumerate who can connect and confirm every role is trusted — the login
roles, plus the database-level `CONNECT` grants shown by `\l` in `psql`
(`PUBLIC` holds `CONNECT` unless it has been revoked):

```sql
SELECT rolname FROM pg_roles WHERE rolcanconnect;
```

A co-tenant database with untrusted roles uses C1 instead. The listener
itself needs only `CONNECT` — no new grants, no schema change, no rows — but
that is not what makes C2 safe; the precondition above is.

### Operating a C2 listener

- **The channel name is a capability.** Generate it high-entropy and
  unguessable; the adapter validates it as a strict PostgreSQL identifier at
  `Open` (length ≤ `NAMEDATALEN`−1) and quotes it for `LISTEN`. **Never log
  it**: roles holding `pg_read_all_stats` can read other sessions' query text
  and thereby learn the name. An unguessable name mitigates eavesdropping and
  injection, but it is not a substitute for the trust precondition.
- **One listener connection per channel, dedicated.** The listener owns one
  connection that is never returned to a pool — a direct or session-pooled
  connection only, because **PgBouncer transaction pooling breaks `LISTEN`**
  (the session must outlive the `LISTEN`). A deployment may run a listener in
  every process (the connection count scales with processes, not workspaces),
  or elect one gateway that receives both channels and relays the wake over
  its own bus; the second shape keeps the direct connection to one process.
  The listener sets an `application_name`, so it is identifiable in
  `pg_stat_activity`.
- **Reconnect backoff starts at `Poll`.** On connection loss the listener
  reconnects with jittered backoff — the schedule starts at the feed's
  default `Poll` (250 ms) with ±20% jitter, doubling to 30 s. The floor is
  the schedule's, not each draw's: a draw may fall below `Poll`, and
  clamping every draw at `Poll` would re-herd a fleet of listeners that
  lost their connections together. The listener then re-issues `LISTEN`
  and fires `OnReconnected`, wired to `hub.WakeAll()` to repair hints
  missed while it was down. A missed wake is indistinguishable from a
  slow poll: delivery falls back to the fence-bounded poll, so the failure
  mode is delay, never loss.
- **Alert on `pg_notification_queue_usage()`.** Postgres's async notification
  queue is bounded, and when it is exhausted, NOTIFY-ing transactions **fail
  at commit** — the hint can fail a durable-looking append. A listener that
  is alive but not draining pins queue cleanup, so sustained appends can
  begin failing while the listener itself looks healthy. Alert on the
  queue-usage fraction (0–1) and treat sustained growth as a listener-health
  alarm before it becomes an append outage.
- **No two-phase commit.** A transaction that executed NOTIFY cannot be
  prepared for two-phase commit: `PREPARE TRANSACTION` fails for it. Do not
  enable C2 on an append path that flows through a 2PC preparer.

The cost when enabled: one extra statement per non-empty append (and per
checkpoint save, when the marker channel is on) plus the commit-time
notify-queue lock, which briefly serialises committing notifiers — the price of
the hint riding the write's own commit.

### When to prefer C1

Prefer C1 for **any shared database, any deployment without a trusted
listener role, and any append rate where the extra commit-time lock is
unwelcome.** C1's hint is best-effort and outside the transaction: a failed
hint never fails the append, and delivery falls back to the poll. C2's one
advantage — zero per-call-site wiring, including caller-owned-transaction
commits — is why it exists as an opt-in behind the preconditions above, not
why it should be the default.
