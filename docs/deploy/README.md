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

## Relay role and RLS bypass (migration 0005)

`Drain` — the delivery relay — and the anchor/verify path (`Anchor`,
`CheckAnchor`, `ListStreams`, `ListAnchors`) are the only readers that
work across workspaces, through the `app.bypass_rls` arm of the `events`
and `unique_claims` policies. Migration 0005 role-gates that arm: it
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

**Appender** — the process calling `Workspace.Append` (or `AppendTx`): it
draws positions, reads the stream head for optimistic concurrency,
inserts the events and their `log_order` attestation (design D1), and
applies uniqueness constraints. `Release` is the only `DELETE` es-lite
ever issues, and it is on the claims index, never on the log:

```sql
GRANT USAGE ON SEQUENCE events_global_position_seq TO <appender>;
GRANT SELECT, INSERT ON events, unique_claims TO <appender>;
GRANT DELETE ON unique_claims TO <appender>;
GRANT INSERT ON log_order TO <appender>;
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
reads `schema_migrations` for the chain's `ChainedSince`. The `SELECT` on
`anchors` is the job's own read path — `es-verify anchor` re-checks every
stored anchor — and `Anchor`'s `RETURNING created_at` (the database-clock
stamp it reports) needs it too. The `USAGE` on the sequence is not
optional: the anchor path fences on the same horizon the feed does
(`settledBound`), whose first statement reads the position sequence
through `pg_sequences` — and the view masks `last_value` for roles
without `USAGE`, which would fold the bound to 0 and make `Anchor`
silently record a root over zero heads as a real anchor:

```sql
GRANT SELECT ON events, log_order, schema_migrations TO <anchor job>;
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
postgres: position sequence events_global_position_seq must use CACHE 1;
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
