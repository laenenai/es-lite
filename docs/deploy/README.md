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
re-running the manual steps only builds 16 redundant child indexes per
index and then fails at `ATTACH PARTITION` (the partition indexes are
already attached). The steps below are for databases upgrading from a
pre-0003 schema where `events` already holds production data.

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
isolation. Both paths verify the membership once per `Store` at startup
and fail fast with `postgres.ErrRelayRoleMissing` instead of silently
reading nothing. The silence would look like an idle backlog for `Drain`
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

The membership verdict is checked once per `Store` and stands for its
lifetime: after granting a missed role, restart the refused processes —
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
  position advance, not a redefinition. (A skip *below* the current
  checkpoint rewinds — Reset semantics — and records the dead letter; that
  is sometimes wanted for replay, but not the normal poison path.)
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

`Observer.OnStall(settledHead, rawHead)` fires — exactly once per stall
episode, probes rate-limited — when a stream's pages come back empty while
committed backlog sits above the settled position. Two regimes produce it:

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
  Crash windows redeliver at most one page — that is the at-least-once
  contract; the consumer's marker deduplicates.
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
