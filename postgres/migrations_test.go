package postgres_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/postgres"
)

// TestFeedIndexesExist pins the feed page indexes (spec §5.2). CREATE INDEX
// on the HASH-partitioned parent creates one child index per partition with
// auto-generated suffixed names, so existence is asserted by matching the
// indexdef column lists in pg_indexes rather than by name: both column sets
// must be present on every events partition (8 here, events_p0..p7).
func TestFeedIndexesExist(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	for _, idx := range []struct {
		name string
		cols string
	}{
		{"events_ws_pos_idx", "(workspace_id, global_position)"},
		{"events_ws_type_pos_idx", "(workspace_id, type_url, global_position)"},
	} {
		rows, err := s.Pool().Query(ctx, `
			SELECT tablename FROM pg_indexes
			WHERE tablename LIKE 'events_p%'
			  AND indexdef ILIKE '%`+idx.cols+`%'
			ORDER BY tablename`)
		if err != nil {
			t.Fatalf("query %s: %v", idx.name, err)
		}
		var tables []string
		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				t.Fatalf("scan %s: %v", idx.name, err)
			}
			tables = append(tables, table)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("rows %s: %v", idx.name, err)
		}
		for _, p := range []string{"events_p0", "events_p1", "events_p2", "events_p3", "events_p4", "events_p5", "events_p6", "events_p7"} {
			if !slices.Contains(tables, p) {
				t.Fatalf("%s: no index matching %s on %s (found on %v)",
					idx.name, idx.cols, p, tables)
			}
		}
	}
}

// TestEventOpsIndexExists pins migration 0015's partial index (ADR 0043
// §2.2): the claims replay orders op-bearing events by global position, and
// without an index whose predicate matches `constraint_ops IS NOT NULL` the
// planner fetches every event in the partition to test the null. The
// predicate and INCLUDE are part of the definition, so match the whole
// indexdef, not just the column list — the plain events_ws_pos_idx has the
// same key columns, and dropping the INCLUDE would quietly reintroduce a
// heap fetch per candidate row.
func TestEventOpsIndexExists(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	rows, err := s.Pool().Query(ctx, `
		SELECT tablename FROM pg_indexes
		WHERE tablename LIKE 'events_p%'
		  AND indexdef ILIKE '%(workspace_id, global_position)%'
		  AND indexdef ILIKE '%INCLUDE (stream_id, constraint_ops)%'
		  AND indexdef ILIKE '%constraint_ops IS NOT NULL%'
		ORDER BY tablename`)
	if err != nil {
		t.Fatalf("query events_ops_idx: %v", err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatalf("scan events_ops_idx: %v", err)
		}
		tables = append(tables, table)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("rows events_ops_idx: %v", err)
	}
	for _, p := range []string{"events_p0", "events_p1", "events_p2", "events_p3", "events_p4", "events_p5", "events_p6", "events_p7"} {
		if !slices.Contains(tables, p) {
			t.Fatalf("events_ops_idx: no partial (workspace_id, global_position) index WHERE constraint_ops IS NOT NULL on %s (found on %v)", p, tables)
		}
	}
}

// TestAllBypassPoliciesRoleGated pins migrations 0007/0009's policy shape
// (review F8/F12) through pg_policies — not file greps, which would
// false-positive on comments (the Task 13 review ruling). All four policies
// that carry a bypass arm — events, unique_claims, anchors and log_order —
// must keep the arm (app.bypass_rls) in both USING and WITH CHECK, gate it on
// es_relay membership (pg_has_role), and guard that test with EXISTS so it
// cannot hard-fail (pg_has_role errors) on a database where the role has not
// been created yet.
func TestAllBypassPoliciesRoleGated(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	for _, table := range []string{"events", "unique_claims", "anchors", "log_order"} {
		rows, err := s.Pool().Query(ctx, `
			SELECT policyname, qual, COALESCE(with_check, '') FROM pg_policies
			WHERE schemaname = 'public' AND tablename = $1`, table)
		if err != nil {
			t.Fatalf("read policies for %s: %v", table, err)
		}
		var policies []struct{ name, qual, withCheck string }
		for rows.Next() {
			var name, qual, withCheck string
			if err := rows.Scan(&name, &qual, &withCheck); err != nil {
				rows.Close()
				t.Fatalf("scan policy for %s: %v", table, err)
			}
			policies = append(policies, struct{ name, qual, withCheck string }{name, qual, withCheck})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("policies rows for %s: %v", table, err)
		}
		if len(policies) != 1 {
			t.Fatalf("%s: %d policies, want exactly 1", table, len(policies))
		}
		p := policies[0]
		wanted := []string{"app.bypass_rls", "pg_has_role", "EXISTS"}
		if table != "anchors" {
			// anchors is store-wide by design (the root spans every
			// workspace), so its policy carries the bypass arm only.
			wanted = append(wanted, "app.workspace_id")
		}
		for _, def := range []struct{ clause, text string }{
			{"USING", p.qual},
			{"WITH CHECK", p.withCheck},
		} {
			for _, want := range wanted {
				if !strings.Contains(def.text, want) {
					t.Errorf("%s: policy %q %s %q lacks %q — the policy's arms must survive in both clauses",
						table, p.name, def.clause, def.text, want)
				}
			}
		}
	}
}

// TestEventsConstraintOpsColumn pins migration 0014's shape (ADR 0043): the
// events parent gains one nullable bytea column for the append's encoded
// constraint ops (partitions inherit it). No new table: the event log is the
// functional source.
func TestEventsConstraintOpsColumn(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	var dataType, nullable string
	if err := s.Pool().QueryRow(ctx, `
		SELECT data_type, is_nullable FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'events' AND column_name = 'constraint_ops'`,
	).Scan(&dataType, &nullable); err != nil {
		t.Fatalf("read events.constraint_ops definition: %v", err)
	}
	if dataType != "bytea" {
		t.Fatalf("events.constraint_ops type = %q, want bytea", dataType)
	}
	if nullable != "YES" {
		t.Fatalf("events.constraint_ops is_nullable = %q, want YES (only the first event of an op-bearing batch carries it)", nullable)
	}
}

// TestSnapshotsRLSShape pins the snapshots table's isolation shape
// (ADR 0042 §3.2): RLS enabled and forced, with exactly one
// workspace-only policy — the feed tables' shape (0004, 0010), not the
// log tables'. The relay never reads snapshots, and a snapshot is the
// workspace's own derived state, so the policy carries no bypass arm:
// even es_relay under app.bypass_rls must be refused. Catalog queries,
// per the TestAllBypassPoliciesRoleGated ruling: pg_policies, not file
// greps, which would false-positive on comments.
func TestSnapshotsRLSShape(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	rows, err := s.Pool().Query(ctx, `
		SELECT policyname, qual, with_check FROM pg_policies
		WHERE schemaname = 'public' AND tablename = 'snapshots'`)
	if err != nil {
		t.Fatalf("read policies for snapshots: %v", err)
	}
	var policies []struct{ name, qual, withCheck string }
	for rows.Next() {
		var name, qual, withCheck string
		if err := rows.Scan(&name, &qual, &withCheck); err != nil {
			rows.Close()
			t.Fatalf("scan policy for snapshots: %v", err)
		}
		policies = append(policies, struct{ name, qual, withCheck string }{name, qual, withCheck})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("policies rows for snapshots: %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("snapshots: %d policies, want exactly 1", len(policies))
	}
	p := policies[0]
	if p.name != "snapshots_workspace_isolation" {
		t.Errorf("snapshots: policy %q, want snapshots_workspace_isolation", p.name)
	}
	for _, def := range []string{p.qual, p.withCheck} {
		if strings.Contains(def, "app.bypass_rls") {
			t.Errorf("snapshots: policy %q references app.bypass_rls (%q) — the policy must have no bypass arm", p.name, def)
		}
		if !strings.Contains(def, "app.workspace_id") {
			t.Errorf("snapshots: policy %q does not key on app.workspace_id (%q)", p.name, def)
		}
	}
	var enabled, forced bool
	if err := s.Pool().QueryRow(ctx,
		`SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::regclass`,
		"public.snapshots",
	).Scan(&enabled, &forced); err != nil {
		t.Fatalf("read pg_class flags for snapshots: %v", err)
	}
	if !enabled || !forced {
		t.Fatalf("snapshots: RLS enabled=%v forced=%v, want both true (FORCE keeps the table owner subject)", enabled, forced)
	}
}

// TestCheckpointsTableDropped pins migration 0008: the bare checkpoints
// table 0001 created is gone. It was dead on Postgres — only SQLite
// implements delivery.Checkpoints, and the feed's per-subscription
// progress lives in feed_checkpoints — and it was the one es-lite
// table without an RLS policy.
func TestCheckpointsTableDropped(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	var dropped bool
	if err := s.Pool().QueryRow(ctx, `SELECT to_regclass('checkpoints') IS NULL`).Scan(&dropped); err != nil {
		t.Fatalf("to_regclass: %v", err)
	}
	if !dropped {
		t.Fatal("the checkpoints table still exists — 0008 drops it (dead on Postgres: only SQLite implements delivery.Checkpoints)")
	}
}

// TestMigrateCancelDoesNotLeakAdvisoryLock is F10's regression test.
//
// Migrate used to hold a session-scoped pg_advisory_lock for the whole
// run and release it with a deferred SELECT pg_advisory_unlock that ran
// on the caller's context. A run cancelled — or failed — at a statement
// boundary returned with the unlock bailed before it was even sent
// (nothing runs on a dead context), so the clean connection went back
// to the pool still holding the session lock: every later Migrate that
// landed on a different connection blocked on it forever. The fix
// scopes the advisory lock to each migration's transaction
// (pg_advisory_xact_lock), so the lock's lifetime is the transaction's
// lifetime by construction — no unlock statement to skip, no clean
// connection for a leftover session lock to latch onto.
//
// The scenario runs against a fresh scratch database the test creates
// and drops (every embedded migration pending, fully isolated from the
// suite's shared database), and cancels Migrate the moment the lock is
// granted — observed in pg_locks from the test's own connection, the
// pg_sleep-free way to cancel "immediately after the lock statement
// takes effect": no server sleeps, no fixed delay to tune. What must
// hold afterwards, from that other connection: no advisory lock for
// the migration key remains in this database once the cancelled
// connection has finished tearing down, and a second Migrate with a
// live context succeeds — the failure mode being fixed is the leak
// wedging every future migration. A reintroduced session lock that
// latches cannot slip through: a latched lock never goes away, and the
// bounded settle window below turns that into a loud failure.
//
// One pgx detail shapes the assertions (verified against the pgx source
// and the live server): cancelling a statement mid-flight tears the
// whole connection down asynchronously — the client marks it dead at
// once, but the server ends the session, and with it every lock the
// session held, only when the disconnect lands. So the count can read
// 1 for the milliseconds the teardown is in flight on ANY
// implementation; the contract under test is that it cannot STAY 1.
//
// pg_locks key mapping (verified against the live server): Migrate
// locks with the single bigint key 0x65736C697465 ("eslite" —
// store.go's migrateLockKey), and pg_locks splits a one-key bigint
// advisory lock across columns — classid carries the high 32 bits
// (0x6573), objid the low 32 bits (0x6C697465) — so the assertions
// re-compose classid/objid rather than filter on objid alone, and pin
// the key: change the constant and this test must change with it.
func TestMigrateCancelDoesNotLeakAdvisoryLock(t *testing.T) {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	ctx := context.Background()

	// The migration lock key, pinned (see the doc comment above).
	const lockKey int64 = 0x65736C697465
	const lockSQL = `SELECT count(*) FROM pg_locks
	                  WHERE locktype = 'advisory'
	                    AND (classid::bigint << 32 | objid::bigint) = $1
	                    AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`

	// A fresh scratch database, created and dropped by the test. The
	// drop is registered FIRST so it runs LAST (t.Cleanup is LIFO):
	// by then every connection to the scratch database — the store's
	// pool and the probe below — is closed; WITH (FORCE) is the
	// backstop if one ever is not.
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for admin: %v", err)
	}
	name := "eslite_migrate_leak_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create scratch database %s: %v", name, err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, `DROP DATABASE `+name+` WITH (FORCE)`); err != nil {
			t.Errorf("drop scratch database %s: %v", name, err)
		}
		admin.Close(ctx)
	})

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	scratchDSN := u.String()

	// The test's own connection to the scratch database: the outside
	// observer of the lock, as a second migrator would be.
	probe, err := pgx.Connect(ctx, scratchDSN)
	if err != nil {
		t.Fatalf("connect probe to %s: %v", name, err)
	}
	t.Cleanup(func() { probe.Close(ctx) })

	s, err := postgres.Open(ctx, scratchDSN, postgres.WithoutAutoMigrate())
	if err != nil {
		t.Fatalf("open store on %s: %v", name, err)
	}
	t.Cleanup(s.Close)
	// Warm the pool so Migrate's first acquire reuses this connection
	// instead of dialing one: the cancellation must meet the run at
	// the lock, not at connection setup.
	if _, err := s.Pool().Exec(ctx, `SELECT 1`); err != nil {
		t.Fatalf("warm pool: %v", err)
	}

	migrateCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.Migrate(migrateCtx) }()

	// Poll pg_locks until the lock is granted, then cancel — the whole
	// race is one cheap count per round trip, so the cancel lands right
	// after the grant while the run is still in its first DDL, long
	// before nine migrations could finish. A run that ends (or a lock
	// that never appears) fails the test loudly instead of spinning.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var held int
		if err := probe.QueryRow(ctx, lockSQL, lockKey).Scan(&held); err != nil {
			t.Fatalf("poll pg_locks: %v", err)
		}
		if held > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("Migrate returned (err=%v) before its lock appeared in pg_locks — the cancellation window was missed", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the migration lock never appeared in pg_locks — Migrate never reached its lock statement")
		}
	}
	cancel()

	// The cancellation must have interrupted the run: nine pending
	// migrations far outlast one poll round trip.
	if err := <-done; err == nil {
		t.Fatal("the cancelled Migrate returned nil — it finished before the cancellation landed, so this run proved nothing")
	}

	// No advisory lock may survive. One pgx detail needs waiting out:
	// cancelling a statement mid-flight tears the connection down
	// asynchronously — the client marks it closed at once, but the
	// server ends the session, and with it the transaction and its
	// advisory lock, only when the disconnect lands. So the count can
	// read 1 for the milliseconds the teardown is in flight; what the
	// fix guarantees is that it cannot STAY 1 — the old session lock,
	// latched on a healthy pooled connection, never went away at all.
	// Give the teardown a generous window, then hold the line.
	leakDeadline := time.Now().Add(5 * time.Second)
	for {
		var leaked int
		if err := probe.QueryRow(ctx, lockSQL, lockKey).Scan(&leaked); err != nil {
			t.Fatalf("count leaked advisory locks: %v", err)
		}
		if leaked == 0 {
			break
		}
		if time.Now().After(leakDeadline) {
			t.Fatalf("a cancelled Migrate still held %d advisory lock(s) for key %d five seconds after the run returned; want 0 — the lock must end with the transaction",
				leaked, lockKey)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A second Migrate with a live context must succeed and finish the
	// pending work: against the leak, a migrator on another connection
	// would block on the abandoned session lock.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate after a cancelled run: %v — the cancelled run's lock is blocking every future migration", err)
	}
	var v int
	if err := probe.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM migrations.goose_migrations WHERE module = 'eventstore'`).Scan(&v); err != nil {
		t.Fatalf("read shared ledger: %v", err)
	}
	if v != 15 {
		t.Fatalf("after the second Migrate the applied version is %d, want 15 (every embedded migration applied)", v)
	}
}

// TestSnapshotMigrationRefusesForeignTable pins ADR 0042 §3.2's refusal.
// snapshot.Cache has been public while unimplemented, so an adopter may
// already own a "snapshots" table of a different shape; the migration's
// CREATE TABLE IF NOT EXISTS would silently adopt it and record 0013 as
// applied — and because the runtime ignores Load/Save errors, a
// wrong-shaped table would degrade to permanent full folds with no
// signal. Shape is the columns, not just their names: the assertion
// refuses a missing or mistyped required column and an extra NOT NULL
// column with no default (the adapter's INSERT names only its own six
// columns, so the extra one has no value to take), and tolerates an
// extra nullable column, which breaks nothing. A refusal must fail the
// migration loudly and must not be recorded: the ledger stays at the
// pre-0013 history until the adopter renames their table.
func TestSnapshotMigrationRefusesForeignTable(t *testing.T) {
	for _, tt := range []struct {
		name    string
		ddl     string
		wantErr bool
		wantV   int
	}{
		{
			name:    "bare foreign table",
			ddl:     `CREATE TABLE snapshots (x int)`,
			wantErr: true,
			wantV:   12,
		},
		{
			// Right names, one wrong type: state text instead of
			// bytea. Name-only adoption would pass this and then fail
			// every Save at runtime, swallowed by the runtime.
			name: "wrong type",
			ddl: `CREATE TABLE snapshots (
				workspace_id text        NOT NULL,
				stream_id    text        NOT NULL,
				version      bigint      NOT NULL,
				fold_version bigint      NOT NULL,
				recorded_at  timestamptz NOT NULL,
				state        text        NOT NULL,
				PRIMARY KEY (workspace_id, stream_id)
			)`,
			wantErr: true,
			wantV:   12,
		},
		{
			// An extra NOT NULL column with no default breaks the
			// adapter's INSERT, which names only its own columns.
			name: "extra not-null column",
			ddl: `CREATE TABLE snapshots (
				workspace_id text        NOT NULL,
				stream_id    text        NOT NULL,
				version      bigint      NOT NULL,
				fold_version bigint      NOT NULL,
				recorded_at  timestamptz NOT NULL,
				state        bytea       NOT NULL,
				extra        text        NOT NULL,
				PRIMARY KEY (workspace_id, stream_id)
			)`,
			wantErr: true,
			wantV:   12,
		},
		{
			// The tolerant case: an extra nullable column breaks
			// nothing, so the migration must go through — this keeps
			// the check from being over-strict.
			name: "extra nullable column",
			ddl: `CREATE TABLE snapshots (
				workspace_id text        NOT NULL,
				stream_id    text        NOT NULL,
				version      bigint      NOT NULL,
				fold_version bigint      NOT NULL,
				recorded_at  timestamptz NOT NULL,
				state        bytea       NOT NULL,
				extra        text,
				PRIMARY KEY (workspace_id, stream_id)
			)`,
			wantErr: false,
			wantV:   15,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := openScratchStore(t)
			ctx := context.Background()

			if _, err := s.Pool().Exec(ctx, tt.ddl); err != nil {
				t.Fatalf("create foreign snapshots table: %v", err)
			}

			err := s.Migrate(ctx)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("Migrate: %v — a table the adapter can use must not be refused (ADR 0042 §3.2)", err)
				}
			} else {
				if err == nil {
					t.Fatal("Migrate succeeded over a foreign snapshots table — 0013 must refuse loudly (ADR 0042 §3.2)")
				}
				for _, want := range []string{"snapshots", "incompatible shape"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("Migrate error %q does not name %q — the refusal must point at the reserved table and its shape", err, want)
					}
				}
			}

			// Migrations run one transaction each (ADR 0010), so a
			// refusal leaves 0001-0012 standing with only 0013 absent:
			// the refused version must not be recorded.
			var v int
			if err := s.Pool().QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM migrations.goose_migrations WHERE module = 'eventstore'`).Scan(&v); err != nil {
				t.Fatalf("read shared ledger: %v", err)
			}
			if v != tt.wantV {
				t.Fatalf("after Migrate the applied version is %d, want %d", v, tt.wantV)
			}
		})
	}
}

// openScratchStore creates a uniquely-named, empty database and opens a store
// against it without auto-migrating, so a test can put the database into the
// state it needs before calling Migrate. Cleanup closes the store before
// dropping the database, because t.Cleanup is LIFO.
func openScratchStore(t *testing.T, opts ...postgres.Option) *postgres.Store {
	t.Helper()

	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for admin: %v", err)
	}
	name := "eslite_ledger_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create scratch database %s: %v", name, err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, `DROP DATABASE `+name+` WITH (FORCE)`); err != nil {
			t.Errorf("drop scratch database %s: %v", name, err)
		}
		admin.Close(ctx)
	})

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name

	all := append([]postgres.Option{postgres.WithoutAutoMigrate()}, opts...)
	s, err := postgres.Open(ctx, u.String(), all...)
	if err != nil {
		t.Fatalf("open store on %s: %v", name, err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestMigrateWritesTheSharedLedger(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	var n int
	if err := s.Pool().QueryRow(ctx, `
		SELECT count(*) FROM migrations.goose_migrations WHERE module = 'eventstore'
	`).Scan(&n); err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if n != 15 {
		t.Fatalf("eventstore ledger rows = %d, want 15", n)
	}

	var legacy bool
	if err := s.Pool().QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&legacy); err != nil {
		t.Fatalf("probe legacy ledger: %v", err)
	}
	if legacy {
		t.Fatal("Migrate still created schema_migrations")
	}
}

func TestMigrateAdoptsLegacySchemaMigrations(t *testing.T) {
	s := openScratchStore(t)
	ctx := context.Background()

	// Build the schema exactly as versions 1-3 left it, then record those
	// versions in the legacy ledger. This is the shape an upgraded database
	// has: real tables, a private history table, and no shared ledger.
	for _, name := range []string{"0001_initial_schema.sql", "0002_hash_chain.sql", "0003_feed_indexes.sql"} {
		sql, err := os.ReadFile(filepath.Join("migrations", name))
		if err != nil {
			t.Fatalf("read legacy migration %s: %v", name, err)
		}
		if _, err := s.Pool().Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply legacy migration %s: %v", name, err)
		}
	}
	if _, err := s.Pool().Exec(ctx, `
		CREATE TABLE schema_migrations (
			version    int         PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		);
		INSERT INTO schema_migrations (version) VALUES (1), (2), (3);
	`); err != nil {
		t.Fatalf("seed legacy ledger: %v", err)
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var adopted, total int
	if err := s.Pool().QueryRow(ctx, `
		SELECT count(*) FROM migrations.goose_migrations
		WHERE module = 'eventstore' AND version <= 3
	`).Scan(&adopted); err != nil {
		t.Fatalf("read adopted rows: %v", err)
	}
	if adopted != 3 {
		t.Fatalf("adopted rows = %d, want 3: the legacy history was not carried over", adopted)
	}
	if err := s.Pool().QueryRow(ctx, `
		SELECT count(*) FROM migrations.goose_migrations WHERE module = 'eventstore'
	`).Scan(&total); err != nil {
		t.Fatalf("read ledger total: %v", err)
	}
	if total != 15 {
		t.Fatalf("ledger rows = %d, want 15 after applying versions 4-15", total)
	}
}

func TestLedgerModuleNameUsesTheOption(t *testing.T) {
	s := openScratchStore(t, postgres.WithModule("custom"))
	ctx := context.Background()

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var custom, eventstore int
	if err := s.Pool().QueryRow(ctx, `
		SELECT count(*) FROM migrations.goose_migrations WHERE module = 'custom'
	`).Scan(&custom); err != nil {
		t.Fatalf("read custom rows: %v", err)
	}
	if err := s.Pool().QueryRow(ctx, `
		SELECT count(*) FROM migrations.goose_migrations WHERE module = 'eventstore'
	`).Scan(&eventstore); err != nil {
		t.Fatalf("read eventstore rows: %v", err)
	}
	if custom != 15 {
		t.Fatalf("custom ledger rows = %d, want 15", custom)
	}
	if eventstore != 0 {
		t.Fatalf("eventstore ledger rows = %d, want 0 when WithModule(custom) is set", eventstore)
	}
}
