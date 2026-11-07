package postgres_test

import (
	"context"
	"net/url"
	"os"
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

// TestAllBypassPoliciesRoleGated pins migrations 0007/0009's policy
// shape (review F8/F12) through pg_policies — not file greps, which
// would false-positive on comments (the Task 13 review ruling). All
// four policies that carry a bypass arm — events, unique_claims,
// anchors and log_order — must keep the arm (app.bypass_rls), gate it
// on es_relay membership (pg_has_role), and guard that test with
// EXISTS so it cannot hard-fail (pg_has_role errors) on a database
// where the role has not been created yet.
func TestAllBypassPoliciesRoleGated(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	for _, table := range []string{"events", "unique_claims", "anchors", "log_order"} {
		rows, err := s.Pool().Query(ctx, `
			SELECT policyname, qual FROM pg_policies
			WHERE schemaname = 'public' AND tablename = $1`, table)
		if err != nil {
			t.Fatalf("read policies for %s: %v", table, err)
		}
		var policies []struct{ name, qual string }
		for rows.Next() {
			var name, qual string
			if err := rows.Scan(&name, &qual); err != nil {
				rows.Close()
				t.Fatalf("scan policy for %s: %v", table, err)
			}
			policies = append(policies, struct{ name, qual string }{name, qual})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("policies rows for %s: %v", table, err)
		}
		if len(policies) != 1 {
			t.Fatalf("%s: %d policies, want exactly 1", table, len(policies))
		}
		p := policies[0]
		for _, want := range []string{
			"app.bypass_rls", // the bypass arm survives...
			"pg_has_role",    // ...gated on es_relay membership (F8)...
			"EXISTS",         // ...behind the missing-role guard (F12)
		} {
			if !strings.Contains(p.qual, want) {
				t.Errorf("%s: policy %q qual %q lacks %q — the bypass arm must be role-gated behind an EXISTS guard",
					table, p.name, p.qual, want)
			}
		}
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
	if err := probe.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	if v != 10 {
		t.Fatalf("after the second Migrate the applied version is %d, want 10 (every embedded migration applied)", v)
	}
}
