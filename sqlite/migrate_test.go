package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/laenenai/es-lite/sqlite"
)

func TestMigrateIdempotent(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared") // auto-migrates
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	var v int
	if err := s.DB().QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatalf("schema_migrations: %v", err)
	}
	if v != 12 {
		t.Fatalf("applied version = %d, want 12 (0001 schema + 0002 hash chain + 0003 feed + 0004 definition fingerprint + 0006 log order + 0007 feed type index + 0008 command dedup + 0009 producer span context + 0010 delegated actor + 0011 snapshot cache + 0012 event constraint ops; no 0005 — Postgres's is RLS-role-specific)", v)
	}
	// Re-running is a no-op, not an error.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}
	// The schema is present.
	var n int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("events table missing: %v", err)
	}
}

func TestWithoutAutoMigrate(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared", sqlite.WithoutAutoMigrate())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	// No schema yet.
	if _, err := s.DB().ExecContext(ctx, `SELECT 1 FROM events`); err == nil {
		t.Fatal("events table exists before Migrate; WithoutAutoMigrate did not skip DDL")
	}
	// Explicit migrate brings it up.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	var n int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("events table missing after Migrate: %v", err)
	}
}

// TestConcurrentMigrateSameFile races two stores opened on ONE file DSN
// (F34): two processes migrating the same database at the same instant
// must not both decide a migration is pending and both apply it. Both
// Migrate calls must return nil (no "duplicate column name", no lost
// constraint race), and every version must be recorded exactly once.
func TestConcurrentMigrateSameFile(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "events.db")

	// Two independent handles on the same file — two "processes" as far
	// as SQLite's file-level write lock is concerned. Auto-migrate is off
	// so the Migrate calls themselves are what race, on the barrier's
	// schedule, not on Open's.
	stores := make([]*sqlite.Store, 2)
	for i := range stores {
		s, err := sqlite.Open(ctx, dsn, sqlite.WithoutAutoMigrate())
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		t.Cleanup(func() { s.Close() })
		stores[i] = s
		// Establish each connection before the barrier. sql.Open is
		// lazy: the physical connection (and its journal_mode(WAL)
		// pragma, which initializes the header of a brand-new file)
		// would otherwise be created inside the race, and two
		// simultaneous first-connections on a fresh file fail with an
		// immediate SQLITE_BUSY that no Migrate algorithm can affect.
		// F34 is the check-then-apply race between two ESTABLISHED
		// connections — a replica racing an already-running one — so
		// that is what this test races.
		if err := s.DB().PingContext(ctx); err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
	}

	// Release barrier: both goroutines exist before either migrates and
	// are freed together, so the two Migrates genuinely overlap.
	start := make(chan struct{})
	errs := make([]error, len(stores))
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = stores[i].Migrate(ctx)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("migrate %d: %v", i, err)
		}
	}

	// Each version exactly once: the recorded set is exactly the
	// migration set (no 0005 — Postgres's is RLS-role-specific, see
	// TestMigrateIdempotent), so a double-apply or a skipped migration
	// both fail here.
	var got []int
	rows, err := stores[0].DB().QueryContext(ctx,
		`SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan version: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate schema_migrations: %v", err)
	}
	want := []int{1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12}
	if !slices.Equal(got, want) {
		t.Fatalf("recorded versions = %v, want each of %v exactly once", got, want)
	}
}

// TestSnapshotsTableColumns pins the snapshot cache table's shape (ADR
// 0042 §3.2): exactly five columns, in the declared order, with
// stream_id as the sole primary key — one SQLite database is one
// workspace, so the row key is the canonical stream id alone.
func TestSnapshotsTableColumns(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared") // auto-migrates
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	rows, err := s.DB().QueryContext(ctx, `PRAGMA table_info(snapshots)`)
	if err != nil {
		t.Fatalf("table_info(snapshots): %v", err)
	}
	defer rows.Close()

	type col struct {
		name    string
		notNull int
		pk      int
	}
	var got []col
	for rows.Next() {
		var cid int
		var c col
		var typ string
		var dflt any // NULL for every column here; must not scan into a typed value
		if err := rows.Scan(&cid, &c.name, &typ, &c.notNull, &dflt, &c.pk); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		got = append(got, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info: %v", err)
	}

	wantNames := []string{"stream_id", "version", "fold_version", "recorded_at", "state"}
	if len(got) != len(wantNames) {
		t.Fatalf("snapshots columns = %+v, want exactly %v", got, wantNames)
	}
	for i, c := range got {
		if c.name != wantNames[i] {
			t.Fatalf("snapshots column %d = %q, want %q (order matters)", i, c.name, wantNames[i])
		}
		if c.notNull != 1 {
			t.Fatalf("snapshots column %d (%s) notnull = %d, want 1 (every column is NOT NULL)", i, c.name, c.notNull)
		}
		if i > 0 && c.pk != 0 {
			t.Fatalf("snapshots column %d (%s) pk = %d, want 0 (stream_id is the sole primary key — no composite)", i, c.name, c.pk)
		}
	}
	if got[0].pk != 1 {
		t.Fatalf("stream_id pk = %d, want 1 (the primary key)", got[0].pk)
	}
}

// TestEventsConstraintOpsColumn pins migration 0012's shape (ADR 0043): the
// events table gains one nullable BLOB column for the append's encoded
// constraint ops. No new table: the events table is the functional source.
func TestEventsConstraintOpsColumn(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared") // auto-migrates
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	rows, err := s.DB().QueryContext(ctx, `PRAGMA table_info(events)`)
	if err != nil {
		t.Fatalf("table_info(events): %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull int
			dflt    any // NULL or a DEFAULT expression; must not scan into a typed value
			pk      int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			t.Fatalf("scan table_info(events): %v", err)
		}
		if name != "constraint_ops" {
			continue
		}
		found = true
		if typ != "BLOB" {
			t.Fatalf("events.constraint_ops type = %q, want BLOB", typ)
		}
		if notNull != 0 {
			t.Fatalf("events.constraint_ops notnull = %d, want 0 (nullable: only the first event of an op-bearing batch carries it)", notNull)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info(events): %v", err)
	}
	if !found {
		t.Fatal("events has no constraint_ops column")
	}
}

// TestSnapshotMigrationRefusesForeignTable proves 0011 refuses loudly
// when a "snapshots" table already exists with a different shape (ADR
// 0042 §3.2). The Cache interface has been public while unimplemented,
// so an adopter may own such a table; a silent IF NOT EXISTS no-op
// would record 0011 as applied and leave aggregate.Runtime's swallowed
// Load/Save errors hiding a permanently dead cache. The migration is
// the only loud place: it must fail, and its version must not be
// recorded — the database stays at the pre-0011 ledger.
func TestSnapshotMigrationRefusesForeignTable(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// A foreign table squatting on the reserved name, wrong shape.
	if _, err := db.ExecContext(ctx, `CREATE TABLE snapshots (x int)`); err != nil {
		t.Fatalf("seed foreign snapshots table: %v", err)
	}

	s, err := sqlite.NewWithDB(ctx, db, sqlite.WithoutAutoMigrate())
	if err != nil {
		t.Fatalf("NewWithDB: %v", err)
	}

	if err := s.Migrate(ctx); err == nil {
		t.Fatal("Migrate over a foreign snapshots table: err = nil, want failure (the refusal must be loud, ADR 0042 §3.2)")
	} else if !strings.Contains(err.Error(), "snapshots") {
		t.Fatalf("Migrate err = %v, want text naming the colliding table (snapshots)", err)
	}

	// The failed 0011 must not be recorded: every earlier migration
	// commits in its own transaction, so the ledger stays exactly where
	// it was before 0011 was attempted.
	var v int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatalf("schema_migrations: %v", err)
	}
	if v != 10 {
		t.Fatalf("schema_migrations max version = %d, want 10 (a failed 0011 must not record itself)", v)
	}
}
