package sqlite_test

import (
	"context"
	"path/filepath"
	"slices"
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
	if v != 8 {
		t.Fatalf("applied version = %d, want 8 (0001 schema + 0002 hash chain + 0003 feed + 0004 definition fingerprint + 0006 log order + 0007 feed type index + 0008 command dedup; no 0005 — Postgres's is RLS-role-specific)", v)
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
	want := []int{1, 2, 3, 4, 6, 7, 8}
	if !slices.Equal(got, want) {
		t.Fatalf("recorded versions = %v, want each of %v exactly once", got, want)
	}
}
