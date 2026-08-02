package sqlite_test

import (
	"context"
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
	if v != 1 {
		t.Fatalf("applied version = %d, want 1", v)
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
