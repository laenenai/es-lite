package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// The binary's first tests: they drive run() end to end — env, openBackend,
// dispatch, exit code — against a migrated SQLite file, exactly as the
// audit runs on a box. F13: an empty stream or an empty database is an
// error, never a clean `ok`.

// newTestDB migrates a fresh SQLite database in a temp dir and points the
// CLI's env at it. The binary opens stores without auto-migrate, so the
// schema must exist before run() is called; the returned store lets a
// subtest seed the database first. Close it before calling run — run opens
// its own handle.
func newTestDB(t *testing.T) *sqlite.Store {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "eslite.db")
	t.Setenv("BACKEND", "sqlite")
	t.Setenv("SQLITE_DSN", dsn)
	s, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// seedStream appends n events to sid through the store itself, so the
// chain in the file was written by the store, not by the test.
func seedStream(t *testing.T, s *sqlite.Store, sid es.StreamID, n int) {
	t.Helper()
	events := make([]es.EventData, n)
	for i := range events {
		events[i] = es.EventData{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte{byte(i + 1)}}
	}
	if _, err := s.Append(context.Background(), es.AppendParams{StreamID: sid, Events: events}); err != nil {
		t.Fatalf("append %d events: %v", n, err)
	}
}

// closeDB closes the seeding handle before run opens its own. Closing
// twice is fine (the cleanup handle no-ops), and every subtest needs it.
func closeDB(t *testing.T, s *sqlite.Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}
}

// TestVerifyStreamEmptyStreamIsNotIntact pins the CLI half of review focus
// 2: an empty stream must never read `ok` (F13), and --allow-empty is the
// only way an empty stream exits 0.
func TestVerifyStreamEmptyStreamIsNotIntact(t *testing.T) {
	t.Run("stream on empty db exits 2", func(t *testing.T) {
		s := newTestDB(t)
		closeDB(t, s)
		if code := run([]string{"stream", "thing:a"}); code != exitUsage {
			t.Errorf("stream on a never-written stream: exit %d, want %d", code, exitUsage)
		}
	})

	t.Run("all on empty db exits 2", func(t *testing.T) {
		s := newTestDB(t)
		closeDB(t, s)
		if code := run([]string{"all"}); code != exitUsage {
			t.Errorf("all on a database with zero streams: exit %d, want %d", code, exitUsage)
		}
	})

	t.Run("stream on ok stream exits 0", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		seedStream(t, s, sid, 2)
		closeDB(t, s)
		if code := run([]string{"stream", sid.Canonical()}); code != exitIntact {
			t.Errorf("stream on an intact chain: exit %d, want %d", code, exitIntact)
		}
	})

	t.Run("stream --allow-empty on empty exits 0", func(t *testing.T) {
		s := newTestDB(t)
		closeDB(t, s)
		if code := run([]string{"stream", "--allow-empty", "thing:a"}); code != exitIntact {
			t.Errorf("stream --allow-empty on a never-written stream: exit %d, want %d", code, exitIntact)
		}
	})

	t.Run("migrate+verify roundtrip sets exit 1 on a broken chain", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		seedStream(t, s, sid, 2)
		// Tamper with v1's payload behind the store's back: the stored hash
		// no longer matches what the row's fields hash to.
		if _, err := s.DB().ExecContext(context.Background(),
			`UPDATE events SET payload = ? WHERE stream_id = ? AND version = 1`,
			[]byte("tampered"), sid.Canonical()); err != nil {
			t.Fatalf("tamper: %v", err)
		}
		closeDB(t, s)
		if code := run([]string{"stream", sid.Canonical()}); code != exitBroken {
			t.Errorf("stream on a tampered chain: exit %d, want %d", code, exitBroken)
		}
	})
}
