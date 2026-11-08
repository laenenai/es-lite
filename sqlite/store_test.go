package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// openStore opens a fresh in-memory store for the (sub)test, keyed by name
// so every case reads its own database.
func openStore(t *testing.T) *sqlite.Store {
	t.Helper()
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared"
	s, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// appendEvents appends n opaque events to sid at the stream's current
// version, failing the test on the first error.
func appendEvents(t *testing.T, ctx context.Context, s *sqlite.Store, sid es.StreamID, n int) {
	t.Helper()
	for i := range n {
		if _, err := s.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i),
			Events: []es.EventData{{
				TypeURL:       "thing.v1.Happened",
				SchemaVersion: 1,
				Payload:       []byte{byte(i + 1)},
			}},
		}); err != nil {
			t.Fatalf("append v%d: %v", i+1, err)
		}
	}
}

// TestReadStreamStreamNotFound pins the ReadStream not-found contract: a
// read from the beginning of a stream that was never written reports
// es.ErrStreamNotFound (naming the stream) instead of a silent empty
// slice — whatever the upper bound, since from the start of the stream an
// empty result means no rows exist at all.
func TestReadStreamStreamNotFound(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, err := es.NewStreamID("thing", "never-written")
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		to   uint64
	}{
		{"whole stream", 0},
		{"bounded prefix", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.ReadStream(ctx, sid, 0, tc.to)
			if !errors.Is(err, es.ErrStreamNotFound) {
				t.Fatalf("ReadStream(0, %d) on a never-written stream: got %v, want ErrStreamNotFound", tc.to, err)
			}
			if !strings.Contains(err.Error(), sid.Canonical()) {
				t.Fatalf("error %q does not name the stream %s", err, sid.Canonical())
			}
		})
	}
}

// TestReadStreamEmptyRangeStillNil pins the other half of the contract: an
// empty range is not "stream not found". Reads with fromVersion > 0, and
// reads from 0 of a stream that exists, return (nil, nil).
func TestReadStreamEmptyRangeStillNil(t *testing.T) {
	ctx := context.Background()

	t.Run("existing stream, read from its head", func(t *testing.T) {
		s := openStore(t)
		sid, err := es.NewStreamID("thing", "existing")
		if err != nil {
			t.Fatal(err)
		}
		appendEvents(t, ctx, s, sid, 2)

		current, err := s.CurrentStreamVersion(ctx, sid)
		if err != nil {
			t.Fatalf("current version: %v", err)
		}
		envs, err := s.ReadStream(ctx, sid, current, 0)
		if err != nil || len(envs) != 0 {
			t.Fatalf("read from head v%d: envs=%d err=%v, want (nil, nil)", current, len(envs), err)
		}
	})

	t.Run("never-written stream, read from above 0", func(t *testing.T) {
		s := openStore(t)
		sid, err := es.NewStreamID("thing", "also-never-written")
		if err != nil {
			t.Fatal(err)
		}
		envs, err := s.ReadStream(ctx, sid, 3, 0)
		if err != nil || len(envs) != 0 {
			t.Fatalf("read from v3 of a never-written stream: envs=%d err=%v, want (nil, nil)", len(envs), err)
		}
	})

	// Head truncation staged out-of-band (the F2 threat model): the stream
	// exists — it still has rows above the bound — so a bounded read from 0
	// that matches nothing is an empty range, not a missing stream.
	t.Run("head-truncated stream, bounded read below the first survivor", func(t *testing.T) {
		s := openStore(t)
		sid, err := es.NewStreamID("thing", "truncated")
		if err != nil {
			t.Fatal(err)
		}
		appendEvents(t, ctx, s, sid, 3)
		if _, err := s.DB().ExecContext(ctx,
			`DELETE FROM events WHERE stream_id = ? AND version <= 2`, sid.Canonical(),
		); err != nil {
			t.Fatalf("stage truncation: %v", err)
		}

		envs, err := s.ReadStream(ctx, sid, 0, 2)
		if err != nil || len(envs) != 0 {
			t.Fatalf("bounded read of a head-truncated stream: envs=%d err=%v, want (nil, nil)", len(envs), err)
		}
		// The whole-stream read still sees the survivor: the stream exists.
		envs, err = s.ReadStream(ctx, sid, 0, 0)
		if err != nil || len(envs) != 1 || envs[0].Version != 3 {
			t.Fatalf("whole read after truncation: envs=%d err=%v, want the v3 survivor alone", len(envs), err)
		}
	})
}

// TestNewWithDBEnforcesSingleConnection pins the single-connection
// contract (F19): a handle handed to NewWithDB is held to the same
// SetMaxOpenConns(1) limit Open enforces, whatever limit the caller had
// configured — the single Go connection is what buys the package's
// busy_timeout/BUSY_SNAPSHOT immunity and memory-DB lifetime.
func TestNewWithDBEnforcesSingleConnection(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(4)

	if _, err := sqlite.NewWithDB(ctx, db); err != nil {
		t.Fatalf("NewWithDB: %v", err)
	}
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("MaxOpenConnections = %d after NewWithDB, want 1 (the package's single-connection contract)", got)
	}
}

// TestCloseDoesNotCloseBorrowedDB pins the other half of the contract
// (F19): a Store built with NewWithDB borrows the caller's handle, so
// Close leaves it open — the caller closes it, exactly as
// postgres.OpenWithPool has always behaved.
func TestCloseDoesNotCloseBorrowedDB(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s, err := sqlite.NewWithDB(ctx, db)
	if err != nil {
		t.Fatalf("NewWithDB: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The handle was borrowed, not owned: the caller's *sql.DB must
	// still be usable after the Store is closed.
	if err := db.Ping(); err != nil {
		t.Fatalf("db.Ping() after Store.Close(): %v (the borrowed handle was closed)", err)
	}
}
