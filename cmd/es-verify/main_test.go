package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// captureOutput runs fn with os.Stdout and os.Stderr redirected into
// pipes and returns what each collected. The copiers drain the pipes
// concurrently, so fn can write freely; the originals are restored even
// on failure.
func captureOutput(t *testing.T, fn func()) (stdout, stderr string) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdout: %v", err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stderr: %v", err)
	}
	os.Stdout, os.Stderr = wOut, wErr
	defer func() {
		os.Stdout, os.Stderr = oldOut, oldErr
		rOut.Close()
		rErr.Close()
	}()
	var outBuf, errBuf syncBuffer
	done := make(chan struct{}, 2)
	go func() { io.Copy(&outBuf, rOut); done <- struct{}{} }()
	go func() { io.Copy(&errBuf, rErr); done <- struct{}{} }()
	fn()
	wOut.Close()
	wErr.Close()
	<-done
	<-done
	return outBuf.String(), errBuf.String()
}

// syncBuffer is a bytes.Buffer safe for the concurrent copiers above.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestVerifyOrderOnSQLite pins the CLI's real sqlite order path (Task 8
// replaced the transitional stub the earlier test pinned): `order`
// deep-verifies the seeded batches and exits 0; `all` verifies the
// streams first and then the log order, exiting 0; and a repositioned
// event — caught by neither chain nor heads — reports BROKEN at the
// tampered batch's from_position and exits 1.
func TestVerifyOrderOnSQLite(t *testing.T) {
	t.Run("order on a seeded db exits 0", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		seedStream(t, s, sid, 2)
		closeDB(t, s)
		var code int
		stdout, _ := captureOutput(t, func() { code = run([]string{"order"}) })
		if code != exitIntact {
			t.Errorf("order on sqlite: exit %d, want %d", code, exitIntact)
		}
		if !strings.Contains(stdout, "log order: 1 batches verified") {
			t.Errorf("order on sqlite: stdout %q, want the one seeded batch verified", stdout)
		}
	})

	t.Run("all verifies streams then order and exits 0", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		seedStream(t, s, sid, 2)
		closeDB(t, s)
		var code int
		stdout, _ := captureOutput(t, func() { code = run([]string{"all"}) })
		if code != exitIntact {
			t.Errorf("all on sqlite: exit %d, want %d", code, exitIntact)
		}
		if !strings.Contains(stdout, "ok      thing:a head=v2") {
			t.Errorf("all on sqlite: stdout %q, want the stream verified ok before the order stage", stdout)
		}
		if !strings.Contains(stdout, "es-verify: 1 streams checked") {
			t.Errorf("all on sqlite: stdout %q, want the streams-checked summary", stdout)
		}
		if !strings.Contains(stdout, "log order: 1 batches verified") {
			t.Errorf("all on sqlite: stdout %q, want the order stage's ok after the streams", stdout)
		}
	})

	t.Run("order reports a repositioned event and exits 1", func(t *testing.T) {
		s := newTestDB(t)
		sid, err := es.NewStreamID("thing", "a")
		if err != nil {
			t.Fatalf("stream id: %v", err)
		}
		// Two single-event batches: their log_order rows attest one
		// position each, so swapping the events' positions is the F11
		// tamper — invisible to the chain and the heads.
		var positions []uint64
		for i := 0; i < 2; i++ {
			res, err := s.Append(context.Background(), es.AppendParams{
				StreamID: sid, ExpectedVersion: uint64(i),
				Events: []es.EventData{seedEvent(i)},
			})
			if err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
			positions = append(positions, res.Envelopes[0].GlobalPosition)
		}
		swapPositions(t, s, sid, positions[0], positions[1])
		closeDB(t, s)
		var code int
		stdout, _ := captureOutput(t, func() { code = run([]string{"order"}) })
		if code != exitBroken {
			t.Errorf("order on a repositioned log: exit %d, want %d", code, exitBroken)
		}
		if !strings.Contains(stdout, fmt.Sprintf("BROKEN  log order at batch from_position %d", positions[0])) {
			t.Errorf("order on a repositioned log: stdout %q, want BROKEN at from_position %d", stdout, positions[0])
		}
	})
}

// seedEvent is one typed event for the order tests above.
func seedEvent(i int) es.EventData {
	return es.EventData{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte{byte(i + 1)}}
}

// swapPositions exchanges the two events' global positions behind the
// store's back. SQLite enforces INTEGER PRIMARY KEY (rowid) uniqueness
// per row inside one UPDATE, so the swap parks both rows on the
// negative mirror of their positions first (positions are always
// positive) and then moves each into the other's old spot.
func swapPositions(t *testing.T, s *sqlite.Store, sid es.StreamID, p1, p2 uint64) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE events SET global_position = -global_position WHERE stream_id = ? AND global_position IN (?, ?)`,
		sid.Canonical(), p1, p2); err != nil {
		t.Fatalf("park positions %d/%d: %v", p1, p2, err)
	}
	res, err := s.DB().ExecContext(ctx,
		`UPDATE events SET global_position = CASE global_position WHEN ? THEN ? WHEN ? THEN ? END
		  WHERE stream_id = ? AND global_position IN (?, ?)`,
		-int64(p1), int64(p2), -int64(p2), int64(p1), sid.Canonical(), -int64(p1), -int64(p2))
	if err != nil {
		t.Fatalf("swap positions %d/%d: %v", p1, p2, err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 2 {
		t.Fatalf("swap positions %d/%d: rows %d (err %v), want 2", p1, p2, n, err)
	}
}
