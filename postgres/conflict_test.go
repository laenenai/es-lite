package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// asConflict asserts the error is a conflict in both forms: the sentinel
// every existing consumer matches, and the structured error that carries
// the versions. It returns the structured error for field assertions.
func asConflict(t *testing.T, err error) *es.ConflictError {
	t.Helper()
	if err == nil {
		t.Fatal("append succeeded, want a conflict")
	}
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("errors.Is(err, ErrConflict) = false for %v — this is the compatibility contract", err)
	}
	var conflict *es.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("errors.As(err, **es.ConflictError) = false for %v — the versions are still only in the message", err)
	}
	return conflict
}

// TestAppendConflictCarriesVersions covers the pre-insert version check on
// both entry points: a caller that loses on a stale ExpectedVersion gets
// the stream's current version as a field, so it never has to parse the
// message or spend a round trip re-reading the head.
func TestAppendConflictCarriesVersions(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_conflict_fields")
	sid := streamID(t)

	// Three events, so the head moves to v3: a Current of 3 can only come
	// from the head read, which distinguishes this path from the backstop.
	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{eventData("a")[0], eventData("b")[0], eventData("c")[0]},
	}); err != nil {
		t.Fatalf("seed append: %v", err)
	}

	t.Run("Append", func(t *testing.T) {
		_, err := ws.Append(ctx, es.AppendParams{
			StreamID: sid, ExpectedVersion: 1, Events: eventData("stale"),
		})
		conflict := asConflict(t, err)
		if conflict.Expected != 1 || conflict.Current != 3 {
			t.Fatalf("conflict = {expected %d, current %d}, want {1, 3}", conflict.Expected, conflict.Current)
		}
		if conflict.StreamID != sid {
			t.Fatalf("conflict.StreamID = %s, want %s", conflict.StreamID, sid)
		}
	})

	// AppendTx shares the same append body; pin it so a future split of
	// the two entry points cannot leave the caller-owned path formatted.
	t.Run("AppendTx", func(t *testing.T) {
		tx, err := s.Pool().Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback(ctx)
		_, err = ws.AppendTx(ctx, narrowTx{tx}, es.AppendParams{
			StreamID: sid, ExpectedVersion: 0, Events: eventData("stale"),
		})
		conflict := asConflict(t, err)
		if conflict.Expected != 0 || conflict.Current != 3 {
			t.Fatalf("conflict = {expected %d, current %d}, want {0, 3}", conflict.Expected, conflict.Current)
		}
	})
}

// TestAppendPkeyBackstopCarriesVersions covers the OTHER conflict path: the
// append-time write race, where a writer commits between this append's head
// read and its insert. The pre-insert check cannot catch it — the loser's
// INSERT violates the (workspace_id, stream_id, version) primary key — and
// that backstop used to report no version at all.
//
// The interleave is forced, not raced. The winner's batch is written but
// uncommitted, so the loser's head read still sees an empty stream and
// passes the version check; the loser's INSERT then blocks on the winner's
// unindexed-yet row until the winner commits, and only then fails. The test
// waits for the loser to actually be blocked on a lock before committing,
// so the pre-insert check can never be the path under test: the winner's
// 3-event batch would make Current 3, where the backstop reports 1.
func TestAppendPkeyBackstopCarriesVersions(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_conflict_backstop")
	sid := streamID(t)

	winner, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin winner: %v", err)
	}
	defer winner.Rollback(ctx)
	loser, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin loser: %v", err)
	}
	defer loser.Rollback(ctx)

	// The loser's backend pid, so the test can watch it block. Taken
	// before its append so the transaction is already on this connection.
	var loserPID int
	if err := loser.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&loserPID); err != nil {
		t.Fatalf("loser pid: %v", err)
	}

	// The winner writes v1..v3 and does NOT commit.
	if _, err := ws.AppendTx(ctx, narrowTx{winner}, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{eventData("w1")[0], eventData("w2")[0], eventData("w3")[0]},
	}); err != nil {
		t.Fatalf("winner append: %v", err)
	}

	// The loser reads the head (sees 0 — the winner is uncommitted), passes
	// the version check, and blocks inserting v1.
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		_, err := ws.AppendTx(ctx, narrowTx{loser}, es.AppendParams{
			StreamID: sid, ExpectedVersion: 0, Events: eventData("l1"),
		})
		done <- result{err}
	}()

	waitBlocked(t, ctx, s, loserPID)
	if err := winner.Commit(ctx); err != nil {
		t.Fatalf("winner commit: %v", err)
	}

	var got result
	select {
	case got = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("loser's append never returned after the winner committed")
	}

	conflict := asConflict(t, got.err)
	if conflict.Expected != 0 {
		t.Fatalf("conflict.Expected = %d, want 0 — what this append asserted", conflict.Expected)
	}
	// 1, not 3: the backstop reports the version the pkey says is taken —
	// the version that cost this append its race. A Current of 3 would mean
	// the pre-insert check ran instead and this test proved nothing.
	if conflict.Current != 1 {
		t.Fatalf("conflict.Current = %d, want 1 (the contested version); 3 means the pre-insert check fired, not the backstop", conflict.Current)
	}
	if conflict.StreamID != sid {
		t.Fatalf("conflict.StreamID = %s, want %s", conflict.StreamID, sid)
	}

	// The winner's batch is the stream; the loser wrote nothing.
	if v, err := ws.CurrentStreamVersion(ctx, sid); err != nil || v != 3 {
		t.Fatalf("head after the race = %d (err %v), want 3", v, err)
	}
}

// waitBlocked blocks until pid is waiting on a lock it has not been
// granted — the loser sitting on the winner's uncommitted index entry.
func waitBlocked(t *testing.T, ctx context.Context, s *postgres.Store, pid int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var blocked bool
		if err := s.Pool().QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)`, pid).Scan(&blocked); err != nil {
			t.Fatalf("poll pg_locks: %v", err)
		}
		if blocked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the losing append never blocked on a lock; the forced interleave did not hold")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestConcurrentAppendersConflictsAreStructured is the race path at scale:
// many appenders contend for the same version, exactly one wins, and EVERY
// loser — whichever of the two paths rejected it — reports both the
// sentinel and the versions. Before this, a loser on the backstop carried
// no version at all.
func TestConcurrentAppendersConflictsAreStructured(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_conflict_race")
	sid := streamID(t)

	const appenders = 16
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		wins int
		errs []error
	)
	start := make(chan struct{})
	for i := 0; i < appenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := ws.Append(ctx, es.AppendParams{
				StreamID: sid, ExpectedVersion: 0, Events: eventData("race"),
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				wins++
				return
			}
			errs = append(errs, err)
		}()
	}
	close(start)
	wg.Wait()

	if wins != 1 {
		t.Fatalf("winners = %d, want exactly 1", wins)
	}
	if len(errs) != appenders-1 {
		t.Fatalf("losers = %d, want %d", len(errs), appenders-1)
	}
	for _, err := range errs {
		conflict := asConflict(t, err)
		if conflict.Expected != 0 {
			t.Fatalf("loser Expected = %d, want 0", conflict.Expected)
		}
		// Either path: the head read reports the committed head, the
		// backstop the contested version. Both are >= the version the
		// winner took, and neither may be the zero value a caller cannot
		// act on.
		if conflict.Current < 1 {
			t.Fatalf("loser Current = %d, want >= 1 — a caller cannot rebase on 0", conflict.Current)
		}
		if conflict.StreamID != sid {
			t.Fatalf("loser StreamID = %s, want %s", conflict.StreamID, sid)
		}
	}

	if v, err := ws.CurrentStreamVersion(ctx, sid); err != nil || v != 1 {
		t.Fatalf("head after the race = %d (err %v), want 1 — only the winner wrote", v, err)
	}
}
