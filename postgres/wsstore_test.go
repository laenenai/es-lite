package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
)

// The fence-round tests in this file are internal (package postgres) on
// purpose: they assert the unexported draws counter (F9) — the
// instrumentation that proves idle polls burn no fence xid — and the
// typed errSequenceCache (F27) by its exported alias, neither of which
// an external postgres_test file can reach. The rest of the suite stays
// external; only what must see inside lives here.

// openStore is the internal-suite counterpart of the external
// postgres_test.openStore: same PG_DSN gate, same per-test isolation (a
// truncate of the shared tables — tests run sequentially), and the same
// idempotent es_relay ensure, because TestSequenceCacheAsserted's
// Anchor half reaches the role-gated bypass path and the suite must be
// green against a bare container whatever the run order.
func openStore(t testing.TB) *Store {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.Pool().Exec(ctx, `DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'es_relay') THEN
				CREATE ROLE es_relay NOLOGIN NOSUPERUSER NOBYPASSRLS;
			END IF;
		END $$`); err != nil {
		t.Fatalf("ensure es_relay: %v", err)
	}
	if _, err := s.Pool().Exec(ctx, `TRUNCATE events, log_order, unique_claims, anchors, feed_checkpoints, feed_leases, feed_dead_letters`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// wsStream returns a stream id for the internal fence tests.
func wsStream(t *testing.T, name string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("fence", name)
	if err != nil {
		t.Fatal(err)
	}
	return sid
}

// drawsOf reads w.draws under the fence mutex: the counter is guarded
// by it (drawFence's single caller, fenceBound, holds it), so the test
// reads must agree — a peek without the lock could observe a torn value
// and would be exactly the access pattern the race detector rejects.
func drawsOf(w *Workspace) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.draws
}

// appendOne appends a single event to ws and returns its
// global_position.
func appendOne(ctx context.Context, t *testing.T, ws *Workspace, sid es.StreamID, expected uint64, typeURL string) uint64 {
	t.Helper()
	res, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: expected,
		Events: []es.EventData{{
			TypeURL:       typeURL,
			SchemaVersion: 1,
			Payload:       []byte(typeURL),
		}},
	})
	if err != nil {
		t.Fatalf("append %s: %v", typeURL, err)
	}
	return res.Envelopes[0].GlobalPosition
}

// pollPage polls the bounded read until it delivers a non-empty page and
// returns it.
func pollPage(ctx context.Context, t *testing.T, ws *Workspace, from uint64, types []string) []es.Envelope {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		page, _, err := ws.ReadAllBounded(ctx, from, 100, 0, types)
		if err != nil {
			t.Fatalf("read page from %d: %v", from, err)
		}
		if len(page) > 0 {
			return page
		}
		if time.Now().After(deadline) {
			t.Fatalf("feed page from %d stayed empty past the deadline", from)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestIdleWorkspaceBurnsNoFence pins the F9 idle short-circuit: a
// workspace whose committed max sits at the caller's cursor is idle, and
// polling it must not draw a fence xid. Another workspace appending does
// not make THIS workspace non-idle — the global sequence moves, but no
// event visible to this workspace's caller does — so without the
// short-circuit every poll of every idle workspace burns a fence
// transaction per poll cycle. The fresh append at the end is the
// positive control: once something new is visible, the next poll must
// draw again.
func TestIdleWorkspaceBurnsNoFence(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const typeURL = "fencetest.v1.E"
	types := []string{typeURL}
	wsA := s.WorkspaceStore("ws_fence_idle_a")
	wsB := s.WorkspaceStore("ws_fence_idle_b")
	sidA := wsStream(t, "idle-a")
	sidB := wsStream(t, "idle-b")

	n := appendOne(ctx, t, wsA, sidA, 0, typeURL)
	if n == 0 {
		t.Fatal("workspace A's event drew position 0")
	}
	// B appends after A is idle: the global sequence last_value moves
	// above n, but A's committed max stays at n.
	appendOne(ctx, t, wsB, sidB, 0, typeURL)

	before := drawsOf(wsA)
	for i := 0; i < 5; i++ {
		envs, truncated, err := wsA.ReadAllBounded(ctx, n, 10, 0, types)
		if err != nil {
			t.Fatalf("idle poll %d: %v", i, err)
		}
		if len(envs) != 0 || truncated {
			t.Fatalf("idle poll %d: %d events, truncated=%t, want an empty page", i, len(envs), truncated)
		}
		if d := drawsOf(wsA); d != before {
			t.Fatalf("idle poll %d advanced draws %d → %d: an idle workspace must not burn a fence xid per poll (F9)", i, before, d)
		}
	}

	// The positive control: a fresh append to A makes it non-idle, so
	// the next poll must draw again — the short-circuit may only skip
	// when nothing new is visible.
	fresh := appendOne(ctx, t, wsA, sidA, 1, typeURL)
	if fresh <= n {
		t.Fatalf("fresh append drew position %d, want > %d", fresh, n)
	}
	grew := drawsOf(wsA)
	page := pollPage(ctx, t, wsA, n, types)
	if len(page) != 1 || page[0].GlobalPosition != fresh {
		t.Fatalf("page after the fresh append = %d events (first at %d), want the one event at %d", len(page), page[0].GlobalPosition, fresh)
	}
	if d := drawsOf(wsA); d <= grew {
		t.Fatalf("the poll after a fresh append drew nothing (draws %d → %d): the short-circuit must not skip visible work", grew, d)
	}
}

// TestHeadEmptyWorkspaceBurnsNoFence pins the other F9 half: Head on a
// workspace with zero events reports 0 without drawing a fence — the
// committed-max short-circuit fires from 0 — while a workspace with a
// committed event still draws for its settled head, so the
// short-circuit cannot degenerate into never fencing at all.
func TestHeadEmptyWorkspaceBurnsNoFence(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const typeURL = "fencetest.v1.Head"
	ws := s.WorkspaceStore("ws_fence_head_empty")
	sid := wsStream(t, "head-empty")

	before := drawsOf(ws)
	head, err := ws.Head(ctx)
	if err != nil {
		t.Fatalf("Head on an empty workspace: %v", err)
	}
	if head != 0 {
		t.Fatalf("Head on an empty workspace = %d, want 0", head)
	}
	if d := drawsOf(ws); d != before {
		t.Fatalf("Head on an empty workspace advanced draws %d → %d: zero events settle without a fence round (F9)", before, d)
	}

	// The positive control: once the workspace has an event, Head's
	// fence round draws again and reports the event's position.
	gp := appendOne(ctx, t, ws, sid, 0, typeURL)
	if gp == 0 {
		t.Fatal("the appended event drew position 0")
	}
	grew := drawsOf(ws)
	head, err = ws.Head(ctx)
	if err != nil {
		t.Fatalf("Head after an append: %v", err)
	}
	if head != gp {
		t.Fatalf("Head after an append = %d, want %d (the settled head)", head, gp)
	}
	if d := drawsOf(ws); d == grew {
		t.Fatal("Head after an append drew no fence: the workspace is no longer idle")
	}
}

// TestSequenceCacheAsserted pins the F27 assertion: with the position
// sequence's cache_size above 1, every fence round refuses to certify a
// bound — a batched nextval could hand a position at or below the
// just-read bound to a LATER transaction, voiding the settledness the
// fence exists to guarantee — so the feed page (ReadAllBounded) and the
// anchor path (Anchor via settledBound) both fail with
// ErrSequenceCacheMisconfigured, whose message names the fix. The
// shared sequence is restored to CACHE 1 on every path (t.Cleanup runs
// after failures too), so the suite stays re-runnable against the
// shared database.
func TestSequenceCacheAsserted(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	if _, err := s.Pool().Exec(ctx, `ALTER SEQUENCE events_global_position_seq CACHE 2`); err != nil {
		t.Fatalf("alter sequence to CACHE 2: %v", err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool().Exec(context.Background(), `ALTER SEQUENCE events_global_position_seq CACHE 1`); err != nil {
			t.Errorf("restore CACHE 1: %v", err)
		}
	})

	const typeURL = "fencetest.v1.Cache"
	ws := s.WorkspaceStore("ws_fence_cache")
	sid := wsStream(t, "cache")
	// The workspace must not be idle at 0, or the feed round would
	// short-circuit before it ever reads the sequence state.
	gp := appendOne(ctx, t, ws, sid, 0, typeURL)
	if gp == 0 {
		t.Fatal("the appended event drew position 0")
	}

	// The feed path: the round reads the sequence state and must refuse
	// to fence over a batched cache.
	_, _, err := ws.ReadAllBounded(ctx, 0, 10, 0, []string{typeURL})
	if !errors.Is(err, ErrSequenceCacheMisconfigured) {
		t.Fatalf("ReadAllBounded under CACHE 2: %v, want ErrSequenceCacheMisconfigured", err)
	}
	if !strings.Contains(err.Error(), "ALTER SEQUENCE events_global_position_seq CACHE 1") {
		t.Fatalf("error %q does not name the fix", err)
	}

	// The anchor path fails with the same typed error, never silently:
	// an anchor recorded over a batched cache could be disputed by its
	// own CheckAnchor once a cached position commits inside its bound.
	if _, err := s.Anchor(ctx, 0); !errors.Is(err, ErrSequenceCacheMisconfigured) {
		t.Fatalf("Anchor under CACHE 2: %v, want ErrSequenceCacheMisconfigured", err)
	}
}
