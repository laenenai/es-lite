package postgres_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// Ordering tests for the feed page under concurrent append transactions
// (spec §5.1, §5.10.1). Postgres assigns global_position at INSERT, not at
// COMMIT, so without a fence a page can deliver a committed position past a
// lower position that is still in flight; the cursor then acks past the gap
// and the event is lost when the in-flight transaction commits.
//
// Each test drives AppendTx through caller-owned pool transactions so the
// INSERT order and the COMMIT order are the test's to choose — the exact
// interleavings the spikes recorded (task-1-report.md §1c, task-1b-report.md
// §1b). Positions are read from the append results, not assumed: the suite
// shares one sequence across tests.

// feedStream returns a "thing"-typed stream id for the feed tests.
func feedStream(t *testing.T, name string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("thing", name)
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	return sid
}

// feedAppendTx appends one event inside the caller-owned transaction and
// returns the global_position the insert drew.
func feedAppendTx(t *testing.T, ctx context.Context, ws *postgres.Workspace, tx pgx.Tx, sid es.StreamID, version uint64, typeURL string) uint64 {
	t.Helper()
	res, err := ws.AppendTx(ctx, narrowTx{tx}, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events: []es.EventData{{
			TypeURL:       typeURL,
			SchemaVersion: 1,
			Payload:       []byte(typeURL),
		}},
	})
	if err != nil {
		t.Fatalf("append %s at v%d: %v", typeURL, version+1, err)
	}
	return res.Envelopes[0].GlobalPosition
}

// positions collects a page's global positions for order assertions.
func positions(envs []es.Envelope) []uint64 {
	out := make([]uint64, len(envs))
	for i, e := range envs {
		out[i] = e.GlobalPosition
	}
	return out
}

// pollFeedPage polls the bounded read until it delivers a non-empty page.
func pollFeedPage(t *testing.T, ctx context.Context, ws *postgres.Workspace, from uint64, types []string) []es.Envelope {
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

// TestReadAllBoundedWaitsForOutOfOrderCommit pins same-order insert with
// out-of-order commit: tx1 draws the lower position, tx2 the higher, and tx2
// commits first. The page must stay empty while tx1 is in flight —
// delivering tx2's event alone would ack the cursor past tx1's position and
// lose it. Once tx1 commits, one page delivers both, in order.
func TestReadAllBoundedWaitsForOutOfOrderCommit(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_feed_ooc")
	const (
		typeA = "feedtest.v1.E1" // tx1, lower position
		typeB = "feedtest.v1.E2" // tx2, higher position
	)
	types := []string{typeA, typeB}

	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	gpA := feedAppendTx(t, ctx, ws, tx1, feedStream(t, "ooc-a"), 0, typeA)

	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	gpB := feedAppendTx(t, ctx, ws, tx2, feedStream(t, "ooc-b"), 0, typeB)
	if gpA >= gpB {
		t.Fatalf("insert order: tx1 gp=%d, tx2 gp=%d, want tx1 lower", gpA, gpB)
	}

	// Commit the HIGHER position first.
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	// gpB is committed and visible while the lower gpA is still in flight:
	// the page must be empty, never [gpB] alone.
	page, truncated, err := ws.ReadAllBounded(ctx, 0, 100, 0, types)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if len(page) != 0 || truncated {
		t.Fatalf("page while gp=%d in flight = %v truncated=%v, want empty: delivering gp=%d alone would ack the cursor past it",
			gpA, positions(page), truncated, gpB)
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	page = pollFeedPage(t, ctx, ws, 0, types)
	if got, want := positions(page), []uint64{gpA, gpB}; !slices.Equal(got, want) {
		t.Fatalf("page = %v, want %v (lower position first)", got, want)
	}
}

// TestReadAllBoundedWaitsForStraddledCommit pins the xid/position straddle:
// tx1 appends, tx2 appends, tx1 appends again inside the same transaction,
// so tx1 owns the positions straddling tx2's. Committing tx1 alone exposes
// both of tx1's events with a live gap between them — the page must stay
// empty, never [gpA, gpC], or the cursor acks past tx2's position. Once tx2
// commits, one page delivers all three in order.
func TestReadAllBoundedWaitsForStraddledCommit(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_feed_straddle")
	const (
		typeA = "feedtest.v1.E1" // tx1, first append
		typeB = "feedtest.v1.E2" // tx2
		typeC = "feedtest.v1.E3" // tx1, second append, same transaction
	)
	types := []string{typeA, typeB, typeC}

	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	sidA := feedStream(t, "straddle-a")
	gpA := feedAppendTx(t, ctx, ws, tx1, sidA, 0, typeA)

	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	gpB := feedAppendTx(t, ctx, ws, tx2, feedStream(t, "straddle-b"), 0, typeB)

	// tx1 appends again inside its still-open transaction.
	gpC := feedAppendTx(t, ctx, ws, tx1, sidA, 1, typeC)
	if !(gpA < gpB && gpB < gpC) {
		t.Fatalf("insert order: gpA=%d gpB=%d gpC=%d, want strictly increasing", gpA, gpB, gpC)
	}

	// Commit tx1 alone: gpA and gpC are visible around a live gap at gpB.
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	page, _, err := ws.ReadAllBounded(ctx, 0, 100, 0, types)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("page over the straddle (gpA=%d gpB=%d gpC=%d, tx2 in flight) = %v, want empty: never [%d, %d] with the gap live",
			gpA, gpB, gpC, positions(page), gpA, gpC)
	}

	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	page = pollFeedPage(t, ctx, ws, 0, types)
	if got, want := positions(page), []uint64{gpA, gpB, gpC}; !slices.Equal(got, want) {
		t.Fatalf("page = %v, want %v (the gap filled, in order)", got, want)
	}
}

// TestReadAllBoundedPassesAbortedHole pins that an aborted append leaves a
// permanent hole the cursor passes without stalling: the page reads empty
// while the would-be hole is in flight (the reader cannot know the
// transaction will abort, so it must not deliver past it), and once the
// abort settles, delivery resumes across the hole — [gpA, gpC], then
// nothing more from gpC.
func TestReadAllBoundedPassesAbortedHole(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_feed_hole")
	const (
		typeA = "feedtest.v1.E1" // tx1, committed
		typeB = "feedtest.v1.E2" // tx2, rolled back — a permanent hole
		typeC = "feedtest.v1.E3" // tx3, committed above the hole
	)
	types := []string{typeA, typeB, typeC}

	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	gpA := feedAppendTx(t, ctx, ws, tx1, feedStream(t, "hole-a"), 0, typeA)
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	// tx2 stays open: its position is in flight between two commits.
	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	gpB := feedAppendTx(t, ctx, ws, tx2, feedStream(t, "hole-b"), 0, typeB)

	tx3, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx3: %v", err)
	}
	defer tx3.Rollback(ctx)
	gpC := feedAppendTx(t, ctx, ws, tx3, feedStream(t, "hole-c"), 0, typeC)
	if err := tx3.Commit(ctx); err != nil {
		t.Fatalf("commit tx3: %v", err)
	}
	if !(gpA < gpB && gpB < gpC) {
		t.Fatalf("insert order: gpA=%d gpB=%d gpC=%d, want strictly increasing", gpA, gpB, gpC)
	}

	// gpA and gpC are committed while gpB is in flight between them: the
	// page must be empty — delivering [gpA, gpC] would ack the cursor past
	// gpB.
	page, _, err := ws.ReadAllBounded(ctx, 0, 100, 0, types)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("page while gp=%d in flight = %v, want empty", gpB, positions(page))
	}

	// tx2 aborts: gpB becomes a permanent hole. Delivery must pass it —
	// the fence clears on the abort, and the hole must not stall the feed.
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("rollback tx2: %v", err)
	}

	page = pollFeedPage(t, ctx, ws, 0, types)
	if got, want := positions(page), []uint64{gpA, gpC}; !slices.Equal(got, want) {
		t.Fatalf("page across the aborted hole = %v, want %v (the hole must not stall delivery)", got, want)
	}

	// Reading from the delivered head is idle: empty, no fence, no stall.
	page, _, err = ws.ReadAllBounded(ctx, gpC, 100, 0, types)
	if err != nil {
		t.Fatalf("read page from %d: %v", gpC, err)
	}
	if len(page) != 0 {
		t.Fatalf("page from the delivered head = %v, want empty", positions(page))
	}
}

// insertPoisonRow inserts one events row with raw SQL, bypassing the
// append-time cap — after Task 5 that is the only way a stored payload can
// exceed the store's cap (a lowered cap, direct SQL or corruption). It
// returns the global_position the insert drew. The test pool connects as
// the table-owning superuser, so the direct insert crosses the RLS the
// workspace-scoped reads still enforce.
func insertPoisonRow(t testing.TB, ctx context.Context, s *postgres.Store, ws string, sid es.StreamID, version uint64, typeURL string, payload []byte) uint64 {
	t.Helper()
	var gp uint64
	err := s.Pool().QueryRow(ctx,
		`INSERT INTO events (
			workspace_id, event_id, stream_type, stream_id, version,
			type_url, schema_version, occurred_at, recorded_at,
			correlation_id, causation_id, command_id,
			actor_type, actor_id, actor_principal, payload
		) VALUES ($1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11,$12, $13,$14,$15,$16)
		RETURNING global_position`,
		ws, "11111111-1111-7111-8111-111111111111", sid.Type, sid.Canonical(), version,
		typeURL, 1, time.Now(), time.Now(),
		"", "", "",
		sid.Type, "poison", "conformance:poison", payload,
	).Scan(&gp)
	if err != nil {
		t.Fatalf("direct insert: %v", err)
	}
	return gp
}
