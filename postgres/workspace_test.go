package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// TestWorkspaceHead covers Workspace.Head: an empty workspace is at head 0,
// and after three events the head is the third event's global_position. Head
// is deliberately not on es.Store — it is the live-feed resume point, not a
// general store read.
func TestWorkspaceHead(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_head")
	sid := streamID(t)

	if head, err := ws.Head(ctx); err != nil || head != 0 {
		t.Fatalf("empty workspace Head = (%d, %v), want (0, nil)", head, err)
	}

	res, err := ws.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{
			{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("a")},
			{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("b")},
			{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("c")},
		},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	want := res.Envelopes[len(res.Envelopes)-1].GlobalPosition

	head, err := ws.Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head != want {
		t.Fatalf("Head = %d, want %d (the third event's global_position)", head, want)
	}
}

// TestHeadIsSettledWhileAppendInFlight pins Head's settle guarantee (spec
// §5.1): while an append is still in flight, Head never reports a position
// at or above it — a checkpoint or resume at the raw max would ack the
// cursor past the in-flight event and lose it when it commits. The shape is
// the out-of-order commit: tx1 draws the lower position and stays open while
// tx2 commits the higher one, so the raw max is above an in-flight position.
// RawHead sees the committed event immediately (the unfiltered view the
// fence-aware resume clamp reads, §5.8); the settled Head stays at or below
// the in-flight position until the
// fence clears, then converges to the full settled max.
func TestHeadIsSettledWhileAppendInFlight(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_head_settled")
	const (
		typeA = "feedtest.v1.E1" // tx1, lower position, held in flight
		typeB = "feedtest.v1.E2" // tx2, higher position, committed
	)

	// tx1 draws the lower position and stays open: an append in flight.
	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	gpA := feedAppendTx(t, ctx, ws, tx1, feedStream(t, "head-settled-a"), 0, typeA)

	// tx2 draws the higher position and commits: gpB is committed and
	// visible while the lower gpA is still in flight.
	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	gpB := feedAppendTx(t, ctx, ws, tx2, feedStream(t, "head-settled-b"), 0, typeB)
	if gpA >= gpB {
		t.Fatalf("insert order: tx1 gp=%d, tx2 gp=%d, want tx1 lower", gpA, gpB)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	// The raw head sees the committed gpB immediately — no fence, no
	// wait: committed backlog above an in-flight predecessor, exactly
	// what stall detection probes for.
	raw, err := ws.RawHead(ctx)
	if err != nil {
		t.Fatalf("RawHead: %v", err)
	}
	if raw != gpB {
		t.Fatalf("RawHead = %d, want %d (the committed event, immediately visible)", raw, gpB)
	}

	// The settled Head must not ack past the in-flight gpA: while tx1 is
	// open it reports at most gpA — never the raw max. Two polls cover
	// both round shapes: the fence-establishing poll and the stored-fence
	// poll.
	for i := 0; i < 2; i++ {
		head, err := ws.Head(ctx)
		if err != nil {
			t.Fatalf("Head poll %d: %v", i, err)
		}
		if head > gpA {
			t.Fatalf("Head poll %d = %d while gp=%d is in flight, want <= %d: a checkpoint at the raw max would ack past it",
				i, head, gpA, gpA)
		}
	}

	// Once tx1 settles, the fence clears and Head converges to the full
	// settled max.
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		head, err := ws.Head(ctx)
		if err != nil {
			t.Fatalf("Head after commit: %v", err)
		}
		if head == gpB {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Head = %d past the deadline, want %d (the settled max once the fence clears)", head, gpB)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestWorkspaceMatchingHeadIsFenceBoundedAndFiltered pins MatchingHead's
// two semantics (spec §5.8): the filtered maximum — only the requested
// types' positions count — and the fence bound — while a transaction
// blocks the fence, MatchingHead reports 0 exactly like Head, reusing the
// stored fence round instead of drawing a fresh one per probe. RawHead
// sees the committed maximum immediately, the contrast that makes stall
// detection meaningful.
func TestWorkspaceMatchingHeadIsFenceBoundedAndFiltered(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_matching_head")
	const (
		typeA = "feedtest.v1.E1" // tx1, lower position, held in flight
		typeB = "feedtest.v1.E2" // tx2, higher position, committed
	)

	// tx1 draws the lower position and stays open.
	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	gpA := feedAppendTx(t, ctx, ws, tx1, feedStream(t, "matching-head-a"), 0, typeA)

	// tx2 draws the higher position and commits.
	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	gpB := feedAppendTx(t, ctx, ws, tx2, feedStream(t, "matching-head-b"), 0, typeB)
	if gpA >= gpB {
		t.Fatalf("insert order: tx1 gp=%d, tx2 gp=%d, want tx1 lower", gpA, gpB)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	// The raw maximum is committed and visible; the matching heads are
	// not: the fence is blocked by tx1, so both filter shapes report 0.
	// The second call reuses the stored fence round — still 0, still no
	// error — instead of drawing a fresh fence per probe.
	if raw, err := ws.RawHead(ctx); err != nil {
		t.Fatalf("RawHead: %v", err)
	} else if raw != gpB {
		t.Fatalf("RawHead = %d, want %d (the committed maximum, unfenced)", raw, gpB)
	}
	for i := 0; i < 2; i++ {
		for _, types := range [][]string{{typeA}, {typeA, typeB}, nil} {
			head, err := ws.MatchingHead(ctx, types)
			if err != nil {
				t.Fatalf("MatchingHead poll %d (%v): %v", i, types, err)
			}
			if head != 0 {
				t.Fatalf("MatchingHead poll %d (%v) = %d while gp=%d is in flight, want 0 (the fence is blocked, like Head)",
					i, types, head, gpA)
			}
		}
	}

	// tx1 settles: the fence clears and MatchingHead reports each type
	// set's own maximum. An empty type set matches nothing.
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}
	cases := []struct {
		types []string
		want  uint64
	}{
		{[]string{typeA}, gpA},
		{[]string{typeB}, gpB},
		{[]string{typeA, typeB}, gpB},
		{[]string{"feedtest.v1.Absent"}, 0},
		{nil, 0},
	}
	for _, c := range cases {
		head, err := ws.MatchingHead(ctx, c.types)
		if err != nil {
			t.Fatalf("MatchingHead (%v): %v", c.types, err)
		}
		if head != c.want {
			t.Fatalf("MatchingHead (%v) = %d, want %d", c.types, head, c.want)
		}
	}
}

// appendEvents is the ReadAllBounded fixture: it appends one event per
// (typeURL, payload) pair to ws and fails the test on the first error.
func appendEvents(ctx context.Context, t *testing.T, ws *postgres.Workspace, sid es.StreamID, events []es.EventData) []es.Envelope {
	t.Helper()
	res, err := ws.Append(ctx, es.AppendParams{StreamID: sid, Events: events})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	return res.Envelopes
}

// tenBytes is a 10-byte payload: a round unit for byte-budget arithmetic.
func tenBytes() []byte { return []byte("0123456789") }

// TestWorkspaceReadAllBoundedFiltersByType: only rows whose type_url is in
// types come back, in global order, from a position; a page cut short by
// limit is not truncation (only the byte budget reports truncated); another
// workspace's rows are invisible (the query runs under the workspace RLS).
func TestWorkspaceReadAllBoundedFiltersByType(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_rab")
	sid := streamID(t)

	appendEvents(ctx, t, ws, sid, []es.EventData{
		{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: []byte("a")},
		{TypeURL: "counter.v1.Closed", SchemaVersion: 1, Payload: []byte("b")},
		{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("cc")},
		{TypeURL: "counter.v1.Closed", SchemaVersion: 1, Payload: []byte("d")},
	})
	// Another workspace's matching event must be invisible.
	other := s.WorkspaceStore("ws_rab_other")
	otherSid := streamID(t)
	appendEvents(ctx, t, other, otherSid, []es.EventData{
		{TypeURL: "counter.v1.Closed", SchemaVersion: 1, Payload: []byte("x")},
	})

	events, truncated, err := ws.ReadAllBounded(ctx, 0, 10, 0, []string{"counter.v1.Closed"})
	if err != nil {
		t.Fatalf("ReadAllBounded: %v", err)
	}
	if truncated {
		t.Fatalf("truncated = true with no byte budget in the way")
	}
	if len(events) != 2 || events[0].Payload[0] != 'b' || events[1].Payload[0] != 'd' {
		t.Fatalf("events = %v, want the two Closed events in global order", summarize(events))
	}

	// A position resumes strictly after it.
	afterFirst := events[0].GlobalPosition
	events, truncated, err = ws.ReadAllBounded(ctx, afterFirst, 10, 0, []string{"counter.v1.Closed"})
	if err != nil || truncated {
		t.Fatalf("resume: events=%v truncated=%t err=%v", summarize(events), truncated, err)
	}
	if len(events) != 1 || events[0].Payload[0] != 'd' {
		t.Fatalf("from %d: events = %v, want only the later Closed event", afterFirst, summarize(events))
	}

	// A page cut short by limit is not truncation.
	events, truncated, err = ws.ReadAllBounded(ctx, 0, 1, 0, []string{"counter.v1.Closed"})
	if err != nil || truncated {
		t.Fatalf("limit cut: events=%v truncated=%t err=%v", summarize(events), truncated, err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %v, want the limit of 1", summarize(events))
	}
}

// TestWorkspaceReadAllBoundedEmptyTypesReturnsNoEvents: an empty type set
// matches nothing — no events, no error, no truncation.
func TestWorkspaceReadAllBoundedEmptyTypesReturnsNoEvents(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_rab_empty")
	appendEvents(ctx, t, ws, streamID(t), []es.EventData{
		{TypeURL: "counter.v1.Closed", SchemaVersion: 1, Payload: []byte("b")},
	})

	for _, types := range [][]string{nil, {}} {
		events, truncated, err := ws.ReadAllBounded(ctx, 0, 10, 1000, types)
		if err != nil || truncated || len(events) != 0 {
			t.Fatalf("types=%v: events=%v truncated=%t err=%v, want none", types, summarize(events), truncated, err)
		}
	}
}

// TestWorkspaceReadAllBoundedStopsAtTheByteBudget: the page stops before an
// event whose payload would push the running total past maxBytes and reports
// truncated=true; an exact fit is not "past" and does not truncate. A
// non-matching oversized event in between eats no budget: filtering happens
// in SQL, before the walk.
func TestWorkspaceReadAllBoundedStopsAtTheByteBudget(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_rab_budget")
	sid := streamID(t)

	appendEvents(ctx, t, ws, sid, []es.EventData{
		{TypeURL: "counter.v1.Closed", SchemaVersion: 1, Payload: tenBytes()},
		{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: tenBytes()}, // not requested
		{TypeURL: "counter.v1.Closed", SchemaVersion: 1, Payload: tenBytes()},
		{TypeURL: "counter.v1.Closed", SchemaVersion: 1, Payload: tenBytes()},
	})

	// 10+10 fit in 25; the third would make 30 > 25.
	events, truncated, err := ws.ReadAllBounded(ctx, 0, 10, 25, []string{"counter.v1.Closed"})
	if err != nil {
		t.Fatalf("ReadAllBounded: %v", err)
	}
	if len(events) != 2 || !truncated {
		t.Fatalf("maxBytes=25: %d events, truncated=%t, want 2 and true", len(events), truncated)
	}

	// 10+10+10 = 30 is not past 30: the whole page fits, nothing was stopped.
	events, truncated, err = ws.ReadAllBounded(ctx, 0, 10, 30, []string{"counter.v1.Closed"})
	if err != nil {
		t.Fatalf("ReadAllBounded: %v", err)
	}
	if len(events) != 3 || truncated {
		t.Fatalf("maxBytes=30: %d events, truncated=%t, want 3 and false", len(events), truncated)
	}
}

// TestWorkspaceReadAllBoundedReturnsTheFirstOversizedEvent: the first event
// of a page is always included, even alone over budget — an oversized event
// must be visible, not forever skipped.
func TestWorkspaceReadAllBoundedReturnsTheFirstOversizedEvent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_rab_oversize")
	sid := streamID(t)

	appendEvents(ctx, t, ws, sid, []es.EventData{
		{TypeURL: "counter.v1.Closed", SchemaVersion: 1, Payload: tenBytes()},
		{TypeURL: "counter.v1.Closed", SchemaVersion: 1, Payload: []byte("e")},
	})

	events, truncated, err := ws.ReadAllBounded(ctx, 0, 10, 3, []string{"counter.v1.Closed"})
	if err != nil {
		t.Fatalf("ReadAllBounded: %v", err)
	}
	if len(events) != 1 || !truncated {
		t.Fatalf("maxBytes=3: %d events, truncated=%t, want the oversized first event and true", len(events), truncated)
	}
	if len(events[0].Payload) != 10 {
		t.Fatalf("first event payload = %d bytes, want the 10-byte oversized event", len(events[0].Payload))
	}
}

// summarize renders an envelope slice for failure messages.
func summarize(events []es.Envelope) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = fmt.Sprintf("%s@%d:%q", e.TypeURL, e.GlobalPosition, e.Payload)
	}
	return out
}
