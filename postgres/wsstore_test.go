package postgres_test

import (
	"context"
	"fmt"
	"testing"

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
