package postgres_test

import (
	"context"
	"testing"

	"github.com/laenenai/es-lite/es"
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
