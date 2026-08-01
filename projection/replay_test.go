package projection_test

import (
	"context"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
	"github.com/laenenai/es-lite/sqlite"
)

func TestReplayPagesLogInOrder(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	sid, _ := es.NewStreamID("counter", "main")
	const n = 25
	for i := range n {
		if _, err := store.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i),
			Events:          []es.EventData{{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte{byte(i)}}},
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// A small batch size forces multiple pages.
	var got []uint64
	last, err := projection.Replay(ctx, store, 0, 7, func(ctx context.Context, batch []es.Envelope) error {
		for _, e := range batch {
			got = append(got, e.GlobalPosition)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != n {
		t.Fatalf("replayed %d events, want %d", len(got), n)
	}
	for i, gp := range got {
		if gp != uint64(i+1) {
			t.Fatalf("out of order at %d: got %d", i, gp)
		}
	}
	if last != uint64(n) {
		t.Fatalf("last position = %d, want %d", last, n)
	}

	// Resuming from the last position yields nothing (exact cutover point).
	more := 0
	tail, err := projection.Replay(ctx, store, last, 7, func(ctx context.Context, batch []es.Envelope) error {
		more += len(batch)
		return nil
	})
	if err != nil {
		t.Fatalf("replay tail: %v", err)
	}
	if more != 0 || tail != last {
		t.Fatalf("resume from %d applied %d more (tail=%d), want 0", last, more, tail)
	}
}
