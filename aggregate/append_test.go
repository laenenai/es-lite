package aggregate_test

import (
	"context"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
)

func TestWithAppendOwnsTheAppend(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, err := es.NewStreamID("counter", "append-seam")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})

	calls := 0
	appendFn := func(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
		calls++
		if p.StreamID != sid {
			t.Fatalf("append stream = %v, want %v", p.StreamID, sid)
		}
		return store.Append(ctx, p)
	}

	res, err := rt.Handle(ctx, sid,
		counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}),
		es.Meta{}, aggregate.WithAppend(appendFn))
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("custom append called %d times, want 1", calls)
	}
	if got := res.State.GetCount(); got != 5 {
		t.Fatalf("count = %d, want 5", got)
	}
}

func TestWithAppendIgnoresNil(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, err := es.NewStreamID("counter", "append-nil")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})

	if _, err := rt.Handle(ctx, sid,
		counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 1}),
		es.Meta{}, aggregate.WithAppend(nil)); err != nil {
		t.Fatalf("Handle() with a nil appender error = %v", err)
	}
}
