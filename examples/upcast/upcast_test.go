package upcast_test

import (
	"context"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/sqlite"
	"github.com/laenenai/es-lite/upcast"
)

// TestSchemaEvolution walks through evolving the Incremented event from
// schema version 1 to 2, where v1's `by` is reinterpreted (here: v1 values
// were in half-units, so an upcaster doubles them to the v2 whole-unit
// meaning). It shows the full path: an old event sits in the log unchanged,
// and an upcaster upgrades it on read.
func TestSchemaEvolution(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, "file:upcast-example?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	sid, _ := es.NewStreamID("counter", "acct-42")

	// 1. Simulate history already on disk: an Initialized, then an
	//    Incremented written under the OLD schema version 1 with by=3 (in
	//    the old half-unit meaning). We write them directly with
	//    SchemaVersion:1 to stand in for "already persisted long ago".
	encInit, err := counterv1.EventCodec{}.Encode(&counterv1.Initialized{Min: 0, Max: 100, Value: 0})
	if err != nil {
		t.Fatalf("encode init: %v", err)
	}
	encInc, err := counterv1.EventCodec{}.Encode(&counterv1.Incremented{By: 3})
	if err != nil {
		t.Fatalf("encode inc: %v", err)
	}
	if _, err := store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventData{
			{TypeURL: encInit.TypeURL, SchemaVersion: 1, Payload: encInit.Payload},
			{TypeURL: encInc.TypeURL, SchemaVersion: 1, Payload: encInc.Payload},
		},
	}); err != nil {
		t.Fatalf("append legacy events: %v", err)
	}

	// 2. The upcaster: for any Incremented stored below version 2, upgrade
	//    it to the current meaning by doubling `by`. Events at version 2+
	//    pass through untouched.
	upcaster := upcast.NewRegistry[counterv1.Event]().Register(
		"counter.v1.Incremented",
		func(fromVersion uint32, e counterv1.Event) (counterv1.Event, error) {
			if inc, ok := e.(*counterv1.Incremented); ok && fromVersion < 2 {
				return &counterv1.Incremented{By: inc.GetBy() * 2}, nil
			}
			return e, nil
		},
	)

	// 3. Wire the runtime WITH the upcaster; the Decider is unchanged and
	//    only ever sees current-schema events.
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithUpcaster(upcaster)

	// 4. Loading folds the (upgraded) history: by=3 -> upcast to 6.
	state, _, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetCount() != 6 {
		t.Fatalf("count = %d, want 6 (legacy by=3 upcast to 6)", state.GetCount())
	}

	// 5. New commands write at the CURRENT schema version (2) — Encode
	//    stamps it — so the upcaster leaves them alone. Adding 4 -> 10.
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 4}), es.Meta{}); err != nil {
		t.Fatalf("handle increment: %v", err)
	}
	state, _, err = rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if state.GetCount() != 10 {
		t.Fatalf("count = %d, want 10 (6 + fresh 4)", state.GetCount())
	}
}
