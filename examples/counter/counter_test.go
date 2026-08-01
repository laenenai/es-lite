package counter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/sqlite"
	"github.com/laenenai/es-lite/upcast"
)

func newRuntime(t *testing.T) (*aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event], *sqlite.Store) {
	t.Helper()
	ctx := context.Background()
	// Distinct in-memory DB per test via a unique name; shared cache so
	// the single connection keeps it alive.
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})
	return rt, store
}

func sid(t *testing.T) es.StreamID {
	t.Helper()
	s, err := es.NewStreamID("counter", "main")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	return s
}

func TestSchemaVersion(t *testing.T) {
	// Incremented declares (es.v1.schema_version) = 2; others default to 1.
	inc, err := counterv1.EventCodec{}.Encode(&counterv1.Incremented{By: 1})
	if err != nil {
		t.Fatalf("encode Incremented: %v", err)
	}
	if inc.SchemaVersion != 2 {
		t.Fatalf("Incremented schema version = %d, want 2", inc.SchemaVersion)
	}
	initEnc, err := counterv1.EventCodec{}.Encode(&counterv1.Initialized{})
	if err != nil {
		t.Fatalf("encode Initialized: %v", err)
	}
	if initEnc.SchemaVersion != 1 {
		t.Fatalf("Initialized schema version = %d, want 1", initEnc.SchemaVersion)
	}
}

func TestUpcaster(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	// An upcaster that, for a pre-v2 Incremented, doubles `by` (simulating a
	// units change that the schema_version bump signals).
	reg := upcast.NewRegistry[counterv1.Event]().Register("counter.v1.Incremented",
		func(from uint32, e counterv1.Event) (counterv1.Event, error) {
			if inc, ok := e.(*counterv1.Incremented); ok && from < 2 {
				return &counterv1.Incremented{By: inc.GetBy() * 2}, nil
			}
			return e, nil
		})
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).WithUpcaster(reg)
	sid := sid(t)

	// Persist an Incremented at the OLD schema version 1 (by=5).
	enc, err := counterv1.EventCodec{}.Encode(&counterv1.Incremented{By: 5})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: enc.TypeURL, SchemaVersion: 1, Payload: enc.Payload}},
	}); err != nil {
		t.Fatalf("append v1 event: %v", err)
	}

	// On load, the upcaster upgrades by=5 -> by=10 before Evolve folds it.
	state, _, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetCount() != 10 {
		t.Fatalf("upcasted count = %d, want 10 (5 doubled)", state.GetCount())
	}
}

func TestHandleAndFold(t *testing.T) {
	ctx := context.Background()
	rt, _ := newRuntime(t)
	stream := sid(t)

	res, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if res.State.GetCount() != 5 || res.ToVersion != 1 {
		t.Fatalf("after init: count=%d version=%d, want 5/1", res.State.GetCount(), res.ToVersion)
	}

	res, err = rt.Handle(ctx, stream, counterv1.Command(&counterv1.Increment{By: 10}), es.Meta{})
	if err != nil {
		t.Fatalf("increment: %v", err)
	}
	if res.State.GetCount() != 15 || res.ToVersion != 2 {
		t.Fatalf("after increment: count=%d version=%d, want 15/2", res.State.GetCount(), res.ToVersion)
	}

	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Decrement{By: 3}), es.Meta{}); err != nil {
		t.Fatalf("decrement: %v", err)
	}

	// Reload from the log (fresh fold) — proves state is derived, not cached.
	state, version, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetCount() != 12 || version != 3 {
		t.Fatalf("reloaded: count=%d version=%d, want 12/3", state.GetCount(), version)
	}
}

func TestDomainRejection(t *testing.T) {
	ctx := context.Background()
	rt, _ := newRuntime(t)
	stream := sid(t)

	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{}); !errors.Is(err, counter.ErrNotInitialized) {
		t.Fatalf("increment before init: got %v, want ErrNotInitialized", err)
	}

	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Init{Min: 0, Max: 10, Initial: 5}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Increment{By: 100}), es.Meta{}); !errors.Is(err, counter.ErrOutOfRange) {
		t.Fatalf("overflow increment: got %v, want ErrOutOfRange", err)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	rt, store := newRuntime(t)
	stream := sid(t)

	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 0}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	// The stream is now at version 1. A direct Append that still expects
	// version 0 simulates a stale writer and must lose the race.
	_, err := store.Append(ctx, es.AppendParams{
		StreamID:        stream,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte{}}},
	})
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("stale append: got %v, want ErrConflict", err)
	}
}

func TestTerminalStream(t *testing.T) {
	ctx := context.Background()
	rt, _ := newRuntime(t)
	stream := sid(t)

	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Init{Min: 0, Max: 10, Initial: 1}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Close{}), es.Meta{}); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{}); !errors.Is(err, es.ErrTerminal) {
		t.Fatalf("increment after close: got %v, want ErrTerminal", err)
	}
}

func TestTimeTravel(t *testing.T) {
	ctx := context.Background()
	rt, _ := newRuntime(t)
	stream := sid(t)

	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	asOf := time.Now().UTC()
	time.Sleep(5 * time.Millisecond) // ensure the next event's recorded_at is strictly later
	if _, err := rt.Handle(ctx, stream, counterv1.Command(&counterv1.Increment{By: 20}), es.Meta{}); err != nil {
		t.Fatalf("increment: %v", err)
	}

	// By version: state after exactly version 1 is the initial value.
	v1, err := rt.LoadAsOfVersion(ctx, stream, 1)
	if err != nil {
		t.Fatalf("as-of-version: %v", err)
	}
	if v1.GetCount() != 5 {
		t.Fatalf("as-of-version 1: count=%d, want 5", v1.GetCount())
	}

	// By wall-clock: state as of just after the init append, before the
	// increment, is likewise 5.
	att, err := rt.LoadAsOfTime(ctx, stream, asOf)
	if err != nil {
		t.Fatalf("as-of-time: %v", err)
	}
	if att.GetCount() != 5 {
		t.Fatalf("as-of-time: count=%d, want 5", att.GetCount())
	}

	// Current is 25.
	cur, _, err := rt.Load(ctx, stream)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cur.GetCount() != 25 {
		t.Fatalf("current: count=%d, want 25", cur.GetCount())
	}
}

func TestDeliveryPoller(t *testing.T) {
	ctx := context.Background()
	rt, store := newRuntime(t)
	stream := sid(t)

	// Produce four events across two streams' worth of commands.
	for _, cmd := range []counterv1.Command{
		&counterv1.Init{Min: 0, Max: 100, Initial: 0},
		&counterv1.Increment{By: 7},
		&counterv1.Decrement{By: 2},
		&counterv1.Close{},
	} {
		if _, err := rt.Handle(ctx, stream, cmd, es.Meta{}); err != nil {
			t.Fatalf("handle %T: %v", cmd, err)
		}
	}

	got := make(chan es.Envelope, 16)
	handler := func(ctx context.Context, batch []es.Envelope) error {
		for _, e := range batch {
			got <- e
		}
		return nil
	}
	p := delivery.NewPoller(store, store, handler, delivery.Config{
		Subscriber:   "test-sub",
		PollInterval: 10 * time.Millisecond,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Run(runCtx)

	want := []string{
		"counter.v1.Initialized",
		"counter.v1.Incremented",
		"counter.v1.Decremented",
		"counter.v1.Closed",
	}
	var lastPos uint64
	for i, wantType := range want {
		select {
		case e := <-got:
			if e.TypeURL != wantType {
				t.Fatalf("event %d: type=%s, want %s", i, e.TypeURL, wantType)
			}
			if e.GlobalPosition <= lastPos {
				t.Fatalf("event %d: global_position %d not increasing (prev %d)", i, e.GlobalPosition, lastPos)
			}
			lastPos = e.GlobalPosition
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d (%s)", i, wantType)
		}
	}

	// The checkpoint should now sit at the last event's position.
	pos, err := store.LoadCheckpoint(ctx, "test-sub")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if pos != lastPos {
		t.Fatalf("checkpoint=%d, want %d", pos, lastPos)
	}
}
