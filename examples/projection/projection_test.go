package projection_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	example "github.com/laenenai/es-lite/examples/projection"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	counterviewv1 "github.com/laenenai/es-lite/gen/counterview/v1"
	"github.com/laenenai/es-lite/projection"
	"github.com/laenenai/es-lite/reaction"
	"github.com/laenenai/es-lite/sqlite"
	"github.com/laenenai/es-lite/upcast"
)

type fixture struct {
	store *sqlite.Store
	rt    *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	store, err := sqlite.Open(context.Background(), "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return fixture{store: store, rt: aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})}
}

func (f fixture) handle(t *testing.T, id string, cmds ...counterv1.Command) {
	t.Helper()
	sid, _ := es.NewStreamID("counter", id)
	for _, c := range cmds {
		if _, err := f.rt.Handle(context.Background(), sid, c, es.Meta{}); err != nil {
			t.Fatalf("%s %T: %v", id, c, err)
		}
	}
}

// runUntil runs the runner over a LogFeed until the marker reaches pos.
func runUntil(t *testing.T, run func(ctx context.Context, feed projection.Feed) error, position func(context.Context) (uint64, error), src projection.Source, pos uint64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, &projection.LogFeed{Source: src, Poll: 5 * time.Millisecond}) }()
	deadline := time.After(5 * time.Second)
	for {
		p, err := position(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if p >= pos {
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("runner stopped with %v", err)
			}
			return
		}
		select {
		case err := <-done:
			t.Fatalf("runner stopped early: %v", err)
		case <-deadline:
			t.Fatalf("marker stuck at %d, want %d", p, pos)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestProjectorLiveOverLogFeed(t *testing.T) {
	f := newFixture(t)
	f.handle(t, "a", &counterv1.Init{Min: 0, Max: 100, Initial: 5}, &counterv1.Increment{By: 10}, &counterv1.Decrement{By: 3})
	f.handle(t, "b", &counterv1.Init{Min: 0, Max: 10, Initial: 1}, &counterv1.Close{DecidedAt: timestamppb.Now()})

	view := example.NewView()
	// The embedded runtime IS the state loader: LoadAsOfVersion has the
	// generated seam's exact signature.
	runner := counterviewv1.NewCounterViewRunner(view, counterviewv1.CounterViewDeps{Counter: f.rt})
	runUntil(t, runner.Run, runner.Position, f.store, 5)

	a, _ := view.Get("a")
	if a.Count != 12 || a.Version != 3 || a.Closed {
		t.Fatalf("view a = %+v", a)
	}
	b, _ := view.Get("b")
	if b.Count != 1 || b.Version != 2 || !b.Closed {
		t.Fatalf("view b = %+v", b)
	}

	// More events: the runner resumes from its marker and never re-applies.
	f.handle(t, "a", &counterv1.Increment{By: 1})
	runUntil(t, runner.Run, runner.Position, f.store, 6)
	if a, _ := view.Get("a"); a.Count != 13 || a.Version != 4 {
		t.Fatalf("view a after resume = %+v", a)
	}
	if got := counterviewv1.CounterViewSubjects("*"); len(got) != 4 || got[0] != "evt.*.counter.initialized" {
		t.Fatalf("subjects = %v", got)
	}
	if got, want := counterviewv1.CounterViewConsumes(), []string{
		"counter.v1.Initialized",
		"counter.v1.Incremented",
		"counter.v1.Decremented",
		"counter.v1.Closed",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("consumes = %v, want %v (the spec's order)", got, want)
	}
	// The generated Definition is the catalogue entry: same name and consumes
	// list, read from the same identifiers, so registration cannot drift.
	def := counterviewv1.CounterViewDefinition()
	if def.Name != counterviewv1.CounterViewName || !reflect.DeepEqual(def.Consumes, counterviewv1.CounterViewConsumes()) {
		t.Fatalf("definition = %+v, want name %q and consumes %v", def, counterviewv1.CounterViewName, counterviewv1.CounterViewConsumes())
	}
}

// TestRebuildEqualsLive: a fresh read model rebuilt from position 0 equals
// the one built by live consumption, because both fetch state as of the
// event's version through the same dispatch path.
func TestRebuildEqualsLive(t *testing.T) {
	f := newFixture(t)
	f.handle(t, "a", &counterv1.Init{Min: 0, Max: 100, Initial: 5}, &counterv1.Increment{By: 10})
	f.handle(t, "b", &counterv1.Init{Min: 0, Max: 100, Initial: 50}, &counterv1.Decrement{By: 7}, &counterv1.Increment{By: 1})
	f.handle(t, "a", &counterv1.Increment{By: 2}, &counterv1.Close{DecidedAt: timestamppb.Now()})

	live := example.NewView()
	liveRunner := counterviewv1.NewCounterViewRunner(live, counterviewv1.CounterViewDeps{Counter: f.rt})
	runUntil(t, liveRunner.Run, liveRunner.Position, f.store, 7)

	rebuilt := example.NewView()
	rebuildRunner := counterviewv1.NewCounterViewRunner(rebuilt, counterviewv1.CounterViewDeps{Counter: f.rt, Marker: projection.NewMemoryMarker()})
	pos, err := rebuildRunner.Rebuild(context.Background(), f.store, 3)
	if err != nil || pos != 7 {
		t.Fatalf("rebuild: pos=%d err=%v", pos, err)
	}
	if !reflect.DeepEqual(live.Snapshot(), rebuilt.Snapshot()) {
		t.Fatalf("rebuild differs from live:\nlive    %+v\nrebuilt %+v", live.Snapshot(), rebuilt.Snapshot())
	}
	// Rebuilding again on the same runner is a no-op (marker already at 7).
	if pos, err := rebuildRunner.Rebuild(context.Background(), f.store, 3); err != nil || pos != 7 {
		t.Fatalf("second rebuild: pos=%d err=%v", pos, err)
	}
}

func TestMetadataOnlyIndex(t *testing.T) {
	f := newFixture(t)
	f.handle(t, "a", &counterv1.Init{Min: 0, Max: 100, Initial: 5}, &counterv1.Increment{By: 1}, &counterv1.Increment{By: 1}, &counterv1.Decrement{By: 1})

	idx := example.NewActivity()
	runner := counterviewv1.NewCounterActivityRunner(idx, counterviewv1.CounterActivityDeps{})
	runUntil(t, runner.Run, runner.Position, f.store, 4)
	if n := idx.Events("a"); n != 3 {
		t.Fatalf("activity a = %d, want 3 (Initialized is not consumed)", n)
	}
}

// conflictOnce wraps a Dispatcher and fails the first dispatch with
// es.ErrConflict, so the reaction's retry path is exercised.
type conflictOnce struct {
	inner    reaction.Dispatcher[counterv1.Command]
	calls    atomic.Int32
	conflict atomic.Bool
}

func (c *conflictOnce) Dispatch(ctx context.Context, sid es.StreamID, cmd counterv1.Command, meta es.Meta) error {
	c.calls.Add(1)
	if c.conflict.CompareAndSwap(true, false) {
		return es.ErrConflict
	}
	return c.inner.Dispatch(ctx, sid, cmd, meta)
}

func TestReactionIssuesCommandAndRetriesConflict(t *testing.T) {
	f := newFixture(t)
	f.handle(t, "a", &counterv1.Init{Min: 3, Max: 100, Initial: 5}, &counterv1.Close{DecidedAt: timestamppb.Now()})

	// A runtime with retry on: the dispatcher must not add its own.
	retrying := aggregate.NewRuntime(f.store, counter.Decider, counterv1.EventCodec{}).WithConflictRetry(3, func(int) time.Duration { return 0 })
	router := &conflictOnce{inner: reaction.RuntimeDispatcher(retrying)}
	router.conflict.Store(true)

	runner := counterviewv1.NewResetOnClosedRunner(example.Reset{}, counterviewv1.ResetOnClosedDeps{
		Counter:       f.rt,
		CounterRouter: router,
		Actor:         es.Actor{Type: "system", ID: "reset"},
	})
	runUntil(t, runner.Run, runner.Position, f.store, 2)

	// The first dispatch conflicted; the wrapped runtime's retry is internal
	// to Handle, so the conflict surfaced to Execute — which must NOT retry
	// itself (that is the dispatcher's job). The runner naks and the feed
	// redelivers; the second dispatch succeeds.
	if n := router.calls.Load(); n != 2 {
		t.Fatalf("dispatch calls = %d, want 2 (conflict, then success)", n)
	}
	next, _ := es.NewStreamID("counter", "a-next")
	st, v, err := f.rt.Load(context.Background(), next)
	if err != nil || v != 1 || st.GetCount() != 3 || st.GetMin() != 3 {
		t.Fatalf("successor = %+v v%d err=%v", st, v, err)
	}
	// The issued command carries the reaction's audit metadata.
	envs, _ := f.store.ReadStream(context.Background(), next, 0, 0)
	closed, _ := f.store.ReadStream(context.Background(), es.StreamID{Type: "counter", ID: "a"}, 1, 2)
	if envs[0].CausationID != closed[0].EventID || envs[0].Actor.ID != "reset" {
		t.Fatalf("issued command meta = %+v, want caused by %s", envs[0], closed[0].EventID)
	}
}

func TestReactionTerminalTargetIsDone(t *testing.T) {
	f := newFixture(t)
	// The successor already exists and is closed: Init on it is ErrTerminal,
	// which the executor treats as done — the marker advances.
	f.handle(t, "a-next", &counterv1.Init{Min: 0, Max: 1, Initial: 0}, &counterv1.Close{DecidedAt: timestamppb.Now()})
	f.handle(t, "a", &counterv1.Init{Min: 0, Max: 100, Initial: 5}, &counterv1.Close{DecidedAt: timestamppb.Now()})

	runner := counterviewv1.NewResetOnClosedRunner(example.Reset{}, counterviewv1.ResetOnClosedDeps{
		Counter:       f.rt,
		CounterRouter: reaction.RuntimeDispatcher(f.rt),
		Actor:         es.Actor{Type: "system", ID: "reset"},
	})
	runUntil(t, runner.Run, runner.Position, f.store, 4)
	next, _ := es.NewStreamID("counter", "a-next")
	if _, v, _ := f.rt.Load(context.Background(), next); v != 2 {
		t.Fatalf("terminal successor was written to: v%d", v)
	}
}

func TestRuntimeDispatcherRetriesWhenRuntimeDoesNot(t *testing.T) {
	// A runtime without WithConflictRetry gets the dispatcher's default
	// budget: a real conflict (a rival append between load and append) is
	// absorbed.
	f := newFixture(t)
	sid, _ := es.NewStreamID("counter", "a")
	f.handle(t, "a", &counterv1.Init{Min: 0, Max: 1000, Initial: 0})
	rival := aggregate.NewRuntime(f.store, counter.Decider, counterv1.EventCodec{})
	racing := &racingStore{Store: f.store, rival: rival, sid: sid}
	plain := aggregate.NewRuntime(racing, counter.Decider, counterv1.EventCodec{})
	d := reaction.RuntimeDispatcher(plain)
	if err := d.Dispatch(context.Background(), sid, &counterv1.Increment{By: 1}, es.Meta{}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if _, v, _ := f.rt.Load(context.Background(), sid); v != 3 {
		t.Fatalf("version = %d, want 3 (rival + retried increment)", v)
	}
}

// racingStore injects one competing append before the first Append.
type racingStore struct {
	es.Store
	rival *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]
	sid   es.StreamID
	done  atomic.Bool
}

func (r *racingStore) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	if r.done.CompareAndSwap(false, true) {
		if _, err := r.rival.Handle(ctx, r.sid, counterv1.Command(&counterv1.Increment{By: 100}), es.Meta{}); err != nil {
			return es.AppendResult{}, err
		}
	}
	return r.Store.Append(ctx, p)
}

// upcastProbe is a CounterViewHandler that records the Incremented event
// exactly as dispatch delivered it — decoded, then run through the deps'
// upcaster seam — alongside the state fetched as of its version.
type upcastProbe struct {
	by    int64
	count int64
	hits  int
}

func (p *upcastProbe) OnInitialized(context.Context, es.Envelope, *counterv1.Initialized, *counterv1.Counter) error {
	return nil
}

func (p *upcastProbe) OnIncremented(_ context.Context, _ es.Envelope, ev *counterv1.Incremented, state *counterv1.Counter) error {
	p.by = ev.GetBy()
	p.count = state.GetCount()
	p.hits++
	return nil
}

func (p *upcastProbe) OnDecremented(context.Context, es.Envelope, *counterv1.Decremented, *counterv1.Counter) error {
	return nil
}

func (p *upcastProbe) OnClosed(context.Context, es.Envelope, *counterv1.Closed, *counterv1.Counter) error {
	return nil
}

// TestProjectionAppliesUpcaster (F20): a v1 Incremented — the examples/upcast
// fixture shape, where by was stored in half-units — must reach the handler
// already upgraded by the generated seam (CounterViewDeps.CounterUpcaster),
// while the state fetched alongside agrees because the runtime folds the
// same upgrade through its own upcaster path. Without the seam the handler
// would see the stored by=3 against a state of 6.
func TestProjectionAppliesUpcaster(t *testing.T) {
	f := newFixture(t)

	// History "already on disk" (the examples/upcast fixture): Initialized,
	// then an Incremented stored under the OLD schema version 1 with by=3.
	sid, _ := es.NewStreamID("counter", "legacy")
	encInit, err := counterv1.EventCodec{}.Encode(&counterv1.Initialized{Min: 0, Max: 100, Value: 0})
	if err != nil {
		t.Fatalf("encode init: %v", err)
	}
	encInc, err := counterv1.EventCodec{}.Encode(&counterv1.Incremented{By: 3})
	if err != nil {
		t.Fatalf("encode inc: %v", err)
	}
	if _, err := f.store.Append(context.Background(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventData{
			{TypeURL: encInit.TypeURL, SchemaVersion: 1, Payload: encInit.Payload},
			{TypeURL: encInc.TypeURL, SchemaVersion: 1, Payload: encInc.Payload},
		},
	}); err != nil {
		t.Fatalf("append legacy events: %v", err)
	}

	// The same upgrade wired twice: into the projection deps (the generated
	// seam) and into the runtime (its own upcaster path, so the fetched
	// state folds the upgrade too).
	legacy := upcast.NewRegistry[counterv1.Event]().Register(
		"counter.v1.Incremented",
		func(fromVersion uint32, e counterv1.Event) (counterv1.Event, error) {
			if inc, ok := e.(*counterv1.Incremented); ok && fromVersion < 2 {
				return &counterv1.Incremented{By: inc.GetBy() * 2}, nil
			}
			return e, nil
		},
	)
	rt := aggregate.NewRuntime(f.store, counter.Decider, counterv1.EventCodec{}).WithUpcaster(legacy)

	probe := &upcastProbe{}
	runner := counterviewv1.NewCounterViewRunner(probe, counterviewv1.CounterViewDeps{Counter: rt, CounterUpcaster: legacy})
	runUntil(t, runner.Run, runner.Position, f.store, 2)

	// The handler observed the upcast event: stored by=3, delivered by=6.
	if probe.hits != 1 || probe.by != 6 {
		t.Fatalf("handler saw by=%d hits=%d, want the upcast event by=6 (stored v1 by=3)", probe.by, probe.hits)
	}
	// ...and the state fetched as of v2 agrees: the runtime folded the same
	// upgrade on its own upcaster path (0 + 6, not 0 + 3).
	if probe.count != 6 {
		t.Fatalf("state as of v2 = %d, want 6", probe.count)
	}
	if st, v, err := rt.Load(context.Background(), sid); err != nil || v != 2 || st.GetCount() != 6 {
		t.Fatalf("load = %+v v%d err=%v, want count 6 at version 2", st, v, err)
	}
}
