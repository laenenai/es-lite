package aggregate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
)

// noOpCapableCounterDecider is counter.Decider with one extra command: a nil
// command decides nothing, so Handle takes its no-op path while still using
// the real counter events, codec and snapshot state codec.
func noOpCapableCounterDecider() es.Decider[*counterv1.Counter, counterv1.Command, counterv1.Event] {
	d := counter.Decider
	d.Decide = func(s *counterv1.Counter, c counterv1.Command) ([]counterv1.Event, []es.ConstraintOp, error) {
		if c == nil {
			return nil, nil, nil
		}
		return counter.Decider.Decide(s, c)
	}
	return d
}

// headFailStore fails only the bounded head read a no-op Handle performs.
// Its StreamID carries no workspace, so the runtime has to read the head
// envelope — and that read is best-effort.
type headFailStore struct{ es.Store }

func (s headFailStore) ReadStream(ctx context.Context, sid es.StreamID, from, to uint64) ([]es.Envelope, error) {
	if from == 0 && to == 1 {
		return nil, errors.New("staged head read failure")
	}
	return s.Store.ReadStream(ctx, sid, from, to)
}

func TestHandleToken(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("counter", "token")
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})

	initRes, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if len(initRes.Envelopes) != 1 {
		t.Fatalf("init envelopes = %d, want 1", len(initRes.Envelopes))
	}
	want := initRes.Envelopes[len(initRes.Envelopes)-1]
	if initRes.Position != want.GlobalPosition || initRes.Position == 0 {
		t.Fatalf("init Position = %d, want %d", initRes.Position, want.GlobalPosition)
	}
	if initRes.Token != (es.Token{Scope: want.Workspace, Position: want.GlobalPosition}) {
		t.Fatalf("init Token = %+v, want scope %q position %d", initRes.Token, want.Workspace, want.GlobalPosition)
	}

	incRes, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 10}), es.Meta{})
	if err != nil {
		t.Fatalf("increment: %v", err)
	}
	last := incRes.Envelopes[len(incRes.Envelopes)-1]
	if incRes.Position != last.GlobalPosition {
		t.Fatalf("increment Position = %d, want %d", incRes.Position, last.GlobalPosition)
	}
	if incRes.Position <= initRes.Position {
		t.Fatalf("increment Position = %d, want > %d", incRes.Position, initRes.Position)
	}
	if incRes.Token != (es.Token{Scope: last.Workspace, Position: last.GlobalPosition}) {
		t.Fatalf("increment Token = %+v, want scope %q position %d", incRes.Token, last.Workspace, last.GlobalPosition)
	}
}

func TestAppendResultPosition(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("thing", "position")

	res, err := store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventData{
			{TypeURL: "thing.v1.Created", SchemaVersion: 1, Payload: []byte("a")},
			{TypeURL: "thing.v1.Updated", SchemaVersion: 1, Payload: []byte("b")},
		},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(res.Envelopes) != 2 {
		t.Fatalf("envelopes = %d, want 2", len(res.Envelopes))
	}
	if want := res.Envelopes[len(res.Envelopes)-1].GlobalPosition; res.Position != want || res.Position == 0 {
		t.Fatalf("Position = %d, want %d", res.Position, want)
	}

	emptySid, _ := es.NewStreamID("thing", "position-empty")
	empty, err := store.Append(ctx, es.AppendParams{StreamID: emptySid, ExpectedVersion: 0})
	if err != nil {
		t.Fatalf("empty append: %v", err)
	}
	if empty.Position != 0 {
		t.Fatalf("empty append Position = %d, want 0", empty.Position)
	}
}

func TestNoOpToken(t *testing.T) {
	ctx := context.Background()

	t.Run("head token", func(t *testing.T) {
		store := openStore(t)
		sid, _ := es.NewStreamID("counter", "noop-head")
		rt := aggregate.NewRuntime(store, noOpCapableCounterDecider(), counterv1.EventCodec{})

		initRes, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{})
		if err != nil {
			t.Fatalf("init: %v", err)
		}
		incRes, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 10}), es.Meta{})
		if err != nil {
			t.Fatalf("increment: %v", err)
		}
		head := incRes.Envelopes[len(incRes.Envelopes)-1]

		res, err := rt.Handle(ctx, sid, nil, es.Meta{})
		if err != nil {
			t.Fatalf("no-op: %v", err)
		}
		if res.FromVersion != res.ToVersion || res.ToVersion != 2 {
			t.Fatalf("no-op versions = %d..%d, want 2..2", res.FromVersion, res.ToVersion)
		}
		if res.Position != head.GlobalPosition {
			t.Fatalf("no-op Position = %d, want head %d", res.Position, head.GlobalPosition)
		}
		if res.Token != (es.Token{Scope: head.Workspace, Position: head.GlobalPosition}) {
			t.Fatalf("no-op Token = %+v, want scope %q position %d", res.Token, head.Workspace, head.GlobalPosition)
		}
		if initRes.Position == 0 {
			t.Fatal("fixture appended at position 0")
		}
	})

	t.Run("warm snapshot has no tail envelopes", func(t *testing.T) {
		store := openStore(t)
		sid, _ := es.NewStreamID("counter", "noop-warm")
		cache := newTestCache()
		rt := aggregate.NewRuntime(store, noOpCapableCounterDecider(), counterv1.EventCodec{}).
			WithSnapshots(cache, counterStateCodec{}, 1)

		if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{}); err != nil {
			t.Fatalf("init: %v", err)
		}
		incRes, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 10}), es.Meta{})
		if err != nil {
			t.Fatalf("increment: %v", err)
		}
		head := incRes.Envelopes[len(incRes.Envelopes)-1]

		snap, ok, err := cache.Load(ctx, sid.Canonical())
		if err != nil || !ok || snap.Version != 2 {
			t.Fatalf("cache snapshot = (%+v, %v, %v), want version 2", snap, ok, err)
		}

		res, err := rt.Handle(ctx, sid, nil, es.Meta{})
		if err != nil {
			t.Fatalf("no-op: %v", err)
		}
		if len(res.Envelopes) != 0 {
			t.Fatalf("no-op envelopes = %d, want 0", len(res.Envelopes))
		}
		if res.Position != head.GlobalPosition {
			t.Fatalf("snapshot-seeded no-op Position = %d, want head %d", res.Position, head.GlobalPosition)
		}
		if res.Token != (es.Token{Scope: head.Workspace, Position: head.GlobalPosition}) {
			t.Fatalf("snapshot-seeded no-op Token = %+v, want scope %q position %d", res.Token, head.Workspace, head.GlobalPosition)
		}
	})

	t.Run("empty stream", func(t *testing.T) {
		store := openStore(t)
		sid, _ := es.NewStreamID("counter", "noop-empty")
		rt := aggregate.NewRuntime(store, noOpCapableCounterDecider(), counterv1.EventCodec{})

		res, err := rt.Handle(ctx, sid, nil, es.Meta{})
		if err != nil {
			t.Fatalf("no-op: %v", err)
		}
		if res.Position != 0 || res.Token != (es.Token{}) {
			t.Fatalf("empty-stream no-op = position %d token %+v, want zero token", res.Position, res.Token)
		}
	})

	t.Run("head read failure yields zero token", func(t *testing.T) {
		store := openStore(t)
		sid, _ := es.NewStreamID("counter", "noop-fail")
		rt := aggregate.NewRuntime(store, noOpCapableCounterDecider(), counterv1.EventCodec{})
		if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{}); err != nil {
			t.Fatalf("init: %v", err)
		}

		failing := aggregate.NewRuntime(headFailStore{Store: store}, noOpCapableCounterDecider(), counterv1.EventCodec{})
		res, err := failing.Handle(ctx, sid, nil, es.Meta{})
		if err != nil {
			t.Fatalf("no-op with a failing head read: %v (a read error must not fail the command)", err)
		}
		if res.Position != 0 || res.Token != (es.Token{}) {
			t.Fatalf("failed head read = position %d token %+v, want zero token", res.Position, res.Token)
		}
	})
}
