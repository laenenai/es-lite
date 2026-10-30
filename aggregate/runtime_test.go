package aggregate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/sqlite"
)

func openStore(t *testing.T) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(context.Background(), "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestLoadAsOfVersionZeroAndOutOfRange(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})

	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 10}), es.Meta{}); err != nil {
		t.Fatalf("increment: %v", err)
	}

	v0, err := rt.LoadAsOfVersion(ctx, sid, 0)
	if err != nil {
		t.Fatalf("as-of 0: %v", err)
	}
	if v0.GetCount() != 0 {
		t.Fatalf("as-of 0 count = %d, want Initial (0)", v0.GetCount())
	}

	v1, err := rt.LoadAsOfVersion(ctx, sid, 1)
	if err != nil {
		t.Fatalf("as-of 1: %v", err)
	}
	if v1.GetCount() != 5 {
		t.Fatalf("as-of 1 count = %d, want 5", v1.GetCount())
	}

	if _, err := rt.LoadAsOfVersion(ctx, sid, 99); !errors.Is(err, es.ErrVersionOutOfRange) {
		t.Fatalf("as-of 99 error = %v, want ErrVersionOutOfRange", err)
	}
}

type noEventState struct{}
type noEventCommand struct{}
type noEvent struct{}

type noEventCodec struct{}

func (noEventCodec) Encode(noEvent) (es.EncodedEvent, error) {
	panic("Encode must not be called for a constraint-only command")
}
func (noEventCodec) Decode(es.EncodedEvent) (noEvent, error) {
	panic("Decode must not be called when there are no events")
}

func TestHandleAppliesConstraintsWithoutEvents(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("thing", "main")

	decider := es.Decider[noEventState, noEventCommand, noEvent]{
		Initial: func() noEventState { return noEventState{} },
		Decide: func(noEventState, noEventCommand) ([]noEvent, []es.ConstraintOp, error) {
			return nil, []es.ConstraintOp{es.Claim("email", "a@b.com")}, nil
		},
		Evolve: func(s noEventState, _ noEvent) noEventState { return s },
	}
	rt := aggregate.NewRuntime(store, decider, noEventCodec{})

	res, err := rt.Handle(ctx, sid, noEventCommand{}, es.Meta{})
	if err != nil {
		t.Fatalf("claim-only handle: %v", err)
	}
	if res.FromVersion != 0 || res.ToVersion != 0 {
		t.Fatalf("claim-only versions = %d/%d, want 0/0", res.FromVersion, res.ToVersion)
	}
	if _, found, err := store.LookupClaim(ctx, "email", "a@b.com"); err != nil || !found {
		t.Fatalf("claim lookup found=%v err=%v, want found", found, err)
	}
}

func TestEmptyAppendChecksVersionAndAppliesConstraints(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("thing", "main")

	if _, err := store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: "thing.v1.Created", SchemaVersion: 1, Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}

	if _, err := store.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: 0}); !errors.Is(err, es.ErrConflict) {
		t.Fatalf("stale empty append error = %v, want ErrConflict", err)
	}

	if _, err := store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Constraints:     []es.ConstraintOp{es.Claim("email", "a@b.com")},
	}); err != nil {
		t.Fatalf("claim-only append: %v", err)
	}
	if _, found, err := store.LookupClaim(ctx, "email", "a@b.com"); err != nil || !found {
		t.Fatalf("claim lookup found=%v err=%v, want found", found, err)
	}
}

func TestInvalidConstraintRejected(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("thing", "main")

	_, err := store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Constraints:     []es.ConstraintOp{{Op: es.ConstraintOpKind(99), Scope: "email", Value: "a@b.com"}},
	})
	if !errors.Is(err, es.ErrInvalidConstraint) {
		t.Fatalf("invalid constraint error = %v, want ErrInvalidConstraint", err)
	}
}

func TestWithChainVerificationPassesOnIntactStream(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).WithChainVerification()
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 10}), es.Meta{}); err != nil {
		t.Fatalf("increment: %v", err)
	}
	if st, v, err := rt.Load(ctx, sid); err != nil || v != 2 || st.GetCount() != 15 {
		t.Fatalf("verified load: count=%d v=%d err=%v", st.GetCount(), v, err)
	}
}
