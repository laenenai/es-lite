package aggregate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

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

func TestHandleRejectsConstraintsWithoutEvents(t *testing.T) {
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

	// Constraint ops ride on the first event of the batch (ADR 0043): a
	// claim with no domain event is refused before the store sees it, so
	// neither a claim nor an event is written.
	if _, err := rt.Handle(ctx, sid, noEventCommand{}, es.Meta{}); !errors.Is(err, es.ErrConstraintWithoutEvent) {
		t.Fatalf("constraint-only handle: got %v, want ErrConstraintWithoutEvent", err)
	}
	if _, found, err := store.LookupClaim(ctx, "email", "a@b.com"); err != nil || found {
		t.Fatalf("claim lookup found=%v err=%v, want absent", found, err)
	}
	if v, err := store.CurrentStreamVersion(ctx, sid); err != nil || v != 0 {
		t.Fatalf("stream version = %d (err %v), want 0 — the rejected append wrote nothing", v, err)
	}
}

type claimState struct{}
type claimEvent struct{ value string }
type claimCommand struct{}

type claimCodec struct{}

func (claimCodec) Encode(e claimEvent) (es.EncodedEvent, error) {
	return es.EncodedEvent{TypeURL: "thing.v1.Claimed", SchemaVersion: 1, Payload: []byte(e.value)}, nil
}
func (claimCodec) Decode(es.EncodedEvent) (claimEvent, error) { return claimEvent{}, nil }

// TestHandleCarriesConstraintOpsOnFirstEnvelope is the runtime positive path:
// a Decide that returns events *and* constraints appends both, and the store
// puts the ops on the first returned envelope only (ADR 0043 §2.1).
func TestHandleCarriesConstraintOpsOnFirstEnvelope(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("thing", "claim-positive")
	ops := []es.ConstraintOp{es.Claim("email", "a@b.com")}

	decider := es.Decider[claimState, claimCommand, claimEvent]{
		Initial: func() claimState { return claimState{} },
		Decide: func(claimState, claimCommand) ([]claimEvent, []es.ConstraintOp, error) {
			return []claimEvent{{value: "first"}, {value: "second"}}, ops, nil
		},
		Evolve: func(s claimState, _ claimEvent) claimState { return s },
	}
	rt := aggregate.NewRuntime(store, decider, claimCodec{})

	res, err := rt.Handle(ctx, sid, claimCommand{}, es.Meta{})
	if err != nil {
		t.Fatalf("handle events with constraints: %v", err)
	}
	if res.ToVersion != 2 || len(res.Envelopes) != 2 {
		t.Fatalf("result = v%d with %d envelopes, want v2 with 2", res.ToVersion, len(res.Envelopes))
	}
	got := res.Envelopes[0].ConstraintOps
	if len(got) != 1 || got[0] != ops[0] {
		t.Errorf("first returned envelope ops = %+v, want %+v", got, ops)
	}
	if res.Envelopes[1].ConstraintOps != nil {
		t.Errorf("second returned envelope ops = %+v, want nil", res.Envelopes[1].ConstraintOps)
	}
	if _, found, err := store.LookupClaim(ctx, "email", "a@b.com"); err != nil || !found {
		t.Fatalf("claim lookup found=%v err=%v, want found", found, err)
	}

	// The ops survive a read back from the store on the same first envelope.
	stream, err := store.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if len(stream) != 2 || len(stream[0].ConstraintOps) != 1 || stream[1].ConstraintOps != nil {
		t.Fatalf("read-back ops = %+v / %+v, want first only", stream[0].ConstraintOps, stream[1].ConstraintOps)
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

	// A zero-event append carrying constraints is now rejected: the ops
	// must ride on an event (ADR 0043).
	if _, err := store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Constraints:     []es.ConstraintOp{es.Claim("email", "a@b.com")},
	}); !errors.Is(err, es.ErrConstraintWithoutEvent) {
		t.Fatalf("claim-only append error = %v, want ErrConstraintWithoutEvent", err)
	}

	// With an event the same claim applies in the append transaction.
	if _, err := store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Events:          []es.EventData{{TypeURL: "thing.v1.Claimed", SchemaVersion: 1, Payload: []byte("y")}},
		Constraints:     []es.ConstraintOp{es.Claim("email", "a@b.com")},
	}); err != nil {
		t.Fatalf("claim-bearing append: %v", err)
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

// Pinned command ids for the deterministic-event-id tests: fixed literals
// so every comparison below is reproducible run to run.
var (
	retryCmdID  = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	controlInit = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	detInitID   = uuid.MustParse("33333333-3333-4333-8333-333333333333")
)

func TestDeterministicEventIDsCollideOnRetry(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	meta := es.Meta{CommandID: retryCmdID}

	// Control — without the option, the same command handled twice appends
	// twice: the adapter mints a fresh UUIDv7 per event, so nothing collides.
	// Today's behavior, pinned.
	control, _ := es.NewStreamID("counter", "det-control")
	ctl := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})
	if _, err := ctl.Handle(ctx, control, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 0}), es.Meta{CommandID: controlInit}); err != nil {
		t.Fatalf("control init: %v", err)
	}
	if _, err := ctl.Handle(ctx, control, counterv1.Command(&counterv1.Increment{By: 1}), meta); err != nil {
		t.Fatalf("control first handle: %v", err)
	}
	if _, err := ctl.Handle(ctx, control, counterv1.Command(&counterv1.Increment{By: 1}), meta); err != nil {
		t.Fatalf("control: retried command without the option must append as today, got %v", err)
	}

	// With WithDeterministicEventIDs, the retry derives the same event id
	// from the command id and collides with the first run's row on the
	// events.event_id unique constraint → es.ErrDuplicateEventID.
	det, _ := es.NewStreamID("counter", "det-main")
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).WithDeterministicEventIDs()
	if _, err := rt.Handle(ctx, det, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 0}), es.Meta{CommandID: detInitID}); err != nil {
		t.Fatalf("det init: %v", err)
	}
	if _, err := rt.Handle(ctx, det, counterv1.Command(&counterv1.Increment{By: 1}), meta); err != nil {
		t.Fatalf("det first handle: %v", err)
	}
	if _, err := rt.Handle(ctx, det, counterv1.Command(&counterv1.Increment{By: 1}), meta); !errors.Is(err, es.ErrDuplicateEventID) {
		t.Fatalf("retried command error = %v, want ErrDuplicateEventID", err)
	}
}

// TestDeterministicEventIDsStableForSameCommandAndIndex pins that the
// derivation is a pure function of (namespace, command id, index): the same
// command id handled in two fresh stores yields the same event id for index
// 0. (Within one store that equality is what makes the retry collide — the
// test above — so stability needs a second store to observe directly.)
func TestDeterministicEventIDsStableForSameCommandAndIndex(t *testing.T) {
	ctx := context.Background()
	meta := es.Meta{CommandID: retryCmdID}

	incrementEventID := func(t *testing.T, dsn string) uuid.UUID {
		t.Helper()
		s, err := sqlite.Open(ctx, "file:"+dsn+"?mode=memory&cache=shared")
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		sid, _ := es.NewStreamID("counter", "det-stable")
		rt := aggregate.NewRuntime(s, counter.Decider, counterv1.EventCodec{}).WithDeterministicEventIDs()
		if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 0}), es.Meta{CommandID: detInitID}); err != nil {
			t.Fatalf("init: %v", err)
		}
		res, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), meta)
		if err != nil {
			t.Fatalf("increment: %v", err)
		}
		return res.Envelopes[0].EventID
	}

	first := incrementEventID(t, t.Name())
	second := incrementEventID(t, t.Name()+"-again")
	if first == uuid.Nil || second == uuid.Nil {
		t.Fatalf("event ids not filled: %s / %s", first, second)
	}
	if first != second {
		t.Fatalf("same command id derived %s then %s, want stable", first, second)
	}
}

// TestDeterministicEventIDsDifferAcrossCommands pins that two different
// command ids never derive the same event id.
func TestDeterministicEventIDsDifferAcrossCommands(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("counter", "det-distinct")
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).WithDeterministicEventIDs()

	resA, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 0}), es.Meta{CommandID: controlInit})
	if err != nil {
		t.Fatalf("first command: %v", err)
	}
	resB, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{CommandID: retryCmdID})
	if err != nil {
		t.Fatalf("second command: %v", err)
	}
	idA, idB := resA.Envelopes[0].EventID, resB.Envelopes[0].EventID
	if idA == uuid.Nil || idB == uuid.Nil {
		t.Fatalf("event ids not filled: %s / %s", idA, idB)
	}
	if idA == idB {
		t.Fatalf("different command ids derived the same event id %s", idA)
	}
}

// TestDeterministicEventIDsWithoutCommandIDNeverCollide pins the nil
// command id edge: with the option on, a caller that passes no CommandID
// keeps today's behavior — the runtime mints one per Handle and the events
// append without colliding, because a nil command id can't derive a fixed
// event id.
func TestDeterministicEventIDsWithoutCommandIDNeverCollide(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("counter", "det-nil-cmd")
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).WithDeterministicEventIDs()

	res1, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 0}), es.Meta{})
	if err != nil {
		t.Fatalf("first nil-command handle: %v", err)
	}
	res2, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{})
	if err != nil {
		t.Fatalf("second nil-command handle: %v", err)
	}
	if res1.Envelopes[0].EventID == res2.Envelopes[0].EventID {
		t.Fatalf("nil command ids shared one event id %s", res1.Envelopes[0].EventID)
	}
}
