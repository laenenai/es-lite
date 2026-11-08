package aggregate_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
)

func TestHandleStampsOccurredAtFromServerClock(t *testing.T) {
	ctx := context.Background()
	rt, sid := initCounter(t)

	before := time.Now().Add(-time.Second)
	clientDecided := time.Now().UTC().Add(-2 * time.Minute)
	res, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{DecidedAt: timestamppb.New(clientDecided)}),
		es.Meta{})
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	after := time.Now().Add(time.Second)
	if len(res.Envelopes) != 1 {
		t.Fatalf("envelopes = %d, want 1", len(res.Envelopes))
	}
	got := res.Envelopes[0].OccurredAt
	if got.Before(before) || got.After(after) {
		t.Fatalf("OccurredAt = %v, want server time in [%v,%v], not client decided_at %v", got, before, after, clientDecided)
	}
	if got.Equal(clientDecided) {
		t.Fatalf("OccurredAt = %v, want server time, never a client-supplied value", got)
	}
}

func TestHandleStampsOccurredAtWithoutDecidedAt(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})

	before := time.Now().Add(-time.Second)
	res, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 1}),
		es.Meta{})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	after := time.Now().Add(time.Second)
	got := res.Envelopes[0].OccurredAt
	if got.Before(before) || got.After(after) {
		t.Fatalf("OccurredAt = %v, want server time in [%v,%v]", got, before, after)
	}
}

func TestHandleRejectsDecidedAtOutsideSkew(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset time.Duration
	}{
		{name: "backdated", offset: -es.MaxDecisionSkew - time.Second},
		{name: "future", offset: es.MaxDecisionSkew + time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rt, sid := initCounter(t)
			decided := time.Now().UTC().Add(tc.offset)

			_, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{DecidedAt: timestamppb.New(decided)}), es.Meta{})
			if !errors.Is(err, es.ErrDecidedAtSkew) {
				t.Fatalf("error = %v, want ErrDecidedAtSkew", err)
			}
			var skew *es.DecidedAtSkewError
			if !errors.As(err, &skew) {
				t.Fatalf("error = %T %v, want *es.DecidedAtSkewError", err, err)
			}
			if !skew.DecidedAt.Equal(decided) || skew.MaxSkew != es.MaxDecisionSkew || skew.ServerTime.IsZero() {
				t.Fatalf("skew error = %+v, want decided=%v max=%v server set", skew, decided, es.MaxDecisionSkew)
			}
			if _, v, err := rt.Load(ctx, sid); err != nil || v != 1 {
				t.Fatalf("version after skew = %d (err %v), want 1 (nothing appended)", v, err)
			}
		})
	}
}

func TestHandleAcceptsDecidedAtWithinSkew(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset time.Duration
	}{
		{name: "slightly backdated", offset: -es.MaxDecisionSkew + time.Second},
		{name: "slightly future", offset: es.MaxDecisionSkew - time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rt, sid := initCounter(t)
			decided := time.Now().UTC().Add(tc.offset)

			before := time.Now().Add(-time.Second)
			res, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{DecidedAt: timestamppb.New(decided)}), es.Meta{})
			if err != nil {
				t.Fatalf("close within skew: %v", err)
			}
			after := time.Now().Add(time.Second)
			got := res.Envelopes[0].OccurredAt
			if got.Before(before) || got.After(after) || got.Equal(decided) {
				t.Fatalf("OccurredAt = %v, want server time, not decided_at %v", got, decided)
			}
		})
	}
}

func TestDeciderSeesServerDecisionTime(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("counter", "main")

	var seen time.Time
	decider := counter.Decider
	inner := counter.Decider.Decide
	decider.Decide = func(s *counterv1.Counter, c counterv1.Command) ([]counterv1.Event, []es.ConstraintOp, error) {
		if close, ok := c.(*counterv1.Close); ok {
			seen = close.DecisionTime()
		}
		return inner(s, c)
	}
	rt := aggregate.NewRuntime(store, decider, counterv1.EventCodec{})
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 1}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}

	clientDecided := time.Now().UTC().Add(-3 * time.Minute)
	before := time.Now().Add(-time.Second)
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{DecidedAt: timestamppb.New(clientDecided)}), es.Meta{}); err != nil {
		t.Fatalf("close: %v", err)
	}
	after := time.Now().Add(time.Second)
	if seen.Before(before) || seen.After(after) {
		t.Fatalf("Decide saw decision time %v, want server time in [%v,%v], not client %v", seen, before, after, clientDecided)
	}
}

func TestExpiryWindowUsesServerTimeNotClientTime(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("thing", "expiry")

	// The client claims a decision 4 minutes ago (inside the skew window) and
	// the invitation expired 1 minute ago. Against the client value it is
	// still valid; against the server clock it is not.
	cmd := &expiryCommand{
		at:      time.Now().UTC().Add(-4 * time.Minute),
		expires: time.Now().UTC().Add(-time.Minute),
	}
	decider := es.Decider[noEventState, *expiryCommand, noEvent]{
		Initial: func() noEventState { return noEventState{} },
		Decide: func(_ noEventState, c *expiryCommand) ([]noEvent, []es.ConstraintOp, error) {
			if !c.DecisionTime().Before(c.expires) {
				return nil, nil, errExpiryWindowClosed
			}
			return nil, nil, nil
		},
		Evolve: func(s noEventState, _ noEvent) noEventState { return s },
	}
	rt := aggregate.NewRuntime(store, decider, noEventCodec{})

	if _, err := rt.Handle(ctx, sid, cmd, es.Meta{}); !errors.Is(err, errExpiryWindowClosed) {
		t.Fatalf("error = %v, want expiry rejection against server time", err)
	}
}

var errExpiryWindowClosed = errors.New("expiry window closed")

type expiryCommand struct {
	at      time.Time
	expires time.Time
}

func (c *expiryCommand) DecisionTime() time.Time     { return c.at }
func (c *expiryCommand) SetDecisionTime(t time.Time) { c.at = t }

func TestDecisionTimeStampUsesSetterForNonProtoCommands(t *testing.T) {
	cmd := &settableDecisionTime{at: time.Now().Add(-time.Hour)}
	server := time.Now().UTC()
	if !es.StampDecisionTime(cmd, server) {
		t.Fatal("StampDecisionTime did not recognize a DecisionTimeSetter")
	}
	if !cmd.at.Equal(server) {
		t.Fatalf("decision time = %v, want %v", cmd.at, server)
	}
}

type settableDecisionTime struct{ at time.Time }

func (c *settableDecisionTime) DecisionTime() time.Time     { return c.at }
func (c *settableDecisionTime) SetDecisionTime(t time.Time) { c.at = t }

// TestUnstampableDecidedAtFails pins the loud failure: a command that
// implements es.DecidedAt but cannot have its decision time replaced —
// hand-written, no SetDecisionTime method, not a proto message — must be
// rejected by HandleAt, not silently run on the client's clock value.
func TestUnstampableDecidedAtFails(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("thing", "unstampable")

	decider := es.Decider[noEventState, *unstampableCommand, noEvent]{
		Initial: func() noEventState { return noEventState{} },
		Decide: func(_ noEventState, _ *unstampableCommand) ([]noEvent, []es.ConstraintOp, error) {
			return nil, nil, nil
		},
		Evolve: func(s noEventState, _ noEvent) noEventState { return s },
	}
	rt := aggregate.NewRuntime(store, decider, noEventCodec{})

	// An in-window decided_at, so the stamp is the only thing that can
	// fail — and would have, silently, before the runtime checked it.
	cmd := &unstampableCommand{at: time.Now().UTC().Add(-time.Minute)}
	_, err := rt.HandleAt(ctx, sid, cmd, es.Meta{}, 0)
	if !errors.Is(err, es.ErrDecidedAtUnstampable) {
		t.Fatalf("error = %v, want ErrDecidedAtUnstampable", err)
	}
	if !strings.Contains(err.Error(), "unstampableCommand") {
		t.Fatalf("error = %q, want it to name the command type", err)
	}
	// The rejection happens before any load or append: the stream stays empty.
	if _, v, lerr := rt.Load(ctx, sid); lerr != nil || v != 0 {
		t.Fatalf("version after rejection = %d (err %v), want 0 (nothing appended)", v, lerr)
	}
}

// unstampableCommand is the broken shape the runtime must catch: it
// reports a decision time but cannot have it replaced — it implements
// neither es.DecisionTimeSetter nor proto.Message, so
// es.StampDecisionTime returns false for it.
type unstampableCommand struct{ at time.Time }

func (c *unstampableCommand) DecisionTime() time.Time { return c.at }
