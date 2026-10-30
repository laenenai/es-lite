package aggregate_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// racingStore wraps a Store and, before delegating each of the first
// `races` Appends, lets a competing runtime append to the same stream — so
// the wrapped Append loses a REAL version race (the store's own check), not
// a simulated one.
type racingStore struct {
	es.Store
	races    int32 // remaining competing appends to inject
	attempts atomic.Int32
	compete  func()
}

func (r *racingStore) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	r.attempts.Add(1)
	if atomic.LoadInt32(&r.races) > 0 {
		atomic.AddInt32(&r.races, -1)
		r.compete()
	}
	return r.Store.Append(ctx, p)
}

func newRacing(t *testing.T, races int32) (*racingStore, *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]) {
	t.Helper()
	ctx := context.Background()
	base := openStore(t)
	sid, _ := es.NewStreamID("counter", "main")
	rival := aggregate.NewRuntime(base, counter.Decider, counterv1.EventCodec{})
	if _, err := rival.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 1000, Initial: 0}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	rs := &racingStore{Store: base, races: races}
	rs.compete = func() {
		if _, err := rival.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 100}), es.Meta{}); err != nil {
			t.Errorf("rival increment: %v", err)
		}
	}
	return rs, nil
}

func TestConflictRetrySucceedsWithinBudget(t *testing.T) {
	ctx := context.Background()
	rs, _ := newRacing(t, 1)
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(rs, counter.Decider, counterv1.EventCodec{}).
		WithConflictRetry(3, func(int) time.Duration { return 0 })

	res, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{})
	if err != nil {
		t.Fatalf("handle with retry: %v", err)
	}
	// First attempt lost to the rival's +100 (v2); the retry reloaded and
	// appended at v3 on top of fresh state.
	if res.ToVersion != 3 || res.State.GetCount() != 101 {
		t.Fatalf("after retry: version=%d count=%d, want 3/101", res.ToVersion, res.State.GetCount())
	}
	if got := rs.attempts.Load(); got != 2 {
		t.Fatalf("append attempts = %d, want 2", got)
	}
	// Every attempt carried the same command id.
	for _, env := range res.Envelopes {
		if env.CommandID != res.Envelopes[0].CommandID {
			t.Fatalf("command id changed across attempts")
		}
	}
}

func TestConflictRetryExhausted(t *testing.T) {
	ctx := context.Background()
	rs, _ := newRacing(t, 100) // always loses
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(rs, counter.Decider, counterv1.EventCodec{}).
		WithConflictRetry(2, func(int) time.Duration { return 0 })

	_, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{})
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("exhausted retry error = %v, want ErrConflict", err)
	}
	if got := rs.attempts.Load(); got != 3 {
		t.Fatalf("append attempts = %d, want 3 (1 + 2 retries)", got)
	}
}

func TestConflictRetryDefaultOff(t *testing.T) {
	ctx := context.Background()
	rs, _ := newRacing(t, 1)
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(rs, counter.Decider, counterv1.EventCodec{})

	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{}); !errors.Is(err, es.ErrConflict) {
		t.Fatalf("no-retry error = %v, want ErrConflict", err)
	}
	if got := rs.attempts.Load(); got != 1 {
		t.Fatalf("append attempts = %d, want 1", got)
	}
}

func TestConflictRetryOnlyRetriesConflict(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{}).
		WithConflictRetry(5, func(int) time.Duration { return time.Hour })

	// A domain rejection must return immediately — no backoff, no retry.
	start := time.Now()
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{}); !errors.Is(err, counter.ErrNotInitialized) {
		t.Fatalf("error = %v, want ErrNotInitialized", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("domain rejection waited on the retry backoff")
	}
}

func TestConflictRetryHonoursContext(t *testing.T) {
	rs, _ := newRacing(t, 100)
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(rs, counter.Decider, counterv1.EventCodec{}).
		WithConflictRetry(5, func(int) time.Duration { return time.Hour })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("ctx cancellation did not cut the backoff short")
	}
}

func TestDefaultBackoffBounds(t *testing.T) {
	for attempt := 1; attempt <= 12; attempt++ {
		for i := 0; i < 50; i++ {
			d := aggregate.DefaultBackoff(attempt)
			if d < 0 || d > 500*time.Millisecond {
				t.Fatalf("attempt %d: backoff %v outside [0, 500ms]", attempt, d)
			}
		}
	}
	if d := aggregate.DefaultBackoff(1); d > 10*time.Millisecond {
		t.Fatalf("attempt 1: backoff %v > 10ms", d)
	}
}

func initCounter(t *testing.T) (*aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event], es.StreamID) {
	t.Helper()
	store := openStore(t)
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(store, counter.Decider, counterv1.EventCodec{})
	if _, err := rt.Handle(context.Background(), sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 1}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	return rt, sid
}

func TestDecidedAtSourcesOccurredAt(t *testing.T) {
	ctx := context.Background()
	rt, sid := initCounter(t)
	decided := time.Date(2026, 3, 3, 10, 0, 0, 123456000, time.UTC) // µs precision survives every backend

	res, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{DecidedAt: timestamppb.New(decided)}), es.Meta{})
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(res.Envelopes) != 1 || !res.Envelopes[0].OccurredAt.Equal(decided) {
		t.Fatalf("OccurredAt = %v, want decided_at %v", res.Envelopes[0].OccurredAt, decided)
	}
}

func TestDecidedAtAgreesWithMeta(t *testing.T) {
	ctx := context.Background()
	rt, sid := initCounter(t)
	decided := time.Date(2026, 3, 3, 10, 0, 0, 0, time.UTC)

	// Setting both to the same instant is fine.
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{DecidedAt: timestamppb.New(decided)}),
		es.Meta{OccurredAt: decided.In(time.FixedZone("x", 3600))}); err != nil {
		t.Fatalf("close with agreeing meta: %v", err)
	}
}

func TestDecidedAtMismatch(t *testing.T) {
	ctx := context.Background()
	rt, sid := initCounter(t)
	decided := time.Date(2026, 3, 3, 10, 0, 0, 0, time.UTC)

	_, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{DecidedAt: timestamppb.New(decided)}),
		es.Meta{OccurredAt: decided.Add(time.Second)})
	if !errors.Is(err, es.ErrDecidedAtMismatch) {
		t.Fatalf("error = %v, want ErrDecidedAtMismatch", err)
	}
	// Nothing was appended.
	if _, v, err := rt.Load(ctx, sid); err != nil || v != 1 {
		t.Fatalf("version after mismatch = %d (err %v), want 1", v, err)
	}
}

func TestDecidedAtMissing(t *testing.T) {
	ctx := context.Background()
	rt, sid := initCounter(t)

	_, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{}), es.Meta{})
	if !errors.Is(err, es.ErrDecidedAtMissing) {
		t.Fatalf("error = %v, want ErrDecidedAtMissing", err)
	}
	if _, v, err := rt.Load(ctx, sid); err != nil || v != 1 {
		t.Fatalf("version after missing = %d (err %v), want 1", v, err)
	}
}
