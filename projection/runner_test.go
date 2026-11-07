package projection_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

// countingMarker is a Marker that counts loads, so the tests can pin that a
// Runner reads its marker once per process rather than per event.
type countingMarker struct {
	loads int
	pos   uint64
}

func (m *countingMarker) Load(_ context.Context, _ string) (uint64, error) {
	m.loads++
	return m.pos, nil
}

func (m *countingMarker) Save(_ context.Context, _ string, position uint64) error {
	m.pos = position
	return nil
}

func dispatchRecorder(seen *[]uint64) func(context.Context, es.Envelope) error {
	return func(_ context.Context, e es.Envelope) error {
		*seen = append(*seen, e.GlobalPosition)
		return nil
	}
}

// TestRunnerSkipsAtOrBelowMarker pins the idempotence boundary: an envelope at
// a position <= the marker is acknowledged without dispatch, including one at
// exactly the marker (the <= in apply is load-bearing — a strict < would
// redeliver the marker event on every cutover).
func TestRunnerSkipsAtOrBelowMarker(t *testing.T) {
	ctx := context.Background()
	marker := projection.NewMemoryMarker()
	if err := marker.Save(ctx, "counter.view", 5); err != nil {
		t.Fatalf("seed marker: %v", err)
	}
	var seen []uint64
	r := &projection.Runner{
		Name:     "counter.view",
		Marker:   marker,
		Dispatch: dispatchRecorder(&seen),
	}
	err := r.Apply(ctx, []es.Envelope{
		{GlobalPosition: 4, TypeURL: "counter.v1.Incremented"}, // below the marker
		{GlobalPosition: 5, TypeURL: "counter.v1.Incremented"}, // exactly the marker
		{GlobalPosition: 6, TypeURL: "counter.v1.Incremented"}, // above the marker
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{6}) {
		t.Fatalf("dispatched %v, want only position 6 (positions <= 5 skipped)", seen)
	}
	pos, err := r.Position(ctx)
	if err != nil {
		t.Fatalf("Position: %v", err)
	}
	if pos != 6 {
		t.Fatalf("Position = %d, want 6", pos)
	}
}

// TestRunnerFilteredEnvelopeAdvancesMarker pins the filtered path: an
// envelope the Filter rejects is never dispatched, but it still advances the
// marker — otherwise every non-consumed event in the log would be re-read
// forever.
func TestRunnerFilteredEnvelopeAdvancesMarker(t *testing.T) {
	ctx := context.Background()
	var seen []uint64
	r := &projection.Runner{
		Name:     "counter.view",
		Filter:   func(e es.Envelope) bool { return e.TypeURL == "counter.v1.Incremented" },
		Dispatch: dispatchRecorder(&seen),
	}
	err := r.Apply(ctx, []es.Envelope{
		{GlobalPosition: 1, TypeURL: "other.v1.Unrelated"},
		{GlobalPosition: 2, TypeURL: "counter.v1.Incremented"},
		{GlobalPosition: 3, TypeURL: "other.v1.Unrelated"},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{2}) {
		t.Fatalf("dispatched %v, want only position 2", seen)
	}
	pos, err := r.Position(ctx)
	if err != nil {
		t.Fatalf("Position: %v", err)
	}
	if pos != 3 {
		t.Fatalf("Position = %d, want 3 (filtered envelopes still advance the marker)", pos)
	}
}

// TestRunnerNilFilterDispatchesEverything pins the Filter contract: nil means
// "dispatch everything" — a LogFeed delivers the whole log, so a projector
// consuming all of it needs no filter to see every envelope.
func TestRunnerNilFilterDispatchesEverything(t *testing.T) {
	ctx := context.Background()
	var seen []uint64
	r := &projection.Runner{
		Name:     "counter.view",
		Dispatch: dispatchRecorder(&seen), // Filter deliberately nil
	}
	err := r.Apply(ctx, []es.Envelope{
		{GlobalPosition: 1, TypeURL: "a.v1.X"},
		{GlobalPosition: 2, TypeURL: "b.v1.Y"},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{1, 2}) {
		t.Fatalf("dispatched %v, want both positions with a nil Filter", seen)
	}
	pos, err := r.Position(ctx)
	if err != nil {
		t.Fatalf("Position: %v", err)
	}
	if pos != 2 {
		t.Fatalf("Position = %d, want 2", pos)
	}
}

// TestRunnerRunReturnsContextErrorOnCancel pins Run's cancellation return: a
// feed that ends with context.Canceled once ctx dies must surface as ctx.Err()
// itself — here DeadlineExceeded, not the feed's Canceled — so the caller can
// distinguish "the context ended" from any other error shape.
func TestRunnerRunReturnsContextErrorOnCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	r := &projection.Runner{Name: "counter.view", Dispatch: func(context.Context, es.Envelope) error { return nil }}
	feed := projection.FeedFunc(func(ctx context.Context, _ func(context.Context, es.Envelope) error) error {
		<-ctx.Done() // the cancellation a live feed reports when ctx ends
		return context.Canceled
	})
	if err := r.Run(ctx, feed); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want ctx.Err() (DeadlineExceeded), not the feed's error", err)
	}
}

// TestRunnerPositionLoadsMarkerOnce pins Position's loading: it reads the
// marker from the Marker (not from thin air), and the load happens once per
// process — later calls and applies are served from the cached position.
func TestRunnerPositionLoadsMarkerOnce(t *testing.T) {
	ctx := context.Background()
	m := &countingMarker{pos: 7}
	var seen []uint64
	r := &projection.Runner{
		Name:     "counter.view",
		Marker:   m,
		Dispatch: dispatchRecorder(&seen),
	}
	pos, err := r.Position(ctx)
	if err != nil {
		t.Fatalf("Position: %v", err)
	}
	if pos != 7 {
		t.Fatalf("Position = %d, want the loaded marker 7", pos)
	}
	if m.loads != 1 {
		t.Fatalf("marker loads = %d, want 1", m.loads)
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 8, TypeURL: "counter.v1.Incremented"}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	pos, err = r.Position(ctx)
	if err != nil {
		t.Fatalf("Position after apply: %v", err)
	}
	if pos != 8 {
		t.Fatalf("Position = %d, want 8", pos)
	}
	if m.loads != 1 {
		t.Fatalf("marker loads = %d, want 1 (loaded once per process)", m.loads)
	}
}

// sliceSource serves a fixed, in-order slice of envelopes as a Source: each
// ReadAll returns the envelopes above fromPosition, up to limit.
type sliceSource []es.Envelope

func (s sliceSource) ReadAll(_ context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	var out []es.Envelope
	for _, e := range s {
		if e.GlobalPosition > fromPosition && (limit <= 0 || len(out) < limit) {
			out = append(out, e)
		}
	}
	return out, nil
}

// retryCall is one observation collected by an OnRetry hook.
type retryCall struct {
	attempt int
	env     es.Envelope
	err     error
	at      time.Time
}

// TestLogFeedReportsRetries pins the retry signal: a handler that fails twice
// then succeeds drives OnRetry with attempt 1 then 2, each call carrying the
// envelope under retry and the error the handler returned, fired before the
// backoff sleep; the observed delays stay within [Poll*2^(attempt-1),
// MaxBackoff].
func TestLogFeedReportsRetries(t *testing.T) {
	errFirst := errors.New("transient: first")
	errSecond := errors.New("transient: second")
	src := sliceSource{{GlobalPosition: 1, TypeURL: "counter.v1.Incremented", Payload: []byte{7}}}
	ctx, cancel := context.WithCancel(context.Background())

	var (
		handled  []es.Envelope
		handleAt []time.Time
		retries  []retryCall
	)
	f := &projection.LogFeed{
		Source:     src,
		Poll:       10 * time.Millisecond,
		MaxBackoff: 40 * time.Millisecond,
		OnRetry: func(_ context.Context, e es.Envelope, err error, attempt int) {
			retries = append(retries, retryCall{attempt: attempt, env: e, err: err, at: time.Now()})
		},
	}
	err := f.Feed(ctx, func(_ context.Context, e es.Envelope) error {
		handled = append(handled, e)
		handleAt = append(handleAt, time.Now())
		switch len(handleAt) {
		case 1:
			return errFirst
		case 2:
			return errSecond
		default:
			cancel() // the delivery succeeded; end the feed's wait
			return nil
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Feed = %v, want context.Canceled after the succeeded delivery", err)
	}
	if len(handleAt) != 3 {
		t.Fatalf("handler called %d times, want 3 (fail, fail, succeed)", len(handleAt))
	}
	if len(retries) != 2 {
		t.Fatalf("OnRetry called %d times, want 2", len(retries))
	}
	wantErrs := []error{errFirst, errSecond}
	for i, r := range retries {
		if r.attempt != i+1 {
			t.Fatalf("OnRetry attempt = %d, want %d", r.attempt, i+1)
		}
		if !reflect.DeepEqual(r.env, handled[i]) {
			t.Fatalf("OnRetry envelope = %+v, want the envelope the handler saw: %+v", r.env, handled[i])
		}
		if r.err != wantErrs[i] {
			t.Fatalf("OnRetry error = %v, want the handler's error %v", r.err, wantErrs[i])
		}
		// The hook fires before the sleep: the gap from the failed attempt
		// to the hook must be dwarfed by the wait from the hook to the next
		// attempt. (A hook after the sleep inverts this.)
		hookGap := r.at.Sub(handleAt[i])
		restGap := handleAt[i+1].Sub(r.at)
		if hookGap >= restGap {
			t.Fatalf("attempt %d: OnRetry %v after the failed attempt but the next attempt only %v later — the hook must fire before the backoff sleep", i+1, hookGap, restGap)
		}
	}
	// The delays: attempt 1 backs off Poll, attempt 2 doubles it, and neither
	// may exceed MaxBackoff. The bounds have headroom so a slow machine
	// cannot flake them; a missing backoff cannot pass the lower bounds.
	delays := []time.Duration{
		handleAt[1].Sub(handleAt[0]),
		handleAt[2].Sub(handleAt[1]),
	}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	for i, d := range delays {
		if d < want[i] {
			t.Fatalf("attempt %d delay = %v, want at least %v", i+1, d, want[i])
		}
		if d > 40*time.Millisecond {
			t.Fatalf("attempt %d delay = %v, exceeds MaxBackoff 40ms", i+1, d)
		}
	}
}

// TestLogFeedRetryBackoffIsCapped pins the cap: the backoff doubles from Poll
// per attempt until it reaches MaxBackoff, then holds there — attempts 3 and
// 4 both wait MaxBackoff (12ms), where an uncapped doubling would sleep 20ms
// and 40ms.
func TestLogFeedRetryBackoffIsCapped(t *testing.T) {
	src := sliceSource{{GlobalPosition: 1, TypeURL: "counter.v1.Incremented"}}
	ctx, cancel := context.WithCancel(context.Background())

	var (
		handleAt []time.Time
		attempts []int
	)
	f := &projection.LogFeed{
		Source:     src,
		Poll:       5 * time.Millisecond,
		MaxBackoff: 12 * time.Millisecond,
		OnRetry: func(_ context.Context, _ es.Envelope, _ error, attempt int) {
			attempts = append(attempts, attempt)
		},
	}
	err := f.Feed(ctx, func(_ context.Context, _ es.Envelope) error {
		handleAt = append(handleAt, time.Now())
		if len(handleAt) < 5 {
			return errors.New("poison")
		}
		cancel() // the delivery succeeded; end the feed's wait
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Feed = %v, want context.Canceled after the succeeded delivery", err)
	}
	if len(handleAt) != 5 {
		t.Fatalf("handler called %d times, want 5 (four failures then success)", len(handleAt))
	}
	if !reflect.DeepEqual(attempts, []int{1, 2, 3, 4}) {
		t.Fatalf("OnRetry attempts = %v, want [1 2 3 4]", attempts)
	}
	// want[i] is the delay after failed attempt i+1: Poll doubling — 5ms,
	// 10ms — then capped at MaxBackoff from attempt 3 on. The lower bounds
	// hold because a timer never fires early; the upper bounds are the cap's
	// teeth, sized to catch uncapped doubling (20ms, 40ms) with room for
	// scheduler stalls.
	want := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 12 * time.Millisecond, 12 * time.Millisecond}
	ceil := []time.Duration{15 * time.Millisecond, 20 * time.Millisecond, 19 * time.Millisecond, 23 * time.Millisecond}
	for i := range want {
		d := handleAt[i+1].Sub(handleAt[i])
		if d < want[i] {
			t.Fatalf("attempt %d delay = %v, want at least %v", i+1, d, want[i])
		}
		if d > ceil[i] {
			t.Fatalf("attempt %d delay = %v, want at most %v (backoff capped at MaxBackoff 12ms)", i+1, d, ceil[i])
		}
	}
}

// TestLogFeedNilOnRetryIsSilent pins the zero values: a LogFeed with no
// OnRetry retries exactly as before — silently, without a panic — and a zero
// MaxBackoff means the default cap, so the backoff still happens rather than
// collapsing to a busy loop.
func TestLogFeedNilOnRetryIsSilent(t *testing.T) {
	src := sliceSource{{GlobalPosition: 1, TypeURL: "counter.v1.Incremented"}}
	ctx, cancel := context.WithCancel(context.Background())

	var at []time.Time
	f := &projection.LogFeed{
		Source: src,
		Poll:   time.Millisecond,
		// OnRetry deliberately nil; MaxBackoff deliberately zero (default).
	}
	err := f.Feed(ctx, func(_ context.Context, _ es.Envelope) error {
		at = append(at, time.Now())
		if len(at) == 1 {
			return errors.New("transient")
		}
		cancel() // the delivery succeeded; end the feed's wait
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Feed = %v, want context.Canceled after the succeeded delivery", err)
	}
	if len(at) != 2 {
		t.Fatalf("handler called %d times, want 2 (one silent retry)", len(at))
	}
	d := at[1].Sub(at[0])
	if d < time.Millisecond {
		t.Fatalf("retry delay = %v, want at least Poll (1ms): a zero MaxBackoff means the default cap, not no backoff", d)
	}
	if d > 20*time.Millisecond {
		t.Fatalf("retry delay = %v, want at most 20ms", d)
	}
}
