package projection_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

type recordingMarker struct {
	saved []struct {
		key string
		pos uint64
	}
	load uint64
	err  error
}

func (r *recordingMarker) Load(_ context.Context, _ string) (uint64, error) {
	return r.load, r.err
}

// Save records successful saves only: a failed save was not persisted, so
// it must not show up in saved.
func (r *recordingMarker) Save(_ context.Context, key string, pos uint64) error {
	if r.err != nil {
		return r.err
	}
	r.saved = append(r.saved, struct {
		key string
		pos uint64
	}{key, pos})
	return nil
}

func TestBufferedMarkerDefersUntilMaxCount(t *testing.T) {
	ctx := context.Background()
	inner := &recordingMarker{}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{MaxCount: 100, MaxInterval: time.Hour})
	for i := uint64(1); i <= 99; i++ {
		_ = m.Save(ctx, "space.authz", i)
	}
	if len(inner.saved) != 0 {
		t.Fatalf("flushed %d saves, want 0", len(inner.saved))
	}
	_ = m.Save(ctx, "space.authz", 100)
	if len(inner.saved) != 1 || inner.saved[0].pos != 100 {
		t.Fatalf("saved = %+v, want one save at 100", inner.saved)
	}
}

func TestBufferedMarkerFlushesAfterInterval(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(0, 0)
	inner := &recordingMarker{}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{MaxCount: 1000, MaxInterval: time.Second, Now: func() time.Time { return now }})
	_ = m.Save(ctx, "p", 1)
	now = now.Add(2 * time.Second)
	_ = m.Save(ctx, "p", 2)
	if len(inner.saved) != 1 || inner.saved[0].pos != 2 {
		t.Fatalf("saved = %+v, want one save at 2", inner.saved)
	}
}

func TestBufferedMarkerLoadFlushesPendingThenDelegates(t *testing.T) {
	ctx := context.Background()
	inner := &recordingMarker{load: 41}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{})
	_ = m.Save(ctx, "p", 42)
	got, err := m.Load(ctx, "p")
	if err != nil || got != 42 {
		t.Fatalf("Load = (%d,%v), want (42,nil)", got, err)
	}
	if len(inner.saved) != 1 {
		t.Fatalf("saves = %d, want 1", len(inner.saved))
	}
}

func TestBufferedMarkerFlushErrorSurfacesAndPendingIsRetried(t *testing.T) {
	ctx := context.Background()
	inner := &recordingMarker{err: errors.New("db down")}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{MaxCount: 2})
	_ = m.Save(ctx, "p", 1)
	if err := m.Save(ctx, "p", 2); err == nil {
		t.Fatal("second Save error = nil, want the flush error")
	}
	inner.err = nil
	if err := m.Flush(ctx); err != nil {
		t.Fatalf("Flush after recovery = %v", err)
	}
	if len(inner.saved) != 1 || inner.saved[0].pos != 2 {
		t.Fatalf("saved = %+v, want one save at 2", inner.saved)
	}
}

func TestBufferedMarkerKeepsTheHighestPendingPosition(t *testing.T) {
	ctx := context.Background()
	inner := &recordingMarker{}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{MaxCount: 2})
	_ = m.Save(ctx, "p", 9)
	_ = m.Save(ctx, "p", 7) // must not rewind the pending position
	if inner.saved[0].pos != 9 {
		t.Fatalf("saved position = %d, want 9", inner.saved[0].pos)
	}
}

func TestBufferedMarkerEmptyFlushIsANoOp(t *testing.T) {
	ctx := context.Background()
	inner := &recordingMarker{}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{MaxCount: 2, MaxInterval: time.Hour})
	if err := m.Flush(ctx); err != nil {
		t.Fatalf("Flush on empty = %v, want nil", err)
	}
	if len(inner.saved) != 0 {
		t.Fatalf("empty Flush saved %+v, want nothing", inner.saved)
	}
	// An empty flush must also leave the count disarmed: one real Save below
	// MaxCount is still deferred.
	_ = m.Save(ctx, "p", 1)
	if len(inner.saved) != 0 {
		t.Fatalf("Save after empty Flush flushed early: %+v", inner.saved)
	}
}

// Save(0) with no pending checkpoint has nothing to persist. It must not
// advance count or firstDirty; otherwise a real position saved later flushes
// immediately (old bug: zero saves armed the triggers forever).
func TestBufferedMarkerZeroPositionSavesDoNotArmFlushTriggers(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(0, 0)
	inner := &recordingMarker{}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{
		MaxCount: 1000, MaxInterval: time.Second,
		Now: func() time.Time { return now },
	})
	_ = m.Save(ctx, "p", 0)
	_ = m.Save(ctx, "p", 0)
	now = now.Add(2 * time.Second) // past MaxInterval from the zero saves
	_ = m.Save(ctx, "p", 1)
	if len(inner.saved) != 0 {
		t.Fatalf("Save(1) flushed immediately after Save(0)s: %+v", inner.saved)
	}
	if err := m.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(inner.saved) != 1 || inner.saved[0].pos != 1 {
		t.Fatalf("saved = %+v, want one save at 1", inner.saved)
	}
}

// partialMarker fails Save on one chosen call. It records successful saves only.
type partialMarker struct {
	calls    int
	failCall int // 1-based call that fails; 0 never fails
	err      error
	saved    []struct {
		key string
		pos uint64
	}
}

func (p *partialMarker) Load(context.Context, string) (uint64, error) { return 0, nil }

func (p *partialMarker) Save(_ context.Context, key string, pos uint64) error {
	p.calls++
	if p.failCall != 0 && p.calls == p.failCall {
		return p.err
	}
	p.saved = append(p.saved, struct {
		key string
		pos uint64
	}{key, pos})
	return nil
}

// A partial flush persists the keys before the failure and leaves the failed
// key (and anything unattempted) pending for the retry.
func TestBufferedMarkerPartialFlushFailureKeepsTheFailedKeyPending(t *testing.T) {
	ctx := context.Background()
	flushErr := errors.New("db down")
	inner := &partialMarker{failCall: 2, err: flushErr}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{MaxCount: 1000, MaxInterval: time.Hour})
	_ = m.Save(ctx, "a", 1)
	_ = m.Save(ctx, "b", 2)

	if err := m.Flush(ctx); !errors.Is(err, flushErr) {
		t.Fatalf("Flush = %v, want the inner error", err)
	}
	if len(inner.saved) != 1 {
		t.Fatalf("saved = %+v, want exactly one key before the failure", inner.saved)
	}
	succeeded := inner.saved[0].key

	inner.failCall = 0 // recovery
	if err := m.Flush(ctx); err != nil {
		t.Fatalf("Flush after recovery = %v", err)
	}
	if len(inner.saved) != 2 {
		t.Fatalf("saved = %+v, want two keys after the retry", inner.saved)
	}
	if inner.saved[1].key == succeeded {
		t.Fatalf("saved = %+v: succeeded key %q was written twice", inner.saved, succeeded)
	}
	seen := map[string]bool{}
	for _, s := range inner.saved {
		seen[s.key] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("saved = %+v, want both keys", inner.saved)
	}
}

// saveRecord is one inner-marker save observed by a channelMarker.
type saveRecord struct {
	key string
	pos uint64
}

// channelMarker hands every Save to the test on a channel. The channel
// gives the assertions a happens-before edge over the flush timer's
// goroutine: reading a plain recordingMarker from the test goroutine would
// race the timer's background flush.
type channelMarker struct {
	saved chan saveRecord
}

func (c *channelMarker) Load(context.Context, string) (uint64, error) { return 0, nil }

func (c *channelMarker) Save(_ context.Context, key string, pos uint64) error {
	c.saved <- saveRecord{key: key, pos: pos}
	return nil
}

// TestBufferedMarkerFlushesWhenIdle pins the idle half of the MaxInterval
// contract: with no further Saves — the feed quiet, the last event long
// applied — the position buffered by a lone Save must still reach the inner
// marker within MaxInterval, not sit pending until the next event or
// forever.
func TestBufferedMarkerFlushesWhenIdle(t *testing.T) {
	inner := &channelMarker{saved: make(chan saveRecord)}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{
		MaxCount: 1000, MaxInterval: 20 * time.Millisecond,
	})
	if err := m.Save(context.Background(), "p", 42); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// MaxInterval promises a bound, not a deadline: wait generously past
	// 20ms so a loaded CI machine cannot flake the timer.
	select {
	case got := <-inner.saved:
		if got.key != "p" || got.pos != 42 {
			t.Fatalf("inner save = %+v, want p@42", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no further Save and MaxInterval long over, yet the pending position never reached the inner marker — the idle flush timer is missing")
	}
}

// TestRunFlushesOnReturn pins Run's exit flush: the last window — a Save
// that never reaches MaxCount and is followed by no further event — must be
// persisted when Run returns, or the crash window stays open until the
// process exits without ever persisting.
func TestRunFlushesOnReturn(t *testing.T) {
	ctx := context.Background()
	inner := &recordingMarker{}
	marker := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{
		MaxCount: 1000, MaxInterval: time.Hour,
	})
	r := &projection.Runner{
		Name:     "counter.view",
		Marker:   marker,
		Dispatch: func(context.Context, es.Envelope) error { return nil },
	}
	feed := projection.FeedFunc(func(ctx context.Context, handle func(context.Context, es.Envelope) error) error {
		return handle(ctx, es.Envelope{GlobalPosition: 7, TypeURL: "counter.v1.Incremented"})
	})
	if err := r.Run(ctx, feed); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(inner.saved) != 1 || inner.saved[0].key != "counter.view" || inner.saved[0].pos != 7 {
		t.Fatalf("inner saves = %+v, want one save at counter.view@7: Run must flush its last window on return", inner.saved)
	}
}

// TestDiscardPreventsResurrect pins Discard, the reset-then-rebuild path:
// a position buffered before an operator's reset must never be flushed
// after it — the stale 42 would resurrect the marker above the freshly
// rebuilt read model and skip the rebuild. After Discard the projection
// also starts clean: the next Save buffers from scratch.
func TestDiscardPreventsResurrect(t *testing.T) {
	ctx := context.Background()
	inner := &recordingMarker{}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{
		MaxCount: 1000, MaxInterval: time.Hour,
	})
	if err := m.Save(ctx, "p", 42); err != nil {
		t.Fatalf("Save: %v", err)
	}
	m.Discard("p")
	// Any flush path — the timer's or an explicit Flush — must not write p.
	if err := m.Flush(ctx); err != nil {
		t.Fatalf("Flush after Discard: %v", err)
	}
	if len(inner.saved) != 0 {
		t.Fatalf("inner saves = %+v, want none: the discarded position was resurrected by the flush", inner.saved)
	}
	// Discard drops state, not the projection: a fresh Save buffers
	// normally, starting from the rebuilt state rather than 42.
	if err := m.Save(ctx, "p", 5); err != nil {
		t.Fatalf("Save after Discard: %v", err)
	}
	if err := m.Flush(ctx); err != nil {
		t.Fatalf("Flush after the fresh Save: %v", err)
	}
	if len(inner.saved) != 1 || inner.saved[0].key != "p" || inner.saved[0].pos != 5 {
		t.Fatalf("inner saves = %+v, want exactly one save at p@5", inner.saved)
	}
}

// TestBufferedMarkerDiscardDropsOnlyOneProjection pins the "one projection"
// in Discard's contract: a reset-then-rebuild resets one read model, so the
// other projections' pending positions must survive the Discard and flush
// as usual.
func TestBufferedMarkerDiscardDropsOnlyOneProjection(t *testing.T) {
	ctx := context.Background()
	inner := &recordingMarker{}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{
		MaxCount: 1000, MaxInterval: time.Hour,
	})
	_ = m.Save(ctx, "reset", 42)
	_ = m.Save(ctx, "live", 7)
	m.Discard("reset")
	if err := m.Flush(ctx); err != nil {
		t.Fatalf("Flush after Discard: %v", err)
	}
	if len(inner.saved) != 1 || inner.saved[0].key != "live" || inner.saved[0].pos != 7 {
		t.Fatalf("inner saves = %+v, want only live@7", inner.saved)
	}
}

// TestBufferedMarkerCloseFlushesAndStopsTheTimer pins Close: it flushes
// what is pending, and the stopped timer must never fire again — no further
// inner writes, no wake-ups leaking out of a shut-down marker. Close on an
// empty marker is a clean nil.
func TestBufferedMarkerCloseFlushesAndStopsTheTimer(t *testing.T) {
	ctx := context.Background()
	inner := &channelMarker{saved: make(chan saveRecord, 1)}
	m := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{
		MaxCount: 1000, MaxInterval: 20 * time.Millisecond,
	})
	if err := m.Save(ctx, "p", 9); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case got := <-inner.saved:
		if got.key != "p" || got.pos != 9 {
			t.Fatalf("inner save = %+v, want p@9", got)
		}
	default:
		t.Fatal("Close did not flush the pending position")
	}
	// The timer is stopped: nothing may arrive after MaxInterval elapses.
	select {
	case got := <-inner.saved:
		t.Fatalf("inner save %+v after Close: the flush timer fired after Close", got)
	case <-time.After(100 * time.Millisecond):
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close on an empty marker = %v, want nil", err)
	}
}
