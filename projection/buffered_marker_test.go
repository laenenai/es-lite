package projection_test

import (
	"context"
	"errors"
	"testing"
	"time"

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
