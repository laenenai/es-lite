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
