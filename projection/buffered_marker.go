package projection

import (
	"context"
	"sync"
	"time"
)

// BufferedMarkerOptions configures a BufferedMarker. Zero fields take the
// defaults: MaxCount 100, MaxInterval 1s, Now time.Now.
type BufferedMarkerOptions struct {
	// MaxCount is the number of Save calls buffered before one is forced
	// through to the inner marker.
	MaxCount int
	// MaxInterval is how long a dirty marker may go unflushed; the next
	// Save after it elapses flushes.
	MaxInterval time.Duration
	// Now reports the current time. Tests inject a fixed clock.
	Now func() time.Time
}

// BufferedMarker is a Marker wrapper that batches checkpoint saves: Save
// only records the position, and the inner marker is written when the save
// count reaches MaxCount or the position has been dirty for MaxInterval —
// whichever comes first. Checkpoints only need the highest position (ADR
// 0005), so buffering trades "a save per event" for "a save per batch"
// while keeping the crash window bounded by MaxInterval.
//
// BufferedMarker never rewinds: a Save of a lower position than the one
// already pending is dropped. A failed flush keeps the pending position and
// returns the error; the next trigger (Save past MaxCount, elapsed
// interval, Load, Flush) retries it. Load flushes first and never returns
// less than the highest position this marker buffered for the projection,
// so a caller reads its own writes. Flush is idempotent and a no-op when
// nothing is pending. It is safe for concurrent use.
type BufferedMarker struct {
	inner Marker
	opts  BufferedMarkerOptions

	mu         sync.Mutex
	pending    map[string]uint64 // projection -> unsaved highest position
	count      int               // Save calls since the last flush
	firstDirty time.Time         // zero when nothing is pending
}

// NewBufferedMarker wraps inner with save batching.
func NewBufferedMarker(inner Marker, opts BufferedMarkerOptions) *BufferedMarker {
	if opts.MaxCount <= 0 {
		opts.MaxCount = 100
	}
	if opts.MaxInterval <= 0 {
		opts.MaxInterval = time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &BufferedMarker{
		inner:   inner,
		opts:    opts,
		pending: map[string]uint64{},
	}
}

// Load flushes buffered saves, then reads the position from the inner
// marker. The returned position is never lower than the highest position
// buffered for this projection, even if the inner marker lags.
func (m *BufferedMarker) Load(ctx context.Context, projection string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pending := m.pending[projection]
	if err := m.flush(ctx); err != nil {
		return 0, err
	}
	got, err := m.inner.Load(ctx, projection)
	if err != nil {
		return 0, err
	}
	if pending > got {
		return pending, nil
	}
	return got, nil
}

// Save records the position and flushes when MaxCount or MaxInterval is
// reached. It returns the flush error if the triggered flush fails; until
// a flush succeeds, the highest position stays pending and is retried.
func (m *BufferedMarker) Save(ctx context.Context, projection string, position uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, pending := m.pending[projection]
	switch {
	case position > prev:
		m.pending[projection] = position
	case position == 0 && !pending:
		// Zero is the marker's zero value and there is no pending write, so
		// this Save has nothing to persist. Counting it would drift count and
		// firstDirty and make a later real Save flush early.
		return nil
	}
	now := m.opts.Now()
	if m.firstDirty.IsZero() {
		m.firstDirty = now
	}
	m.count++
	if m.count >= m.opts.MaxCount || now.Sub(m.firstDirty) >= m.opts.MaxInterval {
		return m.flush(ctx)
	}
	return nil
}

// Flush persists every pending position now. With nothing pending it does
// nothing and returns nil.
func (m *BufferedMarker) Flush(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flush(ctx)
}

// flush writes one inner.Save per pending projection. Callers hold mu.
// Projections that persisted are cleared; on the first failure it stops,
// leaving the failed position (and any not yet attempted) pending, and
// returns the error. Count and firstDirty reset only once everything
// persisted, so a failed flush keeps firing the retry triggers.
func (m *BufferedMarker) flush(ctx context.Context) error {
	if len(m.pending) == 0 {
		// Nothing to persist is a completed flush: clear the triggers so an
		// empty flush cannot leave count/firstDirty armed forever.
		m.count = 0
		m.firstDirty = time.Time{}
		return nil
	}
	var err error
	for key, pos := range m.pending {
		if e := m.inner.Save(ctx, key, pos); e != nil {
			err = e
			break
		}
		delete(m.pending, key)
	}
	if err != nil {
		return err
	}
	m.count = 0
	m.firstDirty = time.Time{}
	return nil
}
