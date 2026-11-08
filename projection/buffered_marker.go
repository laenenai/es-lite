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
	// MaxInterval is how long a dirty marker may go unflushed. The bound is
	// enforced from both sides: a background timer fires a Flush
	// MaxInterval after the first pending Save, and a Save made past the
	// interval flushes too. The timer runs on wall-clock time regardless of
	// an injected Now; Now only drives the Save-side trigger.
	MaxInterval time.Duration
	// Now reports the current time. Tests inject a fixed clock.
	Now func() time.Time
}

// BufferedMarker is a Marker wrapper that batches checkpoint saves: Save
// only records the position, and the inner marker is written when the save
// count reaches MaxCount or the position has been dirty for MaxInterval —
// whichever comes first. Checkpoints only need the highest position (ADR
// 0005), so buffering trades "a save per event" for "a save per batch"
// while keeping the crash window bounded by MaxInterval. The bound holds
// even when no further events arrive: a background timer, armed on the
// first pending Save and disarmed by the flush that drains the buffer,
// fires Flush(context.Background()) on its own goroutine until the inner
// marker is current. Close stops the timer and flushes on the way out of
// the process; Discard drops one projection's pending position for the
// reset-then-rebuild path.
//
// BufferedMarker never rewinds: a Save of a lower position than the one
// already pending is dropped. A failed flush keeps the pending position and
// returns the error; the next trigger (Save past MaxCount, the timer, Load,
// Flush) retries it. Load flushes first and never returns less than the
// highest position this marker buffered for the projection, so a caller
// reads its own writes. Flush is idempotent and a no-op when nothing is
// pending. It is safe for concurrent use.
type BufferedMarker struct {
	inner Marker
	opts  BufferedMarkerOptions

	mu         sync.Mutex
	pending    map[string]uint64 // projection -> unsaved highest position
	count      int               // Save calls since the last flush
	firstDirty time.Time         // zero when nothing is pending
	timer      *time.Timer       // idle flush timer; nil when disarmed
	closed     bool              // set by Close; a closed marker never re-arms
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
// reached. The first Save of a dirty window also arms the idle flush timer,
// so the position cannot outlive MaxInterval even if no further Save ever
// arrives. It returns the flush error if the triggered flush fails; until
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
		// firstDirty and make a later real Save flush early — and it must not
		// arm the timer either.
		return nil
	}
	now := m.opts.Now()
	if m.firstDirty.IsZero() {
		m.firstDirty = now
		// The dirty window just opened: start the countdown.
		m.armTimerLocked()
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

// Discard drops the pending position for one projection — the
// reset-then-rebuild path behind Runner.Rebuild's "reset the marker
// first": the operator resets the inner marker and the read model, and a
// pre-reset position still sitting pending would otherwise be flushed
// right after the reset and resurrect the marker above the freshly
// rebuilt state, skipping the rebuild. Other projections' pending
// positions are left alone.
//
// Resurrect-over-reset caveat: Discard drops only what is buffered at the
// moment it runs. It cannot retract a Save in flight on another goroutine:
// a live runner's advance, unaware of the reset, can record a position
// after Discard returns — and with the old pending floor gone, any
// position sticks — so the next flush persists it, undoing the reset.
// Quiesce the projection's writers (Run returned, no Apply in progress)
// before calling Discard.
func (m *BufferedMarker) Discard(projection string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pending, projection)
	if len(m.pending) == 0 {
		// The discarded projection was the only dirty one: disarm the
		// flush triggers so nothing fires with nothing to flush.
		m.count = 0
		m.firstDirty = time.Time{}
		m.stopTimerLocked()
	}
}

// Close is the shutdown call: it stops the flush timer and flushes whatever
// is pending, so the last window cannot outlive the process. The flush
// runs on context.Background(), so an already-cancelled run context cannot
// cut it short. Close returns the flush error, if any; a failed flush
// keeps its positions pending, and with the timer stopped they stay
// pending until the next Save, Load or Flush. Do not use the marker after
// Close — later Saves still buffer but never arm a timer again.
func (m *BufferedMarker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.stopTimerLocked()
	return m.flush(context.Background())
}

// timerFlush is what the idle timer fires, on the timer's own goroutine:
// the flush goes through the public method so it takes mu like every other
// path. When its own attempt fails with positions still pending, the
// spent timer is re-armed so the retry cadence stays bounded by
// MaxInterval instead of waiting for the next Save; armTimerLocked refuses
// once the marker is closed, so no timer can outlive Close.
func (m *BufferedMarker) timerFlush() {
	if err := m.Flush(context.Background()); err == nil {
		return
	}
	m.mu.Lock()
	if len(m.pending) > 0 {
		m.armTimerLocked()
	}
	m.mu.Unlock()
}

// armTimerLocked arms the idle flush timer: MaxInterval from now it fires
// timerFlush. Callers hold mu.
func (m *BufferedMarker) armTimerLocked() {
	if m.closed {
		return
	}
	m.stopTimerLocked()
	m.timer = time.AfterFunc(m.opts.MaxInterval, m.timerFlush)
}

// stopTimerLocked disarms the idle flush timer, if one is armed. Callers
// hold mu. A callback that already started still finishes — timer.Stop
// cannot recall it — but it runs serialized on mu and, once the marker is
// closed, cannot re-arm.
func (m *BufferedMarker) stopTimerLocked() {
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
}

// flush writes one inner.Save per pending projection. Callers hold mu.
// Projections that persisted are cleared; on the first failure it stops,
// leaving the failed position (and any not yet attempted) pending, and
// returns the error. Count and firstDirty reset only once everything
// persisted, so a failed flush keeps firing the retry triggers — and an
// armed timer stays armed: only a flush that drains the buffer (or empties
// it via Discard) disarms the timer.
func (m *BufferedMarker) flush(ctx context.Context) error {
	if len(m.pending) == 0 {
		// Nothing to persist is a completed flush: clear the triggers so an
		// empty flush cannot leave count/firstDirty armed forever.
		m.count = 0
		m.firstDirty = time.Time{}
		m.stopTimerLocked()
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
	m.stopTimerLocked()
	return nil
}
