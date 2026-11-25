package projection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/laenenai/es-lite/es"
)

// Feed is a live source of envelopes (ADR 0030 §2). Feed delivers each
// envelope to handle, in per-stream order, until ctx ends; a nil return
// means "applied, acknowledge" and an error means "not applied — redeliver
// later". eventfeed implements it over the event feed; LogFeed implements
// it by tailing the log directly.
type Feed interface {
	Feed(ctx context.Context, handle func(ctx context.Context, e es.Envelope) error) error
}

// FeedFunc adapts a function to Feed.
type FeedFunc func(ctx context.Context, handle func(ctx context.Context, e es.Envelope) error) error

// Feed implements Feed.
func (f FeedFunc) Feed(ctx context.Context, handle func(ctx context.Context, e es.Envelope) error) error {
	return f(ctx, handle)
}

// LogFeed is a Feed over the event log itself: it pages Source.ReadAll from
// From (exclusive) in global order and polls every Poll for more. A handler
// error is retried on the same envelope — at-least-once, in order — after a
// backoff that starts at Poll and doubles per attempt, capped at MaxBackoff;
// attempts are unbounded, so a poison event blocks the stream forever, but
// each retry is reported through OnRetry when one is set. From is normally
// the runner's marker; LogFeed itself keeps no checkpoint.
type LogFeed struct {
	Source Source
	From   uint64
	Batch  int           // default 500
	Poll   time.Duration // default 50ms
	// OnRetry is called before every retry backoff with the envelope that
	// failed, the error the handler returned for it and the attempt number
	// (1 = the first retry of the envelope). It is the retry loop's only
	// signal: without it a poison event blocks the stream silently. Nil
	// keeps the loop silent.
	OnRetry func(ctx context.Context, e es.Envelope, err error, attempt int)
	// MaxBackoff caps the retry backoff. Zero means 5s, so a bare LogFeed
	// keeps the documented cadence with the doubling bounded.
	MaxBackoff time.Duration
}

// Feed implements Feed. It returns ctx.Err() when ctx ends, and a Source
// error as-is.
func (f *LogFeed) Feed(ctx context.Context, handle func(ctx context.Context, e es.Envelope) error) error {
	batch, poll := f.Batch, f.Poll
	if batch <= 0 {
		batch = 500
	}
	if poll <= 0 {
		poll = 50 * time.Millisecond
	}
	max := f.MaxBackoff
	if max <= 0 {
		max = 5 * time.Second
	}
	pos := f.From
	for {
		evs, err := f.Source.ReadAll(ctx, pos, batch)
		if err != nil {
			return err
		}
		for _, e := range evs {
			attempt := 1
			delay := poll
			if delay > max {
				delay = max
			}
			for {
				herr := handle(ctx, e)
				if herr == nil {
					break
				}
				if f.OnRetry != nil {
					f.OnRetry(ctx, e, herr, attempt)
				}
				if serr := sleep(ctx, delay); serr != nil {
					return serr
				}
				attempt++
				if delay < max {
					// Double toward the cap; a wrapped product (a
					// MaxBackoff beyond int64's reach) pins at the cap
					// instead of sleeping a negative duration.
					delay *= 2
					if delay > max || delay <= 0 {
						delay = max
					}
				}
			}
			pos = e.GlobalPosition
		}
		if len(evs) == batch {
			continue
		}
		if err := sleep(ctx, poll); err != nil {
			return err
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Runner is the generated projector's engine (ADR 0030 §2): the marker
// load/skip/save, the live loop and the rebuild path, written once. The
// generated code supplies Name, Dispatch and Filter; the application
// supplies the Marker or the scoped ProgressStore. Live and rebuild share one
// apply path, so they cannot drift (ADR 0005).
type Runner struct {
	// Name is the spec name: the marker key and the durable name.
	Name string
	// Scope names the workspace a ProgressStore row belongs to (ADR 0045).
	// It is unused on the legacy Marker path.
	Scope string
	// Marker persists the highest applied global_position. Nil means an
	// in-memory marker — fine for tests, wrong for anything durable. It is
	// unused when ProgressStore is set.
	Marker Marker
	// ProgressStore, when set, persists a scoped checkpoint — position and
	// state — instead of the single-value Marker. Load seeds the runner's
	// position and state from it, every applied batch saves through it
	// write-through, and Rebuild marks it rebuilding until the replay
	// succeeds. Nil keeps the legacy Marker path.
	ProgressStore ProgressStore
	// Progress, when set, receives each checkpoint after its write-through
	// save returns and each state change. It feeds the in-process waiters of
	// read-your-writes; it must not block (ADR 0045).
	Progress ProgressPublisher
	// Dispatch decodes an envelope, fetches state if the spec says so, and
	// calls the handler. Generated.
	Dispatch func(ctx context.Context, e es.Envelope) error
	// Filter reports whether the envelope is one of the consumed types; a
	// non-consumed envelope is acknowledged without dispatch (a LogFeed
	// delivers everything; a filtered feed delivers only matching types).
	// Generated; nil means "dispatch everything".
	Filter func(e es.Envelope) bool
	// OnApplied, when set, is called after the marker for e has been saved.
	// Apply coalesces a batch into one save at its last applied position, so
	// the hook runs once per applied envelope after that batch save; on a
	// mid-batch dispatch error it runs for the applied prefix after the
	// partial save. "Saved" means Marker.Save or ProgressStore.SaveProgress
	// returned; a buffering Marker (such as BufferedMarker) may not have made
	// the position durable yet, so a caller that needs a queryable checkpoint
	// must use a write-through marker or flush before publishing. It is
	// best-effort: its work must not be required for projection correctness,
	// it cannot fail the projection and it is not retried. It runs
	// synchronously on the apply path, so it must be set before Run/Apply,
	// must not panic, and must bound its own work (a publish timeout or a
	// buffered hand-off) or it will stall the projector.
	// It is called for filtered envelopes too, after their marker advance; it
	// is not called for skipped positions or when dispatch fails before any
	// envelope is applied, or when the save fails.
	OnApplied func(ctx context.Context, e es.Envelope, position uint64)

	once   sync.Once
	mu     sync.Mutex
	marker uint64
	loaded bool
	// state is the checkpoint state the next save persists. It defaults to
	// StatusLive; Reset makes it StatusRebuilding and a successful Rebuild
	// makes it StatusLive again.
	state Status
}

func (r *Runner) init() {
	r.once.Do(func() {
		if r.Marker == nil {
			r.Marker = NewMemoryMarker()
		}
		r.mu.Lock()
		if r.state == StatusUnknown {
			r.state = StatusLive
		}
		r.mu.Unlock()
	})
}

// status returns the lifecycle state to persist, defaulting to live.
func (r *Runner) status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == StatusUnknown {
		return StatusLive
	}
	return r.state
}

// setMarker updates the cached applied position.
func (r *Runner) setMarker(pos uint64) {
	r.mu.Lock()
	r.marker = pos
	r.mu.Unlock()
}

// loadMarker reads the marker once per process: the scoped checkpoint when a
// ProgressStore is set, the legacy Marker otherwise.
func (r *Runner) loadMarker(ctx context.Context) error {
	r.init()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loaded {
		return nil
	}
	var pos uint64
	if r.ProgressStore != nil {
		cp, found, err := r.ProgressStore.LoadProgress(ctx, r.Scope, r.Name)
		if err != nil {
			return fmt.Errorf("projection %s: load progress: %w", r.Name, err)
		}
		if found {
			pos = cp.Position
			// Seed the lifecycle state too: a row left rebuilding by a crash
			// mid-rebuild must keep the runner fail-closed, or its first save
			// would flip the row live over a half-rebuilt read model. An
			// absent or unknown state keeps the live default.
			if cp.State != StatusUnknown {
				r.state = cp.State
			}
		}
	} else {
		var err error
		pos, err = r.Marker.Load(ctx, r.Name)
		if err != nil {
			return fmt.Errorf("projection %s: load marker: %w", r.Name, err)
		}
	}
	r.marker, r.loaded = pos, true
	return nil
}

// applyEnvelope handles one envelope idempotently without saving: a position at
// or below the marker is skipped, a filtered envelope is applied without
// dispatch, and an applied envelope advances the cached marker so an
// in-process redelivery skips it. Apply owns the durable save.
func (r *Runner) applyEnvelope(ctx context.Context, e es.Envelope) (bool, error) {
	r.mu.Lock()
	skip := e.GlobalPosition <= r.marker
	r.mu.Unlock()
	if skip {
		return false, nil
	}
	if r.Filter != nil && !r.Filter(e) {
		r.setMarker(e.GlobalPosition)
		return true, nil
	}
	if err := r.Dispatch(ctx, e); err != nil {
		return false, err
	}
	r.setMarker(e.GlobalPosition)
	return true, nil
}

// notifyApplied calls the optional post-apply hook only after advance has
// committed the marker. A nil hook is the pre-existing behaviour.
func (r *Runner) notifyApplied(ctx context.Context, e es.Envelope) {
	if r.OnApplied != nil {
		r.OnApplied(ctx, e, e.GlobalPosition)
	}
}

// advance persists position and the runner's current state: the scoped
// ProgressStore row when set, the legacy Marker otherwise. On success it
// updates the cached marker and then publishes the checkpoint — never before,
// because SaveProgress is write-through and a waiter must not see a position
// the store does not hold.
func (r *Runner) advance(ctx context.Context, pos uint64) error {
	state := r.status()
	cp := Checkpoint{Scope: r.Scope, Projection: r.Name, Position: pos, State: state}
	if r.ProgressStore != nil {
		if err := r.ProgressStore.SaveProgress(ctx, r.Scope, r.Name, cp); err != nil {
			return fmt.Errorf("projection %s: save progress: %w", r.Name, err)
		}
	} else {
		if err := r.Marker.Save(ctx, r.Name, pos); err != nil {
			return fmt.Errorf("projection %s: save marker: %w", r.Name, err)
		}
	}
	r.setMarker(pos)
	if r.Progress != nil {
		r.Progress.Advance(cp)
	}
	return nil
}

// Position returns the highest applied global_position.
func (r *Runner) Position(ctx context.Context) (uint64, error) {
	if err := r.loadMarker(ctx); err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.marker, nil
}

// Run consumes feed until ctx ends. Returns ctx.Err() on cancellation.
// On return it also flushes a marker that buffers saves — any Marker also
// implementing interface{ Flush(context.Context) error }, e.g.
// BufferedMarker — so the last window cannot sit pending with no further
// Save to trigger it. The exit flush runs on context.WithoutCancel(ctx)
// with a 5s timeout: a cancelled Run still flushes, and the flush cannot
// hang Run beyond the timeout. A flush error is returned only when the
// feed itself succeeded; otherwise the feed's error wins and the flush
// error is dropped (a failed flush keeps its positions pending for the
// next trigger).
func (r *Runner) Run(ctx context.Context, feed Feed) error {
	if err := r.loadMarker(ctx); err != nil {
		return err
	}
	err := feed.Feed(ctx, func(ctx context.Context, e es.Envelope) error {
		return r.Apply(ctx, []es.Envelope{e})
	})
	flushErr := r.flushMarker(context.WithoutCancel(ctx))
	if err != nil && errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil {
		return flushErr
	}
	return err
}

// flushMarker flushes r.Marker when it buffers saves. Run calls it on
// every return: the last window has no later Save to trigger it, so
// without this flush it would sit pending until the process exits.
func (r *Runner) flushMarker(ctx context.Context) error {
	flusher, ok := r.Marker.(interface{ Flush(context.Context) error })
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return flusher.Flush(ctx)
}

// Apply handles one ordered batch — the projection.Apply shape, so Replay
// drives the same path Run does. It dispatches every envelope, tracks the last
// successfully applied position and saves once at the end, so one batch is one
// write-through save and one publish. A dispatch failure still saves the
// applied prefix once before returning the error, or a redelivery would restart
// the page. OnApplied runs once per applied envelope, after that save.
func (r *Runner) Apply(ctx context.Context, batch []es.Envelope) error {
	if err := r.loadMarker(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	start := r.marker
	r.mu.Unlock()

	applied := make([]es.Envelope, 0, len(batch))
	for _, e := range batch {
		ok, err := r.applyEnvelope(ctx, e)
		if err != nil {
			if len(applied) == 0 {
				return err
			}
			if serr := r.advance(ctx, applied[len(applied)-1].GlobalPosition); serr != nil {
				r.setMarker(start)
				return serr
			}
			r.notifyAppliedBatch(ctx, applied)
			return err
		}
		if ok {
			applied = append(applied, e)
		}
	}
	if len(applied) == 0 {
		return nil
	}
	if err := r.advance(ctx, applied[len(applied)-1].GlobalPosition); err != nil {
		// applyEnvelope advanced the cached marker per envelope; a failed save
		// must not leave it ahead of the store, or a redelivery would skip the
		// batch and never persist it.
		r.setMarker(start)
		return err
	}
	r.notifyAppliedBatch(ctx, applied)
	return nil
}

// notifyAppliedBatch runs the post-apply hook for every applied envelope, in
// order, after the batch save.
func (r *Runner) notifyAppliedBatch(ctx context.Context, batch []es.Envelope) {
	for _, e := range batch {
		r.notifyApplied(ctx, e)
	}
}

// Reset restarts the projection's progress at position 0 in the rebuilding
// state — the reset half of rebuild (ADR 0005): reset the read model, call
// Reset, then Rebuild. With a ProgressStore it replaces the row; otherwise it
// discards any buffered pending marker position and saves 0. The cached marker
// is cleared so the replay cannot be skipped.
//
// The published progress is the checkpoint the store returned, not just the
// new state: a warm waiter cache must move to the reset position, or a rebuild
// below the old high-water mark would return a false "reached". The legacy
// Marker path has no returned checkpoint, so it publishes the state change
// only (the marker is not part of the waiter cache).
func (r *Runner) Reset(ctx context.Context) error {
	r.init()
	if r.ProgressStore != nil {
		cp, err := r.ProgressStore.ResetProgress(ctx, r.Scope, r.Name, StatusRebuilding)
		if err != nil {
			return fmt.Errorf("projection %s: reset progress: %w", r.Name, err)
		}
		cp.Scope, cp.Projection = r.Scope, r.Name
		r.mu.Lock()
		r.state = StatusRebuilding
		r.marker = 0
		r.loaded = true
		r.mu.Unlock()
		// Advance, not SetStatus: a state change replaces the position, so a
		// warm live@100 becomes rebuilding@0 and a replay advance below 100
		// is visible.
		if r.Progress != nil {
			r.Progress.Advance(cp)
		}
		return nil
	}
	if d, ok := r.Marker.(interface{ Discard(string) }); ok {
		d.Discard(r.Name)
	}
	if err := r.Marker.Save(ctx, r.Name, 0); err != nil {
		return fmt.Errorf("projection %s: reset marker: %w", r.Name, err)
	}
	r.mu.Lock()
	r.state = StatusRebuilding
	r.marker = 0
	r.loaded = true
	r.mu.Unlock()
	if r.Progress != nil {
		r.Progress.SetStatus(r.Scope, r.Name, StatusRebuilding)
	}
	return nil
}

// Rebuild replays src from position 0 through Apply and returns the last
// position applied (ADR 0005). Reset the read model, then call Reset before
// this; otherwise the marker makes every already-applied position a no-op,
// which is the resume-after-crash behaviour, not a rebuild. With a
// BufferedMarker, Reset discards the projection's pending position so a
// pre-reset position cannot be flushed after the reset and resurrect the
// marker above the rebuilt state.
//
// The runner stays rebuilding for the whole replay: every page persists
// StatusRebuilding, and only a successful Replay marks the scope live. A
// source error leaves it rebuilding, so a reader waiting on it cannot see
// "live" over a half-rebuilt read model.
//
// The live transition is fail-closed: the store row is marked live first, and
// the runner's state changes only after that succeeds. If the status write
// fails, Rebuild returns the error with both the row and the runner still
// rebuilding, so the next advance cannot write live.
func (r *Runner) Rebuild(ctx context.Context, src Source, batch int) (uint64, error) {
	pos, err := Replay(ctx, src, 0, batch, r.Apply)
	if err != nil {
		return pos, err
	}
	if r.ProgressStore != nil {
		if err := r.ProgressStore.SetStatus(ctx, r.Scope, r.Name, StatusLive); err != nil {
			r.mu.Lock()
			r.state = StatusRebuilding
			r.mu.Unlock()
			return pos, fmt.Errorf("projection %s: mark live: %w", r.Name, err)
		}
	}
	r.mu.Lock()
	r.state = StatusLive
	r.mu.Unlock()
	if r.Progress != nil {
		r.Progress.SetStatus(r.Scope, r.Name, StatusLive)
	}
	return pos, nil
}

// MemoryMarker is an in-memory Marker for tests and throwaway projectors.
type MemoryMarker struct {
	mu  sync.Mutex
	pos map[string]uint64
}

// NewMemoryMarker returns an empty MemoryMarker.
func NewMemoryMarker() *MemoryMarker { return &MemoryMarker{pos: map[string]uint64{}} }

// Load implements Marker.
func (m *MemoryMarker) Load(_ context.Context, projection string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pos[projection], nil
}

// Save implements Marker.
func (m *MemoryMarker) Save(_ context.Context, projection string, position uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pos[projection] = position
	return nil
}
