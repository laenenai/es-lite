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
// later". natsjs.NewFeed implements it over a JetStream durable consumer;
// LogFeed implements it by tailing the log directly, which is how a
// projector is tested (and run on single-node SQLite) with no NATS.
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
// error is retried on the same envelope after Poll, so delivery is
// at-least-once in order — the same contract as a JetStream nak, without
// the broker. From is normally the runner's marker; LogFeed itself keeps no
// checkpoint.
type LogFeed struct {
	Source Source
	From   uint64
	Batch  int           // default 500
	Poll   time.Duration // default 50ms
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
	pos := f.From
	for {
		evs, err := f.Source.ReadAll(ctx, pos, batch)
		if err != nil {
			return err
		}
		for _, e := range evs {
			for {
				if err := handle(ctx, e); err == nil {
					break
				}
				if err := sleep(ctx, poll); err != nil {
					return err
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
// supplies the Marker. Live and rebuild share one apply path, so they
// cannot drift (ADR 0005).
type Runner struct {
	// Name is the spec name: the marker key and the durable name.
	Name string
	// Marker persists the highest applied global_position. Nil means an
	// in-memory marker — fine for tests, wrong for anything durable.
	Marker Marker
	// Dispatch decodes an envelope, fetches state if the spec says so, and
	// calls the handler. Generated.
	Dispatch func(ctx context.Context, e es.Envelope) error
	// Filter reports whether the envelope is one of the consumed types; a
	// non-consumed envelope is acknowledged without dispatch (a LogFeed
	// delivers everything, a JetStream feed only the filter subjects).
	// Generated; nil means "dispatch everything".
	Filter func(e es.Envelope) bool

	once   sync.Once
	mu     sync.Mutex
	marker uint64
	loaded bool
}

func (r *Runner) init() {
	r.once.Do(func() {
		if r.Marker == nil {
			r.Marker = NewMemoryMarker()
		}
	})
}

// loadMarker reads the marker once per process.
func (r *Runner) loadMarker(ctx context.Context) error {
	r.init()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loaded {
		return nil
	}
	pos, err := r.Marker.Load(ctx, r.Name)
	if err != nil {
		return fmt.Errorf("projection %s: load marker: %w", r.Name, err)
	}
	r.marker, r.loaded = pos, true
	return nil
}

// apply handles one envelope idempotently: positions at or below the
// marker are skipped, a handled position is saved before it is
// acknowledged (ADR 0005).
func (r *Runner) apply(ctx context.Context, e es.Envelope) error {
	if err := r.loadMarker(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	skip := e.GlobalPosition <= r.marker
	r.mu.Unlock()
	if skip {
		return nil
	}
	if r.Filter != nil && !r.Filter(e) {
		return r.advance(ctx, e.GlobalPosition)
	}
	if err := r.Dispatch(ctx, e); err != nil {
		return err
	}
	return r.advance(ctx, e.GlobalPosition)
}

func (r *Runner) advance(ctx context.Context, pos uint64) error {
	if err := r.Marker.Save(ctx, r.Name, pos); err != nil {
		return fmt.Errorf("projection %s: save marker: %w", r.Name, err)
	}
	r.mu.Lock()
	r.marker = pos
	r.mu.Unlock()
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
func (r *Runner) Run(ctx context.Context, feed Feed) error {
	if err := r.loadMarker(ctx); err != nil {
		return err
	}
	err := feed.Feed(ctx, r.apply)
	if err != nil && errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Apply handles one ordered batch — the projection.Apply shape, so Replay
// drives the same path Run does.
func (r *Runner) Apply(ctx context.Context, batch []es.Envelope) error {
	for _, e := range batch {
		if err := r.apply(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// Rebuild replays src from position 0 through Apply and returns the last
// position applied (ADR 0005). Reset the marker and the read model first;
// otherwise the marker makes every already-applied position a no-op, which
// is the resume-after-crash behaviour, not a rebuild.
func (r *Runner) Rebuild(ctx context.Context, src Source, batch int) (uint64, error) {
	return Replay(ctx, src, 0, batch, r.Apply)
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
