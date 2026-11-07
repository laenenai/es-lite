// Package delivery tails the event log and hands batches to a subscriber.
// It is the "log as outbox" relay from docs/adr/0001: no separate outbox
// table, just a cursor over the append-only events table advanced through
// a durable per-subscriber checkpoint.
//
// Delivery is at-least-once. A crash between handling a batch and saving
// the checkpoint re-delivers that batch on restart, so handlers MUST be
// idempotent.
package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/laenenai/es-lite/es"
)

// DefaultDrainRetries is the retry budget a loop applies to a failing
// drain when its config leaves MaxRetries at zero.
const DefaultDrainRetries = 3

// Source is the read side of the log the poller tails. es.Store
// satisfies it.
type Source interface {
	ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error)
}

// Checkpoints persists each subscriber's progress. The sqlite.Store
// implements it.
type Checkpoints interface {
	LoadCheckpoint(ctx context.Context, subscriber string) (uint64, error)
	SaveCheckpoint(ctx context.Context, subscriber string, position uint64) error
}

// Handler processes one ordered batch of events. Returning an error
// aborts the current drain without advancing the checkpoint, so the
// batch is retried on the next drain (at-least-once).
type Handler func(ctx context.Context, batch []es.Envelope) error

// Config configures a Poller.
type Config struct {
	// Subscriber is the checkpoint key; unique per independent consumer.
	Subscriber string

	// BatchSize caps events read per query. Default 100.
	BatchSize int

	// PollInterval is the fallback tick — the safety net that bounds
	// latency when no wake Signal fires (and the sole driver if none is
	// wired). Default 1s.
	PollInterval time.Duration

	// Wake, if set, lets a push source (Postgres LISTEN/NOTIFY, or an
	// in-process Signal on SQLite) trigger an immediate drain instead of
	// waiting for the next tick. A closed channel is tolerated: the loop
	// drains whatever it still holds buffered, then ignores it.
	// Optional.
	Wake <-chan struct{}

	// MaxRetries bounds how often a failing drain is retried (waiting
	// RetryBackoff between attempts) before Run gives up and returns
	// the error. 0 → DefaultDrainRetries (3); negative → never give
	// up: the drain is retried for as long as ctx lives. Optional.
	MaxRetries int

	// RetryBackoff returns how long to wait before retry attempt n
	// (1-based). Default: 100ms per attempt, capped at 5s. Optional.
	RetryBackoff func(attempt int) time.Duration
}

// Poller drains the log for one subscriber.
type Poller struct {
	src     Source
	cp      Checkpoints
	handler Handler
	cfg     Config

	// pos is the cached checkpoint position, valid once posLoaded. Run
	// drives drain from one goroutine, so no lock guards it.
	pos       uint64
	posLoaded bool
}

// NewPoller wires a Poller. Panics if src, cp, Subscriber or handler is
// nil/empty — all are programmer errors, not runtime conditions.
func NewPoller(src Source, cp Checkpoints, handler Handler, cfg Config) *Poller {
	if src == nil {
		panic("delivery: src is required")
	}
	if cp == nil {
		panic("delivery: cp is required")
	}
	if cfg.Subscriber == "" {
		panic("delivery: Config.Subscriber is required")
	}
	if handler == nil {
		panic("delivery: handler is required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	return &Poller{src: src, cp: cp, handler: handler, cfg: cfg}
}

// Run drains until ctx is cancelled. It drains once immediately (to catch
// up on anything appended while it was down), then on each wake or tick.
// A failing drain is retried with backoff up to cfg.MaxRetries before the
// error is returned. Returns ctx.Err() on cancellation.
func (p *Poller) Run(ctx context.Context) error {
	return runUntilCancelled(ctx, loopConfig{
		PollInterval: p.cfg.PollInterval,
		Wake:         p.cfg.Wake,
		MaxRetries:   p.cfg.MaxRetries,
		RetryBackoff: p.cfg.RetryBackoff,
	}, p.drain)
}

// drain reads and handles batches until the log is exhausted, advancing
// the checkpoint after each successfully handled batch. The position is
// cached after every successful SaveCheckpoint and reloaded from the
// store only on the first pass and after an error — the poller is the
// checkpoint's only writer, so between errors the cache and the store
// cannot drift.
func (p *Poller) drain(ctx context.Context) error {
	if !p.posLoaded {
		pos, err := p.cp.LoadCheckpoint(ctx, p.cfg.Subscriber)
		if err != nil {
			return fmt.Errorf("load checkpoint: %w", err)
		}
		p.pos, p.posLoaded = pos, true
	}
	for {
		batch, err := p.src.ReadAll(ctx, p.pos, p.cfg.BatchSize)
		if err != nil {
			p.posLoaded = false // after an error the store is truth again
			return fmt.Errorf("read log: %w", err)
		}
		if len(batch) == 0 {
			return nil // caught up
		}
		if err := p.handler(ctx, batch); err != nil {
			p.posLoaded = false
			return fmt.Errorf("handle batch: %w", err)
		}
		last := batch[len(batch)-1].GlobalPosition
		if err := p.cp.SaveCheckpoint(ctx, p.cfg.Subscriber, last); err != nil {
			p.posLoaded = false
			return fmt.Errorf("save checkpoint: %w", err)
		}
		p.pos = last // the cache matches the store after a successful save
		if len(batch) < p.cfg.BatchSize {
			return nil // partial batch => log exhausted
		}
	}
}

// loopConfig drives both delivery loops: Poller and Relay build one from
// their public configs and route Run through runUntilCancelled, so the
// tick/wake/retry discipline lives in exactly one place.
type loopConfig struct {
	// PollInterval is the fallback tick between wake-driven drains.
	PollInterval time.Duration
	// Wake, if set, triggers an immediate drain. A closed channel is
	// guarded: runUntilCancelled nils its local copy on the first
	// closed receive.
	Wake <-chan struct{}
	// MaxRetries bounds drain retries: 0 → DefaultDrainRetries,
	// negative → never give up.
	MaxRetries int
	// RetryBackoff returns the wait before retry attempt n (1-based).
	RetryBackoff func(attempt int) time.Duration
}

// runUntilCancelled drains once immediately (the catch-up pass), then on
// every tick and every wake signal, until ctx is cancelled. A failing
// drain is retried with backoff up to the configured budget before the
// error is returned. Returns ctx.Err() on cancellation.
func runUntilCancelled(ctx context.Context, cfg loopConfig, drain func(context.Context) error) error {
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// Initial catch-up.
	if err := drainWithRetries(ctx, cfg, drain); err != nil {
		return err
	}

	wake := cfg.Wake
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := drainWithRetries(ctx, cfg, drain); err != nil {
				return err
			}
		case _, ok := <-wake:
			if !ok {
				// A closed channel is always ready; leaving it in the
				// select would spin this loop hot forever. Each fire
				// consumes one buffered signal (ok == true below); once
				// the receive reports closed, nil the local copy — a
				// nil channel never fires — so ticks are the only
				// remaining driver.
				wake = nil
				continue
			}
			if err := drainWithRetries(ctx, cfg, drain); err != nil {
				return err
			}
		}
	}
}

// drainWithRetries runs one drain pass, retrying a failure with backoff
// until it succeeds, ctx ends, or the retry budget is spent. A failure
// after ctx is done reports ctx.Err() — cancellation always wins.
func drainWithRetries(ctx context.Context, cfg loopConfig, drain func(context.Context) error) error {
	err := drain(ctx)
	if err == nil {
		return nil
	}
	budget := cfg.MaxRetries
	if budget == 0 {
		budget = DefaultDrainRetries
	} else if budget < 0 {
		budget = 0 // never give up
	}
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if budget > 0 && attempt > budget {
			return err
		}
		if serr := sleepCtx(ctx, backoffFor(cfg, attempt)); serr != nil {
			return serr
		}
		if err = drain(ctx); err == nil {
			return nil
		}
	}
}

// backoffFor picks the configured backoff for retry attempt n, or the
// linear default: 100ms per attempt, capped at 5s so a never-give-up
// budget still probes periodically instead of sleeping forever.
func backoffFor(cfg loopConfig, attempt int) time.Duration {
	if cfg.RetryBackoff != nil {
		return cfg.RetryBackoff(attempt)
	}
	return min(100*time.Millisecond*time.Duration(attempt), 5*time.Second)
}

// sleepCtx waits for d, returning ctx.Err() if ctx ends first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
