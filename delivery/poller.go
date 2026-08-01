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
	// waiting for the next tick. Optional.
	Wake <-chan struct{}
}

// Poller drains the log for one subscriber.
type Poller struct {
	src     Source
	cp      Checkpoints
	handler Handler
	cfg     Config
}

// NewPoller wires a Poller. Panics if Subscriber or handler is empty —
// both are programmer errors, not runtime conditions.
func NewPoller(src Source, cp Checkpoints, handler Handler, cfg Config) *Poller {
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
// Returns ctx.Err() on cancellation.
func (p *Poller) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	// Initial catch-up.
	if err := p.drain(ctx); err != nil && ctx.Err() == nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := p.drain(ctx); err != nil && ctx.Err() == nil {
				return err
			}
		case <-p.cfg.Wake:
			if err := p.drain(ctx); err != nil && ctx.Err() == nil {
				return err
			}
		}
	}
}

// drain reads and handles batches until the log is exhausted, advancing
// the checkpoint after each successfully handled batch.
func (p *Poller) drain(ctx context.Context) error {
	for {
		pos, err := p.cp.LoadCheckpoint(ctx, p.cfg.Subscriber)
		if err != nil {
			return fmt.Errorf("load checkpoint: %w", err)
		}
		batch, err := p.src.ReadAll(ctx, pos, p.cfg.BatchSize)
		if err != nil {
			return fmt.Errorf("read log: %w", err)
		}
		if len(batch) == 0 {
			return nil // caught up
		}
		if err := p.handler(ctx, batch); err != nil {
			return fmt.Errorf("handle batch: %w", err)
		}
		last := batch[len(batch)-1].GlobalPosition
		if err := p.cp.SaveCheckpoint(ctx, p.cfg.Subscriber, last); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
		if len(batch) < p.cfg.BatchSize {
			return nil // partial batch => log exhausted
		}
	}
}
