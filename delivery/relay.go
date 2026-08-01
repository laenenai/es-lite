package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/laenenai/es-lite/es"
)

// Drainer claims and publishes unpublished events in one gap-safe operation,
// returning the number of rows claimed. The Postgres Store.Drain satisfies
// it. (SQLite delivery uses Poller instead: its single-writer log has a
// gap-free cursor, so no claim is needed.)
type Drainer interface {
	Drain(ctx context.Context, limit int, publish func(context.Context, []es.Envelope) error) (int, error)
}

// RelayConfig configures a Relay.
type RelayConfig struct {
	// BatchSize caps rows claimed per Drain. Default 100.
	BatchSize int
	// PollInterval is the fallback tick when no wake fires. Default 1s.
	PollInterval time.Duration
	// Wake, if set, triggers an immediate drain (Postgres LISTEN/NOTIFY via
	// a delivery.Signal). Optional.
	Wake <-chan struct{}
}

// Relay drives a Drainer continuously, publishing claimed batches through a
// Handler (typically the NATS Publisher.Handle). It is the Postgres-side
// counterpart to Poller: run one per subscriber, as a singleton or
// leader-elected (ADR 0002/0003).
type Relay struct {
	drainer Drainer
	publish Handler
	cfg     RelayConfig
}

// NewRelay wires a Relay. Panics if publish is nil.
func NewRelay(drainer Drainer, publish Handler, cfg RelayConfig) *Relay {
	if publish == nil {
		panic("delivery: Relay publish handler is required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	return &Relay{drainer: drainer, publish: publish, cfg: cfg}
}

// Run drains until ctx is cancelled: an initial catch-up, then on each wake
// or tick. Returns ctx.Err() on cancellation.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	if err := r.drain(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.drain(ctx); err != nil && ctx.Err() == nil {
				return err
			}
		case <-r.cfg.Wake:
			if err := r.drain(ctx); err != nil && ctx.Err() == nil {
				return err
			}
		}
	}
}

// drain claims and publishes batches until the backlog is exhausted (a Drain
// returning fewer than BatchSize rows).
func (r *Relay) drain(ctx context.Context) error {
	for {
		n, err := r.drainer.Drain(ctx, r.cfg.BatchSize, r.publish)
		if err != nil {
			return fmt.Errorf("relay drain: %w", err)
		}
		if n < r.cfg.BatchSize {
			return nil
		}
	}
}
