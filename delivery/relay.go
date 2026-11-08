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
	// Wake, if set, triggers an immediate drain (Postgres LISTEN/NOTIFY
	// via a delivery.Signal). A closed channel is tolerated: the loop
	// drains whatever it still holds buffered, then ignores it.
	// Optional.
	Wake <-chan struct{}
	// MaxRetries bounds how often a failing drain is retried (waiting
	// RetryBackoff between attempts) before Run gives up and returns
	// the error. 0 → DefaultDrainRetries (3); positive → exactly that
	// many retries; negative → fail fast: the first drain error is
	// returned immediately, with no retries (design D10). Optional.
	MaxRetries int
	// RetryBackoff returns how long to wait before retry attempt n
	// (1-based). Default: 100ms per attempt, capped at 5s. Optional.
	RetryBackoff func(attempt int) time.Duration
}

// Relay drives a Drainer continuously, publishing claimed batches through a
// Handler. It is the Postgres-side counterpart to Poller: run one per
// subscriber, as a singleton or behind a database lease (ADR 0002).
type Relay struct {
	drainer Drainer
	publish Handler
	cfg     RelayConfig
}

// NewRelay wires a Relay. Panics if drainer or publish is nil — both are
// programmer errors, not runtime conditions.
func NewRelay(drainer Drainer, publish Handler, cfg RelayConfig) *Relay {
	if drainer == nil {
		panic("delivery: drainer is required")
	}
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

// Run drains until ctx is cancelled: an initial catch-up, then on each
// wake or tick. A failing drain is retried with backoff up to
// cfg.MaxRetries before the error is returned. Returns ctx.Err() on
// cancellation.
func (r *Relay) Run(ctx context.Context) error {
	return runUntilCancelled(ctx, loopConfig{
		PollInterval: r.cfg.PollInterval,
		Wake:         r.cfg.Wake,
		MaxRetries:   r.cfg.MaxRetries,
		RetryBackoff: r.cfg.RetryBackoff,
	}, r.drain)
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
