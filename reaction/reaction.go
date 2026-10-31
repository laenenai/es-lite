// Package reaction is the executor side of ADR 0030 §3: the loop that turns
// the commands a generated reaction handler issues into aggregate.Runtime
// calls, with the contract of contracts' convention 20 — es.ErrConflict is
// retried with bounded backoff, es.ErrTerminal means "already done", any
// other error fails the event so it is redelivered.
package reaction

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

// Dispatcher delivers one command to one stream. aggregate.Runtime
// satisfies it through RuntimeDispatcher; in production contracts wires the
// command gate instead.
type Dispatcher[C any] interface {
	Dispatch(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta) error
}

// DispatcherFunc adapts a function to Dispatcher.
type DispatcherFunc[C any] func(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta) error

// Dispatch implements Dispatcher.
func (f DispatcherFunc[C]) Dispatch(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta) error {
	return f(ctx, sid, cmd, meta)
}

// DefaultConflictRetries is the retry budget RuntimeDispatcher applies to a
// runtime that has none of its own (ADR 0030 §3).
const DefaultConflictRetries = 5

// RuntimeDispatcher adapts an aggregate.Runtime to Dispatcher. A runtime
// without WithConflictRetry gets DefaultConflictRetries attempts with
// aggregate.DefaultBackoff; one that already retries is used as configured.
func RuntimeDispatcher[S, C, E any](rt *aggregate.Runtime[S, C, E]) Dispatcher[C] {
	retries := 0
	if rt.ConflictRetries() == 0 {
		retries = DefaultConflictRetries
	}
	return DispatcherFunc[C](func(ctx context.Context, sid es.StreamID, cmd C, meta es.Meta) error {
		for attempt := 0; ; attempt++ {
			_, err := rt.Handle(ctx, sid, cmd, meta)
			if err == nil || !errors.Is(err, es.ErrConflict) || attempt >= retries {
				return err
			}
			if err := sleep(ctx, aggregate.DefaultBackoff(attempt+1)); err != nil {
				return err
			}
		}
	})
}

// Issue is one command a handler asked to send: the target stream and the
// command. Generated issuers collect these; Execute delivers them.
type Issue[C any] struct {
	StreamID es.StreamID
	Command  C
}

// Execute dispatches every issue in order with meta. es.ErrTerminal on a
// target stream counts as done (the stream is closed; rule 20 says that is
// a no-op, not a failure). Any other error stops and is returned so the
// runner naks the event; a redelivery re-issues every command, which rule
// 20 makes a decider no-op for the ones that already landed.
func Execute[C any](ctx context.Context, d Dispatcher[C], issues []Issue[C], meta es.Meta) error {
	for _, is := range issues {
		err := d.Dispatch(ctx, is.StreamID, is.Command, meta)
		switch {
		case err == nil, errors.Is(err, es.ErrTerminal):
			continue
		default:
			return fmt.Errorf("reaction: dispatch %T to %s: %w", is.Command, is.StreamID, err)
		}
	}
	return nil
}

// MetaFor builds the audit metadata for commands issued in reaction to env:
// caused by the event, in its correlation group, on behalf of the system
// actor the runner was given. CommandID is left for the runtime to mint.
func MetaFor(env es.Envelope, actor es.Actor) es.Meta {
	return es.Meta{
		CausationID:   env.EventID,
		CorrelationID: env.CorrelationID,
		Actor:         actor,
	}
}

// Runner runs a generated reaction: a projection.Runner without Rebuild,
// because replaying a reaction re-issues its commands — safe under rule 20
// but never wanted by default (ADR 0030 §2).
type Runner struct {
	inner *projection.Runner
}

// NewRunner wraps the generated dispatch in a Runner. marker may be nil
// (in-memory).
func NewRunner(name string, marker projection.Marker, dispatch func(context.Context, es.Envelope) error, filter func(es.Envelope) bool) *Runner {
	return &Runner{inner: &projection.Runner{Name: name, Marker: marker, Dispatch: dispatch, Filter: filter}}
}

// Run consumes feed until ctx ends.
func (r *Runner) Run(ctx context.Context, feed projection.Feed) error { return r.inner.Run(ctx, feed) }

// Position returns the highest applied global_position.
func (r *Runner) Position(ctx context.Context) (uint64, error) { return r.inner.Position(ctx) }

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
