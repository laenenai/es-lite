package projection

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/laenenai/es-lite/es"
)

var _ Feed = (*ReconnectingFeed)(nil)

// errNilFeed reports an Open that violated its contract: a nil Feed with a
// nil error. Feed fails fast on it instead of retrying — a nil Feed is a
// programming error, not a transient condition, so retrying would hide it.
var errNilFeed = errors.New("projection: ReconnectingFeed: Open returned a nil feed")

// ReconnectingFeed turns a Feed that may drop — a stream that breaks, a
// broker that restarts — into one that runs until ctx ends. Each attempt
// calls Open for a fresh inner feed; whenever an attempt ends early (Open
// fails, or the inner feed returns while ctx is still live — a live feed
// never returns on its own), the loop backs off and reopens. Inner errors
// are not surfaced; recovering from them is this wrapper's job, and a
// redelivery after a reconnect is deduplicated downstream by the marker
// (ADR 0005). Feed returns ctx.Err() when ctx ends.
type ReconnectingFeed struct {
	// Open opens one attempt of the inner feed. Called again on every
	// reconnect; a live feed should resume where the caller's checkpoint
	// left off, so no state is kept here. When the error is nil the Feed
	// must be non-nil: a (nil, nil) return is a programming error, and
	// Feed fails fast on it rather than retrying.
	Open func(context.Context) (Feed, error)
	// The first backoff, doubled up to Max. Zero values mean 250ms and 30s.
	// Min greater than Max is clamped to Max so a misconfiguration cannot
	// sleep past the documented ceiling.
	Min, Max time.Duration
	// Jitter spreads a computed backoff d. Nil means uniform in [0, d).
	Jitter func(time.Duration) time.Duration
}

// Feed implements Feed. It blocks until ctx ends, reconnecting through
// Open whenever an attempt drops.
func (f *ReconnectingFeed) Feed(ctx context.Context, handle func(ctx context.Context, e es.Envelope) error) error {
	min, max := f.Min, f.Max
	if min <= 0 {
		min = 250 * time.Millisecond
	}
	if max <= 0 {
		max = 30 * time.Second
	}
	if min > max {
		min = max
	}
	delay := min
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		inner, err := f.Open(ctx)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
		} else if inner == nil {
			return errNilFeed
		} else {
			delivered := false
			err = inner.Feed(ctx, func(ctx context.Context, e es.Envelope) error {
				delivered = true
				return handle(ctx, e)
			})
			// A run that delivered reset the backoff: the feed was
			// healthy until just now.
			if delivered {
				delay = min
			}
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
		}
		// The attempt ended early (Open failed, or the inner feed returned
		// while ctx is live). Back off, then reopen.
		if err := sleep(ctx, f.backoff(delay)); err != nil {
			return err
		}
		delay = grow(delay, max)
	}
}

// backoff applies Jitter to one computed delay.
func (f *ReconnectingFeed) backoff(d time.Duration) time.Duration {
	if f.Jitter != nil {
		return f.Jitter(d)
	}
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)))
}

// grow doubles the backoff, capped at max.
func grow(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		d = max
	}
	return d
}
