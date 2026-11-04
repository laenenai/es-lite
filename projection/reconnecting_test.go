package projection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
)

func TestReconnectingFeedReopensAfterError(t *testing.T) {
	opens := 0
	f := &ReconnectingFeed{
		Min: time.Millisecond, Max: time.Millisecond,
		Open: func(context.Context) (Feed, error) {
			opens++
			if opens == 1 {
				return FeedFunc(func(context.Context, func(context.Context, es.Envelope) error) error {
					return errors.New("stream dropped")
				}), nil
			}
			return FeedFunc(func(ctx context.Context, h func(context.Context, es.Envelope) error) error {
				return h(ctx, es.Envelope{GlobalPosition: 1})
			}), nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	got := 0
	err := f.Feed(ctx, func(context.Context, es.Envelope) error { got++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || got != 1 || opens != 2 {
		t.Fatalf("err=%v got=%d opens=%d, want context.Canceled, 1, 2", err, got, opens)
	}
}

func TestReconnectingFeedBackoffIsCapped(t *testing.T) {
	var delays []time.Duration
	ctx, cancel := context.WithCancel(context.Background())
	opens := 0
	f := &ReconnectingFeed{
		Min: time.Second, Max: 4 * time.Second,
		Jitter: func(d time.Duration) time.Duration { delays = append(delays, d); return 0 },
		Open: func(context.Context) (Feed, error) {
			opens++
			if opens == 6 {
				cancel()
			}
			return nil, errors.New("still down")
		},
	}
	_ = f.Feed(ctx, func(context.Context, es.Envelope) error { return nil })
	if len(delays) < 5 {
		t.Fatalf("delays = %d, want at least 5", len(delays))
	}
	for _, d := range delays {
		if d > 4*time.Second {
			t.Fatalf("delay %v exceeds Max", d)
		}
	}
}

// TestReconnectingFeedFailsFastOnNilFeed pins the Open contract: a nil Feed
// with a nil error is a programming error, so Feed fails fast — it returns
// errNilFeed and does not retry.
func TestReconnectingFeedFailsFastOnNilFeed(t *testing.T) {
	opens := 0
	f := &ReconnectingFeed{
		Min: time.Millisecond, Max: time.Millisecond,
		Open: func(context.Context) (Feed, error) {
			opens++
			return nil, nil
		},
	}
	err := f.Feed(context.Background(), func(context.Context, es.Envelope) error { return nil })
	if !errors.Is(err, errNilFeed) {
		t.Fatalf("Feed = %v, want errNilFeed (Open returned a nil feed)", err)
	}
	if opens != 1 {
		t.Fatalf("opens = %d, want 1 (fail fast, no retry)", opens)
	}
}

func TestReconnectingFeedStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := &ReconnectingFeed{Min: time.Hour, Max: time.Hour, Jitter: func(time.Duration) time.Duration { return 0 },
		Open: func(context.Context) (Feed, error) { t.Fatal("Open called after cancel"); return nil, nil }}
	if err := f.Feed(ctx, func(context.Context, es.Envelope) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Feed = %v, want context.Canceled", err)
	}
}

// A misconfigured Min above Max must be clamped to Max: the first backoff is
// then at the documented ceiling, not above it.
func TestReconnectingFeedClampsMinToMax(t *testing.T) {
	var delays []time.Duration
	ctx, cancel := context.WithCancel(context.Background())
	f := &ReconnectingFeed{
		Min: 10 * time.Second, Max: 4 * time.Second,
		Jitter: func(d time.Duration) time.Duration {
			delays = append(delays, d)
			cancel() // let the first backoff be the last
			return 0
		},
		Open: func(context.Context) (Feed, error) { return nil, errors.New("down") },
	}
	_ = f.Feed(ctx, func(context.Context, es.Envelope) error { return nil })
	if len(delays) == 0 {
		t.Fatal("no backoff observed")
	}
	if delays[0] != 4*time.Second {
		t.Fatalf("first backoff = %v, want Max 4s (Min 10s clamped)", delays[0])
	}
}

// A successful delivery makes the next backoff start again at Min instead of
// continuing to grow toward Max.
func TestReconnectingFeedDeliveredAttemptResetsBackoff(t *testing.T) {
	const min, max = time.Millisecond, 8 * time.Millisecond
	var delays []time.Duration
	ctx, cancel := context.WithCancel(context.Background())
	opens := 0
	f := &ReconnectingFeed{
		Min: min, Max: max,
		Jitter: func(d time.Duration) time.Duration {
			delays = append(delays, d)
			return 0
		},
		Open: func(context.Context) (Feed, error) {
			opens++
			switch opens {
			case 3:
				// Deliver one event, then return while ctx is live: the
				// attempt was healthy but dropped.
				return FeedFunc(func(ctx context.Context, h func(context.Context, es.Envelope) error) error {
					return h(ctx, es.Envelope{GlobalPosition: 1})
				}), nil
			case 4:
				cancel()
			}
			return nil, errors.New("down")
		},
	}
	_ = f.Feed(ctx, func(context.Context, es.Envelope) error { return nil })

	want := []time.Duration{min, 2 * min, min}
	if len(delays) != len(want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}
	for i := range want {
		if delays[i] != want[i] {
			t.Fatalf("delays = %v, want %v", delays, want)
		}
	}
}

// A live feed never returns on its own. An inner Feed returning nil while ctx
// is live is a drop: the wrapper backs off and reopens, it does not exit
// successfully.
func TestReconnectingFeedReconnectsAfterNilReturnOnLiveContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	opens := 0
	f := &ReconnectingFeed{
		Min: time.Millisecond, Max: 4 * time.Millisecond,
		Jitter: func(time.Duration) time.Duration {
			if opens >= 3 {
				cancel()
			}
			return 0
		},
		Open: func(context.Context) (Feed, error) {
			opens++
			return FeedFunc(func(context.Context, func(context.Context, es.Envelope) error) error {
				return nil
			}), nil
		},
	}
	err := f.Feed(ctx, func(context.Context, es.Envelope) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Feed = %v, want context.Canceled", err)
	}
	if opens != 3 {
		t.Fatalf("opens = %d, want 3 (nil-returning attempts reconnected)", opens)
	}
}
