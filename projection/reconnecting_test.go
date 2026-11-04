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
