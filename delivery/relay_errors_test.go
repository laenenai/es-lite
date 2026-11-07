package delivery_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
)

// alwaysFailDrainer fails every Drain and counts the attempts.
type alwaysFailDrainer struct {
	err   error
	calls atomic.Int32
}

func (d *alwaysFailDrainer) Drain(ctx context.Context, limit int, publish func(context.Context, []es.Envelope) error) (int, error) {
	d.calls.Add(1)
	return 0, d.err
}

func (d *alwaysFailDrainer) count() int { return int(d.calls.Load()) }

// recoveringDrainer fails its first `fail` Drains, then reports an empty
// backlog (caught up).
type recoveringDrainer struct {
	mu    sync.Mutex
	err   error
	fail  int
	calls int
}

func (d *recoveringDrainer) Drain(ctx context.Context, limit int, publish func(context.Context, []es.Envelope) error) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.calls <= d.fail {
		return 0, d.err
	}
	return 0, nil
}

func (d *recoveringDrainer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func noopPublish(context.Context, []es.Envelope) error { return nil }

// TestRelayMaxRetriesThenReturns pins the retry half of F22 for the
// relay: a failing drain is retried with the configured budget before
// Run gives up and returns the error — it must not exit on the first
// error, and it must not retry forever once the budget is spent.
func TestRelayMaxRetriesThenReturns(t *testing.T) {
	boom := errors.New("drain is down")

	t.Run("default budget is spent then Run returns the error", func(t *testing.T) {
		fd := &alwaysFailDrainer{err: boom}
		// PollInterval never fires: only the initial catch-up's retry
		// budget is under test, with the default policy (MaxRetries 0
		// → DefaultDrainRetries, default backoff).
		relay := delivery.NewRelay(fd, noopPublish, delivery.RelayConfig{
			BatchSize:    10,
			PollInterval: time.Hour,
		})

		done := make(chan error, 1)
		go func() { done <- relay.Run(context.Background()) }()

		select {
		case err := <-done:
			if !errors.Is(err, boom) {
				t.Fatalf("Run = %v, want the drain error", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after spending the retry budget")
		}
		// 1 attempt + DefaultDrainRetries (3) retries.
		if got := fd.count(); got != 4 {
			t.Fatalf("drain attempts = %d, want 4 (1 + 3 retries)", got)
		}
	})

	t.Run("explicit budget is honored", func(t *testing.T) {
		fd := &alwaysFailDrainer{err: boom}
		relay := delivery.NewRelay(fd, noopPublish, delivery.RelayConfig{
			BatchSize:    10,
			PollInterval: time.Hour,
			MaxRetries:   2,
			RetryBackoff: func(int) time.Duration { return 0 },
		})

		done := make(chan error, 1)
		go func() { done <- relay.Run(context.Background()) }()

		select {
		case err := <-done:
			if !errors.Is(err, boom) {
				t.Fatalf("Run = %v, want the drain error", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after spending the retry budget")
		}
		// 1 attempt + 2 retries — not the default 3.
		if got := fd.count(); got != 3 {
			t.Fatalf("drain attempts = %d, want 3 (1 + 2 retries)", got)
		}
	})

	t.Run("negative budget never gives up", func(t *testing.T) {
		// Fails 4 times — one more than the default budget tolerates
		// (1 + 3 retries) — so only a never-give-up policy survives.
		fd := &recoveringDrainer{err: boom, fail: 4}
		relay := delivery.NewRelay(fd, noopPublish, delivery.RelayConfig{
			BatchSize:    10,
			PollInterval: time.Hour,
			MaxRetries:   -1, // retry for as long as ctx lives
			RetryBackoff: func(int) time.Duration { return time.Millisecond },
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- relay.Run(ctx) }()

		// The fifth attempt recovers (empty backlog); a bounded budget
		// would have returned the error after the fourth failure.
		deadline := time.After(2 * time.Second)
		for fd.count() < 5 {
			select {
			case err := <-done:
				t.Fatalf("Run returned %v before the drain recovered", err)
			case <-deadline:
				t.Fatalf("drain attempts = %d, want 5", fd.count())
			case <-time.After(time.Millisecond):
			}
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after cancellation")
		}
	})
}

// TestRelayValidatesDeps pins the constructor contract: a nil drainer
// panics, naming the parameter (Task 28's emitted-guard style).
func TestRelayValidatesDeps(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal(`NewRelay(nil drainer): no panic, want one naming "drainer"`)
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "drainer") {
			t.Fatalf("panic = %q, want it to name %q", msg, "drainer")
		}
	}()
	delivery.NewRelay(nil, noopPublish, delivery.RelayConfig{})
}
