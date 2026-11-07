package delivery_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
)

// errTransient stands in for a store error a retry can clear.
var errTransient = errors.New("transient store error")

// memStore is an in-memory Source + Checkpoints double — the shape
// sqlite.Store hands the poller — with one-shot failure injection per
// method and call counters.
type memStore struct {
	mu     sync.Mutex
	events []es.Envelope
	pos    uint64
	fail   map[string]int
	reads  int
	loads  int
	saves  int
}

func newMemStore() *memStore { return &memStore{fail: map[string]int{}} }

// failOnce makes the next call to method fail once, then behave again.
func (m *memStore) failOnce(method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail[method]++
}

func (m *memStore) append(e es.Envelope) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
}

func (m *memStore) readCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reads
}

func (m *memStore) loadCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loads
}

func (m *memStore) LoadCheckpoint(ctx context.Context, subscriber string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loads++
	if m.fail["load"] > 0 {
		m.fail["load"]--
		return 0, errTransient
	}
	return m.pos, nil
}

func (m *memStore) SaveCheckpoint(ctx context.Context, subscriber string, position uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves++
	if m.fail["save"] > 0 {
		m.fail["save"]--
		return errTransient
	}
	m.pos = position
	return nil
}

func (m *memStore) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	if m.fail["read"] > 0 {
		m.fail["read"]--
		return nil, errTransient
	}
	var out []es.Envelope
	for _, e := range m.events {
		if e.GlobalPosition > fromPosition {
			out = append(out, e)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

// TestPollerRetriesTransientError pins F22's first half: a store error
// on a drain must not end Run. The poller retries, recovers, and keeps
// delivering; the checkpoint is reloaded from the store only on the
// first pass and after the error (never per tick).
func TestPollerRetriesTransientError(t *testing.T) {
	store := newMemStore()
	store.append(es.Envelope{GlobalPosition: 1, TypeURL: "t.first"})
	store.failOnce("load") // the first checkpoint read dies; a retry must clear it

	got := make(chan es.Envelope, 16)
	p := delivery.NewPoller(store, store, func(_ context.Context, batch []es.Envelope) error {
		for _, e := range batch {
			got <- e
		}
		return nil
	}, delivery.Config{Subscriber: "t", PollInterval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// The transient error must not end Run: position 1 still arrives.
	select {
	case e := <-got:
		if e.GlobalPosition != 1 {
			t.Fatalf("first event: position %d, want 1", e.GlobalPosition)
		}
	case err := <-done:
		t.Fatalf("Run gave up after the transient error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first event")
	}

	// Run must keep looping: a later append is still delivered.
	store.append(es.Envelope{GlobalPosition: 2, TypeURL: "t.second"})
	select {
	case e := <-got:
		if e.GlobalPosition != 2 {
			t.Fatalf("second event: position %d, want 2", e.GlobalPosition)
		}
	case err := <-done:
		t.Fatalf("Run exited before the second event: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second event — Run did not continue")
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

	// The position is cached: exactly two loads — the failed one and the
	// post-error reload — never one per tick.
	if loads := store.loadCount(); loads != 2 {
		t.Fatalf("LoadCheckpoint calls = %d, want 2 (first pass + post-error reload)", loads)
	}
}

// TestPollerClosedWakeDoesNotSpin pins F22's second half: a closed Wake
// channel is always ready, so leaving it in the select spins the loop
// hot. The guard must nil its local copy: over 200ms only the ticker
// drives drains, so the read count stays bounded.
func TestPollerClosedWakeDoesNotSpin(t *testing.T) {
	store := newMemStore() // empty log: every drain pass costs one ReadAll
	wake := make(chan struct{})
	close(wake) // a producer's signal channel died mid-flight

	p := delivery.NewPoller(store, store, func(context.Context, []es.Envelope) error { return nil },
		delivery.Config{
			Subscriber:   "t",
			PollInterval: 20 * time.Millisecond,
			Wake:         wake,
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// Over 200ms the 20ms ticker fires at most ~10 times, plus the
	// initial catch-up: well under 25 drains. The unguarded closed wake
	// spins drains as fast as the CPU allows — orders of magnitude more.
	if n := store.readCount(); n > 25 {
		t.Fatalf("%d drains in 200ms: the closed wake channel is spinning the loop", n)
	}
}

// TestPollerValidatesDeps pins the constructor contract: every nil dep
// panics, naming the parameter (Task 28's emitted-guard style).
func TestPollerValidatesDeps(t *testing.T) {
	noop := func(context.Context, []es.Envelope) error { return nil }
	store := newMemStore()

	for _, tc := range []struct {
		name  string
		build func() *delivery.Poller
		want  string
	}{
		{"nil src", func() *delivery.Poller {
			return delivery.NewPoller(nil, store, noop, delivery.Config{Subscriber: "t"})
		}, "src"},
		{"nil cp", func() *delivery.Poller {
			return delivery.NewPoller(store, nil, noop, delivery.Config{Subscriber: "t"})
		}, "cp"},
		{"nil handler", func() *delivery.Poller {
			return delivery.NewPoller(store, store, nil, delivery.Config{Subscriber: "t"})
		}, "handler"},
		{"empty subscriber", func() *delivery.Poller {
			return delivery.NewPoller(store, store, noop, delivery.Config{})
		}, "Subscriber"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("NewPoller: no panic, want one naming %q", tc.want)
				}
				if msg := fmt.Sprint(r); !strings.Contains(msg, tc.want) {
					t.Fatalf("panic = %q, want it to name %q", msg, tc.want)
				}
			}()
			tc.build()
		})
	}
}
