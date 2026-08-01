package delivery_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
)

// fakeDrainer serves a fixed backlog of events in claim batches, mimicking
// postgres.Store.Drain without a database.
type fakeDrainer struct {
	mu     sync.Mutex
	events []es.Envelope
	cursor int
}

func (f *fakeDrainer) Drain(ctx context.Context, limit int, publish func(context.Context, []es.Envelope) error) (int, error) {
	f.mu.Lock()
	end := min(f.cursor+limit, len(f.events))
	batch := f.events[f.cursor:end]
	f.mu.Unlock()
	if len(batch) == 0 {
		return 0, nil
	}
	if err := publish(ctx, batch); err != nil {
		return 0, err
	}
	f.mu.Lock()
	f.cursor = end
	f.mu.Unlock()
	return len(batch), nil
}

func TestRelayDrainsBacklogInOrder(t *testing.T) {
	const total = 250
	fd := &fakeDrainer{}
	for i := range total {
		fd.events = append(fd.events, es.Envelope{GlobalPosition: uint64(i + 1)})
	}

	var (
		mu   sync.Mutex
		got  []uint64
		done = make(chan struct{})
	)
	publish := func(ctx context.Context, batch []es.Envelope) error {
		mu.Lock()
		for _, e := range batch {
			got = append(got, e.GlobalPosition)
		}
		n := len(got)
		mu.Unlock()
		if n >= total {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		return nil
	}

	relay := delivery.NewRelay(fd, publish, delivery.RelayConfig{
		BatchSize:    100,
		PollInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go relay.Run(ctx)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out; drained %d/%d", len(got), total)
	}
	cancel()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != total {
		t.Fatalf("drained %d, want %d", len(got), total)
	}
	for i, gp := range got {
		if gp != uint64(i+1) {
			t.Fatalf("out of order at %d: got %d", i, gp)
		}
	}
}
