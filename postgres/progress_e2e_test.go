package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/projection"
)

// TestReadYourWritesEndToEnd is the composed guarantee over real Postgres: a
// workspace-scoped log, the Postgres progress store, a Runner over
// projection.LogFeed, and a Waiter sharing the runner's publisher. On every
// iteration the token returned by the append must be satisfied before a read
// of the projection can be called fresh.
func TestReadYourWritesEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := openStore(t)
	wsStore := s.WorkspaceStore("ws_e2e")
	progress := postgres.NewProgressStore(s, "")
	waiter := projection.NewWaiter(progress)

	var (
		mu       sync.Mutex
		observed uint64
	)
	runner := &projection.Runner{
		Name:          "e2e",
		Scope:         "ws_e2e",
		ProgressStore: progress,
		Progress:      waiter,
		Dispatch: func(_ context.Context, e es.Envelope) error {
			mu.Lock()
			if e.GlobalPosition > observed {
				observed = e.GlobalPosition
			}
			mu.Unlock()
			return nil
		},
	}

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(runCtx, &projection.LogFeed{Source: wsStore, Poll: 2 * time.Millisecond})
	}()
	defer func() {
		stop()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("runner.Run: %v", err)
		}
		waiter.Close()
	}()

	sid, err := es.NewStreamID("e2e", "notes")
	if err != nil {
		t.Fatal(err)
	}

	var last uint64
	var lastReached bool
	for i := 1; i <= 1000; i++ {
		res, err := wsStore.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i - 1),
			Events: []es.EventData{{
				TypeURL:       "e2e.v1.Added",
				SchemaVersion: 1,
				Payload:       []byte("note"),
			}},
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		tok := es.TokenFor("ws_e2e", res)
		if tok.Position == 0 {
			t.Fatalf("append %d returned a zero token", i)
		}

		wr, err := waiter.WaitFor(ctx, "ws_e2e", "e2e", tok, 2*time.Second)
		if err != nil {
			t.Fatalf("WaitFor %d: %v", i, err)
		}
		if wr.Reached && wr.Status == projection.StatusLive {
			mu.Lock()
			got := observed
			mu.Unlock()
			if got < tok.Position {
				t.Fatalf("iteration %d: reached live at token %d but the read model only observed %d", i, tok.Position, got)
			}
		}
		last = tok.Position
		lastReached = wr.Reached && wr.Status == projection.StatusLive
	}

	if !lastReached {
		// Give the final token one more bounded chance to settle before
		// declaring the composed guarantee broken.
		wr, err := waiter.WaitFor(ctx, "ws_e2e", "e2e", es.Token{Scope: "ws_e2e", Position: last}, 2*time.Second)
		if err != nil {
			t.Fatalf("final WaitFor: %v", err)
		}
		if !wr.Reached || wr.Status != projection.StatusLive {
			t.Fatalf("final token %d = (reached=%v, status=%v), want reached live", last, wr.Reached, wr.Status)
		}
	}

	// The log is applied in order, so observing the last position proves every
	// earlier token was observed too.
	mu.Lock()
	got := observed
	mu.Unlock()
	if got < last {
		t.Fatalf("final observed position = %d, want at least the last token %d", got, last)
	}
}
