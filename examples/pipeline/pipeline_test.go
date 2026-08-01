package pipeline_test

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	kmem "github.com/laenenai/es-lite/keystore/memory"
	"github.com/laenenai/es-lite/natsjs"
	"github.com/laenenai/es-lite/postgres"
)

// TestFullPipeline exercises Postgres -> Relay -> NATS -> projection end to
// end. Requires both PG_DSN and NATS_URL:
//
//	task test:pipeline
func TestFullPipeline(t *testing.T) {
	pgDSN := os.Getenv("PG_DSN")
	natsURL := os.Getenv("NATS_URL")
	if pgDSN == "" || natsURL == "" {
		t.Skip("set PG_DSN and NATS_URL to run the full-pipeline test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)

	// --- Storage: workspace-scoped Postgres with crypto-shredding. ---
	store, err := postgres.Open(ctx, pgDSN, kmem.New())
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer store.Close()
	if _, err := store.Pool().Exec(ctx, `TRUNCATE events, workspace_keys, checkpoints`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	ws := "wspipe" + suffix

	// --- Transport: JetStream stream. ---
	nc, err := natsjs.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	streamName := "ES_PIPE_" + suffix
	if _, err := natsjs.EnsureStream(ctx, js, natsjs.StreamConfig{
		Name:     streamName,
		Subjects: []string{"evt." + ws + ".>"},
	}); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	// --- Write side: handle commands (0 +10 +5 => 15). ---
	rt := aggregate.NewRuntime(store.Workspace(ws), counter.Decider, counterv1.EventCodec{})
	sid, _ := es.NewStreamID("counter", "main")
	for _, cmd := range []counterv1.Command{
		&counterv1.Init{Min: 0, Max: 100, Initial: 0},
		&counterv1.Increment{By: 10},
		&counterv1.Increment{By: 5},
	} {
		if _, err := rt.Handle(ctx, sid, cmd, es.Meta{}); err != nil {
			t.Fatalf("handle %T: %v", cmd, err)
		}
	}

	// --- Relay: drain Postgres -> publish to JetStream. ---
	pub := natsjs.NewPublisher(js, nil)
	relay := delivery.NewRelay(store, pub.Handle, delivery.RelayConfig{
		BatchSize:    10,
		PollInterval: 20 * time.Millisecond,
	})
	relayCtx, stopRelay := context.WithCancel(ctx)
	defer stopRelay()
	go relay.Run(relayCtx)

	// --- Projection: durable consumer folds events into a read model. ---
	var (
		mu    sync.Mutex
		model = map[string]int64{} // stream canonical -> count
	)
	codec := counterv1.EventCodec{}
	consCtx, stopCons := context.WithCancel(ctx)
	defer stopCons()
	go func() {
		_ = natsjs.Consume(consCtx, js, natsjs.ConsumerConfig{
			Stream:        streamName,
			Durable:       "counter-view-" + suffix,
			FilterSubject: "evt." + ws + ".>",
		}, func(ctx context.Context, e es.Envelope) error {
			ev, err := codec.Decode(es.EncodedEvent{TypeURL: e.TypeURL, SchemaVersion: e.SchemaVersion, Payload: e.Payload})
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			switch v := ev.(type) {
			case *counterv1.Initialized:
				model[e.StreamID.Canonical()] = v.GetValue()
			case *counterv1.Incremented:
				model[e.StreamID.Canonical()] += v.GetBy()
			case *counterv1.Decremented:
				model[e.StreamID.Canonical()] -= v.GetBy()
			}
			return nil
		})
	}()

	// The read model should converge to 15.
	deadline := time.After(10 * time.Second)
	for {
		mu.Lock()
		got := model[sid.Canonical()]
		mu.Unlock()
		if got == 15 {
			return // success: the whole pipeline delivered and projected
		}
		select {
		case <-deadline:
			t.Fatalf("read model did not converge; count=%d, want 15", got)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
