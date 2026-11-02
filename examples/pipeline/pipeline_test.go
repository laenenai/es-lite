package pipeline_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
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

	// --- Storage: workspace-scoped Postgres (zero-knowledge; opaque payloads). ---
	store, err := postgres.Open(ctx, pgDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer store.Close()
	if _, err := store.Pool().Exec(ctx, `TRUNCATE events, checkpoints`); err != nil {
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

// TestPipelineDecisionTime proves the ADR 0034 trust boundary end to end: a
// command backdated beyond the skew window is rejected before it reaches
// Postgres, and an accepted command's event time is stamped from the server
// clock and preserved through the relay, NATS and the projection.
func TestPipelineDecisionTime(t *testing.T) {
	pgDSN := os.Getenv("PG_DSN")
	natsURL := os.Getenv("NATS_URL")
	if pgDSN == "" || natsURL == "" {
		t.Skip("set PG_DSN and NATS_URL to run the full-pipeline test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)

	store, err := postgres.Open(ctx, pgDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer store.Close()
	if _, err := store.Pool().Exec(ctx, `TRUNCATE events, checkpoints`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	ws := "wsdecide" + suffix

	nc, err := natsjs.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	streamName := "ES_DECIDE_" + suffix
	if _, err := natsjs.EnsureStream(ctx, js, natsjs.StreamConfig{
		Name:     streamName,
		Subjects: []string{"evt." + ws + ".>"},
	}); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	rt := aggregate.NewRuntime(store.Workspace(ws), counter.Decider, counterv1.EventCodec{})
	sid, _ := es.NewStreamID("counter", "decision")
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 0}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Backdated beyond the allowed skew: rejected before any append.
	_, err = rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{
		DecidedAt: timestamppb.New(time.Now().UTC().Add(-es.MaxDecisionSkew - time.Minute)),
	}), es.Meta{})
	if !errors.Is(err, es.ErrDecidedAtSkew) {
		t.Fatalf("backdated close error = %v, want ErrDecidedAtSkew", err)
	}
	if _, version, err := rt.Load(ctx, sid); err != nil || version != 1 {
		t.Fatalf("version after rejected close = %d (err %v), want 1 (nothing appended)", version, err)
	}

	// Start the read side only after the rejected command. The store is the
	// authority for "nothing was appended", so the test needs no fragile
	// negative timing assertion.
	pub := natsjs.NewPublisher(js, nil)
	relay := delivery.NewRelay(store, pub.Handle, delivery.RelayConfig{
		BatchSize:    10,
		PollInterval: 20 * time.Millisecond,
	})
	relayCtx, stopRelay := context.WithCancel(ctx)
	defer stopRelay()
	go relay.Run(relayCtx)

	var (
		mu       sync.Mutex
		closedAt time.Time
	)
	codec := counterv1.EventCodec{}
	consCtx, stopCons := context.WithCancel(ctx)
	defer stopCons()
	go func() {
		_ = natsjs.Consume(consCtx, js, natsjs.ConsumerConfig{
			Stream:        streamName,
			Durable:       "decision-view-" + suffix,
			FilterSubject: "evt." + ws + ".>",
		}, func(ctx context.Context, e es.Envelope) error {
			ev, err := codec.Decode(es.EncodedEvent{TypeURL: e.TypeURL, SchemaVersion: e.SchemaVersion, Payload: e.Payload})
			if err != nil {
				return err
			}
			if _, ok := ev.(*counterv1.Closed); ok {
				mu.Lock()
				closedAt = e.OccurredAt
				mu.Unlock()
			}
			return nil
		})
	}()

	// Within the skew window, but the client value is still not the event
	// time: the runtime stamps OccurredAt from the server clock.
	clientDecided := time.Now().UTC().Add(-3 * time.Minute)
	before := time.Now().Add(-time.Second)
	res, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Close{
		DecidedAt: timestamppb.New(clientDecided),
	}), es.Meta{})
	if err != nil {
		t.Fatalf("close within skew: %v", err)
	}
	after := time.Now().Add(time.Second)
	got := res.Envelopes[0].OccurredAt
	if got.Before(before) || got.After(after) || got.Equal(clientDecided) {
		t.Fatalf("OccurredAt = %v, want server time in [%v,%v], not client %v", got, before, after, clientDecided)
	}

	deadline := time.After(10 * time.Second)
	for {
		mu.Lock()
		projected := closedAt
		mu.Unlock()
		if !projected.IsZero() {
			if projected.Before(before) || projected.After(after) || projected.Equal(clientDecided) {
				t.Fatalf("projected OccurredAt = %v, want server time in [%v,%v], not client %v", projected, before, after, clientDecided)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("projection did not receive Closed")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
