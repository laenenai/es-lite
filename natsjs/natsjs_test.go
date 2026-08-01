package natsjs_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/natsjs"
)

// Integration test against a real NATS server with JetStream. Skip unless
// NATS_URL is set:
//
//	docker run -d --rm --name nats -p 4222:4222 nats:2.10 -js
//	NATS_URL=nats://127.0.0.1:4222 go test ./natsjs/...
func TestPublishDedupAndConsume(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("set NATS_URL to run the NATS/JetStream integration test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nc, err := natsjs.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	// Unique stream per run (subjects namespaced by workspace token below).
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	streamName := "ES_TEST_" + suffix
	ws := "ws" + suffix
	if _, err := natsjs.EnsureStream(ctx, js, natsjs.StreamConfig{
		Name:     streamName,
		Subjects: []string{"evt." + ws + ".>"},
	}); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	pub := natsjs.NewPublisher(js, nil)
	sid, _ := es.NewStreamID("counter", "main")
	batch := make([]es.Envelope, 3)
	for i := range batch {
		batch[i] = es.Envelope{
			EventID:        uuid.New(),
			StreamID:       sid,
			Workspace:      ws,
			Version:        uint64(i + 1),
			GlobalPosition: uint64(i + 1),
			TypeURL:        "counter.v1.Incremented",
			SchemaVersion:  1,
			OccurredAt:     time.Now().UTC(),
			RecordedAt:     time.Now().UTC(),
			Payload:        []byte("event-" + strconv.Itoa(i)),
		}
	}

	if err := pub.Handle(ctx, batch); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Republish the same batch: JetStream must dedup on Nats-Msg-Id.
	if err := pub.Handle(ctx, batch); err != nil {
		t.Fatalf("republish: %v", err)
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 3 {
		t.Fatalf("stream has %d msgs after double publish, want 3 (dedup failed)", info.State.Msgs)
	}

	// Subject derivation sanity.
	if got := natsjs.DefaultSubject(batch[0]); got != "evt."+ws+".counter.incremented" {
		t.Fatalf("subject = %q", got)
	}

	// Consume the 3 events through a durable consumer.
	got := make(chan es.Envelope, 8)
	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		_ = natsjs.Consume(consumeCtx, js, natsjs.ConsumerConfig{
			Stream:  streamName,
			Durable: "test-projection",
		}, func(ctx context.Context, e es.Envelope) error {
			got <- e
			return nil
		})
	}()

	seen := map[uint64]es.Envelope{}
	for len(seen) < 3 {
		select {
		case e := <-got:
			seen[e.GlobalPosition] = e
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out; got %d/3 events", len(seen))
		}
	}
	for i := uint64(1); i <= 3; i++ {
		e, ok := seen[i]
		if !ok {
			t.Fatalf("missing event gp=%d", i)
		}
		if e.StreamID.Type != "counter" || e.Workspace != ws || e.TypeURL != "counter.v1.Incremented" {
			t.Fatalf("decoded envelope wrong: %+v", e)
		}
		if string(e.Payload) != "event-"+strconv.Itoa(int(i-1)) {
			t.Fatalf("gp=%d payload=%q", i, e.Payload)
		}
	}
}
