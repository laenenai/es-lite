package projection_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/laenenai/es-lite/delivery"
	"github.com/laenenai/es-lite/es"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	counterviewv1 "github.com/laenenai/es-lite/gen/counterview/v1"
	"github.com/laenenai/es-lite/natsjs"
)

// TestProjectorOverJetStream runs the generated CounterActivity runner over
// natsjs.NewFeed with the generated filter subjects (two of them: the
// multi-filter consumer of NATS 2.10), fed by the SQLite poller -> JetStream
// relay. Skips without NATS_URL.
func TestProjectorOverJetStream(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("set NATS_URL to run the JetStream projector test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFixture(t)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)

	nc, err := natsjs.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	streamName := "ES_PROJ_" + suffix
	if _, err := natsjs.EnsureStream(ctx, js, natsjs.StreamConfig{Name: streamName, Subjects: []string{"evt.default.>"}}); err != nil {
		t.Fatal(err)
	}
	// SQLite is single-workspace: subjects use the "default" token.
	// Namespace the durable and the stream per run instead.
	pub := natsjs.NewPublisher(js, nil)
	poller := delivery.NewPoller(f.store, f.store, pub.Handle, delivery.Config{Subscriber: "relay-" + suffix, PollInterval: 10 * time.Millisecond})
	go poller.Run(ctx)

	f.handle(t, "a", &counterv1.Init{Min: 0, Max: 100, Initial: 5}, &counterv1.Increment{By: 1}, &counterv1.Decrement{By: 1}, &counterv1.Close{DecidedAt: timestamppb.Now()})

	idx := NewActivityProbe()
	runner := counterviewv1.NewCounterActivityRunner(idx, counterviewv1.CounterActivityDeps{})
	feed := natsjs.NewFeed(js, natsjs.ConsumerConfig{
		Stream:         streamName,
		Durable:        "activity-" + suffix,
		FilterSubjects: counterviewv1.CounterActivitySubjects(""),
		Gaps:           natsjs.NewGapDetector(),
	})
	go func() { _ = runner.Run(ctx, feed) }()

	deadline := time.After(10 * time.Second)
	for idx.Events("a") < 2 {
		select {
		case <-deadline:
			t.Fatalf("activity a = %d after 10s, want 2", idx.Events("a"))
		case <-time.After(20 * time.Millisecond):
		}
	}
	time.Sleep(200 * time.Millisecond)
	if n := idx.Events("a"); n != 2 {
		t.Fatalf("activity a = %d, want exactly 2 (Initialized/Closed filtered out by subject)", n)
	}
	// Other events must not even reach the handler through the filter:
	// the marker nonetheless sits at the last consumed position.
	if pos, _ := runner.Position(ctx); pos != 3 {
		t.Fatalf("marker = %d, want 3 (the Decremented event)", pos)
	}
}

// activityProbe is the Activity handler with a seen-type check.
type activityProbe struct {
	*activityImpl
}

func NewActivityProbe() *activityProbe { return &activityProbe{activityImpl: newActivityImpl()} }

type activityImpl struct{ events map[string]uint64 }

func newActivityImpl() *activityImpl { return &activityImpl{events: map[string]uint64{}} }

func (a *activityProbe) OnIncremented(_ context.Context, env es.Envelope) error {
	a.events[env.StreamID.ID]++
	return nil
}
func (a *activityProbe) OnDecremented(_ context.Context, env es.Envelope) error {
	a.events[env.StreamID.ID]++
	return nil
}
func (a *activityProbe) Events(id string) uint64 { return a.events[id] }
