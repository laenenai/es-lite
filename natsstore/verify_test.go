package natsstore_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/natsstore"
	"github.com/laenenai/es-lite/sqlite"
)

// TestVerifyStreamOverNATS proves ADR 0029 §3: a client that only holds
// the sealed bytes and the hashes the server returned can verify the chain
// through the NATS store, with no key and no new Store method. Skips
// without NATS_URL.
func TestVerifyStreamOverNATS(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("set NATS_URL to run the NATS store verification test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	prefix := "svc.eslite.verify" + strconv.FormatInt(time.Now().UnixNano(), 36)
	srv := natsstore.NewServer(func(string) es.Store { return store }, natsstore.WithServerPrefix(prefix))
	go func() { _ = srv.Serve(ctx, nc) }()
	time.Sleep(200 * time.Millisecond)

	remote := natsstore.NewClient(nc, natsstore.WithClientPrefix(prefix)).Workspace("ws1")
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(remote, counter.Decider, counterv1.EventCodec{})
	for _, cmd := range []counterv1.Command{&counterv1.Init{Min: 0, Max: 100, Initial: 1}, &counterv1.Increment{By: 2}} {
		if _, err := rt.Handle(ctx, sid, cmd, es.Meta{}); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	rep, err := es.VerifyStreamWith(ctx, remote, sid, es.VerifyOptions{Strict: true})
	if err != nil || !rep.Intact() || rep.Head != 2 {
		t.Fatalf("verify over NATS: err=%v rep=%+v", err, rep)
	}
}
