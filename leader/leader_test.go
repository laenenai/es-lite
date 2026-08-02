package leader_test

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/leader"
)

// Integration test against a real NATS server with JetStream. Skip unless
// NATS_URL is set:
//
//	docker run -d --rm --name nats -p 4222:4222 nats:2.10 -js
//	NATS_URL=nats://127.0.0.1:4222 go test ./leader/...
func dialJS(t *testing.T) (jetstream.JetStream, func()) {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("set NATS_URL to run the leader election integration test")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		t.Fatalf("jetstream: %v", err)
	}
	return js, nc.Close
}

// uniqueKey namespaces the leadership key per test run so parallel/repeated
// runs don't contend on a stale lease.
func uniqueKey() string {
	return "test." + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// TestSingleLeader: with several candidates racing, exactly one runs onLeader
// at a time, and each becomes leader for a positive duration.
func TestSingleLeader(t *testing.T) {
	js, closeConn := dialJS(t)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := leader.Config{Key: uniqueKey(), TTL: 2 * time.Second}
	var active int32  // concurrent leaders — must never exceed 1
	var elected int32 // total distinct leadership grants observed
	var violated int32
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		id := "cand-" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			c := cfg
			c.ID = id
			_ = leader.Run(ctx, js, c, func(lctx context.Context) {
				atomic.AddInt32(&elected, 1)
				if atomic.AddInt32(&active, 1) > 1 {
					atomic.StoreInt32(&violated, 1)
				}
				<-lctx.Done()
				atomic.AddInt32(&active, -1)
			})
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&violated) != 0 {
		t.Fatal("observed more than one concurrent leader")
	}
	if atomic.LoadInt32(&elected) == 0 {
		t.Fatal("no candidate ever became leader")
	}
}

// TestFailover: when the current leader steps down (its outer ctx is cancelled),
// a different candidate takes over.
func TestFailover(t *testing.T) {
	js, closeConn := dialJS(t)
	defer closeConn()

	key := uniqueKey()
	ttl := 2 * time.Second
	leaders := make(chan string, 4)

	cancels := map[string]context.CancelFunc{}
	var mu sync.Mutex
	start := func(id string) {
		ctx, cancel := context.WithCancel(context.Background())
		mu.Lock()
		cancels[id] = cancel
		mu.Unlock()
		go func() {
			_ = leader.Run(ctx, js, leader.Config{Key: key, ID: id, TTL: ttl}, func(lctx context.Context) {
				leaders <- id
				<-lctx.Done()
			})
		}()
	}
	start("A")
	start("B")
	defer func() {
		mu.Lock()
		for _, c := range cancels {
			c()
		}
		mu.Unlock()
	}()

	// Whoever wins first, cancel that candidate so it relinquishes the lease;
	// the other must then take over within a couple of lease periods.
	first := recvLeader(t, leaders, 5*time.Second)
	mu.Lock()
	cancels[first]()
	mu.Unlock()

	next := recvLeader(t, leaders, 6*time.Second)
	if next == first {
		t.Fatalf("expected failover to a different candidate, got %q twice", next)
	}
}

func recvLeader(t *testing.T, ch <-chan string, d time.Duration) string {
	t.Helper()
	select {
	case id := <-ch:
		return id
	case <-time.After(d):
		t.Fatalf("timed out waiting for a leader")
		return ""
	}
}
