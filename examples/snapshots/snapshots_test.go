package snapshots_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/internal/memdek"
	kmem "github.com/laenenai/es-lite/keystore/memory"
	"github.com/laenenai/es-lite/shred"
	"github.com/laenenai/es-lite/snapshot/natskv"
	"github.com/laenenai/es-lite/sqlite"
)

// counterStateCodec serializes the proto Counter state for the snapshot cache.
type counterStateCodec struct{}

func (counterStateCodec) Encode(s *counterv1.Counter) ([]byte, error) { return proto.Marshal(s) }
func (counterStateCodec) Decode(b []byte) (*counterv1.Counter, error) {
	var c counterv1.Counter
	err := proto.Unmarshal(b, &c)
	return &c, err
}

// Requires NATS_URL (JetStream). See task test:snapshots.
func TestSnapshotCache(t *testing.T) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		t.Skip("set NATS_URL to run the snapshot-cache integration test")
	}
	ctx := context.Background()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	const ws = "wssnap"
	bucket := "es_snap_" + suffix
	shredder := shred.New(kmem.New(), memdek.New())
	cache, err := natskv.New(ctx, js, shredder, ws, natskv.Config{Bucket: bucket, TTL: time.Hour})
	if err != nil {
		t.Fatalf("cache: %v", err)
	}

	openStore := func(name string) *sqlite.Store {
		s, err := sqlite.Open(ctx, "file:"+name+suffix+"?mode=memory&cache=shared")
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	codec := counterv1.EventCodec{}
	stateCodec := counterStateCodec{}
	sid, _ := es.NewStreamID("counter", "main")

	// Write through the snapshot cache while handling commands (0 +10 +5 = 15).
	store := openStore("primary")
	rt := aggregate.NewRuntime(store, counter.Decider, codec).WithSnapshots(cache, stateCodec, 1)
	for _, cmd := range []counterv1.Command{
		&counterv1.Init{Min: 0, Max: 100, Initial: 0},
		&counterv1.Increment{By: 10},
		&counterv1.Increment{By: 5},
	} {
		if _, err := rt.Handle(ctx, sid, cmd, es.Meta{}); err != nil {
			t.Fatalf("handle %T: %v", cmd, err)
		}
	}

	// 1. Snapshot exists at version 3 with the right state.
	snap, ok, err := cache.Load(ctx, sid.Canonical())
	if err != nil || !ok {
		t.Fatalf("snapshot load: ok=%v err=%v", ok, err)
	}
	if snap.Version != 3 {
		t.Fatalf("snapshot version = %d, want 3", snap.Version)
	}

	// 2. Snapshot SEEDS a fresh runtime over an EMPTY store — proving the state
	//    came from the snapshot, not from folding events.
	empty := openStore("empty")
	rt2 := aggregate.NewRuntime(empty, counter.Decider, codec).WithSnapshots(cache, stateCodec, 1)
	s2, v2, err := rt2.Load(ctx, sid)
	if err != nil {
		t.Fatalf("seeded load: %v", err)
	}
	if s2.GetCount() != 15 || v2 != 3 {
		t.Fatalf("seeded load: count=%d version=%d, want 15/3 (from snapshot alone)", s2.GetCount(), v2)
	}

	// 3. Bounded-staleness read — cache-only, no store hit.
	s3, v3, fresh, err := rt.LoadStale(ctx, sid, 5*time.Minute)
	if err != nil || !fresh {
		t.Fatalf("LoadStale: fresh=%v err=%v", fresh, err)
	}
	if s3.GetCount() != 15 || v3 != 3 {
		t.Fatalf("LoadStale: count=%d version=%d, want 15/3", s3.GetCount(), v3)
	}

	// 4. Fold-version bump invalidates the snapshot (ignored, refold). Over the
	//    empty store that means Initial state (count 0).
	rt3 := aggregate.NewRuntime(empty, counter.Decider, codec).WithSnapshots(cache, stateCodec, 2)
	s4, _, err := rt3.Load(ctx, sid)
	if err != nil {
		t.Fatalf("fold-version load: %v", err)
	}
	if s4.GetCount() != 0 {
		t.Fatalf("stale fold-version snapshot was used: count=%d, want 0", s4.GetCount())
	}

	// 5. Zero-knowledge: the state stored in KV is ciphertext, not the proto.
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		t.Fatalf("kv: %v", err)
	}
	raw, err := kv.Get(ctx, ws+".counter_main")
	if err != nil {
		t.Fatalf("kv get: %v", err)
	}
	var e struct {
		State []byte `json:"state"`
	}
	if err := json.Unmarshal(raw.Value(), &e); err != nil {
		t.Fatalf("unmarshal entry: %v", err)
	}
	plaintext, _ := stateCodec.Encode(&counterv1.Counter{Initialized: true, Min: 0, Max: 100, Count: 15})
	if bytes.Equal(e.State, plaintext) {
		t.Fatal("snapshot state stored as plaintext proto — not zero-knowledge")
	}
	if len(e.State) != len(plaintext)+28 { // AES-256-GCM: 12 nonce + 16 tag
		t.Fatalf("stored state len = %d, want %d (ciphertext)", len(e.State), len(plaintext)+28)
	}
}
