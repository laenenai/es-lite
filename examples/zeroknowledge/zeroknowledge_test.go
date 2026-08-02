package zeroknowledge_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/cryptostore"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	kmem "github.com/laenenai/es-lite/keystore/memory"
	"github.com/laenenai/es-lite/natsstore"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/shred"
)

// Requires PG_DSN and NATS_URL. See task test:zeroknowledge.
func TestZeroKnowledgeRoundTrip(t *testing.T) {
	pgDSN := os.Getenv("PG_DSN")
	natsURL := os.Getenv("NATS_URL")
	if pgDSN == "" || natsURL == "" {
		t.Skip("set PG_DSN and NATS_URL to run the zero-knowledge integration test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server: Postgres WITHOUT a keystore (holds no keys), over NATS. ---
	serverStore, err := postgres.Open(ctx, pgDSN, nil)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer serverStore.Close()
	if _, err := serverStore.Pool().Exec(ctx, `TRUNCATE events, workspace_keys, checkpoints, unique_claims`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	serverNC, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("server nats: %v", err)
	}
	defer serverNC.Close()
	srvCtx, srvStop := context.WithCancel(ctx)
	defer srvStop()
	go func() { _ = natsstore.NewServer(serverStore.Workspace).Serve(srvCtx, serverNC) }()

	// --- Client: holds keys (in-memory keystore), encrypts client-side. ---
	clientNC, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("client nats: %v", err)
	}
	defer clientNC.Close()
	client := natsstore.NewClient(clientNC)
	shredder := shred.New(kmem.New(), shred.NewMemDEKStore())
	const ws = "wszk"
	inner := client.Workspace(ws)
	encStore := cryptostore.New(inner, shredder, ws)

	// Wait for the server's subscriptions to be ready.
	sid, _ := es.NewStreamID("counter", "main")
	ready := false
	for i := 0; i < 60; i++ {
		if _, err := inner.CurrentStreamVersion(ctx, sid); err == nil {
			ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatal("server did not become ready")
	}

	// --- Commands through the normal runtime (client encrypts over NATS). ---
	rt := aggregate.NewRuntime(encStore, counter.Decider, counterv1.EventCodec{})
	for _, cmd := range []counterv1.Command{
		&counterv1.Init{Min: 0, Max: 100, Initial: 0},
		&counterv1.Increment{By: 10},
		&counterv1.Increment{By: 5},
	} {
		if _, err := rt.Handle(ctx, sid, cmd, es.Meta{}); err != nil {
			t.Fatalf("handle %T: %v", cmd, err)
		}
	}
	state, _, err := rt.Load(ctx, sid) // reads over NATS + decrypts + folds
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetCount() != 15 {
		t.Fatalf("count = %d, want 15", state.GetCount())
	}

	// --- Zero-knowledge: what the server stored is ciphertext, not the proto. ---
	plaintext, err := counterv1.EventCodec{}.Encode(&counterv1.Initialized{Min: 0, Max: 100, Value: 0})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	raw := rawServerPayload(t, serverStore, ws, sid.Canonical(), 1)
	if bytes.Equal(raw, plaintext.Payload) {
		t.Fatal("server stored plaintext proto — not zero-knowledge")
	}
	// AES-256-GCM overhead: 12-byte nonce + 16-byte tag.
	if len(raw) != len(plaintext.Payload)+28 {
		t.Fatalf("stored payload len = %d, want %d (ciphertext)", len(raw), len(plaintext.Payload)+28)
	}

	// --- Uniqueness over NATS with a client-computed HMAC value_key. ---
	sidA, _ := es.NewStreamID("thing", "a")
	sidB, _ := es.NewStreamID("thing", "b")
	claim := es.AppendParams{
		StreamID:        sidA,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: "test.v1.E", SchemaVersion: 1, Payload: []byte("x")}},
		Constraints:     []es.ConstraintOp{es.Claim("email", "a@b.com", true)},
	}
	if _, err := encStore.Append(ctx, claim); err != nil {
		t.Fatalf("first claim over NATS: %v", err)
	}
	claim.StreamID = sidB
	if _, err := encStore.Append(ctx, claim); !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("conflicting claim over NATS: got %v, want ErrConstraintViolated", err)
	}
}

// rawServerPayload reads the ciphertext the server actually stored, bypassing
// RLS — proving the server holds no plaintext.
func rawServerPayload(t *testing.T, s *postgres.Store, ws, canonicalStream string, version int) []byte {
	t.Helper()
	ctx := context.Background()
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('app.bypass_rls', 'on', true)`); err != nil {
		t.Fatal(err)
	}
	var p []byte
	if err := tx.QueryRow(ctx,
		`SELECT payload FROM events WHERE workspace_id=$1 AND stream_id=$2 AND version=$3`,
		ws, canonicalStream, version,
	).Scan(&p); err != nil {
		t.Fatalf("raw payload: %v", err)
	}
	return p
}
