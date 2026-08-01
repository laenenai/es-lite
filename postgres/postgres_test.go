package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/keystore"
	kmem "github.com/laenenai/es-lite/keystore/memory"
	"github.com/laenenai/es-lite/postgres"
)

// Integration tests against a real Postgres. Skip unless PG_DSN is set:
//
//	docker run -d --rm --name pg -p 5433:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=eslite postgres:17
//	PG_DSN='postgres://postgres:pw@127.0.0.1:5433/eslite?sslmode=disable' go test ./postgres/...
func openStore(t *testing.T) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	ctx := context.Background()
	s, err := postgres.Open(ctx, dsn, kmem.New())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Isolate each test: tests run sequentially, so a truncate is safe and
	// keeps the shared partitioned tables clean.
	if _, err := s.Pool().Exec(ctx, `TRUNCATE events, workspace_keys, checkpoints`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func streamID(t *testing.T) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("counter", "main")
	if err != nil {
		t.Fatal(err)
	}
	return sid
}

func TestAppendFoldTimeTravel(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	rt := aggregate.NewRuntime(s.Workspace("ws_a"), counter.Decider, counterv1.EventCodec{})
	sid := streamID(t)

	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 20}), es.Meta{}); err != nil {
		t.Fatalf("increment: %v", err)
	}

	state, version, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetCount() != 25 || version != 2 {
		t.Fatalf("count=%d version=%d, want 25/2", state.GetCount(), version)
	}

	past, err := rt.LoadAsOfVersion(ctx, sid, 1)
	if err != nil {
		t.Fatalf("as-of-version: %v", err)
	}
	if past.GetCount() != 5 {
		t.Fatalf("as-of v1: count=%d, want 5", past.GetCount())
	}
}

func TestWorkspaceIsolationRLS(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := streamID(t)

	rtA := aggregate.NewRuntime(s.Workspace("ws_a"), counter.Decider, counterv1.EventCodec{})
	if _, err := rtA.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 10, Initial: 1}), es.Meta{}); err != nil {
		t.Fatalf("init in ws_a: %v", err)
	}

	// A different workspace sees an empty stream for the same StreamID: RLS
	// blocks cross-workspace reads even with the identical id.
	evs, err := s.Workspace("ws_b").ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read ws_b: %v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("ws_b saw %d events for ws_a's stream; RLS leak", len(evs))
	}

	// ws_a still sees its own event.
	evs, err = s.Workspace("ws_a").ReadStream(ctx, sid, 0, 0)
	if err != nil || len(evs) != 1 {
		t.Fatalf("ws_a read: len=%d err=%v, want 1", len(evs), err)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_a")
	sid := streamID(t)

	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	// Stale expected version loses.
	_, err := ws.Append(ctx, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("y")}},
	})
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("stale append: got %v, want ErrConflict", err)
	}
}

func TestEncryptionAtRestAndShred(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_secret")
	sid := streamID(t)

	marker := []byte("PLAINTEXT-CANARY-abc123")
	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: marker}},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Raw column must be ciphertext: the plaintext canary must not appear.
	raw := rawPayload(t, s, "ws_secret", sid.Canonical(), 1)
	if bytes.Contains(raw, marker) {
		t.Fatal("plaintext canary found in stored payload; not encrypted at rest")
	}

	// Normal read decrypts back to the canary.
	evs, err := ws.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(evs) != 1 || !bytes.Equal(evs[0].Payload, marker) {
		t.Fatalf("decrypted read mismatch: %v", evs)
	}

	// Crypto-shred the workspace: the same read now fails ErrShredded.
	if err := s.ForgetWorkspace(ctx, "ws_secret"); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if _, err := ws.ReadStream(ctx, sid, 0, 0); !errors.Is(err, keystore.ErrShredded) {
		t.Fatalf("read after shred: got %v, want ErrShredded", err)
	}
}

func TestDrainClaimDelivery(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := streamID(t)

	// Events across two workspaces.
	for _, ws := range []string{"ws_a", "ws_b"} {
		rt := aggregate.NewRuntime(s.Workspace(ws), counter.Decider, counterv1.EventCodec{})
		if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 0}), es.Meta{}); err != nil {
			t.Fatalf("init %s: %v", ws, err)
		}
		if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 3}), es.Meta{}); err != nil {
			t.Fatalf("inc %s: %v", ws, err)
		}
	}

	var got []es.Envelope
	n, err := s.Drain(ctx, 100, func(ctx context.Context, batch []es.Envelope) error {
		got = append(got, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 4 || len(got) != 4 {
		t.Fatalf("drain claimed=%d delivered=%d, want 4/4", n, len(got))
	}
	// Global order is monotonic across workspaces.
	for i := 1; i < len(got); i++ {
		if got[i].GlobalPosition <= got[i-1].GlobalPosition {
			t.Fatalf("global_position not increasing at %d", i)
		}
	}
	// Everything is now published: a second drain claims nothing.
	n2, err := s.Drain(ctx, 100, func(context.Context, []es.Envelope) error { return nil })
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second drain claimed %d, want 0", n2)
	}
}

// rawPayload reads the stored (encrypted) payload bytes, bypassing RLS and
// decryption, to prove what is actually on disk.
func rawPayload(t *testing.T, s *postgres.Store, ws, canonicalStream string, version int) []byte {
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
