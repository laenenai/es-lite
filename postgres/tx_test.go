package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/laenenai/es-lite/es"
)

// narrowTx exposes exactly the three statement methods tenancy.TenantTx
// exposes. It proves the transaction-aware store API needs no Commit,
// Rollback, Begin or pool: a tenant-scoped handle can be passed directly.
type narrowTx struct{ tx pgx.Tx }

func (n narrowTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return n.tx.Exec(ctx, sql, args...)
}

func (n narrowTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return n.tx.Query(ctx, sql, args...)
}

func (n narrowTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return n.tx.QueryRow(ctx, sql, args...)
}

func eventData(payload string) []es.EventData {
	return []es.EventData{{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: []byte(payload)}}
}

func TestAppendTxCommitPersists(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_tx")
	sid := streamID(t)

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	res, err := ws.AppendTx(ctx, narrowTx{tx}, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          eventData("inside-tx"),
	})
	if err != nil {
		t.Fatalf("append tx: %v", err)
	}
	if res.FromVersion != 0 || res.ToVersion != 1 || len(res.Envelopes) != 1 {
		t.Fatalf("result = %+v, want v0->v1 with one envelope", res)
	}

	// Read-your-writes through the caller's transaction.
	rows, err := ws.ReadStreamTx(ctx, narrowTx{tx}, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream tx: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows inside tx = %d, want 1", len(rows))
	}
	got := rows[0]
	if got.Workspace != "ws_tx" || !bytes.Equal(got.Payload, []byte("inside-tx")) {
		t.Fatalf("row = workspace %q payload %q", got.Workspace, got.Payload)
	}
	if got.GlobalPosition == 0 || got.RecordedAt.IsZero() {
		t.Fatalf("row missing global_position/recorded_at: %+v", got)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The same event is visible through the ordinary, self-transacting store.
	rows, err = s.Workspace("ws_tx").ReadStream(ctx, sid, 0, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows after commit = %d (err %v), want 1", len(rows), err)
	}
}

func TestAppendTxRollbackDiscardsEventsAndClaims(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_tx_rollback")
	sid := streamID(t)

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := ws.AppendTx(ctx, narrowTx{tx}, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          eventData("rolled-back"),
		Constraints:     []es.ConstraintOp{es.Claim("email", "rollback@example.test")},
	}); err != nil {
		t.Fatalf("append tx: %v", err)
	}
	if rows, err := ws.ReadStreamTx(ctx, narrowTx{tx}, sid, 0, 0); err != nil || len(rows) != 1 {
		t.Fatalf("rows before rollback = %d (err %v), want 1", len(rows), err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	rows, err := s.Workspace("ws_tx_rollback").ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read after rollback: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows after rollback = %d, want 0", len(rows))
	}
	if v, err := s.Workspace("ws_tx_rollback").CurrentStreamVersion(ctx, sid); err != nil || v != 0 {
		t.Fatalf("version after rollback = %d (err %v), want 0", v, err)
	}
	if _, found, err := s.Workspace("ws_tx_rollback").LookupClaim(ctx, "email", "rollback@example.test"); err != nil || found {
		t.Fatalf("claim after rollback found=%v err=%v, want absent", found, err)
	}
}

func TestAppendTxWorkspaceIsolationInOneTransaction(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	a := s.WorkspaceStore("ws_tx_a")
	b := s.WorkspaceStore("ws_tx_b")
	sid := streamID(t)

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	narrow := narrowTx{tx}

	if _, err := a.AppendTx(ctx, narrow, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0, Events: eventData("a"),
	}); err != nil {
		t.Fatalf("append a: %v", err)
	}
	if rows, err := b.ReadStreamTx(ctx, narrow, sid, 0, 0); err != nil || len(rows) != 0 {
		t.Fatalf("workspace b saw %d of a's events (err %v), want 0", len(rows), err)
	}
	if rows, err := a.ReadStreamTx(ctx, narrow, sid, 0, 0); err != nil || len(rows) != 1 {
		t.Fatalf("workspace a saw %d events (err %v), want 1", len(rows), err)
	}

	// The same StreamID is an independent, empty stream in workspace b.
	if _, err := b.AppendTx(ctx, narrow, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0, Events: eventData("b"),
	}); err != nil {
		t.Fatalf("append b: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	for _, tc := range []struct{ ws, want string }{{"ws_tx_a", "a"}, {"ws_tx_b", "b"}} {
		rows, err := s.Workspace(tc.ws).ReadStream(ctx, sid, 0, 0)
		if err != nil || len(rows) != 1 {
			t.Fatalf("%s rows = %d (err %v), want 1", tc.ws, len(rows), err)
		}
		if rows[0].Workspace != tc.ws || !bytes.Equal(rows[0].Payload, []byte(tc.want)) {
			t.Fatalf("%s row = workspace %q payload %q", tc.ws, rows[0].Workspace, rows[0].Payload)
		}
	}
}

func TestAppendTxPreservesCallerTenantContext(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_tx_ctx")
	sid := streamID(t)

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', 'tenant-a', true)`); err != nil {
		t.Fatalf("set caller tenant context: %v", err)
	}

	if _, err := ws.AppendTx(ctx, narrowTx{tx}, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0, Events: eventData("tenant-scoped"),
	}); err != nil {
		t.Fatalf("append tx: %v", err)
	}

	var tenant, workspace string
	if err := tx.QueryRow(ctx,
		`SELECT current_setting('app.tenant_id', true), current_setting('app.workspace_id', true)`,
	).Scan(&tenant, &workspace); err != nil {
		t.Fatalf("read settings: %v", err)
	}
	if tenant != "tenant-a" {
		t.Fatalf("app.tenant_id = %q, want caller's tenant-a untouched", tenant)
	}
	if workspace != "ws_tx_ctx" {
		t.Fatalf("app.workspace_id = %q, want es-lite's ws_tx_ctx", workspace)
	}
}

func TestAppendTxOptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_tx_conflict")
	sid := streamID(t)

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	narrow := narrowTx{tx}

	if _, err := ws.AppendTx(ctx, narrow, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0, Events: eventData("first"),
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	_, err = ws.AppendTx(ctx, narrow, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0, Events: eventData("stale"),
	})
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("stale append error = %v, want ErrConflict", err)
	}
	rows, err := ws.ReadStreamTx(ctx, narrow, sid, 0, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows after conflict = %d (err %v), want 1", len(rows), err)
	}
}

func TestAppendTxChainPositionAndRecordedAt(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_tx_chain")
	sid := streamID(t)
	occurred := time.Date(2026, 11, 2, 9, 30, 0, 123456000, time.UTC)

	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin first: %v", err)
	}
	first, err := ws.AppendTx(ctx, narrowTx{tx1}, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          append(eventData("one"), eventData("two")...),
		OccurredAt:      occurred,
	})
	if err != nil {
		tx1.Rollback(ctx)
		t.Fatalf("append first: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit first: %v", err)
	}

	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin second: %v", err)
	}
	second, err := ws.AppendTx(ctx, narrowTx{tx2}, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 2,
		Events:          eventData("three"),
		OccurredAt:      occurred,
	})
	if err != nil {
		tx2.Rollback(ctx)
		t.Fatalf("append second: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit second: %v", err)
	}

	rows, err := s.Workspace("ws_tx_chain").ReadStream(ctx, sid, 0, 0)
	if err != nil || len(rows) != 3 {
		t.Fatalf("rows = %d (err %v), want 3", len(rows), err)
	}
	if !rows[0].OccurredAt.Equal(es.TruncateMicros(occurred)) {
		t.Fatalf("occurred_at = %v, want %v", rows[0].OccurredAt, es.TruncateMicros(occurred))
	}
	if !bytes.Equal(rows[0].PrevHash, es.ZeroHash[:]) ||
		!bytes.Equal(rows[1].PrevHash, rows[0].Hash) ||
		!bytes.Equal(rows[2].PrevHash, rows[1].Hash) {
		t.Fatal("prev_hash linkage wrong across caller transactions")
	}
	for i, e := range rows {
		if h := es.EnvelopeHash(e); !bytes.Equal(h[:], e.Hash) {
			t.Fatalf("v%d: recomputed hash differs", e.Version)
		}
		if i > 0 && rows[i].GlobalPosition <= rows[i-1].GlobalPosition {
			t.Fatalf("global_position not increasing at v%d", e.Version)
		}
		if i > 0 && rows[i].RecordedAt.Before(rows[i-1].RecordedAt) {
			t.Fatalf("recorded_at decreased at v%d", e.Version)
		}
	}
	for _, res := range []es.AppendResult{first, second} {
		for _, e := range res.Envelopes {
			if e.GlobalPosition == 0 || e.RecordedAt.IsZero() {
				t.Fatalf("returned envelope missing global_position/recorded_at: %+v", e)
			}
		}
	}

	since, err := s.ChainedSince(ctx)
	if err != nil || since.IsZero() {
		t.Fatalf("ChainedSince = %v (err %v)", since, err)
	}
	rep, err := es.VerifyStreamWith(ctx, s.Workspace("ws_tx_chain"), sid,
		es.VerifyOptions{ChainedSince: since, Strict: true})
	if err != nil || !rep.Intact() || rep.ChainedFrom != 1 || rep.Head != 3 {
		t.Fatalf("verify: err=%v rep=%+v", err, rep)
	}
}
