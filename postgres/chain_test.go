package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
)

// Hash-chain integration tests (ADR 0029); skip without PG_DSN like the rest.

func TestChainAndVerifyPostgres(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_chain")
	sid := streamID(t)
	rt := aggregate.NewRuntime(ws, counter.Decider, counterv1.EventCodec{})
	for _, cmd := range []counterv1.Command{
		&counterv1.Init{Min: 0, Max: 100, Initial: 5},
		&counterv1.Increment{By: 10},
		&counterv1.Increment{By: 20},
	} {
		if _, err := rt.Handle(ctx, sid, cmd, es.Meta{}); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	rows, err := ws.ReadStream(ctx, sid, 0, 0)
	if err != nil || len(rows) != 3 {
		t.Fatalf("read: %d rows err=%v", len(rows), err)
	}
	if !bytes.Equal(rows[0].PrevHash, es.ZeroHash[:]) || !bytes.Equal(rows[1].PrevHash, rows[0].Hash) || !bytes.Equal(rows[2].PrevHash, rows[1].Hash) {
		t.Fatal("prev_hash linkage wrong")
	}
	for _, e := range rows {
		if e.Workspace != "ws_chain" {
			t.Fatalf("v%d workspace = %q", e.Version, e.Workspace)
		}
		if h := es.EnvelopeHash(e); !bytes.Equal(h[:], e.Hash) {
			t.Fatalf("v%d: recomputed hash differs from the stored one", e.Version)
		}
	}
	since, err := s.ChainedSince(ctx)
	if err != nil || since.IsZero() {
		t.Fatalf("ChainedSince = %v err=%v", since, err)
	}
	rep, err := es.VerifyStreamWith(ctx, ws, sid, es.VerifyOptions{ChainedSince: since, Strict: true})
	if err != nil || !rep.Intact() || rep.ChainedFrom != 1 || rep.Head != 3 {
		t.Fatalf("verify: err=%v rep=%+v", err, rep)
	}

	// Tamper in place (RLS bypassed, as a DB operator would be).
	if _, err := s.Pool().Exec(ctx,
		`SELECT set_config('app.bypass_rls','on',false)`); err != nil {
		t.Fatal(err)
	}
	payload := append([]byte(nil), rows[1].Payload...)
	payload[len(payload)-1] ^= 0x01
	tag, err := s.Pool().Exec(ctx, `UPDATE events SET payload = $1 WHERE workspace_id = 'ws_chain' AND stream_id = $2 AND version = 2`, payload, sid.Canonical())
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("tamper: %v (rows %d)", err, tag.RowsAffected())
	}
	rep, err = es.VerifyStream(ctx, ws, sid)
	if !errors.Is(err, es.ErrChainBroken) || rep.BrokenAt != 2 {
		t.Fatalf("post-tamper: err=%v brokenAt=%d", err, rep.BrokenAt)
	}
	verifying := aggregate.NewRuntime(ws, counter.Decider, counterv1.EventCodec{}).WithChainVerification()
	if _, _, err := verifying.Load(ctx, sid); !errors.Is(err, es.ErrChainBroken) {
		t.Fatalf("verifying load err = %v", err)
	}
}

// TestRecordedAtMonotonicUnderContention is the ES-03 regression on the
// backend where it mattered: concurrent writers racing on one stream must
// produce recorded_at values that never decrease by version, because the
// clock is sampled after the version check and the loser never commits.
func TestRecordedAtMonotonicUnderContention(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_race")
	sid := streamID(t)
	rt := aggregate.NewRuntime(ws, counter.Decider, counterv1.EventCodec{}).WithConflictRetry(50, nil)
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 1 << 30, Initial: 0}), es.Meta{}); err != nil {
		t.Fatal(err)
	}
	const writers, each = 8, 10
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 1}), es.Meta{}); err != nil {
					t.Errorf("increment: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	rows, err := ws.ReadStream(ctx, sid, 0, 0)
	if err != nil || len(rows) != 1+writers*each {
		t.Fatalf("rows = %d err=%v", len(rows), err)
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].RecordedAt.Before(rows[i-1].RecordedAt) {
			t.Fatalf("recorded_at not monotonic at v%d", rows[i].Version)
		}
	}
	if rep, err := es.VerifyStream(ctx, ws, sid); err != nil || !rep.Intact() {
		t.Fatalf("verify after contention: err=%v rep=%+v", err, rep)
	}
	for _, e := range rows {
		got, err := ws.ReadStreamAsOf(ctx, sid, e.RecordedAt)
		if err != nil || uint64(len(got)) < e.Version {
			t.Fatalf("as-of v%d returned %d rows (err %v)", e.Version, len(got), err)
		}
	}
}

func TestAnchorsPostgres(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := streamID(t)
	for _, w := range []string{"ws_a", "ws_b"} {
		if _, err := s.Workspace(w).Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: 0,
			Events: []es.EventData{{TypeURL: "t.v1.E", SchemaVersion: 1, Payload: []byte("x")}}}); err != nil {
			t.Fatal(err)
		}
	}
	a, err := s.Anchor(ctx, 0)
	if err != nil || a.StreamCount != 2 {
		t.Fatalf("anchor: %+v err=%v", a, err)
	}
	if err := s.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("check: %v", err)
	}
	refs, err := s.ListStreams(ctx)
	if err != nil || len(refs) != 2 || refs[0].Workspace != "ws_a" || refs[1].Workspace != "ws_b" {
		t.Fatalf("streams = %+v err=%v", refs, err)
	}
	list, err := s.ListAnchors(ctx)
	if err != nil || len(list) != 1 || list[0].ID != a.ID {
		t.Fatalf("list = %+v err=%v", list, err)
	}
	// Anchors are admin-only: a workspace-scoped session sees none. Row-level
	// security never applies to a superuser (not even with FORCE), so the
	// negative check is only meaningful under a regular role — CI connects
	// as `postgres` and gets the positive checks above only.
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var super bool
	if err := tx.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&super); err != nil {
		t.Fatal(err)
	}
	if super {
		t.Log("connected as a superuser: RLS is bypassed by Postgres, skipping the anchors-invisible check")
		return
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.workspace_id', 'ws_a', true)`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM anchors`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("anchors visible under RLS: n=%d err=%v", n, err)
	}
}
