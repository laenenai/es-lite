package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestAnchorFenceBlockedErrorOnCtxEnd pins the operator-facing wording of
// the anchor fence's ctx-end path (the breaking-changes entry promises
// `postgres: anchor fence still blocked`): while an append blocks the
// fence, an Anchor call whose context ends first must fail with that
// phrase wrapping context.DeadlineExceeded — whichever side of the poll
// cycle the end lands on. The deadline deliberately fires while a
// fenceCleared round trip is in flight: that is the poll-error branch,
// which unfixed code answered with a bare `check fence: … context deadline
// exceeded`, losing the promised phrase. The parked-select branch always
// carried it; with the fix both do, so the test is green either way and
// the tuned deadline only decides how sharp the regression pin is.
// (102.8ms measured as the center of the mid-poll hit plateau on the dev
// box — the ~100ms poll tick fires a couple of ms late under go test's
// timer slack; a machine that parks the deadline in the select instead
// still passes, just via the other branch.)
func TestAnchorFenceBlockedErrorOnCtxEnd(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_anchor_ctx")

	// An append held in flight blocks the fence: Anchor cannot resolve a
	// settled bound until it settles, so the ctx ends first.
	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	feedAppendTx(t, ctx, ws, tx1, feedStream(t, "anchor-ctx-block"), 0, "anchortest.v1.B")

	actx, cancel := context.WithTimeout(ctx, 102*time.Millisecond+800*time.Microsecond)
	defer cancel()
	_, err = s.Anchor(actx, 0)
	if err == nil {
		t.Fatal("Anchor under a blocked fence with an expiring ctx: want an error")
	}
	if !strings.Contains(err.Error(), "anchor fence still blocked") {
		t.Fatalf("Anchor err = %q, want the mandated anchor fence wording", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Anchor err = %v, want context.DeadlineExceeded wrapped", err)
	}
}

// TestAnchorStableUnderConcurrentAppend pins F1: an anchor recorded under
// live concurrent appends must verify forever after. Postgres assigns
// global_position at INSERT, not at COMMIT, so the committed max can sit
// above an in-flight lower position — and Anchor reads the head and the
// stream heads as two READ COMMITTED statements, i.e. two snapshots. The
// unfenced Anchor saw max=pC but heads without the in-flight pB and
// recorded a root over the torn head; once pB committed, CheckAnchor
// disputed its own anchor with a false ErrAnchorMismatch (the live repro
// from the review).
//
// The shape: A commits at pA, B is held in flight at pB via AppendTx, C
// commits at pC > pB, then Anchor(0) runs while B is still uncommitted.
// The fenced Anchor draws the horizon fence (spec §5.1.1) and cannot
// record until B settles — while the unfenced one records immediately —
// so the anchor call must overlap B's commit. The test settles B once the
// unfenced Anchor has recorded (a committed anchor row means the torn
// root is already set) or once no anchor can appear (only the fenced
// Anchor can be waiting on B, which only the test can settle). Either
// world is then deterministic on committed state, not on timing: a torn
// root is torn before B commits, and a fenced Anchor only records after
// B settles. CheckAnchor must report nil — nothing was tampered.
func TestAnchorStableUnderConcurrentAppend(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_anchor_race")

	// A commits first: pA is the settled head when the race starts.
	tx0, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx0: %v", err)
	}
	pA := feedAppendTx(t, ctx, ws, tx0, feedStream(t, "anchor-stable-a"), 0, "anchortest.v1.A")
	if err := tx0.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}

	// B draws pB and stays open: the append in flight below the head.
	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	pB := feedAppendTx(t, ctx, ws, tx1, feedStream(t, "anchor-stable-b"), 0, "anchortest.v1.B")

	// C commits at pC above the in-flight pB: the committed max now sits
	// above an uncommitted position — the torn head.
	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	pC := feedAppendTx(t, ctx, ws, tx2, feedStream(t, "anchor-stable-c"), 0, "anchortest.v1.C")
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit C: %v", err)
	}
	if !(pA < pB && pB < pC) {
		t.Fatalf("insert order: pA=%d pB=%d pC=%d, want pA < pB < pC", pA, pB, pC)
	}

	// Anchor over the whole log while B is in flight.
	type anchorResult struct {
		a   es.Anchor
		err error
	}
	anchorCh := make(chan anchorResult, 1)
	go func() {
		a, err := s.Anchor(ctx, 0)
		anchorCh <- anchorResult{a, err}
	}()

	// Release B: when a new anchor row is committed the unfenced Anchor
	// has already recorded its root over heads that miss B (its heads
	// statement ran before the insert, while B was provably still in
	// flight); when no row can appear the fenced Anchor is provably
	// waiting on the fence, which B blocks. Settle B either way — each
	// world can only record in the order it already recorded in.
	deadline := time.Now().Add(2 * time.Second)
	for {
		list, err := s.ListAnchors(ctx)
		if err != nil {
			t.Fatalf("list anchors: %v", err)
		}
		if len(list) > 0 {
			break // the anchor row is committed; its root is already computed
		}
		if time.Now().After(deadline) {
			break // the fenced anchor is waiting on B; settle it
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit B: %v", err)
	}

	res := <-anchorCh
	if res.err != nil {
		t.Fatalf("Anchor: %v", res.err)
	}
	// B committed below the anchored head: the anchor's root must already
	// account for it, or the bound must have excluded the torn range. A
	// mismatch here is a false tamper alarm — the bug.
	if err := s.CheckAnchor(ctx, res.a); err != nil {
		t.Fatalf("CheckAnchor after concurrent append: %v", err)
	}
}

// TestCheckAnchorStableUnderConcurrentAppend pins the recheck side of F1:
// an anchor stays verifiable while appends race around it. The anchor is
// taken first, over a quiescent log (its bound is the last drawn position,
// permanently settled by the fence); then an append is held in flight at
// pB above that bound, a higher append commits at pC, and the held one
// commits between the Anchor and CheckAnchor calls — the exact window in
// which an unfenced recheck could see a head that is not final. Positions
// above the anchor's bound cannot change its root, and nothing below the
// bound was still in flight when it was recorded, so CheckAnchor must
// report nil: the anchor verifies despite the concurrent appends.
func TestCheckAnchorStableUnderConcurrentAppend(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_anchor_check_race")

	// A commits: the whole settled log at anchor time.
	tx0, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx0: %v", err)
	}
	pA := feedAppendTx(t, ctx, ws, tx0, feedStream(t, "anchor-check-a"), 0, "anchortest.v1.A")
	if err := tx0.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}

	// Anchor before any concurrency: the bound is pA, the last drawn
	// position, and the fence has settled it.
	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("Anchor: %v", err)
	}
	if a.UptoGlobalPosition != pA || a.StreamCount != 1 {
		t.Fatalf("anchor = %+v, want bound %d over 1 stream", a, pA)
	}

	// B draws pB above the bound and stays open: the in-flight append.
	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	pB := feedAppendTx(t, ctx, ws, tx1, feedStream(t, "anchor-check-b"), 0, "anchortest.v1.B")

	// C commits at pC above the in-flight pB.
	tx2, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	pC := feedAppendTx(t, ctx, ws, tx2, feedStream(t, "anchor-check-c"), 0, "anchortest.v1.C")
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit C: %v", err)
	}
	if !(pA < pB && pB < pC) {
		t.Fatalf("insert order: pA=%d pB=%d pC=%d, want pA < pB < pC", pA, pB, pC)
	}

	// B commits between the Anchor and CheckAnchor calls.
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit B: %v", err)
	}

	// The recheck must still agree: B and C sit above the anchor's bound,
	// and the bound itself was settled when recorded.
	if err := s.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("CheckAnchor after concurrent append: %v", err)
	}
}
