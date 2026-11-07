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
	"github.com/laenenai/es-lite/postgres"
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

// Log-order attestation (design D1, finding F11). The per-stream chain
// and the heads anchor bind what each stream contains; nothing bound the
// order in which events landed in the global log. Every append now
// writes one log_order row hashed over exactly its own events, anchors
// aggregate those rows into an order root, and VerifyOrder recomputes
// each row's hash from the events — the check that catches a
// repositioned event, which leaves every chain and every head intact.

// appendOrderBatch appends n events to sid as one batch and returns the
// result — the (position, hash) pairs the batch's log_order row must
// attest.
func appendOrderBatch(t *testing.T, ctx context.Context, ws *postgres.Workspace, sid es.StreamID, expected uint64, n int) es.AppendResult {
	t.Helper()
	events := make([]es.EventData, n)
	for i := range events {
		events[i] = es.EventData{TypeURL: "ordertest.v1.E", SchemaVersion: 1, Payload: []byte{byte('a' + i)}}
	}
	res, err := ws.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: expected, Events: events})
	if err != nil {
		t.Fatalf("append %d events at v%d: %v", n, expected+1, err)
	}
	return res
}

// TestOrderVerifyDetectsRepositionedEvent is the F11 repro — the whole
// reason D1 exists: a repositioned event leaves the per-stream chain and
// every stream head untouched (hashes and versions do not move), so only
// the log-order attestation can catch it. Two single-event batches are
// appended; their global positions are then swapped behind the store's
// back, directly on the pool, as the DB operator es-verify exists to
// catch; VerifyOrder must report the first batch whose recomputed order
// hash disagrees with the log_order row its append wrote. The same call
// before the tamper is the deep-verify green path.
func TestOrderVerifyDetectsRepositionedEvent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const wsID = "ws_order_verify"
	ws := s.WorkspaceStore(wsID)
	sid := feedStream(t, "order-verify")

	res1 := appendOrderBatch(t, ctx, ws, sid, 0, 1)
	res2 := appendOrderBatch(t, ctx, ws, sid, 1, 1)
	p1, p2 := res1.Envelopes[0].GlobalPosition, res2.Envelopes[0].GlobalPosition
	if !(p1 < p2) {
		t.Fatalf("positions: p1=%d p2=%d, want p1 < p2", p1, p2)
	}

	// The green path: every covered row's hash recomputes from the events.
	rep, err := s.VerifyOrder(ctx, 0)
	if err != nil {
		t.Fatalf("verify order before the tamper: %v", err)
	}
	if !rep.OK || rep.RowsChecked != 2 {
		t.Fatalf("verify order before the tamper = %+v, want ok over 2 rows", rep)
	}

	// The rows agree with what the appends hashed: positions and hashes
	// straight from the append results, one row per batch.
	want1 := es.BatchOrderHash([]es.OrderEntry{{Position: p1, Hash: res1.Envelopes[0].Hash}})
	want2 := es.BatchOrderHash([]es.OrderEntry{{Position: p2, Hash: res2.Envelopes[0].Hash}})
	rows, err := s.OrderRows(ctx, 0)
	if err != nil {
		t.Fatalf("order rows: %v", err)
	}
	if len(rows) != 2 ||
		rows[0].From != p1 || rows[0].To != p1 || rows[0].Count != 1 || !bytes.Equal(rows[0].BatchHash, want1[:]) ||
		rows[1].From != p2 || rows[1].To != p2 || rows[1].Count != 1 || !bytes.Equal(rows[1].BatchHash, want2[:]) {
		t.Fatalf("log_order rows = %+v, want one row per batch at %d and %d carrying the appends' hashes", rows, p1, p2)
	}

	// The tamper: swap the two events' global positions on the pool. The
	// per-stream chain and the heads survive it — verify the stream stays
	// intact, so what follows is genuinely order-only detection.
	tag, err := s.Pool().Exec(ctx,
		`UPDATE events
		    SET global_position = CASE global_position WHEN $1 THEN $2 WHEN $2 THEN $1 END
		  WHERE workspace_id = $3 AND stream_id = $4 AND global_position IN ($1, $2)`,
		int64(p1), int64(p2), wsID, sid.Canonical())
	if err != nil || tag.RowsAffected() != 2 {
		t.Fatalf("swap positions: %v (rows %d)", err, tag.RowsAffected())
	}
	if crep, cerr := es.VerifyStream(ctx, ws, sid); cerr != nil || !crep.Intact() {
		t.Fatalf("stream verify after the swap: err=%v rep=%+v — the chain must be intact, or this is not the F11 shape", cerr, crep)
	}

	rep, err = s.VerifyOrder(ctx, 0)
	if err != nil {
		t.Fatalf("verify order after the tamper: %v", err)
	}
	if rep.OK || rep.Mismatch == nil {
		t.Fatalf("verify order after the swap = %+v, want a batch-hash mismatch", rep)
	}
	if rep.Mismatch.From != p1 {
		t.Fatalf("first mismatch from_position = %d, want %d (rows report in from_position order)", rep.Mismatch.From, p1)
	}
	if !bytes.Equal(rep.Mismatch.ExpectedBatchHash, want1[:]) {
		t.Fatalf("expected hash = %x, want the stored batch hash %x", rep.Mismatch.ExpectedBatchHash, want1)
	}
	// The first batch's range now holds the other event: the recomputed
	// hash covers (p1, hash2), not the (p1, hash1) the append attested.
	got := es.BatchOrderHash([]es.OrderEntry{{Position: p1, Hash: res2.Envelopes[0].Hash}})
	if !bytes.Equal(rep.Mismatch.ActualBatchHash, got[:]) {
		t.Fatalf("actual hash = %x, want the recomputed hash %x", rep.Mismatch.ActualBatchHash, got)
	}
}

// TestAnchorRecordsOrderRoot pins the anchor-side wiring: Anchor
// computes its order root over the log_order rows inside the same
// settled, fenced transaction as the heads, and the three anchor order
// fields agree with what OrderRows reads — OrderCount counts the
// covered rows, OrderFromPosition is the first row's From, and
// OrderRootHash is es.OrderRoot over exactly those rows. CheckAnchor
// must then agree, heads and order together.
func TestAnchorRecordsOrderRoot(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	wsA := s.WorkspaceStore("ws_order_anchor_a")
	wsB := s.WorkspaceStore("ws_order_anchor_b")
	sidA := feedStream(t, "order-anchor-a")
	sidB := feedStream(t, "order-anchor-b")

	// A 3-event batch and a 1-event batch, in different workspaces: the
	// order root spans workspaces exactly like the heads root does.
	resA := appendOrderBatch(t, ctx, wsA, sidA, 0, 3)
	resB := appendOrderBatch(t, ctx, wsB, sidB, 0, 1)

	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	if a.StreamCount != 2 {
		t.Fatalf("anchor stream count = %d, want 2", a.StreamCount)
	}

	rows, err := s.OrderRows(ctx, a.UptoGlobalPosition)
	if err != nil {
		t.Fatalf("order rows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("order rows = %d, want 2 (one per batch)", len(rows))
	}
	// The multi-event batch's row spans its first and last event's
	// positions and carries the hash over exactly its own entries.
	entriesA := []es.OrderEntry{
		{Position: resA.Envelopes[0].GlobalPosition, Hash: resA.Envelopes[0].Hash},
		{Position: resA.Envelopes[1].GlobalPosition, Hash: resA.Envelopes[1].Hash},
		{Position: resA.Envelopes[2].GlobalPosition, Hash: resA.Envelopes[2].Hash},
	}
	batchA := es.BatchOrderHash(entriesA)
	if rows[0].From != resA.Envelopes[0].GlobalPosition ||
		rows[0].To != resA.Envelopes[2].GlobalPosition ||
		rows[0].Count != 3 ||
		!bytes.Equal(rows[0].BatchHash, batchA[:]) {
		t.Fatalf("multi-event batch row = %+v, want range [%d,%d] count 3 with the batch's own hash",
			rows[0], resA.Envelopes[0].GlobalPosition, resA.Envelopes[2].GlobalPosition)
	}
	// The single-event batch's row does the same across its workspace.
	batchB := es.BatchOrderHash([]es.OrderEntry{{Position: resB.Envelopes[0].GlobalPosition, Hash: resB.Envelopes[0].Hash}})
	if rows[1].From != resB.Envelopes[0].GlobalPosition || rows[1].To != resB.Envelopes[0].GlobalPosition ||
		rows[1].Count != 1 || !bytes.Equal(rows[1].BatchHash, batchB[:]) {
		t.Fatalf("single-event batch row = %+v, want range [%d,%d] count 1 with the batch's own hash",
			rows[1], resB.Envelopes[0].GlobalPosition, resB.Envelopes[0].GlobalPosition)
	}

	if a.OrderCount != 2 {
		t.Fatalf("anchor OrderCount = %d, want 2 (the number of covered log_order rows)", a.OrderCount)
	}
	if a.OrderFromPosition != rows[0].From {
		t.Fatalf("anchor OrderFromPosition = %d, want the first row's From %d", a.OrderFromPosition, rows[0].From)
	}
	wantRoot := es.OrderRoot(rows)
	if len(a.OrderRootHash) != es.HashSize || !bytes.Equal(a.OrderRootHash, wantRoot[:]) {
		t.Fatalf("anchor OrderRootHash = %x, want the root over the covered rows %x", a.OrderRootHash, wantRoot)
	}
	if err := s.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("check anchor: %v", err)
	}

	// The deep verify agrees over the same log.
	rep, err := s.VerifyOrder(ctx, 0)
	if err != nil || !rep.OK || rep.RowsChecked != 2 {
		t.Fatalf("verify order = %+v err=%v, want ok over 2 rows", rep, err)
	}
}

// TestCheckAnchorCatchesWholesaleOrderRewrite pins the order root's
// half of the ADR 0029 trust split (design D1): the realistic attack is
// not a bare reposition (the deep verify catches that) but a
// repositioned event WITH the log_order rows rewritten to match — a
// self-consistent cover-up. The heads root still matches, and even
// VerifyOrder passes (rows and events agree again); only the anchor's
// exported OrderRootHash can dispute it, and CheckAnchor must. The same
// anchor with OrderRootHash nil (the legacy marker, anchors recorded
// before log-order attestation) skips the order comparison and
// verifies heads-only: nil is exclusively the legacy marker.
func TestCheckAnchorCatchesWholesaleOrderRewrite(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const wsID = "ws_order_check"
	ws := s.WorkspaceStore(wsID)
	sid := feedStream(t, "order-check")

	res1 := appendOrderBatch(t, ctx, ws, sid, 0, 1)
	res2 := appendOrderBatch(t, ctx, ws, sid, 1, 1)
	p1, p2 := res1.Envelopes[0].GlobalPosition, res2.Envelopes[0].GlobalPosition
	if !(p1 < p2) {
		t.Fatalf("positions: p1=%d p2=%d, want p1 < p2", p1, p2)
	}

	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	if a.OrderCount != 2 || a.OrderFromPosition != p1 {
		t.Fatalf("anchor order fields = count %d from %d, want 2/%d", a.OrderCount, a.OrderFromPosition, p1)
	}

	// The attack: swap the two events' positions AND rewrite both
	// log_order rows to match the new arrangement — the cover-up that
	// leaves the log self-consistent.
	if _, err := s.Pool().Exec(ctx,
		`UPDATE events
		    SET global_position = CASE global_position WHEN $1 THEN $2 WHEN $2 THEN $1 END
		  WHERE workspace_id = $3 AND stream_id = $4 AND global_position IN ($1, $2)`,
		int64(p1), int64(p2), wsID, sid.Canonical()); err != nil {
		t.Fatalf("swap positions: %v", err)
	}
	// The first batch's row now covers the event that sits at p1 —
	// batch 2's — and vice versa; the rewrite re-hashes each row over
	// exactly that.
	newHash1 := es.BatchOrderHash([]es.OrderEntry{{Position: p1, Hash: res2.Envelopes[0].Hash}})
	newHash2 := es.BatchOrderHash([]es.OrderEntry{{Position: p2, Hash: res1.Envelopes[0].Hash}})
	for _, rw := range []struct {
		from uint64
		hash [es.HashSize]byte
	}{
		{p1, newHash1},
		{p2, newHash2},
	} {
		tag, err := s.Pool().Exec(ctx,
			`UPDATE log_order SET batch_hash = $1 WHERE workspace_id = $2 AND stream_id = $3 AND from_position = $4`,
			rw.hash[:], wsID, sid.Canonical(), int64(rw.from))
		if err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("rewrite log_order row at %d: %v (rows %d)", rw.from, err, tag.RowsAffected())
		}
	}

	// The cover-up is self-consistent: the deep verify passes, because
	// rows and events agree again — a bare deep verify certifies
	// nothing against an attacker who can rewrite the rows. That is
	// exactly why the order root is exported with the anchor.
	rep, err := s.VerifyOrder(ctx, 0)
	if err != nil || !rep.OK || rep.RowsChecked != 2 {
		t.Fatalf("verify order after the cover-up = %+v err=%v, want ok — the rewrite is self-consistent", rep, err)
	}

	// Heads-only — the legacy shape: the anchor still verifies, because
	// the heads did not change; nil OrderRootHash is the legacy marker
	// and skips the order comparison.
	legacy := a
	legacy.OrderRootHash = nil
	if err := s.CheckAnchor(ctx, legacy); err != nil {
		t.Fatalf("legacy heads-only check after the rewrite: %v — the heads did not change", err)
	}

	// The full check: the order fields dispute what neither the heads
	// nor the deep verify can.
	if err := s.CheckAnchor(ctx, a); !errors.Is(err, es.ErrAnchorMismatch) {
		t.Fatalf("check after the rewrite = %v, want ErrAnchorMismatch — the exported order root disputes the wholesale rewrite", err)
	}
}

// TestAnchorEmptyOrderContract pins the empty-set contract carried from
// Task 6's ledger: a new-style anchor ALWAYS carries an order root, even
// when it covers zero log_order rows — a real hash over an empty row
// set, never nil (nil is exclusively the legacy marker) — with
// OrderFromPosition 0 and OrderCount 0. CheckAnchor must verify such an
// anchor, and VerifyOrder reports zero rows checked for the CLI to
// treat as EMPTY rather than a vacuous pass (F13).
func TestAnchorEmptyOrderContract(t *testing.T) {
	ctx := context.Background()
	s := openStore(t) // truncated: no events, no log_order rows

	a, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("anchor over an empty log: %v", err)
	}
	if a.StreamCount != 0 {
		t.Fatalf("anchor stream count = %d, want 0", a.StreamCount)
	}
	if a.OrderCount != 0 || a.OrderFromPosition != 0 {
		t.Fatalf("anchor order fields = count %d from %d, want 0/0", a.OrderCount, a.OrderFromPosition)
	}
	empty := es.OrderRoot(nil)
	if len(a.OrderRootHash) != es.HashSize || !bytes.Equal(a.OrderRootHash, empty[:]) {
		t.Fatalf("anchor OrderRootHash = %x, want the real empty-set root %x — never nil", a.OrderRootHash, empty)
	}
	if err := s.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("check empty-order anchor: %v", err)
	}
	rows, err := s.OrderRows(ctx, a.UptoGlobalPosition)
	if err != nil || len(rows) != 0 {
		t.Fatalf("order rows = %d err=%v, want 0", len(rows), err)
	}
	rep, err := s.VerifyOrder(ctx, 0)
	if err != nil || !rep.OK || rep.RowsChecked != 0 {
		t.Fatalf("verify order = %+v err=%v, want ok over 0 rows", rep, err)
	}
}
