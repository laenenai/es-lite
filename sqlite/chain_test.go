package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/sqlite"
)

func openChainStore(t *testing.T) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(context.Background(), "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func ev(i byte) es.EventData {
	return es.EventData{TypeURL: "thing.v1.Happened", SchemaVersion: 1, Payload: []byte{i}}
}

func TestChainAcrossBatches(t *testing.T) {
	ctx := context.Background()
	s := openChainStore(t)
	sid, _ := es.NewStreamID("thing", "a")

	r1, err := s.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: 0, Events: []es.EventData{ev(1), ev(2)}})
	if err != nil {
		t.Fatalf("append 1: %v", err)
	}
	r2, err := s.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: 2, Events: []es.EventData{ev(3)}})
	if err != nil {
		t.Fatalf("append 2: %v", err)
	}
	// Returned envelopes carry the chain.
	if !bytes.Equal(r1.Envelopes[0].PrevHash, es.ZeroHash[:]) {
		t.Fatal("v1 prev_hash is not ZeroHash")
	}
	if !bytes.Equal(r1.Envelopes[1].PrevHash, r1.Envelopes[0].Hash) {
		t.Fatal("v2 does not chain to v1 within the batch")
	}
	if !bytes.Equal(r2.Envelopes[0].PrevHash, r1.Envelopes[1].Hash) {
		t.Fatal("v3 does not chain to v2 across batches")
	}
	// The returned envelope hashes identically to the stored row.
	rows, err := s.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for i, e := range rows {
		h := es.EnvelopeHash(e)
		if !bytes.Equal(h[:], e.Hash) {
			t.Fatalf("v%d: stored hash != recomputed from the read row", e.Version)
		}
		want := append(r1.Envelopes, r2.Envelopes...)[i]
		if !bytes.Equal(want.Hash, e.Hash) || !want.RecordedAt.Equal(e.RecordedAt) || !want.OccurredAt.Equal(e.OccurredAt) {
			t.Fatalf("v%d: returned envelope differs from the stored row", e.Version)
		}
	}
	rep, err := es.VerifyStream(ctx, s, sid)
	if err != nil || !rep.Intact() || rep.ChainedFrom != 1 || rep.Head != 3 {
		t.Fatalf("verify: err=%v rep=%+v", err, rep)
	}
}

func TestRaceLoserLeavesNoFork(t *testing.T) {
	ctx := context.Background()
	s := openChainStore(t)
	sid, _ := es.NewStreamID("thing", "a")
	if _, err := s.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: 0, Events: []es.EventData{ev(1)}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := s.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: 1, Events: []es.EventData{ev(2)}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	// A stale writer on the same head loses — and leaves nothing behind.
	if _, err := s.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: 1, Events: []es.EventData{ev(9)}}); !errors.Is(err, es.ErrConflict) {
		t.Fatalf("stale append err = %v, want ErrConflict", err)
	}
	var n int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("rows = %d (err %v), want 2", n, err)
	}
	if rep, err := es.VerifyStream(ctx, s, sid); err != nil || !rep.Intact() {
		t.Fatalf("verify after race: err=%v rep=%+v", err, rep)
	}
}

func TestTamperedRowFailsVerification(t *testing.T) {
	ctx := context.Background()
	s := openChainStore(t)
	sid, _ := es.NewStreamID("counter", "main")
	rt := aggregate.NewRuntime(s, counter.Decider, counterv1.EventCodec{})
	for _, cmd := range []counterv1.Command{
		&counterv1.Init{Min: 0, Max: 100, Initial: 5},
		&counterv1.Increment{By: 10},
		&counterv1.Increment{By: 20},
	} {
		if _, err := rt.Handle(ctx, sid, cmd, es.Meta{}); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	if rep, err := es.VerifyStream(ctx, s, sid); err != nil || !rep.Intact() {
		t.Fatalf("pre-tamper verify: err=%v rep=%+v", err, rep)
	}

	// The database operator edits v2's payload in place (by=10 -> by=11:
	// the proto varint byte flips), leaving the hash columns untouched.
	var payload []byte
	if err := s.DB().QueryRowContext(ctx, `SELECT payload FROM events WHERE stream_id = ? AND version = 2`, sid.Canonical()).Scan(&payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	payload[len(payload)-1] ^= 0x01
	if _, err := s.DB().ExecContext(ctx, `UPDATE events SET payload = ? WHERE stream_id = ? AND version = 2`, payload, sid.Canonical()); err != nil {
		t.Fatalf("tamper: %v", err)
	}

	rep, err := es.VerifyStream(ctx, s, sid)
	if !errors.Is(err, es.ErrChainBroken) || rep.BrokenAt != 2 {
		t.Fatalf("post-tamper verify: err=%v brokenAt=%d, want broken at 2", err, rep.BrokenAt)
	}
	// Plain loads still fold the tampered value (the chain is evidence,
	// not enforcement) ...
	if st, _, err := rt.Load(ctx, sid); err != nil || st.GetCount() != 36 {
		t.Fatalf("plain load: count=%d err=%v, want 36 (tampered)", st.GetCount(), err)
	}
	// ... unless the runtime verifies on read.
	verifying := aggregate.NewRuntime(s, counter.Decider, counterv1.EventCodec{}).WithChainVerification()
	if _, _, err := verifying.Load(ctx, sid); !errors.Is(err, es.ErrChainBroken) {
		t.Fatalf("verifying load err = %v, want ErrChainBroken", err)
	}
	if _, err := verifying.LoadAsOfVersion(ctx, sid, 1); err != nil {
		t.Fatalf("verifying load of the untouched prefix: %v", err)
	}
}

// TestAsOfTimeCannotSkipAVersion is the ES-03 regression: recorded_at is
// sampled after the version check inside the transaction, so it is
// monotonic per stream and the as-of-time read of any event's recorded_at
// returns exactly the prefix up to that event — never a later version
// without an earlier one.
func TestAsOfTimeCannotSkipAVersion(t *testing.T) {
	ctx := context.Background()
	s := openChainStore(t)
	sid, _ := es.NewStreamID("thing", "a")
	const n = 50
	for i := 0; i < n; i++ {
		// Hand the store an OccurredAt from the past to prove recorded_at is
		// the store's clock, not the caller's.
		if _, err := s.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: uint64(i), Events: []es.EventData{ev(byte(i))}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	rows, err := s.ReadStream(ctx, sid, 0, 0)
	if err != nil || len(rows) != n {
		t.Fatalf("read: %d rows, err %v", len(rows), err)
	}
	for i := 1; i < n; i++ {
		if rows[i].RecordedAt.Before(rows[i-1].RecordedAt) {
			t.Fatalf("recorded_at not monotonic: v%d %s < v%d %s", rows[i].Version, rows[i].RecordedAt, rows[i-1].Version, rows[i-1].RecordedAt)
		}
	}
	for _, e := range rows {
		got, err := s.ReadStreamAsOf(ctx, sid, e.RecordedAt)
		if err != nil {
			t.Fatalf("as-of v%d: %v", e.Version, err)
		}
		// Everything up to and including this version, contiguous — and
		// possibly later versions that share the same µs tick, but never a
		// gap.
		if uint64(len(got)) < e.Version {
			t.Fatalf("as-of v%d returned %d rows, skipped a version", e.Version, len(got))
		}
		for j, g := range got {
			if g.Version != uint64(j+1) {
				t.Fatalf("as-of v%d: row %d is v%d", e.Version, j, g.Version)
			}
			if g.RecordedAt.After(e.RecordedAt) {
				t.Fatalf("as-of v%d returned v%d recorded later", e.Version, g.Version)
			}
		}
	}
}

func TestAnchorsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openChainStore(t)
	a1, _ := es.NewStreamID("thing", "a")
	b1, _ := es.NewStreamID("thing", "b")
	for i := 0; i < 3; i++ {
		if _, err := s.Append(ctx, es.AppendParams{StreamID: a1, ExpectedVersion: uint64(i), Events: []es.EventData{ev(1)}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Append(ctx, es.AppendParams{StreamID: b1, ExpectedVersion: 0, Events: []es.EventData{ev(1)}}); err != nil {
		t.Fatal(err)
	}

	anchor, err := s.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	if anchor.StreamCount != 2 || anchor.UptoGlobalPosition != 4 || len(anchor.RootHash) != es.HashSize {
		t.Fatalf("anchor = %+v", anchor)
	}
	if err := s.CheckAnchor(ctx, anchor); err != nil {
		t.Fatalf("check: %v", err)
	}
	list, err := s.ListAnchors(ctx)
	if err != nil || len(list) != 1 || list[0].ID != anchor.ID || !bytes.Equal(list[0].RootHash, anchor.RootHash) {
		t.Fatalf("list = %+v err=%v", list, err)
	}
	refs, err := s.ListStreams(ctx)
	if err != nil || len(refs) != 2 {
		t.Fatalf("streams = %+v err=%v", refs, err)
	}

	// Appending more does not invalidate an older anchor: it is keyed by
	// global_position.
	if _, err := s.Append(ctx, es.AppendParams{StreamID: b1, ExpectedVersion: 1, Events: []es.EventData{ev(2)}}); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckAnchor(ctx, anchor); err != nil {
		t.Fatalf("check after append: %v", err)
	}
	// Deleting the tail of a stream below the anchored position does.
	if _, err := s.DB().ExecContext(ctx, `DELETE FROM events WHERE stream_id = ? AND version = 3`, a1.Canonical()); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckAnchor(ctx, anchor); !errors.Is(err, es.ErrAnchorMismatch) {
		t.Fatalf("check after truncation err = %v, want ErrAnchorMismatch", err)
	}
	// A truncated tail is not visible to the chain alone.
	if rep, err := es.VerifyStream(ctx, s, a1); err != nil || !rep.Intact() || rep.Head != 2 {
		t.Fatalf("verify after truncation: err=%v rep=%+v (truncation is anchor-detectable only)", err, rep)
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
func appendOrderBatch(t *testing.T, ctx context.Context, s *sqlite.Store, sid es.StreamID, expected uint64, n int) es.AppendResult {
	t.Helper()
	events := make([]es.EventData, n)
	for i := range events {
		events[i] = es.EventData{TypeURL: "ordertest.v1.E", SchemaVersion: 1, Payload: []byte{byte('a' + i)}}
	}
	res, err := s.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: expected, Events: events})
	if err != nil {
		t.Fatalf("append %d events at v%d: %v", n, expected+1, err)
	}
	return res
}

// swapPositions exchanges the two events' global positions behind the
// store's back, as the DB operator es-verify exists to catch. SQLite
// enforces INTEGER PRIMARY KEY (rowid) uniqueness per row inside one
// UPDATE — a single CASE swap collides mid-statement, where Postgres
// survives it — so the swap parks both rows on the negative mirror of
// their positions first (positions are always positive, so the mirrors
// are free) and then moves each into the other's old spot. Two
// statements, same effect.
func swapPositions(t *testing.T, ctx context.Context, s *sqlite.Store, sid es.StreamID, p1, p2 uint64) {
	t.Helper()
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE events SET global_position = -global_position WHERE stream_id = ? AND global_position IN (?, ?)`,
		sid.Canonical(), p1, p2); err != nil {
		t.Fatalf("park positions %d/%d: %v", p1, p2, err)
	}
	res, err := s.DB().ExecContext(ctx,
		`UPDATE events SET global_position = CASE global_position WHEN ? THEN ? WHEN ? THEN ? END
		  WHERE stream_id = ? AND global_position IN (?, ?)`,
		-int64(p1), int64(p2), -int64(p2), int64(p1), sid.Canonical(), -int64(p1), -int64(p2))
	if err != nil {
		t.Fatalf("swap positions %d/%d: %v", p1, p2, err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 2 {
		t.Fatalf("swap positions %d/%d: rows %d (err %v), want 2", p1, p2, n, err)
	}
}

// TestSQLiteOrderVerifyDetectsRepositionedEvent is the F11 repro — the
// whole reason D1 exists: a repositioned event leaves the per-stream
// chain and every stream head untouched (hashes and versions do not
// move), so only the log-order attestation can catch it. Two
// single-event batches are appended; their global positions are then
// swapped behind the store's back, directly on the database, as the DB
// operator es-verify exists to catch; VerifyOrder must report the first
// batch whose recomputed order hash disagrees with the log_order row
// its append wrote. The same call before the tamper is the deep-verify
// green path.
func TestSQLiteOrderVerifyDetectsRepositionedEvent(t *testing.T) {
	ctx := context.Background()
	s := openChainStore(t)
	sid, _ := es.NewStreamID("thing", "order-verify")

	res1 := appendOrderBatch(t, ctx, s, sid, 0, 1)
	res2 := appendOrderBatch(t, ctx, s, sid, 1, 1)
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

	// The tamper: swap the two events' global positions. The per-stream
	// chain and the heads survive it — the stream stays intact, so what
	// follows is genuinely order-only detection.
	swapPositions(t, ctx, s, sid, p1, p2)
	if crep, cerr := es.VerifyStream(ctx, s, sid); cerr != nil || !crep.Intact() {
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

// TestSQLiteAnchorRecordsOrderRoot pins the anchor-side wiring: Anchor
// computes its order root over the log_order rows inside the same
// bounded read as the heads, and the three anchor order fields agree
// with what OrderRows reads — OrderCount counts the covered rows,
// OrderFromPosition is the first row's From, and OrderRootHash is
// es.OrderRoot over exactly those rows. SQLite is single-workspace, so
// the postgres test's two workspaces become two streams: the order
// root spans streams exactly like the heads root does. CheckAnchor
// must then agree, heads and order together.
func TestSQLiteAnchorRecordsOrderRoot(t *testing.T) {
	ctx := context.Background()
	s := openChainStore(t)
	sidA, _ := es.NewStreamID("thing", "order-anchor-a")
	sidB, _ := es.NewStreamID("thing", "order-anchor-b")

	// A 3-event batch and a 1-event batch, to different streams: the
	// order root spans streams exactly like the heads root does.
	resA := appendOrderBatch(t, ctx, s, sidA, 0, 3)
	resB := appendOrderBatch(t, ctx, s, sidB, 0, 1)

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
	// The single-event batch's row does the same across its stream.
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

// TestSQLiteCheckAnchorCatchesWholesaleOrderRewrite pins the order
// root's half of the ADR 0029 trust split (design D1): the realistic
// attack is not a bare reposition (the deep verify catches that) but a
// repositioned event WITH the log_order rows rewritten to match — a
// self-consistent cover-up. The heads root still matches, and even
// VerifyOrder passes (rows and events agree again); only the anchor's
// exported OrderRootHash can dispute it, and CheckAnchor must. The
// same anchor with OrderRootHash nil (the legacy marker, anchors
// recorded before log-order attestation) skips the order comparison
// and verifies heads-only: nil is exclusively the legacy marker.
func TestSQLiteCheckAnchorCatchesWholesaleOrderRewrite(t *testing.T) {
	ctx := context.Background()
	s := openChainStore(t)
	sid, _ := es.NewStreamID("thing", "order-check")

	res1 := appendOrderBatch(t, ctx, s, sid, 0, 1)
	res2 := appendOrderBatch(t, ctx, s, sid, 1, 1)
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
	swapPositions(t, ctx, s, sid, p1, p2)
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
		res, err := s.DB().ExecContext(ctx,
			`UPDATE log_order SET batch_hash = ? WHERE stream_id = ? AND from_position = ?`,
			rw.hash[:], sid.Canonical(), rw.from)
		if err != nil {
			t.Fatalf("rewrite log_order row at %d: %v", rw.from, err)
		}
		if n, err := res.RowsAffected(); err != nil || n != 1 {
			t.Fatalf("rewrite log_order row at %d: rows %d (err %v), want 1", rw.from, n, err)
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

// TestSQLiteAnchorEmptyOrderContract pins the empty-set contract
// carried from Task 6's ledger: a new-style anchor ALWAYS carries an
// order root, even when it covers zero log_order rows — a real hash
// over an empty row set, never nil (nil is exclusively the legacy
// marker) — with OrderFromPosition 0 and OrderCount 0. CheckAnchor
// must verify such an anchor, and VerifyOrder reports zero rows
// checked for the CLI to treat as EMPTY rather than a vacuous pass
// (F13).
func TestSQLiteAnchorEmptyOrderContract(t *testing.T) {
	ctx := context.Background()
	s := openChainStore(t) // empty: no events, no log_order rows

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
