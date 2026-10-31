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
