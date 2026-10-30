package es_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
)

// memStore is a minimal es.Store over a slice: what a verifier sees, with
// the rows under the test's control so each kind of damage can be staged.
type memStore struct {
	es.Store
	rows []es.Envelope
}

func (m *memStore) ReadStream(_ context.Context, sid es.StreamID, from, to uint64) ([]es.Envelope, error) {
	var out []es.Envelope
	for _, e := range m.rows {
		if e.StreamID == sid && e.Version > from && (to == 0 || e.Version <= to) {
			out = append(out, e)
		}
	}
	if len(out) == 0 && from == 0 {
		return nil, es.ErrStreamNotFound
	}
	return out, nil
}

var sidMain = es.StreamID{Type: "counter", ID: "main"}

// chain builds n chained envelopes the way an adapter would; the first
// `unchained` of them are left without hashes (pre-0002 rows).
func chain(n, unchained int) []es.Envelope {
	rows := make([]es.Envelope, n)
	var prev [es.HashSize]byte
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range rows {
		v := uint64(i + 1)
		e := es.Envelope{
			EventID:        uuid.New(),
			StreamID:       sidMain,
			Version:        v,
			GlobalPosition: v,
			TypeURL:        "counter.v1.Incremented",
			SchemaVersion:  1,
			OccurredAt:     base.Add(time.Duration(i) * time.Second),
			RecordedAt:     base.Add(time.Duration(i) * time.Second),
			Payload:        []byte{byte(i)},
		}
		if i >= unchained {
			switch {
			case i == 0:
				prev = es.ZeroHash
			case i == unchained:
				prev = es.UnchainedHeadHash(sidMain, uint64(i))
			}
			e.PrevHash = append([]byte(nil), prev[:]...)
			h := es.EnvelopeHash(e)
			e.Hash = h[:]
			prev = h
		}
		rows[i] = e
	}
	return rows
}

func TestVerifyStreamIntact(t *testing.T) {
	rep, err := es.VerifyStreamWith(context.Background(), &memStore{rows: chain(1203, 0)}, sidMain, es.VerifyOptions{Page: 100})
	if err != nil || !rep.Intact() {
		t.Fatalf("intact chain: err=%v report=%+v", err, rep)
	}
	if rep.ChainedFrom != 1 || rep.Head != 1203 || len(rep.HeadHash) != es.HashSize {
		t.Fatalf("report = %+v", rep)
	}
}

func TestVerifyStreamEmpty(t *testing.T) {
	rep, err := es.VerifyStream(context.Background(), &memStore{}, sidMain)
	if err != nil || rep.Head != 0 || rep.ChainedFrom != 0 {
		t.Fatalf("empty stream: err=%v report=%+v", err, rep)
	}
}

func TestVerifyStreamMutatedPayload(t *testing.T) {
	rows := chain(5, 0)
	rows[2].Payload[0] ^= 0xff
	rep, err := es.VerifyStream(context.Background(), &memStore{rows: rows}, sidMain)
	if !errors.Is(err, es.ErrChainBroken) || rep.BrokenAt != 3 {
		t.Fatalf("mutated v3: err=%v brokenAt=%d", err, rep.BrokenAt)
	}
}

func TestVerifyStreamRehashedRowStillBreaksLink(t *testing.T) {
	// An operator who edits a row AND recomputes its hash is caught at the
	// next row, whose prev_hash no longer links.
	rows := chain(5, 0)
	rows[2].Payload[0] ^= 0xff
	h := es.EnvelopeHash(rows[2])
	rows[2].Hash = h[:]
	rep, err := es.VerifyStream(context.Background(), &memStore{rows: rows}, sidMain)
	if !errors.Is(err, es.ErrChainBroken) || rep.BrokenAt != 4 {
		t.Fatalf("rehashed v3: err=%v brokenAt=%d, want break at 4", err, rep.BrokenAt)
	}
}

func TestVerifyStreamSwappedOrder(t *testing.T) {
	rows := chain(5, 0)
	rows[1], rows[2] = rows[2], rows[1] // versions 3,2 out of order
	rep, err := es.VerifyStream(context.Background(), &memStore{rows: rows}, sidMain)
	if !errors.Is(err, es.ErrChainBroken) || rep.BrokenAt != 3 {
		t.Fatalf("swapped: err=%v brokenAt=%d", err, rep.BrokenAt)
	}
}

func TestVerifyStreamMissingVersion(t *testing.T) {
	rows := chain(5, 0)
	rows = append(rows[:2], rows[3:]...) // drop v3
	rep, err := es.VerifyStream(context.Background(), &memStore{rows: rows}, sidMain)
	if !errors.Is(err, es.ErrChainBroken) || rep.BrokenAt != 4 {
		t.Fatalf("missing v3: err=%v brokenAt=%d", err, rep.BrokenAt)
	}
}

func TestVerifyStreamTruncatedTailIsAnchorDetectableOnly(t *testing.T) {
	full := chain(5, 0)
	headsBefore := []es.StreamHead{{StreamID: sidMain, Version: 5, Hash: full[4].Hash}}
	truncated := full[:3]

	// The chain itself is intact — a verifier cannot know v4, v5 existed.
	rep, err := es.VerifyStream(context.Background(), &memStore{rows: truncated}, sidMain)
	if err != nil || !rep.Intact() || rep.Head != 3 {
		t.Fatalf("truncated tail must verify as intact up to its head: err=%v rep=%+v", err, rep)
	}
	// Only an anchor taken before the truncation tells.
	headsAfter := []es.StreamHead{{StreamID: sidMain, Version: rep.Head, Hash: rep.HeadHash}}
	if es.AnchorRoot(headsBefore) == es.AnchorRoot(headsAfter) {
		t.Fatal("anchor root must change when the tail is truncated")
	}
}

func TestVerifyStreamPopulatedStore(t *testing.T) {
	rows := chain(6, 3) // v1–v3 pre-chain, v4 chains to UnchainedHeadHash(sid, 3)
	rep, err := es.VerifyStream(context.Background(), &memStore{rows: rows}, sidMain)
	if err != nil || !rep.Intact() {
		t.Fatalf("populated store: err=%v rep=%+v", err, rep)
	}
	if rep.ChainedFrom != 4 || rep.Head != 6 {
		t.Fatalf("ChainedFrom=%d Head=%d, want 4/6", rep.ChainedFrom, rep.Head)
	}
	// Strict mode refuses a partially attested stream.
	rep, err = es.VerifyStreamWith(context.Background(), &memStore{rows: rows}, sidMain, es.VerifyOptions{Strict: true})
	if !errors.Is(err, es.ErrChainBroken) {
		t.Fatalf("strict on populated store: err=%v", err)
	}
	// An unhashed row AFTER a chained one is a break (the store always
	// hashes after 0002).
	bad := chain(6, 3)
	bad[5].Hash, bad[5].PrevHash = nil, nil
	rep, err = es.VerifyStream(context.Background(), &memStore{rows: bad}, sidMain)
	if !errors.Is(err, es.ErrChainBroken) || rep.BrokenAt != 6 {
		t.Fatalf("unhashed after chained: err=%v brokenAt=%d", err, rep.BrokenAt)
	}
	// An unhashed row recorded after the migration instant is a break too.
	since := rows[1].RecordedAt // between v2 and v3
	rep, err = es.VerifyStreamWith(context.Background(), &memStore{rows: rows}, sidMain, es.VerifyOptions{ChainedSince: since})
	if !errors.Is(err, es.ErrChainBroken) || rep.BrokenAt != 3 {
		t.Fatalf("unhashed after ChainedSince: err=%v brokenAt=%d", err, rep.BrokenAt)
	}
}

func TestChainCheckerMidStreamStart(t *testing.T) {
	rows := chain(5, 0)
	c := es.NewChainChecker(sidMain)
	for _, e := range rows[2:] { // a snapshot-seeded load from v3
		if err := c.Next(e); err != nil {
			t.Fatalf("mid-stream v%d: %v", e.Version, err)
		}
	}
	if rep := c.Report(); rep.Head != 5 || rep.ChainedFrom != 3 {
		t.Fatalf("report = %+v", rep)
	}
}
