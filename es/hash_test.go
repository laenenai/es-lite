package es_test

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
)

// Golden vectors for the canonical encoding (ADR 0029 §2). The expected
// values were produced by an independent reimplementation of the encoding
// (Python: struct + hashlib, see the ADR's field list), so an external
// auditor can validate theirs against the same fixtures. Changing any of
// these means changing the chain format — a new domain separator, not an
// edit here.

func mustUUID(s string) uuid.UUID { return uuid.MustParse(s) }

var goldenV1 = es.Envelope{
	EventID:       mustUUID("0192f3a0-0000-7000-8000-000000000001"),
	StreamID:      es.StreamID{Type: "counter", ID: "main"},
	Version:       1,
	TypeURL:       "counter.v1.Initialized",
	SchemaVersion: 1,
	OccurredAt:    time.Date(2026, 3, 3, 10, 0, 0, 123456000, time.UTC),
	RecordedAt:    time.Date(2026, 3, 3, 10, 0, 1, 654321000, time.UTC),
	CorrelationID: mustUUID("0192f3a0-0000-7000-8000-0000000000c0"),
	CommandID:     mustUUID("0192f3a0-0000-7000-8000-0000000000c1"),
	Actor:         es.Actor{Type: "user", ID: "alice"},
	Payload:       []byte{0x08, 0x05, 0x10, 0x64},
	PrevHash:      es.ZeroHash[:],
}

const (
	goldenV1Canonical = "0000001365732d6c6974652e656e76656c6f70652e7631000000000000000c636f756e7465723a6d61696e00000000000000010192f3a000007000800000000000000100000016636f756e7465722e76312e496e697469616c697a65640000000100064c1bc4386a4000064c1bc44fc6310192f3a00000700080000000000000c0000000000000000000000000000000000192f3a00000700080000000000000c1000000047573657200000005616c69636500000004080510640000000000000000000000000000000000000000000000000000000000000000"
	goldenV1Hash      = "bef51179d91145c58b6d8a9984e67667570a70550cc7f8ec2644918bdfb2310c"
	goldenV2Hash      = "9bfcdff651e5d56dca6bda50f294a4c2f7b56fd1296a73d7428107d7ff2b539b"
	goldenWSHash      = "57e2901d2e5cd1bef47267971a47dbaf7043b85cbf42fdb0e1792b9a9b27f3f8"
	goldenUnchained   = "17d28c6596c090407641f11ce7c3e902d16ca979d422522862a423ab4337a7f9"
)

func TestCanonicalEnvelopeGolden(t *testing.T) {
	got := hex.EncodeToString(es.CanonicalEnvelope(goldenV1))
	if got != goldenV1Canonical {
		t.Fatalf("canonical encoding drifted:\n got %s\nwant %s", got, goldenV1Canonical)
	}
	if len(es.CanonicalEnvelope(goldenV1)) != 218 {
		t.Fatalf("canonical length = %d, want 218", len(es.CanonicalEnvelope(goldenV1)))
	}
}

func TestEnvelopeHashGolden(t *testing.T) {
	h1 := es.EnvelopeHash(goldenV1)
	if got := hex.EncodeToString(h1[:]); got != goldenV1Hash {
		t.Fatalf("v1 hash = %s, want %s", got, goldenV1Hash)
	}

	// v2 chains to v1: prev_hash is v1's hash; different type, schema 2,
	// different timestamps (one at exactly 1µs past the second).
	v2 := goldenV1
	v2.EventID = mustUUID("0192f3a0-0000-7000-8000-000000000002")
	v2.Version = 2
	v2.TypeURL = "counter.v1.Incremented"
	v2.SchemaVersion = 2
	v2.OccurredAt = time.Date(2026, 3, 3, 10, 0, 2, 0, time.UTC)
	v2.RecordedAt = time.Date(2026, 3, 3, 10, 0, 2, 1000, time.UTC)
	v2.Payload = []byte{0x08, 0x0a}
	v2.PrevHash = h1[:]
	h2 := es.EnvelopeHash(v2)
	if got := hex.EncodeToString(h2[:]); got != goldenV2Hash {
		t.Fatalf("v2 hash = %s, want %s", got, goldenV2Hash)
	}

	// Workspace set, empty actor/payload/ids, zero timestamps (encode as 0).
	ws := goldenV1
	ws.Workspace = "ws_a"
	ws.Actor = es.Actor{}
	ws.Payload = nil
	ws.CorrelationID = uuid.Nil
	ws.CommandID = uuid.Nil
	ws.OccurredAt = time.Time{}
	ws.RecordedAt = time.Time{}
	hw := es.EnvelopeHash(ws)
	if got := hex.EncodeToString(hw[:]); got != goldenWSHash {
		t.Fatalf("workspace hash = %s, want %s", got, goldenWSHash)
	}
}

func TestUnchainedHeadHashGolden(t *testing.T) {
	u := es.UnchainedHeadHash(es.StreamID{Type: "counter", ID: "main"}, 7)
	if got := hex.EncodeToString(u[:]); got != goldenUnchained {
		t.Fatalf("unchained head hash = %s, want %s", got, goldenUnchained)
	}
}

func TestHashSensitivity(t *testing.T) {
	base := es.EnvelopeHash(goldenV1)
	mutations := map[string]func(e *es.Envelope){
		"payload byte":     func(e *es.Envelope) { e.Payload[0] ^= 0x01 },
		"version":          func(e *es.Envelope) { e.Version++ },
		"type url":         func(e *es.Envelope) { e.TypeURL += "x" },
		"recorded_at 1µs":  func(e *es.Envelope) { e.RecordedAt = e.RecordedAt.Add(time.Microsecond) },
		"actor id":         func(e *es.Envelope) { e.Actor.ID = "bob" },
		"prev hash":        func(e *es.Envelope) { e.PrevHash = append([]byte{1}, e.PrevHash[1:]...) },
		"workspace":        func(e *es.Envelope) { e.Workspace = "w" },
		"nil prev == zero": nil, // control: a nil PrevHash encodes as ZeroHash
	}
	for name, mut := range mutations {
		e := goldenV1
		e.Payload = append([]byte(nil), goldenV1.Payload...)
		if mut == nil {
			e.PrevHash = nil
			if es.EnvelopeHash(e) != base {
				t.Fatalf("%s: nil prev_hash must hash as ZeroHash", name)
			}
			continue
		}
		mut(&e)
		if es.EnvelopeHash(e) == base {
			t.Fatalf("%s: hash unchanged after mutation", name)
		}
	}
	// Sub-microsecond precision is NOT attested: the adapters truncate.
	e := goldenV1
	e.RecordedAt = e.RecordedAt.Add(500 * time.Nanosecond)
	if es.EnvelopeHash(e) != base {
		t.Fatal("sub-µs change altered the hash; encoding must truncate to µs")
	}
	// GlobalPosition is deliberately excluded.
	e = goldenV1
	e.GlobalPosition = 99
	if es.EnvelopeHash(e) != base {
		t.Fatal("global_position must not be part of the hash")
	}
}

func TestAnchorRootOrderIndependent(t *testing.T) {
	a := es.StreamHead{Workspace: "w", StreamID: es.StreamID{Type: "a", ID: "1"}, Version: 3, Hash: make([]byte, es.HashSize)}
	b := es.StreamHead{Workspace: "w", StreamID: es.StreamID{Type: "b", ID: "1"}, Version: 1}
	if es.AnchorRoot([]es.StreamHead{a, b}) != es.AnchorRoot([]es.StreamHead{b, a}) {
		t.Fatal("anchor root depends on input order")
	}
	b2 := b
	b2.Version = 2
	if es.AnchorRoot([]es.StreamHead{a, b}) == es.AnchorRoot([]es.StreamHead{a, b2}) {
		t.Fatal("anchor root insensitive to a head version change")
	}
	if es.AnchorRoot([]es.StreamHead{a, b}) == es.AnchorRoot([]es.StreamHead{a}) {
		t.Fatal("anchor root insensitive to a dropped stream")
	}
}
