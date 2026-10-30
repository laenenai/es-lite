package natsjs_test

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/natsjs"
)

func chained(n int) []es.Envelope {
	sid, _ := es.NewStreamID("counter", "main")
	out := make([]es.Envelope, n)
	prev := es.ZeroHash[:]
	for i := range out {
		e := es.Envelope{StreamID: sid, Workspace: "ws", Version: uint64(i + 1), GlobalPosition: uint64(i + 1),
			TypeURL: "counter.v1.Incremented", Payload: []byte{byte(i)}, PrevHash: prev}
		h := es.EnvelopeHash(e)
		e.Hash = h[:]
		prev = e.Hash
		out[i] = e
	}
	return out
}

func TestGapDetector(t *testing.T) {
	evs := chained(4)
	g := natsjs.NewGapDetector()
	if err := g.Check(evs[1]); err != nil { // mid-stream start is accepted
		t.Fatalf("first seen: %v", err)
	}
	if err := g.Check(evs[1]); err != nil { // redelivery
		t.Fatalf("redelivery: %v", err)
	}
	if err := g.Check(evs[3]); !errors.Is(err, natsjs.ErrGap) { // v4 after v2
		t.Fatalf("skipped version err = %v, want ErrGap", err)
	}
	if err := g.Check(evs[2]); err != nil {
		t.Fatalf("v3: %v", err)
	}
	// v4 with a prev_hash that does not link (a different row's hash).
	bad := evs[3]
	bad.PrevHash = evs[0].Hash
	if err := g.Check(bad); !errors.Is(err, natsjs.ErrGap) {
		t.Fatalf("broken link err = %v, want ErrGap", err)
	}
	if err := g.Check(evs[3]); err != nil {
		t.Fatalf("v4: %v", err)
	}
	// Unhashed events (pre-0002) are only version-checked.
	plain := evs[3]
	plain.Version, plain.Hash, plain.PrevHash = 5, nil, nil
	if err := g.Check(plain); err != nil {
		t.Fatalf("unhashed next: %v", err)
	}
	g.Forget("ws", evs[0].StreamID)
	if err := g.Check(evs[0]); err != nil {
		t.Fatalf("after forget: %v", err)
	}
}

func TestHashHeadersRoundTrip(t *testing.T) {
	e := chained(1)[0]
	msg := natsjs.MsgFor("evt.ws.counter.incremented", e)
	if msg.Header.Get(natsjs.HdrHash) == "" || msg.Header.Get(natsjs.HdrPrevHash) == "" {
		t.Fatal("hash headers missing")
	}
	got, err := natsjs.DecodeEnvelope(msg.Header, msg.Data)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Hash) != string(e.Hash) || string(got.PrevHash) != string(e.PrevHash) {
		t.Fatal("hash headers did not round-trip")
	}
	h := es.EnvelopeHash(got)
	if string(h[:]) != string(got.Hash) {
		t.Fatal("decoded envelope does not re-hash to Es-Hash")
	}
	// Unhashed rows carry no headers.
	e.Hash, e.PrevHash = nil, nil
	msg = natsjs.MsgFor("s", e)
	if _, ok := msg.Header[natsjs.HdrHash]; ok {
		t.Fatal("Es-Hash set for an unhashed row")
	}
	got, _ = natsjs.DecodeEnvelope(nats.Header{natsjs.HdrStream: []string{"counter:main"}}, nil)
	if got.Hash != nil {
		t.Fatal("hash decoded from absent header")
	}
}

func TestSubjectFor(t *testing.T) {
	cases := map[[3]string]string{
		{"ws_a", "counter", "counter.v1.Incremented"}: "evt.ws_a.counter.incremented",
		{"", "counter", "Incremented"}:                "evt.default.counter.incremented",
		{"*", "space", "ont.party.space.v1.Created"}:  "evt.*.space.created",
		{"a.b", "x y", "T"}:                           "evt.a_b.x_y.t",
	}
	for in, want := range cases {
		if got := natsjs.SubjectFor(in[0], in[1], in[2]); got != want {
			t.Fatalf("SubjectFor(%q,%q,%q) = %q, want %q", in[0], in[1], in[2], got, want)
		}
	}
}
