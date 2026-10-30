package natsstore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
)

func TestEnvelopeWireRoundTripKeepsHashes(t *testing.T) {
	sid, _ := es.NewStreamID("counter", "main")
	e := es.Envelope{
		EventID: uuid.New(), StreamID: sid, Workspace: "ws", Version: 2, GlobalPosition: 9,
		TypeURL: "counter.v1.Incremented", SchemaVersion: 1,
		OccurredAt: es.TruncateMicros(time.Now()), RecordedAt: es.TruncateMicros(time.Now()),
		CorrelationID: uuid.New(), CausationID: uuid.New(), CommandID: uuid.New(),
		Actor: es.Actor{Type: "user", ID: "a"}, Payload: []byte("sealed"),
		PrevHash: make([]byte, es.HashSize),
	}
	h := es.EnvelopeHash(e)
	e.Hash = h[:]

	b, err := json.Marshal(toEnvelopeWire(e))
	if err != nil {
		t.Fatal(err)
	}
	var w envelopeWire
	if err := json.Unmarshal(b, &w); err != nil {
		t.Fatal(err)
	}
	got, err := fromEnvelopeWire(w)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Hash) != string(e.Hash) || string(got.PrevHash) != string(e.PrevHash) {
		t.Fatal("hashes did not survive the wire")
	}
	if rh := es.EnvelopeHash(got); string(rh[:]) != string(got.Hash) {
		t.Fatal("envelope decoded from the wire does not re-hash: a client cannot verify")
	}
}
