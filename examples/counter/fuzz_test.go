package counter_test

import (
	"errors"
	"testing"

	"github.com/laenenai/es-lite/es"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
)

// The codecs' closed variant sets, mirrored from the generated switches
// (gen/counter/v1/counter_es.pb.go). Decode must accept exactly these URLs
// and classify every other URL as es.ErrUnknownEventType.
var knownEventURLs = map[string]bool{
	"counter.v1.Initialized": true,
	"counter.v1.Incremented": true,
	"counter.v1.Decremented": true,
	"counter.v1.Closed":      true,
}

var knownCommandURLs = map[string]bool{
	"counter.v1.Init":      true,
	"counter.v1.Increment": true,
	"counter.v1.Decrement": true,
	"counter.v1.Close":     true,
}

// FuzzCounterCodecDecode fuzzes the generated counter codecs (review
// finding F28): arbitrary type URLs and payload bytes must never panic.
// An unknown URL must come back as es.ErrUnknownEventType — the
// version-skew signal the store and upcast machinery rely on — and a
// known URL must never be misclassified as unknown (its payload may still
// fail to unmarshal, but that is a wire-format error, not a type error).
// The event and command URL sets are disjoint, so each decode exercises
// both classifications as the fuzzer varies the URL.
func FuzzCounterCodecDecode(f *testing.F) {
	f.Add("counter.v1.Initialized", []byte("\x08\x01\x10\x0a\x18\x03")) // Initialized{Min:1, Max:10, Value:3}
	f.Add("counter.v1.Incremented", []byte("\x08\x05"))                 // Incremented{By:5}
	f.Add("counter.v1.Closed", []byte(""))                              // Closed{} — empty message
	f.Add("counter.v1.Init", []byte("\x08\x01\x10\x0a\x18\x03"))        // command URL through the event codec
	f.Add("counter.v1.Initialize", []byte(""))                          // prefix of a known URL — still unknown
	f.Add("example.v1.UnheardOf", []byte("\xff\xff\xff\xff"))           // unknown URL plus garbage wire bytes
	f.Add("", []byte("\x08\x01"))                                       // empty URL
	f.Fuzz(func(t *testing.T, typeURL string, payload []byte) {
		enc := es.EncodedEvent{TypeURL: typeURL, Payload: payload}

		ev, err := (counterv1.EventCodec{}).Decode(enc)
		if knownEventURLs[typeURL] {
			if errors.Is(err, es.ErrUnknownEventType) {
				t.Fatalf("event codec rejected known URL %q as unknown", typeURL)
			}
			if err == nil && ev == nil {
				t.Fatalf("event codec decoded %q to a nil event with no error", typeURL)
			}
		} else if err == nil || ev != nil || !errors.Is(err, es.ErrUnknownEventType) {
			t.Fatalf("event codec on unknown URL %q = (%v, %v), want es.ErrUnknownEventType", typeURL, ev, err)
		}

		cmd, err := (counterv1.CommandCodec{}).Decode(enc)
		if knownCommandURLs[typeURL] {
			if errors.Is(err, es.ErrUnknownEventType) {
				t.Fatalf("command codec rejected known URL %q as unknown", typeURL)
			}
			if err == nil && cmd == nil {
				t.Fatalf("command codec decoded %q to a nil command with no error", typeURL)
			}
		} else if err == nil || cmd != nil || !errors.Is(err, es.ErrUnknownEventType) {
			t.Fatalf("command codec on unknown URL %q = (%v, %v), want es.ErrUnknownEventType", typeURL, cmd, err)
		}
	})
}
