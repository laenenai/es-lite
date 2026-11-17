package sqlite

import (
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
)

// Unit tests for decodeSpanContext, the decode behind scanEnvelope's
// three span-context columns. The rebuild migration's CHECK makes every
// malformed shape below unreachable through the store — no SQL writer
// can plant one, which is why these cases are unit-tested against the
// helper directly rather than through the database: the tolerance is the
// second line of defence, for a restored dump or a constraint bypassed
// during a migration (ADR 0040 §4).
//
// SQLite's typing is dynamic: trace_flags is declared INTEGER, but a
// restored dump can hold a REAL, TEXT or BLOB value there, and the old
// *int64 scan target failed the whole read on a TEXT value — the one
// thing telemetry must never do. flags now scans as any, which accepts
// every driver value, and this helper decides: whatever the stored
// value, every case decodes to a value, never an error, and "absent" is
// exactly the zero SpanContext.

// One valid id pair, shared by every table case that varies only the
// flags: 16 and 8 non-zero bytes, the W3C Trace Context wire sizes.
var (
	decodeTraceID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	decodeSpanID  = [8]byte{21, 22, 23, 24, 25, 26, 27, 28}
)

func TestDecodeSpanContext(t *testing.T) {
	for _, tc := range []struct {
		name            string
		traceID, spanID []byte
		flags           any
		want            es.SpanContext
	}{
		// The absent shape: three NULLs, the shape of every row
		// written before migration 0009 and of every append without
		// a context.
		{"all three NULL", nil, nil, nil, es.SpanContext{}},
		// NULL trace_flags reads as absent even with ids alongside:
		// the CHECK ties NULL flags to NULL ids, so the pair exists
		// only in a restored dump, and a NULL flags column means
		// the row recorded no context to trust those ids against.
		{"NULL flags with ids", decodeTraceID[:], decodeSpanID[:], nil, es.SpanContext{}},
		// Out-of-range integers read as absent: narrowing such a
		// value to uint8 would fabricate a context (300 would parse
		// as 44).
		{"flags -1", decodeTraceID[:], decodeSpanID[:], int64(-1), es.SpanContext{}},
		{"flags 300", decodeTraceID[:], decodeSpanID[:], int64(300), es.SpanContext{}},
		// Every non-int64 type reads as absent, never a scan error:
		// the dynamic-typing values a restored dump can land in the
		// INTEGER column.
		{"REAL flags", decodeTraceID[:], decodeSpanID[:], float64(1), es.SpanContext{}},
		{"TEXT flags", decodeTraceID[:], decodeSpanID[:], "abc", es.SpanContext{}},
		{"BLOB flags", decodeTraceID[:], decodeSpanID[:], []byte{1}, es.SpanContext{}},
		{"bool flags", decodeTraceID[:], decodeSpanID[:], true, es.SpanContext{}},
		{"TIME flags", decodeTraceID[:], decodeSpanID[:], time.Unix(0, 0), es.SpanContext{}},
		// In-range int64 flags with valid ids are the only populated
		// shapes — sampled (1, 255) and unsampled (0, a valid W3C
		// link target the store itself can write).
		{"flags 1", decodeTraceID[:], decodeSpanID[:], int64(1),
			es.SpanContext{TraceID: decodeTraceID, SpanID: decodeSpanID, Flags: 1}},
		{"flags 255", decodeTraceID[:], decodeSpanID[:], int64(255),
			es.SpanContext{TraceID: decodeTraceID, SpanID: decodeSpanID, Flags: 255}},
		{"flags 0", decodeTraceID[:], decodeSpanID[:], int64(0),
			es.SpanContext{TraceID: decodeTraceID, SpanID: decodeSpanID}},
		// Non-NULL flags without ids is the half-context the CHECK
		// forbids: with flags set it does not parse, and with flags 0
		// it is the absent shape — both read as absent.
		{"flags 0 without ids", nil, nil, int64(0), es.SpanContext{}},
		{"flags 1 without ids", nil, nil, int64(1), es.SpanContext{}},
		// Wrong-length ids do not parse — the CHECK pins 16 and 8.
		{"15-byte trace id", decodeTraceID[:15], decodeSpanID[:], int64(1), es.SpanContext{}},
		{"7-byte span id", decodeTraceID[:], decodeSpanID[:7], int64(1), es.SpanContext{}},
		// Right lengths but all-zero ids do not parse either: an
		// all-zero id is not a usable trace partner.
		{"all-zero trace id", make([]byte, 16), decodeSpanID[:], int64(1), es.SpanContext{}},
		{"all-zero span id", decodeTraceID[:], make([]byte, 8), int64(1), es.SpanContext{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := decodeSpanContext(tc.traceID, tc.spanID, tc.flags); got != tc.want {
				t.Fatalf("decodeSpanContext = %+v, want %+v", got, tc.want)
			}
		})
	}
}
