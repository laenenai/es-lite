package es

// SpanContext is the producer's trace context (ADR 0040), carried
// alongside a command or event so consumers can join the resulting
// events back to the producing trace. The ids follow the W3C Trace
// Context wire sizes: a 16-byte trace id and an 8-byte span id, plus a
// single flags byte.
//
// The zero SpanContext means "absent": a caller without a trace context
// sends it and nothing is recorded. It is not valid to send one id
// without the other — use the zero value or a fully populated context.
type SpanContext struct {
	TraceID [16]byte
	SpanID  [8]byte
	Flags   uint8
}

// IsValid reports whether the context carries a usable trace: both ids
// non-zero. Flags do not affect validity; the zero SpanContext is
// invalid.
func (s SpanContext) IsValid() bool {
	return s.TraceID != [16]byte{} && s.SpanID != [8]byte{}
}

// ParseSpanContext builds a SpanContext from raw trace-context bytes,
// rejecting anything a producer should not have emitted. The rule,
// which both stores and the platform's feed client rely on: it returns
// the zero value and nil when both ids are empty and flags == 0
// (absent); it returns ErrMalformedSpanContext when both ids are empty
// but flags are not, when either id is the wrong length, and when
// either id is all zeros; otherwise it returns the populated value. On
// any error the returned SpanContext is the zero value, and the error
// is ErrMalformedSpanContext itself — match it with errors.Is or
// equality.
func ParseSpanContext(traceID, spanID []byte, flags uint8) (SpanContext, error) {
	if len(traceID) == 0 && len(spanID) == 0 {
		if flags == 0 {
			return SpanContext{}, nil
		}
		return SpanContext{}, ErrMalformedSpanContext
	}
	if len(traceID) != 16 || len(spanID) != 8 {
		return SpanContext{}, ErrMalformedSpanContext
	}

	var sc SpanContext
	copy(sc.TraceID[:], traceID)
	copy(sc.SpanID[:], spanID)
	if sc.TraceID == [16]byte{} || sc.SpanID == [8]byte{} {
		return SpanContext{}, ErrMalformedSpanContext
	}
	sc.Flags = flags
	return sc, nil
}
