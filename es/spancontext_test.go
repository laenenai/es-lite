package es_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/laenenai/es-lite/es"
)

func TestParseSpanContext(t *testing.T) {
	trace := bytes.Repeat([]byte{0xab}, 16)
	span := bytes.Repeat([]byte{0xcd}, 8)
	zero16 := make([]byte, 16)
	zero8 := make([]byte, 8)

	tests := []struct {
		name        string
		trace, span []byte
		flags       uint8
		wantErr     bool
		wantValid   bool
	}{
		{"valid", trace, span, 1, false, true},
		{"valid all flags", trace, span, 0xff, false, true},
		{"absent", nil, nil, 0, false, false},
		{"partial span", trace, nil, 0, true, false},
		{"partial trace", nil, span, 0, true, false},
		{"trace too short", trace[:15], span, 0, true, false},
		{"span too short", trace, span[:7], 0, true, false},
		{"zero trace id", zero16, span, 0, true, false},
		{"zero span id", trace, zero8, 0, true, false},
		{"flags without ids", nil, nil, 1, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc, err := es.ParseSpanContext(tt.trace, tt.span, tt.flags)

			if tt.wantErr {
				if !errors.Is(err, es.ErrMalformedSpanContext) {
					t.Fatalf("ParseSpanContext() error = %v, want ErrMalformedSpanContext", err)
				}
				if sc != (es.SpanContext{}) {
					t.Fatalf("ParseSpanContext() on error = %+v, want zero SpanContext", sc)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseSpanContext() unexpected error = %v", err)
			}
			if got := sc.IsValid(); got != tt.wantValid {
				t.Fatalf("ParseSpanContext().IsValid() = %v, want %v", got, tt.wantValid)
			}
			if tt.wantValid {
				if sc.TraceID != ([16]byte(bytes.Repeat([]byte{0xab}, 16))) {
					t.Errorf("TraceID = %x, want %x", sc.TraceID, trace)
				}
				if sc.SpanID != ([8]byte(bytes.Repeat([]byte{0xcd}, 8))) {
					t.Errorf("SpanID = %x, want %x", sc.SpanID, span)
				}
				if sc.Flags != tt.flags {
					t.Errorf("Flags = %#x, want %#x", sc.Flags, tt.flags)
				}
			}
		})
	}
}

func TestSpanContextIsValid(t *testing.T) {
	var zero es.SpanContext
	if zero.IsValid() {
		t.Error("zero SpanContext is valid, want invalid")
	}

	sc := es.SpanContext{
		TraceID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Flags:   1,
	}
	if !sc.IsValid() {
		t.Error("populated SpanContext is invalid, want valid")
	}

	zeroTrace := sc
	zeroTrace.TraceID = [16]byte{}
	if zeroTrace.IsValid() {
		t.Error("SpanContext with zero TraceID is valid, want invalid")
	}

	zeroSpan := sc
	zeroSpan.SpanID = [8]byte{}
	if zeroSpan.IsValid() {
		t.Error("SpanContext with zero SpanID is valid, want invalid")
	}
}
