package es

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestConstraintOpsRoundTrip(t *testing.T) {
	ops := []ConstraintOp{
		Claim("email", "a@b"),
		Release("email", "c\nd"),
		Claim("slug", string([]byte{0xff, 0x00, 0x80})),
	}
	got, err := DecodeConstraintOps(EncodeConstraintOps(ops))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, ops) {
		t.Fatalf("round trip = %#v, want %#v", got, ops)
	}
}

func TestConstraintOpsEmpty(t *testing.T) {
	b := EncodeConstraintOps(nil)
	got, err := DecodeConstraintOps(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("empty ops decode to %d ops, want 0", len(got))
	}
}

func TestConstraintOpsLayout(t *testing.T) {
	b := EncodeConstraintOps([]ConstraintOp{Claim("email", "a@b")})
	want := append([]byte{0, 0, 0, 25}, []byte("es-lite.constraint-ops.v1")...)
	want = append(want, 0, 0, 0, 1, 0, 0, 0, 0, 5)
	want = append(want, []byte("email")...)
	want = append(want, 0, 0, 0, 3)
	want = append(want, []byte("a@b")...)
	if !bytes.Equal(b, want) {
		t.Fatalf("encoding =\n%x\nwant\n%x", b, want)
	}
}

func TestConstraintOpsRejects(t *testing.T) {
	good := EncodeConstraintOps([]ConstraintOp{Claim("email", "a@b")})
	cases := map[string][]byte{
		"nil":            nil,
		"short":          {0, 0},
		"wrong domain":   append(append([]byte{0, 0, 0, 25}, bytes.Repeat([]byte{'x'}, 25)...), good[29:]...),
		"trailing bytes": append(append([]byte{}, good...), 0),
		"unknown kind":   func() []byte { b := append([]byte{}, good...); b[33] = 9; return b }(),
		// One Claim encodes to 17 bytes after the count; a count of 18 must
		// trip the count guard before any op is parsed.
		"count too high": func() []byte { b := append([]byte{}, good...); b[29] = 0; b[32] = 18; return b }(),
		"huge length": func() []byte {
			b := append([]byte{}, good...)
			b[34], b[35], b[36], b[37] = 0xff, 0xff, 0xff, 0xff
			return b
		}(),
	}
	for name, b := range cases {
		if _, err := DecodeConstraintOps(b); err == nil {
			t.Errorf("%s: DecodeConstraintOps accepted malformed input", name)
		}
	}
	for i := 0; i < len(good); i++ {
		if _, err := DecodeConstraintOps(good[:i]); err == nil {
			t.Errorf("truncated at %d: accepted", i)
		}
	}

	// The count guard is pinned by its own message, not merely by a later
	// parse failing: count=18 exceeds the 17 remaining bytes.
	countTooHigh := append([]byte{}, good...)
	countTooHigh[29], countTooHigh[32] = 0, 18
	if _, err := DecodeConstraintOps(countTooHigh); err == nil ||
		!strings.Contains(err.Error(), "count 18 exceeds 17 remaining bytes") {
		t.Errorf("count too high error = %v, want the count guard's message", err)
	}
}

// TestConstraintOpsTruncatesRejectedDomain pins the error bound: a wrong
// domain is attacker-controlled, so the error quotes at most 64 of its bytes
// instead of echoing the whole blob.
func TestConstraintOpsTruncatesRejectedDomain(t *testing.T) {
	domain := strings.Repeat("x", 4096)
	// u32 length prefix 4096 (0x00001000), then the wrong domain.
	blob := append([]byte{0x00, 0x00, 0x10, 0x00}, []byte(domain)...)
	_, err := DecodeConstraintOps(blob)
	if err == nil {
		t.Fatal("DecodeConstraintOps accepted a 4096-byte wrong domain")
	}
	msg := err.Error()
	if len(msg) > 256 {
		t.Fatalf("error is %d bytes, want it bounded: %q", len(msg), msg)
	}
	if !strings.Contains(msg, strings.Repeat("x", 64)) {
		t.Errorf("error does not quote the 64-byte prefix: %q", msg)
	}
	if strings.Contains(msg, strings.Repeat("x", 65)) {
		t.Errorf("error quotes more than 64 domain bytes: %q", msg)
	}
}
