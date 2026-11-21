package es

import (
	"bytes"
	"reflect"
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
		"count too high": func() []byte { b := append([]byte{}, good...); b[29] = 0; b[32] = 2; return b }(),
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
}
