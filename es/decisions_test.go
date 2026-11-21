package es

import (
	"bytes"
	"reflect"
	"testing"
)

func TestDecisionOpsRoundTrip(t *testing.T) {
	ops := []ConstraintOp{
		Claim("email", "a@b"),
		Release("email", "c\nd"),
		Claim("slug", string([]byte{0xff, 0x00, 0x80})),
	}
	got, err := DecodeDecisionOps(EncodeDecisionOps(ops))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, ops) {
		t.Fatalf("round trip = %#v, want %#v", got, ops)
	}
}

func TestDecisionOpsEmpty(t *testing.T) {
	b := EncodeDecisionOps(nil)
	got, err := DecodeDecisionOps(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("empty ops decode to %d ops, want 0", len(got))
	}
}

func TestDecisionOpsLayout(t *testing.T) {
	b := EncodeDecisionOps([]ConstraintOp{Claim("email", "a@b")})
	want := append([]byte{0, 0, 0, 27}, []byte("es-lite.append-decisions.v1")...)
	want = append(want, 0, 0, 0, 1, 0, 0, 0, 0, 5)
	want = append(want, []byte("email")...)
	want = append(want, 0, 0, 0, 3)
	want = append(want, []byte("a@b")...)
	if !bytes.Equal(b, want) {
		t.Fatalf("encoding =\n%x\nwant\n%x", b, want)
	}
}

func TestDecisionOpsRejects(t *testing.T) {
	good := EncodeDecisionOps([]ConstraintOp{Claim("email", "a@b")})
	cases := map[string][]byte{
		"nil":            nil,
		"short":          {0, 0},
		"wrong domain":   append(append([]byte{0, 0, 0, 27}, bytes.Repeat([]byte{'x'}, 27)...), good[31:]...),
		"trailing bytes": append(append([]byte{}, good...), 0),
		"unknown kind":   func() []byte { b := append([]byte{}, good...); b[35] = 9; return b }(),
		"count too high": func() []byte { b := append([]byte{}, good...); b[31] = 0; b[34] = 2; return b }(),
		"huge length": func() []byte {
			b := append([]byte{}, good...)
			b[36], b[37], b[38], b[39] = 0xff, 0xff, 0xff, 0xff
			return b
		}(),
	}
	for name, b := range cases {
		if _, err := DecodeDecisionOps(b); err == nil {
			t.Errorf("%s: DecodeDecisionOps accepted malformed input", name)
		}
	}
	for i := 0; i < len(good); i++ {
		if _, err := DecodeDecisionOps(good[:i]); err == nil {
			t.Errorf("truncated at %d: accepted", i)
		}
	}
}
