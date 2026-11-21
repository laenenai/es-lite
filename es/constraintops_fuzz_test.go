package es

import (
	"reflect"
	"testing"
)

// FuzzDecodeConstraintOps checks the restore-path decoder against arbitrary
// bytes: it must never panic, and every accepted encoding must round-trip
// through EncodeConstraintOps unchanged. The input is attacker-reachable (an
// event row's constraint_ops column), so the seeds include the rejection
// shapes the table tests pin.
func FuzzDecodeConstraintOps(f *testing.F) {
	f.Add(EncodeConstraintOps([]ConstraintOp{
		Claim("user.email", "mac-1"),
		Release("user.email", "mac-2"),
	}))
	f.Add(EncodeConstraintOps(nil))
	f.Add([]byte(nil))
	f.Add([]byte("es-lite.constraint-ops.v1"))
	f.Add([]byte{0, 0, 0, 25, 'e', 's'})
	f.Fuzz(func(t *testing.T, b []byte) {
		ops, err := DecodeConstraintOps(b)
		if err != nil {
			return
		}
		again, err := DecodeConstraintOps(EncodeConstraintOps(ops))
		if err != nil {
			t.Fatalf("re-encoded ops failed to decode: %v", err)
		}
		if !reflect.DeepEqual(ops, again) {
			t.Fatalf("round trip mismatch: %#v != %#v", ops, again)
		}
	})
}
