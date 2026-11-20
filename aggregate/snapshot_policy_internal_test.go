package aggregate

import "testing"

// TestSnapshotDue pins the ADR 0042 §3.1 predicate exactly: an unset
// policy keeps today's write-through (always due), an explicit 0 disables
// automatic writes, and otherwise a snapshot is due once the append carries
// the stream to seed+n or beyond — with to > seed guarding the
// subtraction against wrapping on a no-op append.
func TestSnapshotDue(t *testing.T) {
	cases := []struct {
		name    string
		set     bool
		n, seed uint64
		to      uint64
		want    bool
	}{
		{"unset writes every append", false, 0, 5, 6, true},
		{"zero disables", true, 0, 5, 6, false},
		{"below the gap", true, 3, 3, 5, false},
		{"at the gap", true, 3, 3, 6, true},
		{"beyond the gap", true, 3, 3, 7, true},
		{"batch jumps the gap", true, 3, 0, 4, true},
		{"full fold seeds from zero", true, 3, 0, 2, false},
		{"zero version never writes", true, 3, 0, 0, false},
	}
	for _, c := range cases {
		r := &Runtime[struct{}, struct{}, struct{}]{snapshotEverySet: c.set, snapshotEvery: c.n}
		if got := r.snapshotDue(c.seed, c.to); got != c.want {
			t.Errorf("%s: snapshotDue(%d,%d)=%v, want %v", c.name, c.seed, c.to, got, c.want)
		}
	}
}
