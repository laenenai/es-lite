package projection_test

import (
	"strings"
	"testing"

	"github.com/laenenai/es-lite/projection"
)

// xorFF flips every byte of s (0x00 <-> 0xff and friends): a fixed,
// length-preserving bijection used to derive a genuinely different
// consumed-type list from the fuzzer's input without moving part
// boundaries — the mutation must change content, not just re-parse it.
func xorFF(s string) string {
	b := []byte(s)
	for i, c := range b {
		b[i] = c ^ 0xff
	}
	return string(b)
}

// sameTypeSet reports whether two consumed-type lists hold the same SET of
// types, ignoring order and duplicates. Hash deliberately hashes the full
// slice, duplicates included, but a set-level change must always move it —
// mutations that merely permute or duplicate existing types are not set
// changes and assert nothing.
func sameTypeSet(a, b []string) bool {
	seen := make(map[string]bool, len(a))
	for _, t := range a {
		seen[t] = true
	}
	other := make(map[string]bool, len(b))
	for _, t := range b {
		other[t] = true
	}
	if len(seen) != len(other) {
		return false
	}
	for t := range other {
		if !seen[t] {
			return false
		}
	}
	return true
}

// FuzzDefinitionHashConsumes fuzzes the definition fingerprint over
// hostile Consumes content (review finding F28): control bytes (NUL is the
// hash layout's own separator), unicode and empty parts. Three properties:
//
//   - Reordering a declaration is not a definition change (spec §5.5):
//     the reversed list must hash identically, duplicates and all.
//   - A mutation that changes the consumed-type SET is a definition
//     change: two derived lists — every part byte-flipped, and one part's
//     first byte flipped (an empty part grows a NUL, changing the length,
//     which alone must move the join) — must never collide with the base
//     hash whenever their type set really differs.
//   - The name is part of the identity: a flipped name over the same set
//     must hash differently (skipped for the empty name, the flip's fixed
//     point).
func FuzzDefinitionHashConsumes(f *testing.F) {
	// Consumed-type lists are comma-joined so parts can freely carry NUL,
	// unicode and empty segments.
	f.Add("counter.view", "counter.v1.Initialized,counter.v1.Incremented")
	f.Add("p", "a\x00b,b")                      // NUL inside a consumed type
	f.Add("", ",,")                             // empty parts
	f.Add("n", "\x9e,a")                        // set closed under the byte-flip mutation
	f.Add("проект", "событие.v1.Δ,🎲.v1.rolled") // unicode name and parts
	f.Add("name\x00nul", "x")
	f.Fuzz(func(t *testing.T, name, joined string) {
		consumes := strings.Split(joined, ",")
		base := projection.Definition{Name: name, Consumes: consumes}

		// Order (and duplication) is not identity.
		reversed := make([]string, len(consumes))
		for i, c := range consumes {
			reversed[len(consumes)-1-i] = c
		}
		if got := (projection.Definition{Name: name, Consumes: reversed}).Hash(); got != base.Hash() {
			t.Fatalf("reordering Consumes %q changed the hash of %q", consumes, name)
		}

		// Set-changing mutations must never collide.
		flippedAll := make([]string, len(consumes))
		for i, c := range consumes {
			flippedAll[i] = xorFF(c)
		}
		flippedOne := make([]string, len(consumes))
		copy(flippedOne, consumes)
		if n := len(flippedOne); n > 0 {
			i := len(name) % n
			if flippedOne[i] == "" {
				flippedOne[i] = "\x00"
			} else {
				flippedOne[i] = xorFF(flippedOne[i][:1]) + flippedOne[i][1:]
			}
		}
		for _, mutated := range [][]string{flippedAll, flippedOne} {
			if sameTypeSet(mutated, consumes) {
				continue
			}
			if got := (projection.Definition{Name: name, Consumes: mutated}).Hash(); got == base.Hash() {
				t.Fatalf("distinct consumed-type sets collided: %q vs %q (name %q)", consumes, mutated, name)
			}
		}

		// The name is part of the identity.
		if flipped := xorFF(name); flipped != name {
			if got := (projection.Definition{Name: flipped, Consumes: consumes}).Hash(); got == base.Hash() {
				t.Fatalf("name change %q -> %q left the hash unchanged", name, flipped)
			}
		}
	})
}
