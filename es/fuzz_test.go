package es_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
)

// FuzzParseCanonical fuzzes the storage-form parser (review finding F28):
// any byte string must either parse into a well-formed StreamID that
// round-trips through Canonical unchanged, or be rejected with exactly
// ErrInvalidStreamID — never anything else, and never a panic. The
// round-trip check pins the split contract: a successful parse means the
// input was exactly Type + ":" + ID with slug-valid parts, so
// re-canonicalizing is the identity and re-parsing is stable.
func FuzzParseCanonical(f *testing.F) {
	for _, seed := range []string{
		"counter:main",     // canonical happy path
		"a:b",              // minimal valid form
		"a-b_c:x-y_z",      // every allowed slug character
		"counter:",         // empty ID
		":main",            // empty Type
		":",                // separator only
		"",                 // empty input
		"a:b:c",            // second separator leaks into the ID slug
		"Counter:Main",     // uppercase is rejected
		"counter:maïn",     // unicode is rejected
		"counter:ma\x00in", // control byte is rejected
		strings.Repeat("a", 128) + ":" + strings.Repeat("b", 128), // max-length slugs
		strings.Repeat("a", 129) + ":" + strings.Repeat("b", 129), // over-length slugs
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, canonical string) {
		sid, err := es.ParseCanonical(canonical)
		if err != nil {
			if !errors.Is(err, es.ErrInvalidStreamID) {
				t.Fatalf("ParseCanonical(%q) error = %v, want ErrInvalidStreamID", canonical, err)
			}
			return
		}
		if err := sid.Validate(); err != nil {
			t.Fatalf("ParseCanonical(%q) = %+v, which does not validate: %v", canonical, sid, err)
		}
		if got := sid.Canonical(); got != canonical {
			t.Fatalf("ParseCanonical(%q) round-tripped to %q, want the identity", canonical, got)
		}
		again, err := es.ParseCanonical(sid.Canonical())
		if err != nil || again != sid {
			t.Fatalf("re-parsing %q = %+v, %v; want %+v, <nil>", sid.Canonical(), again, err, sid)
		}
	})
}
