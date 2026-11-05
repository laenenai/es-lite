package projection_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/laenenai/es-lite/projection"
)

// TestDefinitionHashIsStable pins the definition fingerprint (spec §5.5):
// the hash is deterministic, insensitive to the order of Consumes, and
// sensitive to the name and the type set. The golden values pin the
// exact byte layout — "es-lite/definition\x00" + Name + "\x00" + the
// NUL-joined sorted Consumes — because the fingerprint must be stable
// across processes and releases: a changed formula would strand every
// deployed checkpoint behind the feed's definition-drift refusal.
func TestDefinitionHashIsStable(t *testing.T) {
	one := projection.Definition{Name: "counter.view", Consumes: []string{"counter.v1.Incremented"}}
	if got := one.Hash(); got != "f72e9d8512d02b94b3dea11d7ce4b27a89a01fa40272d100751ad0426783f3ab" {
		t.Fatalf("Hash(%+v) = %q, want the pinned digest of the exact byte layout", one, got)
	}
	two := projection.Definition{Name: "counter.view", Consumes: []string{"b.v1.B", "a.v1.A"}}
	if got := two.Hash(); got != "4bcf3165192dbb2d2ac02a619f50c1cf7ce5603d83286e2a7c01277167aa1036" {
		t.Fatalf("Hash(%+v) = %q, want the pinned digest (Consumes sorted before the join)", two, got)
	}

	// Identical definitions hash equal.
	same := projection.Definition{Name: "counter.view", Consumes: []string{"counter.v1.Incremented"}}
	if one.Hash() != same.Hash() {
		t.Fatal("identical definitions hash different")
	}

	// Consumes order is irrelevant: the fingerprint sorts before joining.
	reordered := projection.Definition{Name: "counter.view", Consumes: []string{"a.v1.A", "b.v1.B"}}
	if two.Hash() != reordered.Hash() {
		t.Fatal("reordered Consumes hash different")
	}

	// A name change is a different projection identity.
	renamed := projection.Definition{Name: "counter.activity", Consumes: []string{"counter.v1.Incremented"}}
	if one.Hash() == renamed.Hash() {
		t.Fatal("a name change hashed equal")
	}

	// A type-set change is a different identity, whichever way it moves.
	if one.Hash() == two.Hash() {
		t.Fatal("an added consumed type hashed equal")
	}
	dropped := projection.Definition{Name: "counter.view", Consumes: []string{"a.v1.A"}}
	if reordered.Hash() == dropped.Hash() {
		t.Fatal("a removed consumed type hashed equal")
	}
}

func TestCatalogDefinitionAndOrder(t *testing.T) {
	authz := []string{"space.v1.Created", "space.v1.Granted"}
	activity := []string{"counter.v1.Incremented"}
	c := projection.NewCatalog(
		projection.Definition{Name: "space.authz", Consumes: authz},
		projection.Definition{Name: "counter.activity", Consumes: activity},
	)

	got, ok := c.Definition("space.authz")
	if !ok {
		t.Fatal("space.authz not found")
	}
	if got.Name != "space.authz" || !reflect.DeepEqual(got.Consumes, authz) {
		t.Fatalf("Definition(space.authz) = %+v", got)
	}
	if _, ok := c.Definition("nope"); ok {
		t.Fatal("unknown definition reported found")
	}
	// Definitions preserves the registration order rather than sorting.
	if got := c.Definitions(); len(got) != 2 || got[0].Name != "space.authz" || got[1].Name != "counter.activity" {
		t.Fatalf("Definitions() order = %+v", got)
	}
}

// The catalogue must be immutable from both sides: mutating the slices passed
// to NewCatalog or the values/Consumes returned by Catalog cannot change what
// a later read sees. A registry seeded from generated definitions relies on
// this to stay a snapshot.
func TestCatalogIsImmutable(t *testing.T) {
	consumes := []string{"a.v1.One"}
	c := projection.NewCatalog(projection.Definition{Name: "p", Consumes: consumes})

	consumes[0] = "mutated"

	got, _ := c.Definition("p")
	got.Name = "renamed"
	got.Consumes[0] = "mutated-again"

	again, _ := c.Definition("p")
	if again.Name != "p" || !reflect.DeepEqual(again.Consumes, []string{"a.v1.One"}) {
		t.Fatalf("catalogue changed through an alias: %+v", again)
	}

	all := c.Definitions()
	all[0].Consumes[0] = "mutated-third"
	last, _ := c.Definition("p")
	if last.Consumes[0] != "a.v1.One" {
		t.Fatalf("Definitions() aliases the catalogue: %+v", last)
	}
}

func TestCatalogFirstDuplicateWins(t *testing.T) {
	c := projection.NewCatalog(
		projection.Definition{Name: "p", Consumes: []string{"first"}},
		projection.Definition{Name: "p", Consumes: []string{"second"}},
	)
	got, _ := c.Definition("p")
	if !reflect.DeepEqual(got.Consumes, []string{"first"}) {
		t.Fatalf("duplicate name resolved to %+v, want the first definition", got)
	}
	if n := len(c.Definitions()); n != 1 {
		t.Fatalf("Definitions() length = %d, want 1", n)
	}
}

func TestCatalogEmpty(t *testing.T) {
	c := projection.NewCatalog()
	if got := c.Definitions(); len(got) != 0 {
		t.Fatalf("Definitions() = %+v, want empty", got)
	}
	if _, ok := c.Definition("p"); ok {
		t.Fatal("empty catalogue reported a definition")
	}
}

// Reads are safe concurrently: the catalogue is constructed once and shared
// by every worker/goroutine that resolves a projection.
func TestCatalogConcurrentReads(t *testing.T) {
	c := projection.NewCatalog(
		projection.Definition{Name: "a", Consumes: []string{"x"}},
		projection.Definition{Name: "b", Consumes: []string{"y"}},
	)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if d, ok := c.Definition("a"); !ok || d.Name != "a" {
					t.Errorf("Definition(a) = %+v, %v", d, ok)
					return
				}
				_ = c.Definitions()
			}
		}()
	}
	wg.Wait()
}
