package projection_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/laenenai/es-lite/projection"
)

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
