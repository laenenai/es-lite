package upcast_test

import (
	"testing"

	"github.com/laenenai/es-lite/upcast"
)

func TestRegistryDispatch(t *testing.T) {
	r := upcast.NewRegistry[string]().Register("greet", func(from uint32, e string) (string, error) {
		if from < 2 {
			return e + "!", nil // upgrade v1 -> v2
		}
		return e, nil
	})

	// Registered type, old version: transformed.
	if got, _ := r.Upcast("greet", 1, "hi"); got != "hi!" {
		t.Fatalf("v1 upcast = %q, want %q", got, "hi!")
	}
	// Registered type, current version: the func chooses to no-op.
	if got, _ := r.Upcast("greet", 2, "hi"); got != "hi" {
		t.Fatalf("v2 upcast = %q, want %q", got, "hi")
	}
	// Unregistered type: pass through unchanged.
	if got, _ := r.Upcast("other", 1, "x"); got != "x" {
		t.Fatalf("unregistered = %q, want %q", got, "x")
	}
}
