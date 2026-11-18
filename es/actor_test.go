package es_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/laenenai/es-lite/es"
)

// TestActorRefZero pins the zero contract the rest of the feature leans
// on (ADR 0041 §3.1): the zero ActorRef means "no delegation recorded",
// and — because Actor.Validate must be able to catch it — a ref with
// only one field set is NOT zero.
func TestActorRefZero(t *testing.T) {
	var zero es.ActorRef
	if !zero.IsZero() {
		t.Fatal("the zero ActorRef must report IsZero")
	}
	if got := zero.Principal(); got != "" {
		t.Fatalf("zero Principal() = %q, want \"\"", got)
	}
	for name, ref := range map[string]es.ActorRef{
		"type only": {Type: "service"},
		"id only":   {ID: "agent-runner"},
	} {
		if ref.IsZero() {
			t.Fatalf("%s: a partially filled ActorRef must not be zero — a partial Via must be detectable", name)
		}
	}
}

// TestActorRefPrincipalMatchesActor pins the byte-identity ADR 0041 §5
// demands: via_principal is written from Via.Principal() exactly the way
// actor_principal is written from Actor.Principal(), so the two must
// produce the same string for the same type and id — including the
// empty one for the zero value, and even for values Validate would
// reject (the writers must never diverge).
func TestActorRefPrincipalMatchesActor(t *testing.T) {
	for name, ref := range map[string]es.ActorRef{
		"zero":      {},
		"populated": {Type: "service", ID: "agent-runner"},
		"type only": {Type: "service"},
		"id only":   {ID: "agent-runner"},
	} {
		want := es.Actor{Type: ref.Type, ID: ref.ID}.Principal()
		if got := ref.Principal(); got != want {
			t.Fatalf("%s: ActorRef.Principal() = %q, want %q (Actor.Principal for the same type/id)", name, got, want)
		}
	}
	if got, want := (es.ActorRef{Type: "service", ID: "agent-runner"}).Principal(), "service:agent-runner"; got != want {
		t.Fatalf("Principal() = %q, want %q", got, want)
	}
}

// TestActorIsZeroIgnoresVia pins that Actor.IsZero keeps its pre-0041
// meaning — no type and no id — and that Via leaks into neither IsZero
// nor Principal: the accountable subject stays Actor alone.
func TestActorIsZeroIgnoresVia(t *testing.T) {
	a := es.Actor{Via: es.ActorRef{Type: "service", ID: "agent-runner"}}
	if !a.IsZero() {
		t.Fatal("Actor.IsZero must ignore Via and keep its current meaning")
	}
	if got := a.Principal(); got != "" {
		t.Fatalf("Principal() = %q, want \"\" — Via must not leak into the accountable principal", got)
	}
}

// TestActorValidate pins the append-seam rule both stores will call
// (ADR 0041 §3.4): the zero Actor stays valid (system-internal writes),
// a Via must be complete, and it must accompany an accountable actor.
// The invalid cases must match ErrInvalidDelegation through wrapping.
func TestActorValidate(t *testing.T) {
	for name, tc := range map[string]struct {
		actor es.Actor
		valid bool
	}{
		"zero actor":      {es.Actor{}, true},
		"plain actor":     {es.Actor{Type: "user", ID: "alice"}, true},
		"complete via":    {es.Actor{Type: "user", ID: "alice", Via: es.ActorRef{Type: "service", ID: "agent-runner"}}, true},
		"via type only":   {es.Actor{Type: "user", ID: "alice", Via: es.ActorRef{Type: "service"}}, false},
		"via id only":     {es.Actor{Type: "user", ID: "alice", Via: es.ActorRef{ID: "agent-runner"}}, false},
		"via, zero actor": {es.Actor{Via: es.ActorRef{Type: "service", ID: "agent-runner"}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.actor.Validate()
			if tc.valid {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, es.ErrInvalidDelegation) {
				t.Fatalf("Validate() = %v, want an error matching ErrInvalidDelegation", err)
			}
			// The stores return it inside their own context, so the
			// sentinel must survive wrapping.
			if !errors.Is(fmt.Errorf("append: %w", err), es.ErrInvalidDelegation) {
				t.Fatal("ErrInvalidDelegation does not survive wrapping")
			}
		})
	}
}
