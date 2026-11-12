package es_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/laenenai/es-lite/es"
)

// TestConflictErrorIsAndAs pins the compatibility contract the store
// backends rely on when they return *es.ConflictError instead of a
// formatted string: every consumer that matches errors.Is(err,
// es.ErrConflict) keeps matching, and a consumer that wants the versions
// gets them through errors.As — bare and wrapped.
func TestConflictErrorIsAndAs(t *testing.T) {
	sid, err := es.NewStreamID("counter", "main")
	if err != nil {
		t.Fatal(err)
	}
	conflict := &es.ConflictError{StreamID: sid, Expected: 4, Current: 7}

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"bare", conflict},
		{"wrapped once", fmt.Errorf("handle: %w", conflict)},
		{"wrapped twice", fmt.Errorf("outer: %w", fmt.Errorf("handle: %w", conflict))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, es.ErrConflict) {
				t.Fatalf("errors.Is(%v, ErrConflict) = false, want true — existing consumers match on this", tc.err)
			}
			var got *es.ConflictError
			if !errors.As(tc.err, &got) {
				t.Fatalf("errors.As(%v, **ConflictError) = false, want true", tc.err)
			}
			if got.StreamID != sid || got.Expected != 4 || got.Current != 7 {
				t.Fatalf("as = {%s %d %d}, want {%s 4 7}", got.StreamID, got.Expected, got.Current, sid)
			}
		})
	}

	// It must not match the sibling sentinels: a conflict is not a rerun
	// of an event the log already stores (F25), and nothing raced on ids.
	if errors.Is(conflict, es.ErrDuplicateEventID) {
		t.Fatal("ConflictError matches ErrDuplicateEventID — the F25 classification would be lost")
	}
}

// TestConflictErrorMessageShape pins the message text the store backends
// used to format by hand, so swapping the string for the struct does not
// change the shape of anyone's logs.
func TestConflictErrorMessageShape(t *testing.T) {
	sid, err := es.NewStreamID("counter", "main")
	if err != nil {
		t.Fatal(err)
	}
	got := (&es.ConflictError{StreamID: sid, Expected: 4, Current: 7}).Error()
	want := fmt.Sprintf("%v: stream %s at v%d, expected v%d", es.ErrConflict, sid, 7, 4)
	if got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
