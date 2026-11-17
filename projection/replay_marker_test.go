package projection_test

import (
	"context"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

// TestIsReplaying pins the replay marker (ADR 0040 §3.6): a plain context
// reports false, the context Replay hands its apply callback reports true —
// so a consumer's dispatch wrapper can suppress span links during a rebuild —
// and Runner.Rebuild inherits the marker because it goes through Replay.
func TestIsReplaying(t *testing.T) {
	if projection.IsReplaying(context.Background()) {
		t.Fatal("IsReplaying is true on a plain context, want false")
	}

	var seen bool
	src := sliceSource{{GlobalPosition: 1, TypeURL: "counter.v1.Incremented"}}
	last, err := projection.Replay(context.Background(), src, 0, 10,
		func(ctx context.Context, _ []es.Envelope) error {
			seen = projection.IsReplaying(ctx)
			return nil
		})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if last != 1 {
		t.Fatalf("last position = %d, want 1", last)
	}
	if !seen {
		t.Fatal("IsReplaying is false inside Replay's apply callback, want true")
	}

	// Rebuild drives Replay, so Dispatch sees the marker too.
	var dispatchSeen bool
	r := &projection.Runner{
		Name:   "counter.view",
		Marker: projection.NewMemoryMarker(),
		Dispatch: func(ctx context.Context, _ es.Envelope) error {
			dispatchSeen = projection.IsReplaying(ctx)
			return nil
		},
	}
	if _, err := r.Rebuild(context.Background(), sliceSource{{GlobalPosition: 1, TypeURL: "counter.v1.Incremented"}}, 10); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if !dispatchSeen {
		t.Fatal("IsReplaying is false inside Dispatch during Rebuild, want true")
	}
}

// TestIsReplayingFalseOutsideReplay pins the live path: a direct Runner.Apply
// call is live delivery, not a rebuild, so IsReplaying must report false
// inside Dispatch — only Replay marks the context.
func TestIsReplayingFalseOutsideReplay(t *testing.T) {
	var dispatchSeen bool
	r := &projection.Runner{
		Name:   "counter.view",
		Marker: projection.NewMemoryMarker(),
		Dispatch: func(ctx context.Context, _ es.Envelope) error {
			dispatchSeen = projection.IsReplaying(ctx)
			return nil
		},
	}
	if err := r.Apply(context.Background(), []es.Envelope{{GlobalPosition: 1, TypeURL: "counter.v1.Incremented"}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if dispatchSeen {
		t.Fatal("IsReplaying is true inside Dispatch during a direct Apply, want false (live delivery)")
	}
}
