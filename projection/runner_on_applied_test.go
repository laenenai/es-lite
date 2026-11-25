package projection_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

type savingMarker struct {
	pos   uint64
	saves []uint64
}

func (m *savingMarker) Load(context.Context, string) (uint64, error) { return m.pos, nil }

func (m *savingMarker) Save(_ context.Context, _ string, position uint64) error {
	m.pos = position
	m.saves = append(m.saves, position)
	return nil
}

type failingMarker struct{}

func (m *failingMarker) Load(context.Context, string) (uint64, error) { return 0, nil }
func (m *failingMarker) Save(context.Context, string, uint64) error   { return errors.New("ack failed") }

func TestRunnerOnAppliedAfterMarkerSave(t *testing.T) {
	ctx := context.Background()
	m := &savingMarker{}
	var got []uint64
	r := &projection.Runner{
		Name:     "counter.view",
		Marker:   m,
		Dispatch: func(context.Context, es.Envelope) error { return nil },
		OnApplied: func(_ context.Context, e es.Envelope, position uint64) {
			// Apply coalesces the batch into one save at the last applied
			// position, so by the time the hook runs the marker covers e:
			// it may be ahead of e, never behind. The hook must not run
			// before that save.
			if m.pos < position {
				t.Errorf("OnApplied(%d) while marker is at %d; hook must run after the covering save", position, m.pos)
			}
			if e.GlobalPosition != position || e.TypeURL != "counter.v1.Incremented" {
				t.Errorf("OnApplied envelope = {position:%d type:%q}, want {position:%d type:%q}", e.GlobalPosition, e.TypeURL, position, "counter.v1.Incremented")
			}
			got = append(got, position)
		},
	}
	if err := r.Apply(ctx, []es.Envelope{
		{GlobalPosition: 1, TypeURL: "counter.v1.Incremented"},
		{GlobalPosition: 2, TypeURL: "counter.v1.Incremented"},
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("OnApplied positions = %v, want [1 2]", got)
	}
}

func TestRunnerOnAppliedSkipsPositionsAtOrBelowMarker(t *testing.T) {
	ctx := context.Background()
	m := &savingMarker{pos: 5}
	var got []uint64
	r := &projection.Runner{
		Name:      "counter.view",
		Marker:    m,
		Dispatch:  func(context.Context, es.Envelope) error { return nil },
		OnApplied: func(_ context.Context, _ es.Envelope, position uint64) { got = append(got, position) },
	}
	if err := r.Apply(ctx, []es.Envelope{
		{GlobalPosition: 4},
		{GlobalPosition: 5},
		{GlobalPosition: 6},
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{6}) {
		t.Fatalf("OnApplied positions = %v, want only [6]", got)
	}
}

func TestRunnerOnAppliedRunsForFilteredEnvelopeAfterAdvance(t *testing.T) {
	ctx := context.Background()
	m := &savingMarker{}
	var got []uint64
	r := &projection.Runner{
		Name:   "counter.view",
		Marker: m,
		Filter: func(es.Envelope) bool { return false },
		Dispatch: func(context.Context, es.Envelope) error {
			t.Fatal("Dispatch called for a filtered envelope")
			return nil
		},
		OnApplied: func(_ context.Context, e es.Envelope, position uint64) {
			if m.pos != position {
				t.Errorf("OnApplied(%d) while marker is at %d; filtered advance must be durable first", position, m.pos)
			}
			if e.GlobalPosition != position || e.TypeURL != "other.v1.Unrelated" {
				t.Errorf("OnApplied envelope = {position:%d type:%q}, want {position:%d type:%q}", e.GlobalPosition, e.TypeURL, position, "other.v1.Unrelated")
			}
			got = append(got, position)
		},
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 7, TypeURL: "other.v1.Unrelated"}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{7}) {
		t.Fatalf("OnApplied positions = %v, want [7]", got)
	}
}

func TestRunnerOnAppliedNotCalledWhenDispatchFails(t *testing.T) {
	ctx := context.Background()
	m := &savingMarker{}
	var got []uint64
	r := &projection.Runner{
		Name:      "counter.view",
		Marker:    m,
		Dispatch:  func(context.Context, es.Envelope) error { return errors.New("boom") },
		OnApplied: func(_ context.Context, _ es.Envelope, position uint64) { got = append(got, position) },
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 1}}); err == nil {
		t.Fatal("Apply = nil, want the dispatch error")
	}
	if len(got) != 0 {
		t.Fatalf("OnApplied = %v, want none", got)
	}
	if len(m.saves) != 0 {
		t.Fatalf("marker saves = %v, want none", m.saves)
	}
}

func TestRunnerOnAppliedNotCalledWhenMarkerSaveFails(t *testing.T) {
	ctx := context.Background()
	m := &failingMarker{}
	var got []uint64
	r := &projection.Runner{
		Name:      "counter.view",
		Marker:    m,
		Dispatch:  func(context.Context, es.Envelope) error { return nil },
		OnApplied: func(_ context.Context, _ es.Envelope, position uint64) { got = append(got, position) },
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 1}}); err == nil {
		t.Fatal("Apply = nil, want the marker save error")
	}
	if len(got) != 0 {
		t.Fatalf("OnApplied = %v, want none", got)
	}
}

func TestRunnerOnAppliedWithBufferedMarkerSeesUndurablePosition(t *testing.T) {
	ctx := context.Background()
	inner := &savingMarker{}
	buffered := projection.NewBufferedMarker(inner, projection.BufferedMarkerOptions{
		MaxCount:    100,
		MaxInterval: time.Hour,
	})
	defer buffered.Close()

	var got []uint64
	r := &projection.Runner{
		Name:     "counter.view",
		Marker:   buffered,
		Dispatch: func(context.Context, es.Envelope) error { return nil },
		OnApplied: func(_ context.Context, _ es.Envelope, position uint64) {
			got = append(got, position)
			if inner.pos != 0 {
				t.Errorf("inner marker = %d, want 0: a buffered Save is not durable when OnApplied runs", inner.pos)
			}
		},
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 1, TypeURL: "counter.v1.Incremented"}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("OnApplied positions = %v, want [1]", got)
	}
}

func TestRunnerNilOnAppliedIsUnchanged(t *testing.T) {
	ctx := context.Background()
	var seen []uint64
	r := &projection.Runner{
		Name:     "counter.view",
		Dispatch: func(_ context.Context, e es.Envelope) error { seen = append(seen, e.GlobalPosition); return nil },
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 1}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{1}) {
		t.Fatalf("dispatched %v, want [1]", seen)
	}
}
