package projection

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
)

// rebuildReadModel is the fake read model the rebuild tests clear between
// Reset and the replay. Dispatch appends the envelope payload so a test can
// tell which events were applied.
type rebuildReadModel struct {
	rows []string
}

func (m *rebuildReadModel) apply(_ context.Context, e es.Envelope) error {
	m.rows = append(m.rows, string(e.Payload))
	return nil
}

func (m *rebuildReadModel) clear() { m.rows = nil }

// TestRebuildProgressStatusGate isolates the state gate from the position
// gate: the checkpoint is advanced above the token while the projection is
// still rebuilding, and the read model is cleared. A wait for that token must
// fail closed on the state even though the position alone would satisfy it.
func TestRebuildProgressStatusGate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryProgressStore()
	waiter := NewWaiter(store)
	defer waiter.Close()
	model := &rebuildReadModel{}
	runner := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      waiter,
		Dispatch:      model.apply,
	}

	if err := runner.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	// Advance the durable checkpoint past the token while the state stays
	// rebuilding, then throw the read model away: this is the dangerous
	// mid-rebuild window a reader must never see as reached.
	if err := runner.Apply(ctx, []es.Envelope{{GlobalPosition: 7, Payload: []byte("stale")}}); err != nil {
		t.Fatalf("Apply during rebuild: %v", err)
	}
	model.clear()

	tok := es.Token{Scope: "ws", Position: 2}
	res, err := waiter.WaitFor(ctx, "ws", "p", tok, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if res.Reached {
		t.Fatalf("WaitFor reached = true at checkpoint %+v during a rebuild", res.Checkpoint)
	}
	if res.Status != StatusRebuilding {
		t.Fatalf("WaitFor status = %v, want %v", res.Status, StatusRebuilding)
	}
	if res.Checkpoint.Position < tok.Position {
		t.Fatalf("checkpoint position = %d, want at or above the token %d so the test isolates the state gate", res.Checkpoint.Position, tok.Position)
	}
	if got := len(model.rows); got != 0 {
		t.Fatalf("read model rows = %d, want 0 after the clear", got)
	}
}

// TestRebuildProgressSuccess pins the happy path: Reset marks the projection
// rebuilding, Rebuild replays a two-event source, the state becomes live and a
// wait for the last applied position is reached over the rebuilt read model.
func TestRebuildProgressSuccess(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryProgressStore()
	waiter := NewWaiter(store)
	defer waiter.Close()
	model := &rebuildReadModel{}
	runner := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      waiter,
		Dispatch:      model.apply,
	}

	if err := runner.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	model.clear()

	src := progressSource{
		{GlobalPosition: 1, Payload: []byte("one")},
		{GlobalPosition: 2, Payload: []byte("two")},
	}
	pos, err := runner.Rebuild(ctx, src, 10)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if pos != 2 {
		t.Fatalf("Rebuild position = %d, want 2", pos)
	}
	if !reflect.DeepEqual(model.rows, []string{"one", "two"}) {
		t.Fatalf("read model = %v, want the two replayed rows", model.rows)
	}

	cp, found, err := store.LoadProgress(ctx, "ws", "p")
	if err != nil {
		t.Fatalf("LoadProgress: %v", err)
	}
	if !found || cp.Position != 2 || cp.State != StatusLive {
		t.Fatalf("checkpoint = (%+v, found=%v), want position 2 state live", cp, found)
	}

	tok := es.Token{Scope: "ws", Position: 2}
	res, err := waiter.WaitFor(ctx, "ws", "p", tok, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if !res.Reached || res.Status != StatusLive {
		t.Fatalf("WaitFor = (reached=%v, status=%v), want reached live", res.Reached, res.Status)
	}
}

// TestResetPublishesReturnedCheckpoint pins the reset/cache asymmetry: Reset
// must publish the checkpoint the store returned (position 0, rebuilding), not
// just the state, or a warm cache keeps its old high position and a rebuild
// below it returns a false "reached".
func TestResetPublishesReturnedCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryProgressStore()
	waiter := NewWaiter(store)
	defer waiter.Close()
	model := &rebuildReadModel{}
	runner := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      waiter,
		Dispatch:      model.apply,
	}

	// Warm the cache at 100/live for the same key.
	waiter.Advance(Checkpoint{Scope: "ws", Projection: "p", Position: 100, State: StatusLive})
	warm, err := waiter.WaitFor(ctx, "ws", "p", es.Token{Scope: "ws", Position: 100}, 100*time.Millisecond)
	if err != nil || !warm.Reached {
		t.Fatalf("warm wait = (%+v, %v), want reached", warm, err)
	}

	if err := runner.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	model.clear()

	src := progressSource{
		{GlobalPosition: 1, Payload: []byte("one")},
		{GlobalPosition: 2, Payload: []byte("two")},
	}
	if _, err := runner.Rebuild(ctx, src, 10); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	res, err := waiter.WaitFor(ctx, "ws", "p", es.Token{Scope: "ws", Position: 50}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitFor(50): %v", err)
	}
	if res.Reached {
		t.Fatalf("WaitFor(50) reached at %+v after a rebuild at position 2; the reset left a stale 100 cached", res.Checkpoint)
	}

	res, err = waiter.WaitFor(ctx, "ws", "p", es.Token{Scope: "ws", Position: 2}, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitFor(2): %v", err)
	}
	if !res.Reached || res.Status != StatusLive {
		t.Fatalf("WaitFor(2) = (reached=%v, status=%v), want reached live", res.Reached, res.Status)
	}
}

// TestRebuildProgressSourceError pins the fail-closed replay: a source error
// returns the last applied position but leaves the checkpoint rebuilding, so a
// wait for even that position is not reached.
func TestRebuildProgressSourceError(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryProgressStore()
	waiter := NewWaiter(store)
	defer waiter.Close()
	model := &rebuildReadModel{}
	runner := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      waiter,
		Dispatch:      model.apply,
	}

	if err := runner.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	model.clear()

	errSource := errors.New("read failed")
	src := &progressFailingSource{
		pages: [][]es.Envelope{{{GlobalPosition: 1, Payload: []byte("one")}}},
		err:   errSource,
	}
	// Batch 1 keeps the first page full, so Replay asks for another page and
	// the source's error surfaces instead of a short page ending the replay.
	pos, err := runner.Rebuild(ctx, src, 1)
	if !errors.Is(err, errSource) {
		t.Fatalf("Rebuild error = %v, want the source error", err)
	}
	if pos != 1 {
		t.Fatalf("Rebuild position = %d, want the last applied 1", pos)
	}

	cp, found, err := store.LoadProgress(ctx, "ws", "p")
	if err != nil {
		t.Fatalf("LoadProgress: %v", err)
	}
	if !found || cp.State != StatusRebuilding {
		t.Fatalf("checkpoint = (%+v, found=%v), want a rebuilding row after the source error", cp, found)
	}

	tok := es.Token{Scope: "ws", Position: 1}
	res, err := waiter.WaitFor(ctx, "ws", "p", tok, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if res.Reached {
		t.Fatalf("WaitFor reached = true after a failed rebuild: %+v", res.Checkpoint)
	}
	if res.Status != StatusRebuilding {
		t.Fatalf("WaitFor status = %v, want %v", res.Status, StatusRebuilding)
	}
}
