package projection_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/projection"
)

func TestMemoryProgressStoreSaveLoad(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	want := projection.Checkpoint{
		Scope:      "workspace-a",
		Projection: "counter",
		Position:   42,
		State:      projection.StatusLive,
	}
	if err := store.SaveProgress(ctx, "workspace-a", "counter", want); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, found, err := store.LoadProgress(ctx, "workspace-a", "counter")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true")
	}
	if got.Scope != want.Scope || got.Projection != want.Projection {
		t.Errorf("key = (%q, %q), want (%q, %q)", got.Scope, got.Projection, want.Scope, want.Projection)
	}
	if got.Position != want.Position {
		t.Errorf("position = %d, want %d", got.Position, want.Position)
	}
	if got.State != want.State {
		t.Errorf("state = %v, want %v", got.State, want.State)
	}
	if got.UpdatedAt.IsZero() {
		t.Error("UpdatedAt is zero")
	}
	if got.UpdatedAt.Location() != time.UTC {
		t.Errorf("UpdatedAt location = %v, want UTC", got.UpdatedAt.Location())
	}
}

func TestMemoryProgressStoreLowerPositionInSameStateKeepsHigher(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	saveCheckpoint(t, store, "s", "p", 10, projection.StatusLive)
	first, _, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	saveCheckpoint(t, store, "s", "p", 4, projection.StatusLive)

	got, _, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Position != 10 {
		t.Errorf("position = %d, want 10 (never backwards in the same state)", got.Position)
	}
	if !got.UpdatedAt.After(first.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want after %v (a successful save refreshes it)", got.UpdatedAt, first.UpdatedAt)
	}
}

func TestMemoryProgressStoreHigherPositionAdvances(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	saveCheckpoint(t, store, "s", "p", 4, projection.StatusLive)
	saveCheckpoint(t, store, "s", "p", 10, projection.StatusLive)

	got, _, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Position != 10 {
		t.Errorf("position = %d, want 10", got.Position)
	}
}

func TestMemoryProgressStoreStateChangeReplacesPosition(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	saveCheckpoint(t, store, "s", "p", 10, projection.StatusLive)
	saveCheckpoint(t, store, "s", "p", 3, projection.StatusRebuilding)

	got, _, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Position != 3 {
		t.Errorf("position = %d, want 3 (a state change replaces the position)", got.Position)
	}
	if got.State != projection.StatusRebuilding {
		t.Errorf("state = %v, want %v", got.State, projection.StatusRebuilding)
	}
}

func TestMemoryProgressStoreResetProgress(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	saveCheckpoint(t, store, "s", "p", 10, projection.StatusLive)

	cp, err := store.ResetProgress(ctx, "s", "p", projection.StatusRebuilding)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if cp.Position != 0 {
		t.Errorf("returned position = %d, want 0", cp.Position)
	}
	if cp.State != projection.StatusRebuilding {
		t.Errorf("returned state = %v, want %v", cp.State, projection.StatusRebuilding)
	}

	got, found, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !found {
		t.Fatal("found = false after reset, want true")
	}
	if got.Position != 0 || got.State != projection.StatusRebuilding {
		t.Errorf("stored = (%d, %v), want (0, %v)", got.Position, got.State, projection.StatusRebuilding)
	}
}

func TestMemoryProgressStoreSetStatusKeepsPosition(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	saveCheckpoint(t, store, "s", "p", 7, projection.StatusLive)
	before, _, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	if err := store.SetStatus(ctx, "s", "p", projection.StatusStalled); err != nil {
		t.Fatalf("set status: %v", err)
	}

	got, _, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Position != 7 {
		t.Errorf("position = %d, want 7 (SetStatus never moves the position)", got.Position)
	}
	if got.State != projection.StatusStalled {
		t.Errorf("state = %v, want %v", got.State, projection.StatusStalled)
	}
	if !got.UpdatedAt.After(before.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want after %v", got.UpdatedAt, before.UpdatedAt)
	}
}

func TestMemoryProgressStoreSetStatusCreatesAbsentRow(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	if err := store.SetStatus(ctx, "s", "p", projection.StatusStalled); err != nil {
		t.Fatalf("set status: %v", err)
	}

	got, found, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !found {
		t.Fatal("found = false, want a zero-position row")
	}
	if got.Position != 0 {
		t.Errorf("position = %d, want 0", got.Position)
	}
	if got.State != projection.StatusStalled {
		t.Errorf("state = %v, want %v", got.State, projection.StatusStalled)
	}
}

func TestMemoryProgressStoreScopesAreIsolated(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	saveCheckpoint(t, store, "workspace-a", "counter", 1, projection.StatusLive)
	saveCheckpoint(t, store, "workspace-b", "counter", 99, projection.StatusRebuilding)

	a, _, err := store.LoadProgress(ctx, "workspace-a", "counter")
	if err != nil {
		t.Fatalf("load a: %v", err)
	}
	b, _, err := store.LoadProgress(ctx, "workspace-b", "counter")
	if err != nil {
		t.Fatalf("load b: %v", err)
	}
	if a.Position != 1 || a.State != projection.StatusLive {
		t.Errorf("scope a = (%d, %v), want (1, %v)", a.Position, a.State, projection.StatusLive)
	}
	if b.Position != 99 || b.State != projection.StatusRebuilding {
		t.Errorf("scope b = (%d, %v), want (99, %v)", b.Position, b.State, projection.StatusRebuilding)
	}
}

func TestMemoryProgressStoreLoadAbsent(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	got, found, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if found {
		t.Error("found = true, want false")
	}
	if got != (projection.Checkpoint{}) {
		t.Errorf("checkpoint = %+v, want the zero value", got)
	}
}

func TestMemoryProgressStoreConcurrentSaves(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemoryProgressStore()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				pos := uint64(g*100 + i + 1)
				if err := store.SaveProgress(ctx, "s", "p", projection.Checkpoint{
					Position: pos,
					State:    projection.StatusLive,
				}); err != nil {
					t.Errorf("save %d: %v", pos, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got, _, err := store.LoadProgress(ctx, "s", "p")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Position != 800 {
		t.Errorf("position = %d, want 800", got.Position)
	}
}

func TestStatusStringRoundTrip(t *testing.T) {
	tests := []struct {
		status projection.Status
		text   string
	}{
		{projection.StatusUnknown, "unknown"},
		{projection.StatusLive, "live"},
		{projection.StatusRebuilding, "rebuilding"},
		{projection.StatusStalled, "stalled"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.text {
			t.Errorf("%d.String() = %q, want %q", tt.status, got, tt.text)
		}
		got, ok := projection.ParseStatus(tt.text)
		if !ok {
			t.Errorf("ParseStatus(%q) not ok", tt.text)
			continue
		}
		if got != tt.status {
			t.Errorf("ParseStatus(%q) = %v, want %v", tt.text, got, tt.status)
		}
	}
	if _, ok := projection.ParseStatus("bogus"); ok {
		t.Error("ParseStatus(\"bogus\") ok = true, want false")
	}
}

func saveCheckpoint(t *testing.T, store projection.ProgressStore, scope, proj string, pos uint64, state projection.Status) {
	t.Helper()
	if err := store.SaveProgress(context.Background(), scope, proj, projection.Checkpoint{
		Position: pos,
		State:    state,
	}); err != nil {
		t.Fatalf("save (%d, %v): %v", pos, state, err)
	}
}
