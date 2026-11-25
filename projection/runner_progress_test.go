package projection

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
)

// recordingProgressStore is a ProgressStore that records the checkpoints the
// runner asked it to persist, so a test can count saves and pin the state the
// runner passed. LoadProgress/SetStatus/ResetProgress read and write the
// in-memory store, so the runner sees ordinary store semantics.
type recordingProgressStore struct {
	inner        *MemoryProgressStore
	order        *[]string // optional: appends "save" for ordering assertions
	saves        []Checkpoint
	saveErr      error
	setStatusErr error
}

func newRecordingProgressStore() *recordingProgressStore {
	return &recordingProgressStore{inner: NewMemoryProgressStore()}
}

func (s *recordingProgressStore) LoadProgress(ctx context.Context, scope, projection string) (Checkpoint, bool, error) {
	return s.inner.LoadProgress(ctx, scope, projection)
}

func (s *recordingProgressStore) SaveProgress(ctx context.Context, scope, projection string, cp Checkpoint) error {
	if s.order != nil {
		*s.order = append(*s.order, "save")
	}
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saves = append(s.saves, cp)
	return s.inner.SaveProgress(ctx, scope, projection, cp)
}

func (s *recordingProgressStore) ResetProgress(ctx context.Context, scope, projection string, state Status) (Checkpoint, error) {
	return s.inner.ResetProgress(ctx, scope, projection, state)
}

func (s *recordingProgressStore) SetStatus(ctx context.Context, scope, projection string, state Status) error {
	if s.setStatusErr != nil {
		return s.setStatusErr
	}
	return s.inner.SetStatus(ctx, scope, projection, state)
}

// stored returns the checkpoint the inner store holds, failing when absent.
func (s *recordingProgressStore) stored(t *testing.T, scope, projection string) Checkpoint {
	t.Helper()
	cp, found, err := s.inner.LoadProgress(context.Background(), scope, projection)
	if err != nil {
		t.Fatalf("LoadProgress: %v", err)
	}
	if !found {
		t.Fatalf("no checkpoint for (%q, %q)", scope, projection)
	}
	return cp
}

// statusCall is one SetStatus observation.
type statusCall struct {
	scope      string
	projection string
	state      Status
}

// recordingPublisher records the checkpoint advances and status changes the
// runner publishes to in-process waiters.
type recordingPublisher struct {
	advances []Checkpoint
	statuses []statusCall
	order    *[]string // optional: appends "advance"/"status:<state>"
}

func (p *recordingPublisher) Advance(cp Checkpoint) {
	if p.order != nil {
		*p.order = append(*p.order, "advance")
	}
	p.advances = append(p.advances, cp)
}

func (p *recordingPublisher) SetStatus(scope, projection string, state Status) {
	if p.order != nil {
		*p.order = append(*p.order, "status:"+state.String())
	}
	p.statuses = append(p.statuses, statusCall{scope: scope, projection: projection, state: state})
}

// progressSource serves a fixed slice like sliceSource, but lives in this
// package so the internal tests can use it.
type progressSource []es.Envelope

func (s progressSource) ReadAll(_ context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	var out []es.Envelope
	for _, e := range s {
		if e.GlobalPosition > fromPosition && (limit <= 0 || len(out) < limit) {
			out = append(out, e)
		}
	}
	return out, nil
}

// progressFailingSource serves its pages in order and then returns err instead
// of an empty page, so a Replay fails after applying at least one page.
type progressFailingSource struct {
	pages [][]es.Envelope
	next  int
	err   error
}

func (s *progressFailingSource) ReadAll(context.Context, uint64, int) ([]es.Envelope, error) {
	if s.next < len(s.pages) {
		page := s.pages[s.next]
		s.next++
		return page, nil
	}
	return nil, s.err
}

// progressMarker is a Marker that records every saved position.
type progressMarker struct {
	pos   uint64
	saved []uint64
}

func (m *progressMarker) Load(context.Context, string) (uint64, error) { return m.pos, nil }

func (m *progressMarker) Save(_ context.Context, _ string, position uint64) error {
	m.pos = position
	m.saved = append(m.saved, position)
	return nil
}

// TestRunnerProgressApplySavesThenPublishes pins the store path's order:
// dispatch, save, then publish. Publishing before the write-through save
// returns would let a waiter see a position the store does not hold.
func TestRunnerProgressApplySavesThenPublishes(t *testing.T) {
	ctx := context.Background()
	var order []string
	store := newRecordingProgressStore()
	store.order = &order
	pub := &recordingPublisher{order: &order}
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      pub,
		Dispatch: func(context.Context, es.Envelope) error {
			order = append(order, "dispatch")
			return nil
		},
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 9}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if want := []string{"dispatch", "save", "advance"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v (dispatch, save, publish)", order, want)
	}
	want := Checkpoint{Scope: "ws", Projection: "p", Position: 9, State: StatusLive}
	if len(store.saves) != 1 || store.saves[0] != want {
		t.Fatalf("saves = %+v, want one save of %+v", store.saves, want)
	}
	if len(pub.advances) != 1 || pub.advances[0] != want {
		t.Fatalf("advances = %+v, want one advance of %+v", pub.advances, want)
	}
}

// TestRunnerProgressApplyCoalescesBatch pins batch coalescing: a batch of
// three dispatches persists and publishes once, at the last position, and
// OnApplied still fires once per applied envelope — but only after that one
// save.
func TestRunnerProgressApplyCoalescesBatch(t *testing.T) {
	ctx := context.Background()
	store := newRecordingProgressStore()
	pub := &recordingPublisher{}
	var (
		dispatched  []uint64
		applied     []uint64
		savesAtHook []int
	)
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      pub,
		Dispatch: func(_ context.Context, e es.Envelope) error {
			dispatched = append(dispatched, e.GlobalPosition)
			return nil
		},
		OnApplied: func(_ context.Context, e es.Envelope, position uint64) {
			applied = append(applied, position)
			savesAtHook = append(savesAtHook, len(store.saves))
		},
	}
	if err := r.Apply(ctx, []es.Envelope{
		{GlobalPosition: 1},
		{GlobalPosition: 2},
		{GlobalPosition: 3},
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(dispatched, []uint64{1, 2, 3}) {
		t.Fatalf("dispatched = %v, want [1 2 3]", dispatched)
	}
	if len(store.saves) != 1 {
		t.Fatalf("saves = %d, want exactly 1 for a three-event batch", len(store.saves))
	}
	if store.saves[0].Position != 3 || store.saves[0].State != StatusLive {
		t.Fatalf("save = %+v, want position 3 state live", store.saves[0])
	}
	if len(pub.advances) != 1 || pub.advances[0].Position != 3 {
		t.Fatalf("advances = %+v, want exactly one at position 3", pub.advances)
	}
	if !reflect.DeepEqual(applied, []uint64{1, 2, 3}) {
		t.Fatalf("OnApplied positions = %v, want [1 2 3]", applied)
	}
	for i, n := range savesAtHook {
		if n != 1 {
			t.Fatalf("OnApplied(%d) ran after %d saves, want the single batch save first", applied[i], n)
		}
	}
}

// TestRunnerProgressApplyStopsAtFailingEvent pins the partial-batch rule: a
// dispatch error still persists the last successfully applied position once
// (so a redelivery does not restart the page) and returns the dispatch error;
// the next Apply skips the persisted prefix and retries the failure.
func TestRunnerProgressApplyStopsAtFailingEvent(t *testing.T) {
	ctx := context.Background()
	errDispatch := errors.New("dispatch failed")
	fail := true
	store := newRecordingProgressStore()
	pub := &recordingPublisher{}
	var applied []uint64
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      pub,
		Dispatch: func(_ context.Context, e es.Envelope) error {
			if fail && e.GlobalPosition == 3 {
				return errDispatch
			}
			return nil
		},
		OnApplied: func(_ context.Context, _ es.Envelope, position uint64) {
			applied = append(applied, position)
		},
	}
	batch := []es.Envelope{{GlobalPosition: 1}, {GlobalPosition: 2}, {GlobalPosition: 3}}
	if err := r.Apply(ctx, batch); err != errDispatch {
		t.Fatalf("Apply = %v, want the dispatch error", err)
	}
	if len(store.saves) != 1 || store.saves[0].Position != 2 {
		t.Fatalf("saves = %+v, want one save at the last applied position 2", store.saves)
	}
	if len(pub.advances) != 1 || pub.advances[0].Position != 2 {
		t.Fatalf("advances = %+v, want only position 2", pub.advances)
	}
	if !reflect.DeepEqual(applied, []uint64{1, 2}) {
		t.Fatalf("OnApplied positions = %v, want the applied prefix [1 2]", applied)
	}

	fail = false
	if err := r.Apply(ctx, batch); err != nil {
		t.Fatalf("Apply after the failure: %v", err)
	}
	if !reflect.DeepEqual(store.saves, []Checkpoint{
		{Scope: "ws", Projection: "p", Position: 2, State: StatusLive},
		{Scope: "ws", Projection: "p", Position: 3, State: StatusLive},
	}) {
		t.Fatalf("saves after redelivery = %+v, want position 2 then 3 (the prefix was skipped)", store.saves)
	}
}

// TestRunnerProgressFilteredEventSavesAndPublishes pins the filtered path: an
// envelope no dispatch consumes still advances the persisted checkpoint and
// publishes it, or every reader would wait behind it forever.
func TestRunnerProgressFilteredEventSavesAndPublishes(t *testing.T) {
	ctx := context.Background()
	store := newRecordingProgressStore()
	pub := &recordingPublisher{}
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      pub,
		Filter:        func(es.Envelope) bool { return false },
		Dispatch: func(context.Context, es.Envelope) error {
			t.Fatal("Dispatch called for a filtered envelope")
			return nil
		},
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 7}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	want := Checkpoint{Scope: "ws", Projection: "p", Position: 7, State: StatusLive}
	if len(store.saves) != 1 || store.saves[0] != want {
		t.Fatalf("saves = %+v, want one save of %+v", store.saves, want)
	}
	if len(pub.advances) != 1 || pub.advances[0] != want {
		t.Fatalf("advances = %+v, want one advance of %+v", pub.advances, want)
	}
}

// TestRunnerProgressSaveErrorDoesNotPublish pins the write-through contract: a
// failed save publishes nothing and is returned, and the in-memory marker does
// not advance, so a redelivery dispatches and saves again instead of being
// skipped forever.
func TestRunnerProgressSaveErrorDoesNotPublish(t *testing.T) {
	ctx := context.Background()
	errSave := errors.New("save failed")
	store := newRecordingProgressStore()
	store.saveErr = errSave
	pub := &recordingPublisher{}
	var dispatched []uint64
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      pub,
		Dispatch: func(_ context.Context, e es.Envelope) error {
			dispatched = append(dispatched, e.GlobalPosition)
			return nil
		},
	}
	batch := []es.Envelope{{GlobalPosition: 1}}
	if err := r.Apply(ctx, batch); !errors.Is(err, errSave) {
		t.Fatalf("Apply = %v, want the save error", err)
	}
	if len(pub.advances) != 0 {
		t.Fatalf("advances = %+v, want none before the save returns", pub.advances)
	}
	if pos, err := r.Position(ctx); err != nil || pos != 0 {
		t.Fatalf("Position = (%d, %v), want (0, nil): a failed save must not advance the marker", pos, err)
	}

	store.saveErr = nil
	if err := r.Apply(ctx, batch); err != nil {
		t.Fatalf("Apply after the save failure: %v", err)
	}
	if !reflect.DeepEqual(dispatched, []uint64{1, 1}) {
		t.Fatalf("dispatched = %v, want [1 1] (retried after the failed save)", dispatched)
	}
	if len(store.saves) != 1 || store.saves[0].Position != 1 {
		t.Fatalf("saves = %+v, want one save at 1 after the retry", store.saves)
	}
}

// TestRunnerProgressLoadsStoredPosition pins loadMarker's store path: a found
// scoped checkpoint seeds the runner, so positions at or below it are skipped.
func TestRunnerProgressLoadsStoredPosition(t *testing.T) {
	ctx := context.Background()
	store := newRecordingProgressStore()
	if err := store.inner.SaveProgress(ctx, "ws", "p", Checkpoint{Position: 5, State: StatusLive}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	var seen []uint64
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Dispatch: func(_ context.Context, e es.Envelope) error {
			seen = append(seen, e.GlobalPosition)
			return nil
		},
	}
	if err := r.Apply(ctx, []es.Envelope{
		{GlobalPosition: 4},
		{GlobalPosition: 5},
		{GlobalPosition: 6},
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{6}) {
		t.Fatalf("dispatched = %v, want only 6 (positions <= the stored 5 skipped)", seen)
	}
	if pos, err := r.Position(ctx); err != nil || pos != 6 {
		t.Fatalf("Position = (%d, %v), want (6, nil)", pos, err)
	}
}

// TestRunnerProgressLoadSeedsRebuildingState pins the fail-closed restart: a
// row left rebuilding by a crash mid-rebuild must seed both the position and
// the state, so a fresh runner's first save stays rebuilding instead of
// flipping the row live over a half-rebuilt read model.
func TestRunnerProgressLoadSeedsRebuildingState(t *testing.T) {
	ctx := context.Background()
	store := newRecordingProgressStore()
	if err := store.inner.SaveProgress(ctx, "ws", "p", Checkpoint{Position: 10, State: StatusRebuilding}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Dispatch:      func(context.Context, es.Envelope) error { return nil },
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 11}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(store.saves) != 1 || store.saves[0].State != StatusRebuilding {
		t.Fatalf("saves = %+v, want one save with state %v (seeded from the row)", store.saves, StatusRebuilding)
	}
	if got := store.stored(t, "ws", "p"); got.Position != 11 || got.State != StatusRebuilding {
		t.Fatalf("stored = (%d, %v), want (11, %v)", got.Position, got.State, StatusRebuilding)
	}
}

// TestRunnerProgressApplyDuringRebuildKeepsRebuilding pins Review Focus 3: a
// replay page persists StateRebuilding, so a waiter cannot see "live" over a
// half-rebuilt read model. Only a successful Rebuild flips the state to live.
func TestRunnerProgressApplyDuringRebuildKeepsRebuilding(t *testing.T) {
	ctx := context.Background()
	store := newRecordingProgressStore()
	pub := &recordingPublisher{}
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      pub,
		Dispatch:      func(context.Context, es.Envelope) error { return nil },
	}
	if err := r.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 5}}); err != nil {
		t.Fatalf("Apply while rebuilding: %v", err)
	}
	if got := store.stored(t, "ws", "p"); got.Position != 5 || got.State != StatusRebuilding {
		t.Fatalf("stored = (%d, %v), want (5, %v)", got.Position, got.State, StatusRebuilding)
	}
	if len(pub.advances) != 2 || pub.advances[0].Position != 0 || pub.advances[0].State != StatusRebuilding {
		t.Fatalf("advances = %+v, want the reset checkpoint (0, rebuilding) then the apply", pub.advances)
	}
	if last := pub.advances[len(pub.advances)-1]; last.Position != 5 || last.State != StatusRebuilding {
		t.Fatalf("last advance = %+v, want (5, rebuilding)", last)
	}

	if _, err := r.Rebuild(ctx, progressSource{}, 10); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if got := store.stored(t, "ws", "p"); got.State != StatusLive {
		t.Fatalf("state after Rebuild = %v, want %v", got.State, StatusLive)
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 6}}); err != nil {
		t.Fatalf("Apply after Rebuild: %v", err)
	}
	if got := store.stored(t, "ws", "p"); got.Position != 6 || got.State != StatusLive {
		t.Fatalf("stored = (%d, %v), want (6, %v) after the live apply", got.Position, got.State, StatusLive)
	}
}

// TestRunnerResetSetsPositionStateAndStatus pins Reset's store path: it replaces
// the row with position 0 / rebuilding, clears the runner's cached marker so
// nothing below the old position is skipped, and publishes the checkpoint the
// store returned (position and state).
func TestRunnerResetSetsPositionStateAndStatus(t *testing.T) {
	ctx := context.Background()
	store := newRecordingProgressStore()
	if err := store.inner.SaveProgress(ctx, "ws", "p", Checkpoint{Position: 10, State: StatusLive}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	pub := &recordingPublisher{}
	var seen []uint64
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      pub,
		Dispatch: func(_ context.Context, e es.Envelope) error {
			seen = append(seen, e.GlobalPosition)
			return nil
		},
	}
	if pos, err := r.Position(ctx); err != nil || pos != 10 {
		t.Fatalf("Position before Reset = (%d, %v), want (10, nil)", pos, err)
	}
	if err := r.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if got := store.stored(t, "ws", "p"); got.Position != 0 || got.State != StatusRebuilding {
		t.Fatalf("stored after Reset = (%d, %v), want (0, %v)", got.Position, got.State, StatusRebuilding)
	}
	if len(pub.advances) != 1 || pub.advances[0].Scope != "ws" || pub.advances[0].Projection != "p" ||
		pub.advances[0].Position != 0 || pub.advances[0].State != StatusRebuilding {
		t.Fatalf("published advances = %+v, want one (ws, p, 0, rebuilding): Reset must publish the returned checkpoint, not just the state", pub.advances)
	}
	if len(pub.statuses) != 0 {
		t.Fatalf("published statuses = %+v, want none from the store path", pub.statuses)
	}
	if r.marker != 0 || !r.loaded || r.state != StatusRebuilding {
		t.Fatalf("runner after Reset = {marker:%d loaded:%v state:%v}, want {0 true rebuilding}", r.marker, r.loaded, r.state)
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 3}}); err != nil {
		t.Fatalf("Apply after Reset: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{3}) {
		t.Fatalf("dispatched = %v, want [3]: the cached marker must be cleared", seen)
	}
	if got := store.stored(t, "ws", "p"); got.Position != 3 || got.State != StatusRebuilding {
		t.Fatalf("stored after Apply = (%d, %v), want (3, %v)", got.Position, got.State, StatusRebuilding)
	}
}

// TestRunnerResetLegacyMarkerPath pins the legacy reset: without a store, Reset
// discards any buffered pending position before saving 0, so the pre-reset
// position cannot flush after the reset and resurrect the marker.
func TestRunnerResetLegacyMarkerPath(t *testing.T) {
	ctx := context.Background()
	inner := &progressMarker{}
	buffered := NewBufferedMarker(inner, BufferedMarkerOptions{MaxCount: 1000, MaxInterval: time.Hour})
	r := &Runner{
		Name:     "p",
		Marker:   buffered,
		Dispatch: func(context.Context, es.Envelope) error { return nil },
	}
	if err := r.Apply(ctx, []es.Envelope{{GlobalPosition: 42}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(inner.saved) != 0 {
		t.Fatalf("inner saves = %v before Reset, want none (the position is buffered)", inner.saved)
	}
	if err := r.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := buffered.Flush(ctx); err != nil {
		t.Fatalf("Flush after Reset: %v", err)
	}
	for _, pos := range inner.saved {
		if pos == 42 {
			t.Fatalf("inner saves = %v, want the pre-reset 42 discarded, not flushed after Reset", inner.saved)
		}
	}
	if pos, err := r.Position(ctx); err != nil || pos != 0 {
		t.Fatalf("Position after Reset = (%d, %v), want (0, nil)", pos, err)
	}
}

// TestRunnerRebuildStatus pins Rebuild's live transition: every page of a
// successful replay persists rebuilding, and only the completed Rebuild
// publishes live. A source error leaves the projection rebuilding.
func TestRunnerRebuildStatus(t *testing.T) {
	ctx := context.Background()
	src := progressSource{
		{GlobalPosition: 1},
		{GlobalPosition: 2},
	}

	t.Run("success", func(t *testing.T) {
		store := newRecordingProgressStore()
		pub := &recordingPublisher{}
		r := &Runner{
			Name:          "p",
			Scope:         "ws",
			ProgressStore: store,
			Progress:      pub,
			Dispatch:      func(context.Context, es.Envelope) error { return nil },
		}
		if err := r.Reset(ctx); err != nil {
			t.Fatalf("Reset: %v", err)
		}
		pos, err := r.Rebuild(ctx, src, 1)
		if err != nil || pos != 2 {
			t.Fatalf("Rebuild = (%d, %v), want (2, nil)", pos, err)
		}
		if len(store.saves) != 2 {
			t.Fatalf("saves = %+v, want one per page (2)", store.saves)
		}
		for i, cp := range store.saves {
			if cp.State != StatusRebuilding {
				t.Fatalf("save %d state = %v, want %v during the replay", i, cp.State, StatusRebuilding)
			}
		}
		for i, cp := range pub.advances {
			if cp.State != StatusRebuilding {
				t.Fatalf("advance %d state = %v, want %v (never live mid-rebuild)", i, cp.State, StatusRebuilding)
			}
		}
		if got := store.stored(t, "ws", "p"); got.Position != 2 || got.State != StatusLive {
			t.Fatalf("stored after Rebuild = (%d, %v), want (2, %v)", got.Position, got.State, StatusLive)
		}
		if want := []statusCall{
			{scope: "ws", projection: "p", state: StatusLive},
		}; !reflect.DeepEqual(pub.statuses, want) {
			t.Fatalf("published statuses = %+v, want %+v (Reset publishes an advance, not a status)", pub.statuses, want)
		}
	})

	t.Run("source error", func(t *testing.T) {
		errSource := errors.New("read failed")
		store := newRecordingProgressStore()
		pub := &recordingPublisher{}
		r := &Runner{
			Name:          "p",
			Scope:         "ws",
			ProgressStore: store,
			Progress:      pub,
			Dispatch:      func(context.Context, es.Envelope) error { return nil },
		}
		if err := r.Reset(ctx); err != nil {
			t.Fatalf("Reset: %v", err)
		}
		failing := &progressFailingSource{
			pages: [][]es.Envelope{{{GlobalPosition: 1}}},
			err:   errSource,
		}
		pos, err := r.Rebuild(ctx, failing, 1)
		if !errors.Is(err, errSource) {
			t.Fatalf("Rebuild error = %v, want the source error", err)
		}
		if pos != 1 {
			t.Fatalf("Rebuild position = %d, want the last applied 1", pos)
		}
		if got := store.stored(t, "ws", "p"); got.State != StatusRebuilding {
			t.Fatalf("state after a failed Rebuild = %v, want %v", got.State, StatusRebuilding)
		}
		for _, s := range pub.statuses {
			if s.state == StatusLive {
				t.Fatalf("published statuses = %+v, want no live after a failed Rebuild", pub.statuses)
			}
		}
	})
}

// TestRunnerRebuildStatusSetStatusErrorKeepsRebuilding pins the fail-closed
// live transition: when the store cannot record live, Rebuild returns the
// error and both the row and the runner's in-memory state stay rebuilding, so
// the next advance cannot write live over a half-gated rebuild.
func TestRunnerRebuildStatusSetStatusErrorKeepsRebuilding(t *testing.T) {
	ctx := context.Background()
	errStatus := errors.New("status write failed")
	store := newRecordingProgressStore()
	pub := &recordingPublisher{}
	r := &Runner{
		Name:          "p",
		Scope:         "ws",
		ProgressStore: store,
		Progress:      pub,
		Dispatch:      func(context.Context, es.Envelope) error { return nil },
	}
	if err := r.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	store.setStatusErr = errStatus
	pos, err := r.Rebuild(ctx, progressSource{{GlobalPosition: 1}}, 10)
	if !errors.Is(err, errStatus) {
		t.Fatalf("Rebuild error = %v, want the status store error", err)
	}
	if pos != 1 {
		t.Fatalf("Rebuild position = %d, want the applied 1", pos)
	}
	if got := store.stored(t, "ws", "p"); got.State != StatusRebuilding {
		t.Fatalf("stored state = %v, want %v after the failed live write", got.State, StatusRebuilding)
	}
	if r.state != StatusRebuilding {
		t.Fatalf("runner state = %v, want %v after the failed live write", r.state, StatusRebuilding)
	}
	for _, s := range pub.statuses {
		if s.state == StatusLive {
			t.Fatalf("published statuses = %+v, want no live after the failed live write", pub.statuses)
		}
	}
}

// TestRunnerProgressLegacyMarkerPath pins that a nil ProgressStore keeps the
// legacy Marker path working, coalesced the same way: one save at the last
// position of the batch.
func TestRunnerProgressLegacyMarkerPath(t *testing.T) {
	ctx := context.Background()
	m := &progressMarker{}
	r := &Runner{
		Name:     "p",
		Marker:   m,
		Dispatch: func(context.Context, es.Envelope) error { return nil },
	}
	if err := r.Apply(ctx, []es.Envelope{
		{GlobalPosition: 1},
		{GlobalPosition: 2},
		{GlobalPosition: 3},
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(m.saved, []uint64{3}) {
		t.Fatalf("marker saves = %v, want one save at 3", m.saved)
	}
	if pos, err := r.Position(ctx); err != nil || pos != 3 {
		t.Fatalf("Position = (%d, %v), want (3, nil)", pos, err)
	}
}
