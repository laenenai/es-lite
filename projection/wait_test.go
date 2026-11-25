package projection

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"go.uber.org/goleak"
)

// countingProgressStore wraps MemoryProgressStore and counts LoadProgress
// calls, optionally per scope and optionally failing them. It exists so the
// waiter tests can prove that the shared refresher reads once per tick rather
// than once per waiter, and that hints with no local waiters read nothing.
type countingProgressStore struct {
	mu      sync.Mutex
	inner   *MemoryProgressStore
	loads   int
	byScope map[string]int
	loadErr error
}

func newCountingProgressStore() *countingProgressStore {
	return &countingProgressStore{
		inner:   NewMemoryProgressStore(),
		byScope: make(map[string]int),
	}
}

func (s *countingProgressStore) LoadProgress(ctx context.Context, scope, projection string) (Checkpoint, bool, error) {
	s.mu.Lock()
	s.loads++
	s.byScope[scope]++
	err := s.loadErr
	s.mu.Unlock()
	if err != nil {
		return Checkpoint{}, false, err
	}
	return s.inner.LoadProgress(ctx, scope, projection)
}

func (s *countingProgressStore) SaveProgress(ctx context.Context, scope, projection string, cp Checkpoint) error {
	return s.inner.SaveProgress(ctx, scope, projection, cp)
}

func (s *countingProgressStore) ResetProgress(ctx context.Context, scope, projection string, state Status) (Checkpoint, error) {
	return s.inner.ResetProgress(ctx, scope, projection, state)
}

func (s *countingProgressStore) SetStatus(ctx context.Context, scope, projection string, state Status) error {
	return s.inner.SetStatus(ctx, scope, projection, state)
}

func (s *countingProgressStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loads
}

func (s *countingProgressStore) countFor(scope string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byScope[scope]
}

func (s *countingProgressStore) setLoadErr(err error) {
	s.mu.Lock()
	s.loadErr = err
	s.mu.Unlock()
}

// gatedLoadStore blocks the next LoadProgress until release closes, after
// signalling loaded. It makes the register/re-check race deterministic: the
// test advances the cache while the fast-path store read is in flight.
type gatedLoadStore struct {
	inner *MemoryProgressStore

	mu      sync.Mutex
	loaded  chan struct{}
	release chan struct{}
}

func newGatedLoadStore() *gatedLoadStore {
	return &gatedLoadStore{inner: NewMemoryProgressStore()}
}

func (s *gatedLoadStore) gate(loaded, release chan struct{}) {
	s.mu.Lock()
	s.loaded, s.release = loaded, release
	s.mu.Unlock()
}

func (s *gatedLoadStore) LoadProgress(ctx context.Context, scope, projection string) (Checkpoint, bool, error) {
	s.mu.Lock()
	loaded, release := s.loaded, s.release
	s.loaded, s.release = nil, nil
	s.mu.Unlock()
	if loaded != nil {
		close(loaded)
		<-release
	}
	return s.inner.LoadProgress(ctx, scope, projection)
}

func (s *gatedLoadStore) SaveProgress(ctx context.Context, scope, projection string, cp Checkpoint) error {
	return s.inner.SaveProgress(ctx, scope, projection, cp)
}

func (s *gatedLoadStore) ResetProgress(ctx context.Context, scope, projection string, state Status) (Checkpoint, error) {
	return s.inner.ResetProgress(ctx, scope, projection, state)
}

func (s *gatedLoadStore) SetStatus(ctx context.Context, scope, projection string, state Status) error {
	return s.inner.SetStatus(ctx, scope, projection, state)
}

// ctxCancelStore blocks its one LoadProgress until the caller's context ends
// and then returns that context error, emulating a ctx-aware database driver.
type ctxCancelStore struct {
	*MemoryProgressStore
	started chan struct{}
}

func newCtxCancelStore() *ctxCancelStore {
	return &ctxCancelStore{
		MemoryProgressStore: NewMemoryProgressStore(),
		started:             make(chan struct{}),
	}
}

func (s *ctxCancelStore) LoadProgress(ctx context.Context, scope, projection string) (Checkpoint, bool, error) {
	close(s.started)
	<-ctx.Done()
	return Checkpoint{}, false, ctx.Err()
}

// blockingReadStore blocks every LoadProgress until its context ends and then
// returns that context error, emulating a hung store driver. A one-second
// failsafe surfaces the unbounded read as an error instead of hanging the test
// forever when the waiter does not bound the read itself.
type blockingReadStore struct {
	*MemoryProgressStore
	failsafe time.Duration
}

func (s *blockingReadStore) LoadProgress(ctx context.Context, _ string, _ string) (Checkpoint, bool, error) {
	timer := time.NewTimer(s.failsafe)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Checkpoint{}, false, ctx.Err()
	case <-timer.C:
		return Checkpoint{}, false, errors.New("LoadProgress was not bounded by the wait deadline")
	}
}

// deadlineAdvanceStore returns position 0 on the first read and the token's
// position on every later read, so a wait can only succeed via the final read
// at its deadline.
type deadlineAdvanceStore struct {
	*MemoryProgressStore

	mu    sync.Mutex
	loads int
}

func (s *deadlineAdvanceStore) LoadProgress(_ context.Context, scope, projection string) (Checkpoint, bool, error) {
	s.mu.Lock()
	s.loads++
	n := s.loads
	s.mu.Unlock()
	position := uint64(7)
	if n == 1 {
		position = 0
	}
	return Checkpoint{Scope: scope, Projection: projection, Position: position, State: StatusLive}, true, nil
}

func (s *deadlineAdvanceStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loads
}

// alwaysFoundStore returns a live checkpoint for every key and counts reads
// per projection, so the hub-bound test can prove an evicted key is re-read
// without materialising thousands of rows.
type alwaysFoundStore struct {
	mu       sync.Mutex
	position uint64
	loads    map[string]int
}

func newAlwaysFoundStore(position uint64) *alwaysFoundStore {
	return &alwaysFoundStore{position: position, loads: make(map[string]int)}
}

func (s *alwaysFoundStore) LoadProgress(_ context.Context, scope, projection string) (Checkpoint, bool, error) {
	s.mu.Lock()
	s.loads[projection]++
	s.mu.Unlock()
	return Checkpoint{Scope: scope, Projection: projection, Position: s.position, State: StatusLive}, true, nil
}

func (s *alwaysFoundStore) SaveProgress(context.Context, string, string, Checkpoint) error {
	return nil
}

func (s *alwaysFoundStore) ResetProgress(_ context.Context, scope, projection string, state Status) (Checkpoint, error) {
	return Checkpoint{Scope: scope, Projection: projection, State: state}, nil
}

func (s *alwaysFoundStore) SetStatus(context.Context, string, string, Status) error { return nil }

func (s *alwaysFoundStore) loadsFor(projection string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loads[projection]
}

// scriptedLoadStore returns scripted checkpoints in call order and lets the
// test hold each read open, so two cold reads can be installed in a chosen
// order.
type scriptedLoadStore struct {
	*MemoryProgressStore

	mu       sync.Mutex
	results  []Checkpoint
	calls    int
	started  chan int
	releases []chan struct{}
}

func newScriptedLoadStore(results ...Checkpoint) *scriptedLoadStore {
	s := &scriptedLoadStore{
		MemoryProgressStore: NewMemoryProgressStore(),
		results:             results,
		started:             make(chan int, len(results)),
		releases:            make([]chan struct{}, len(results)),
	}
	for i := range s.releases {
		s.releases[i] = make(chan struct{})
	}
	return s
}

func (s *scriptedLoadStore) LoadProgress(_ context.Context, scope, projection string) (Checkpoint, bool, error) {
	s.mu.Lock()
	i := s.calls
	s.calls++
	s.mu.Unlock()
	if i >= len(s.results) {
		return Checkpoint{}, false, nil
	}
	s.started <- i
	<-s.releases[i]
	cp := s.results[i]
	cp.Scope, cp.Projection = scope, projection
	return cp, true, nil
}

func mustSave(t *testing.T, store ProgressStore, scope, projection string, position uint64, state Status) {
	t.Helper()
	if err := store.SaveProgress(context.Background(), scope, projection, Checkpoint{
		Scope:      scope,
		Projection: projection,
		Position:   position,
		State:      state,
	}); err != nil {
		t.Fatalf("save progress: %v", err)
	}
}

// newTestWaiter closes the waiter at the end of the test so no refresher
// goroutine outlives it.
func newTestWaiter(t *testing.T, store ProgressStore, opts ...WaiterOption) *Waiter {
	t.Helper()
	w := NewWaiter(store, opts...)
	t.Cleanup(w.Close)
	return w
}

// waitRegistered waits until n waiters are registered on one key.
func waitRegistered(t *testing.T, w *Waiter, scope, projection string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		got := 0
		if k := w.keys[progressKey{scope: scope, projection: projection}]; k != nil {
			got = k.waiters
		}
		w.mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiters on (%q, %q) = %d, want %d", scope, projection, got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitTotal waits until n waiters are registered across every key.
func waitTotal(t *testing.T, w *Waiter, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		got := w.total
		w.mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("total waiters = %d, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitCachePosition waits until the hub's cached checkpoint for the key has
// the wanted position.
func waitCachePosition(t *testing.T, w *Waiter, scope, projection string, position uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		var got uint64
		var found bool
		if k := w.keys[progressKey{scope: scope, projection: projection}]; k != nil {
			got, found = k.cp.Position, k.found
		}
		w.mu.Unlock()
		if found && got == position {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cached position on (%q, %q) = %d (found %v), want %d", scope, projection, got, found, position)
		}
		time.Sleep(time.Millisecond)
	}
}

type waitOutcome struct {
	result WaitResult
	err    error
}

func startWait(ctx context.Context, w *Waiter, scope, projection string, tok es.Token, timeout time.Duration) <-chan waitOutcome {
	ch := make(chan waitOutcome, 1)
	go func() {
		res, err := w.WaitFor(ctx, scope, projection, tok, timeout)
		ch <- waitOutcome{result: res, err: err}
	}()
	return ch
}

func await(t *testing.T, ch <-chan waitOutcome, d time.Duration) waitOutcome {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(d):
		t.Fatalf("WaitFor did not return within %v", d)
		return waitOutcome{}
	}
}

func token(scope string, position uint64) es.Token {
	return es.Token{Scope: scope, Position: position}
}

func TestWaiterZeroToken(t *testing.T) {
	store := newCountingProgressStore()
	w := newTestWaiter(t, store)

	res, err := w.WaitFor(context.Background(), "ws", "users", es.Token{}, time.Second)
	if err != nil {
		t.Fatalf("WaitFor(zero) error = %v", err)
	}
	if !res.Reached {
		t.Fatalf("WaitFor(zero) Reached = false, want true")
	}
	if got := store.count(); got != 0 {
		t.Fatalf("store reads = %d, want 0 for a zero token", got)
	}
}

func TestWaiterAlreadySatisfied(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 10, StatusLive)
	w := newTestWaiter(t, store)

	res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 10), 50*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitFor error = %v", err)
	}
	if !res.Reached {
		t.Fatalf("Reached = false, want true")
	}
	if res.Status != StatusLive {
		t.Fatalf("Status = %v, want live", res.Status)
	}
	if res.Checkpoint.Position != 10 {
		t.Fatalf("position = %d, want 10", res.Checkpoint.Position)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("store reads = %d, want 1", got)
	}

	// A warm cache must answer without another store read.
	res, err = w.WaitFor(context.Background(), "ws", "users", token("ws", 9), 50*time.Millisecond)
	if err != nil || !res.Reached {
		t.Fatalf("warm WaitFor = (%+v, %v), want reached", res, err)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("store reads after warm hit = %d, want still 1", got)
	}
}

func TestWaiterAdvanceReleasesButLowerDoesNot(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 0, StatusLive)
	w := newTestWaiter(t, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := startWait(ctx, w, "ws", "users", token("ws", 5), 2*time.Second)
	waitRegistered(t, w, "ws", "users", 1)

	w.Advance(Checkpoint{Scope: "ws", Projection: "users", Position: 3, State: StatusLive})
	select {
	case o := <-ch:
		t.Fatalf("wait released by an advance below the token: %+v", o)
	case <-time.After(25 * time.Millisecond):
	}

	mustSave(t, store, "ws", "users", 5, StatusLive)
	w.Advance(Checkpoint{Scope: "ws", Projection: "users", Position: 5, State: StatusLive})
	o := await(t, ch, time.Second)
	if o.err != nil || !o.result.Reached {
		t.Fatalf("after advance = (%+v, %v), want reached", o.result, o.err)
	}
	if o.result.Checkpoint.Position != 5 {
		t.Fatalf("reached position = %d, want 5", o.result.Checkpoint.Position)
	}
}

func TestWaiterRegisterRecheckRace(t *testing.T) {
	store := newGatedLoadStore()
	for i := 0; i < 1000; i++ {
		loaded := make(chan struct{})
		release := make(chan struct{})
		store.gate(loaded, release)

		w := NewWaiter(store, WithFallback(time.Hour))
		ctx, cancel := context.WithCancel(context.Background())
		ch := startWait(ctx, w, "ws", "users", token("ws", 10), 2*time.Second)

		select {
		case <-loaded:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: LoadProgress was not called", i)
		}

		// The advance lands after the fast-path read started and before the
		// waiter registers. The re-check after registration must catch it.
		w.Advance(Checkpoint{Scope: "ws", Projection: "users", Position: 10, State: StatusLive})
		close(release)

		o := await(t, ch, time.Second)
		if o.err != nil || !o.result.Reached {
			t.Fatalf("iteration %d: = (%+v, %v), want reached", i, o.result, o.err)
		}
		cancel()
		w.Close()
	}
}

func TestWaiterTimeout(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 0, StatusLive)
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	const timeout = 20 * time.Millisecond
	start := time.Now()
	res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 5), timeout)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("timeout error = %v, want nil", err)
	}
	if res.Reached {
		t.Fatalf("Reached = true, want false on timeout")
	}
	if res.Waited < timeout {
		t.Fatalf("Waited = %v, want at least %v", res.Waited, timeout)
	}
	if elapsed < timeout {
		t.Fatalf("elapsed = %v, want at least %v", elapsed, timeout)
	}
}

// TestWaiterColdReadBoundedByDeadline pins I2: a hung cold read is bounded by
// the wait deadline, and its expiry is a timeout result (Reached:false, nil),
// not a store error, even without WithBestEffort.
func TestWaiterColdReadBoundedByDeadline(t *testing.T) {
	store := &blockingReadStore{MemoryProgressStore: NewMemoryProgressStore(), failsafe: time.Second}
	w := newTestWaiter(t, store)

	const timeout = 30 * time.Millisecond
	start := time.Now()
	res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 5), timeout)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("bounded read error = %v, want nil (a wait deadline is not a store error)", err)
	}
	if res.Reached {
		t.Fatalf("Reached = true, want false after a bounded cold read")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("WaitFor returned after %v, want within a small multiple of %v", elapsed, timeout)
	}
}

// TestWaiterFinalReadAtDeadline pins I3: when the deadline timer fires the
// waiter performs one last store read, so a notification lost for the whole
// wait still ends reached instead of timing out.
func TestWaiterFinalReadAtDeadline(t *testing.T) {
	store := &deadlineAdvanceStore{MemoryProgressStore: NewMemoryProgressStore()}
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	const timeout = 50 * time.Millisecond
	res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 7), timeout)
	if err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if !res.Reached || res.Status != StatusLive {
		t.Fatalf("WaitFor = (reached=%v, status=%v), want reached live via the final read", res.Reached, res.Status)
	}
	if res.Checkpoint.Position != 7 {
		t.Fatalf("final checkpoint position = %d, want 7", res.Checkpoint.Position)
	}
	if got := store.count(); got < 2 {
		t.Fatalf("store reads = %d, want at least 2 (fast path plus the final read)", got)
	}
}

// TestWaiterCachedReachedBeatsCap pins M11: an already-satisfied cached token
// returns reached even when the waiter registry is at its cap; the cap only
// gates registration, never a decision the cache can make.
func TestWaiterCachedReachedBeatsCap(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "a", 10, StatusLive)
	w := newTestWaiter(t, store, WithMaxWaiters(1, 1), WithFallback(time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := startWait(ctx, w, "ws", "a", token("ws", 20), 5*time.Second)
	waitRegistered(t, w, "ws", "a", 1)

	start := time.Now()
	res, err := w.WaitFor(context.Background(), "ws", "a", token("ws", 10), time.Second)
	if err != nil {
		t.Fatalf("cached reached wait: %v", err)
	}
	if !res.Reached {
		t.Fatalf("cached reached wait = %+v, want reached despite the cap", res)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("cached reached wait blocked for %v, want an immediate decision", elapsed)
	}

	cancel()
	if o := await(t, ch, time.Second); !errors.Is(o.err, context.Canceled) {
		t.Fatalf("held waiter error = %v, want context.Canceled", o.err)
	}
}

// TestWaiterZeroPositionToken pins M7: {Scope: ws, Position: 0} is the zero
// position for a scope and is reached immediately without touching the store.
func TestWaiterZeroPositionToken(t *testing.T) {
	store := newCountingProgressStore()
	w := newTestWaiter(t, store)

	res, err := w.WaitFor(context.Background(), "ws", "users", es.Token{Scope: "ws", Position: 0}, time.Second)
	if err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if !res.Reached {
		t.Fatalf("WaitFor = %+v, want reached for a zero-position token", res)
	}
	if got := store.count(); got != 0 {
		t.Fatalf("LoadProgress calls = %d, want 0 for a zero-position token", got)
	}
}

func TestWaiterContextCancellation(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 0, StatusLive)
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	ch := startWait(ctx, w, "ws", "users", token("ws", 5), 2*time.Second)
	waitRegistered(t, w, "ws", "users", 1)
	cancel()

	o := await(t, ch, time.Second)
	if !errors.Is(o.err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", o.err)
	}
	if o.result.Reached {
		t.Fatalf("Reached = true, want false on cancellation")
	}
}

func TestWaiterTokenScopeValidation(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "", "sqlite-projection", 10, StatusLive)
	mustSave(t, store, "ws", "users", 10, StatusLive)
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	t.Run("mismatch", func(t *testing.T) {
		_, err := w.WaitFor(context.Background(), "ws", "users", token("other", 5), 20*time.Millisecond)
		if !errors.Is(err, es.ErrScopeMismatch) {
			t.Fatalf("error = %v, want es.ErrScopeMismatch", err)
		}
	})

	t.Run("empty token scope with empty caller scope", func(t *testing.T) {
		res, err := w.WaitFor(context.Background(), "", "sqlite-projection", token("", 5), 20*time.Millisecond)
		if err != nil {
			t.Fatalf("error = %v, want nil", err)
		}
		if !res.Reached {
			t.Fatalf("Reached = false, want true")
		}
	})

	t.Run("empty token scope against a workspace", func(t *testing.T) {
		_, err := w.WaitFor(context.Background(), "ws", "users", token("", 5), 20*time.Millisecond)
		if !errors.Is(err, es.ErrScopeMismatch) {
			t.Fatalf("error = %v, want es.ErrScopeMismatch", err)
		}
	})

	t.Run("oversized scope", func(t *testing.T) {
		scope := strings.Repeat("x", 513)
		before := store.count()
		_, err := w.WaitFor(context.Background(), scope, "users", token(scope, 5), 20*time.Millisecond)
		if !errors.Is(err, es.ErrInvalidToken) {
			t.Fatalf("error = %v, want es.ErrInvalidToken", err)
		}
		if got := store.count(); got != before {
			t.Fatalf("store reads = %d, want no read for an invalid token", got-before)
		}
	})
}

func TestWaiterStatusBeatsPosition(t *testing.T) {
	for _, state := range []Status{StatusRebuilding, StatusStalled} {
		t.Run(state.String(), func(t *testing.T) {
			store := newCountingProgressStore()
			mustSave(t, store, "ws", "users", 10, state)
			w := newTestWaiter(t, store, WithFallback(time.Hour))

			start := time.Now()
			res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 5), 2*time.Second)
			if err != nil {
				t.Fatalf("error = %v", err)
			}
			if res.Reached {
				t.Fatalf("Reached = true, want false for %v", state)
			}
			if res.Status != state {
				t.Fatalf("Status = %v, want %v", res.Status, state)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("blocked %v, want an immediate status return", elapsed)
			}
		})
	}
}

func TestWaiterSetStatusWakes(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 10, StatusLive)
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := startWait(ctx, w, "ws", "users", token("ws", 20), 2*time.Second)
	waitRegistered(t, w, "ws", "users", 1)

	w.SetStatus("ws", "users", StatusRebuilding)
	o := await(t, ch, time.Second)
	if o.err != nil {
		t.Fatalf("error = %v", o.err)
	}
	if o.result.Reached || o.result.Status != StatusRebuilding {
		t.Fatalf("result = %+v, want not reached with rebuilding", o.result)
	}
}

func TestWaiterAdvanceStateChangeReplacesPosition(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 10, StatusLive)
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	if _, found, err := w.Checkpoint(context.Background(), "ws", "users"); err != nil || !found {
		t.Fatalf("warm checkpoint = (found %v, err %v)", found, err)
	}
	w.Advance(Checkpoint{Scope: "ws", Projection: "users", Position: 3, State: StatusLive})
	if cp, _, err := w.Checkpoint(context.Background(), "ws", "users"); err != nil || cp.Position != 10 {
		t.Fatalf("lower same-state advance moved the cache to %d (err %v), want 10", cp.Position, err)
	}

	w.Advance(Checkpoint{Scope: "ws", Projection: "users", Position: 0, State: StatusRebuilding})

	cp, found, err := w.Checkpoint(context.Background(), "ws", "users")
	if err != nil || !found {
		t.Fatalf("checkpoint after state change = (found %v, err %v)", found, err)
	}
	if cp.Position != 0 || cp.State != StatusRebuilding {
		t.Fatalf("cached checkpoint = %+v, want position 0 rebuilding", cp)
	}
}

func TestNormalizeWaitTimeout(t *testing.T) {
	if got := normalizeWaitTimeout(0); got != defaultWaitTimeout {
		t.Fatalf("zero timeout = %v, want %v", got, defaultWaitTimeout)
	}
	if got := normalizeWaitTimeout(-time.Second); got != defaultWaitTimeout {
		t.Fatalf("negative timeout = %v, want %v", got, defaultWaitTimeout)
	}
	if got := normalizeWaitTimeout(10 * time.Second); got != maxWaitTimeout {
		t.Fatalf("long timeout = %v, want %v", got, maxWaitTimeout)
	}
	if got := normalizeWaitTimeout(50 * time.Millisecond); got != 50*time.Millisecond {
		t.Fatalf("in-range timeout = %v, want 50ms", got)
	}
}

func TestWaiterMissingRow(t *testing.T) {
	store := newCountingProgressStore()
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := startWait(ctx, w, "ws", "users", token("ws", 5), 2*time.Second)
	waitRegistered(t, w, "ws", "users", 1)

	select {
	case o := <-ch:
		t.Fatalf("missing row short-circuited: %+v", o)
	case <-time.After(25 * time.Millisecond):
	}

	w.Advance(Checkpoint{Scope: "ws", Projection: "users", Position: 5, State: StatusLive})
	o := await(t, ch, time.Second)
	if o.err != nil || !o.result.Reached {
		t.Fatalf("after advance = (%+v, %v), want reached", o.result, o.err)
	}

	status, err := w.Status(context.Background(), "ws", "absent")
	if err != nil {
		t.Fatalf("Status error = %v", err)
	}
	if status != StatusUnknown {
		t.Fatalf("Status on a missing row = %v, want unknown", status)
	}
}

func TestWaiterLostNotificationFallback(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 0, StatusLive)
	w := newTestWaiter(t, store, WithFallback(5*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := startWait(ctx, w, "ws", "users", token("ws", 10), 2*time.Second)
	waitRegistered(t, w, "ws", "users", 1)

	// Advance the store behind the waiter without calling Advance: only the
	// jittered fallback can satisfy the wait.
	mustSave(t, store, "ws", "users", 10, StatusLive)

	o := await(t, ch, time.Second)
	if o.err != nil || !o.result.Reached {
		t.Fatalf("fallback result = (%+v, %v), want reached", o.result, o.err)
	}
}

func TestWaiterSharedRefresher(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 0, StatusLive)
	w := newTestWaiter(t, store, WithFallback(5*time.Millisecond))

	// Warm the cache so each waiter's fast path is a cache hit.
	if _, found, err := w.Checkpoint(context.Background(), "ws", "users"); err != nil || !found {
		t.Fatalf("warm checkpoint = (found %v, err %v)", found, err)
	}
	before := store.count()

	const n = 100
	results := make(chan waitOutcome, n)
	for i := 0; i < n; i++ {
		go func() {
			res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 10), 2*time.Second)
			results <- waitOutcome{result: res, err: err}
		}()
	}
	waitRegistered(t, w, "ws", "users", n)

	// Let several fallback ticks run. A per-waiter refresher would cost 100
	// reads here; the shared worker costs roughly one per tick.
	time.Sleep(40 * time.Millisecond)
	loads := store.count() - before
	if loads == 0 {
		t.Fatalf("fallback performed no store reads")
	}
	if loads >= n/2 {
		t.Fatalf("store reads = %d for %d waiters, want roughly one per tick", loads, n)
	}

	mustSave(t, store, "ws", "users", 10, StatusLive)
	w.Advance(Checkpoint{Scope: "ws", Projection: "users", Position: 10, State: StatusLive})
	for i := 0; i < n; i++ {
		o := await(t, results, 2*time.Second)
		if o.err != nil || !o.result.Reached {
			t.Fatalf("waiter %d = (%+v, %v), want reached", i, o.result, o.err)
		}
	}
}

func TestWaiterHintIsNotData(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 5, StatusLive)
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	if _, found, err := w.Checkpoint(context.Background(), "ws", "users"); err != nil || !found {
		t.Fatalf("warm checkpoint = (found %v, err %v)", found, err)
	}

	// A forged publisher value must never be trusted as progress.
	w.Advance(Checkpoint{Scope: "ws", Projection: "users", Position: 1_000_000_000, State: StatusLive})

	// The hint is only effective while a local waiter exists, so hold one
	// open on a token above the forged position.
	ctx, cancel := context.WithCancel(context.Background())
	ch := startWait(ctx, w, "ws", "users", token("ws", 1_000_000_001), 5*time.Second)
	waitRegistered(t, w, "ws", "users", 1)

	w.EnqueueScopeRefresh("ws")
	waitCachePosition(t, w, "ws", "users", 5)

	cp, found, err := w.Checkpoint(context.Background(), "ws", "users")
	if err != nil || !found {
		t.Fatalf("checkpoint after refresh = (found %v, err %v)", found, err)
	}
	if cp.Position != 5 {
		t.Fatalf("cached position = %d, want the store's authoritative 5", cp.Position)
	}

	res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 1_000_000_000), 25*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitFor error = %v", err)
	}
	if res.Reached {
		t.Fatalf("Reached = true for a forged position, want false")
	}

	cancel()
	o := await(t, ch, time.Second)
	if !errors.Is(o.err, context.Canceled) {
		t.Fatalf("held waiter error = %v, want context.Canceled", o.err)
	}
}

func TestWaiterHintWithoutLocalWaiters(t *testing.T) {
	store := newCountingProgressStore()
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	// No worker has started and no key has waiters: both hints are dropped
	// without a store read and without blocking.
	done := make(chan struct{})
	go func() {
		w.EnqueueScopeRefresh("ws")
		w.EnqueueRefreshAll()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("hints blocked")
	}
	time.Sleep(20 * time.Millisecond)
	if got := store.count(); got != 0 {
		t.Fatalf("store reads = %d, want 0 with no local waiters", got)
	}

	// With the worker running for another key, a hint for a scope that has no
	// local waiters must still perform no read.
	mustSave(t, store, "ws", "a", 0, StatusLive)
	ctx, cancel := context.WithCancel(context.Background())
	ch := startWait(ctx, w, "ws", "a", token("ws", 5), 5*time.Second)
	waitRegistered(t, w, "ws", "a", 1)

	before := store.countFor("other")
	w.EnqueueScopeRefresh("other")
	time.Sleep(30 * time.Millisecond)
	if got := store.countFor("other") - before; got != 0 {
		t.Fatalf("store reads for an idle scope = %d, want 0", got)
	}

	cancel()
	o := await(t, ch, time.Second)
	if !errors.Is(o.err, context.Canceled) {
		t.Fatalf("waiter error = %v, want context.Canceled", o.err)
	}
}

func TestWaiterRefreshAllReconnect(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "p1", 1, StatusLive)
	mustSave(t, store, "ws", "p2", 2, StatusLive)
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch1 := startWait(ctx, w, "ws", "p1", token("ws", 10), 5*time.Second)
	ch2 := startWait(ctx, w, "ws", "p2", token("ws", 10), 5*time.Second)
	waitRegistered(t, w, "ws", "p1", 1)
	waitRegistered(t, w, "ws", "p2", 1)

	// Move both rows behind the hub, then repair as a listener reconnect would.
	mustSave(t, store, "ws", "p1", 5, StatusLive)
	mustSave(t, store, "ws", "p2", 7, StatusLive)
	w.EnqueueRefreshAll()

	waitCachePosition(t, w, "ws", "p1", 5)
	waitCachePosition(t, w, "ws", "p2", 7)

	cancel()
	for _, ch := range []<-chan waitOutcome{ch1, ch2} {
		o := await(t, ch, time.Second)
		if !errors.Is(o.err, context.Canceled) {
			t.Fatalf("waiter error = %v, want context.Canceled", o.err)
		}
	}
}

func TestWaiterCapsFailOpen(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "a", 0, StatusLive)
	mustSave(t, store, "ws", "b", 0, StatusLive)
	w := newTestWaiter(t, store, WithMaxWaiters(1, 1), WithFallback(time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := startWait(ctx, w, "ws", "a", token("ws", 5), 5*time.Second)
	waitRegistered(t, w, "ws", "a", 1)
	reads := store.count()

	start := time.Now()
	res, err := w.WaitFor(context.Background(), "ws", "a", token("ws", 5), time.Second)
	if err != nil || res.Reached {
		t.Fatalf("second waiter on the key = (%+v, %v), want Reached false and nil", res, err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("second waiter blocked for %v, want an immediate fail-open", elapsed)
	}

	start = time.Now()
	res, err = w.WaitFor(context.Background(), "ws", "b", token("ws", 5), time.Second)
	if err != nil || res.Reached {
		t.Fatalf("first waiter on another key = (%+v, %v), want Reached false and nil", res, err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("cross-key waiter blocked for %v, want an immediate fail-open", elapsed)
	}
	// The cap check now follows the cache decision (M11): the cold key b is
	// read once before the fail-open, while the warm key a (above) added no
	// read. A cached reached token is decided before the cap regardless.
	if got := store.count(); got != reads+1 {
		t.Fatalf("store reads after the cold cross-key fail-open = %d, want %d", got, reads+1)
	}

	cancel()
	o := await(t, ch, time.Second)
	if !errors.Is(o.err, context.Canceled) {
		t.Fatalf("held waiter error = %v, want context.Canceled", o.err)
	}
}

func TestWaiterBestEffort(t *testing.T) {
	sentinel := errors.New("progress store unavailable")
	store := newCountingProgressStore()
	store.setLoadErr(sentinel)

	strict := newTestWaiter(t, store)
	res, err := strict.WaitFor(context.Background(), "ws", "users", token("ws", 5), 20*time.Millisecond)
	if !errors.Is(err, sentinel) {
		t.Fatalf("strict error = %v, want the store error", err)
	}
	if res.Reached {
		t.Fatalf("strict Reached = true, want false")
	}

	best := newTestWaiter(t, store, WithBestEffort())
	res, err = best.WaitFor(context.Background(), "ws", "users", token("ws", 5), 20*time.Millisecond)
	if err != nil {
		t.Fatalf("best-effort error = %v, want nil", err)
	}
	if res.Reached {
		t.Fatalf("best-effort Reached = true, want false")
	}

	// Validation and cancellation are not store errors: they still fail.
	if _, err := best.WaitFor(context.Background(), "ws", "users", token("other", 5), 20*time.Millisecond); !errors.Is(err, es.ErrScopeMismatch) {
		t.Fatalf("best-effort scope error = %v, want es.ErrScopeMismatch", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := best.WaitFor(ctx, "ws", "users", token("ws", 5), 20*time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("best-effort cancellation error = %v, want context.Canceled", err)
	}
}

func TestWaiterTenThousandReleased(t *testing.T) {
	// Verify the goroutine hygiene at the end: the refresher must be gone
	// after Close. Registering waiters are not goroutines owned by the Waiter.
	defer goleak.VerifyNone(t)

	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 0, StatusLive)
	w := NewWaiter(store, WithFallback(time.Hour), WithMaxWaiters(20000, 20000))
	defer w.Close()

	const n = 10000
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 1), 10*time.Second)
			if err != nil {
				errs <- fmt.Errorf("waiter %d: %w", i, err)
				return
			}
			if !res.Reached {
				errs <- fmt.Errorf("waiter %d: not reached", i)
			}
		}(i)
	}

	waitTotal(t, w, n)
	w.Advance(Checkpoint{Scope: "ws", Projection: "users", Position: 1, State: StatusLive})
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestWaiterStatusColdCache(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 3, StatusRebuilding)
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	status, err := w.Status(context.Background(), "ws", "users")
	if err != nil {
		t.Fatalf("Status error = %v", err)
	}
	if status != StatusRebuilding {
		t.Fatalf("Status = %v, want rebuilding", status)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("store reads = %d, want 1", got)
	}

	if _, err := w.Status(context.Background(), "ws", "users"); err != nil {
		t.Fatalf("warm Status error = %v", err)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("store reads after warm Status = %d, want still 1", got)
	}

	status, err = w.Status(context.Background(), "ws", "absent")
	if err != nil {
		t.Fatalf("missing Status error = %v", err)
	}
	if status != StatusUnknown {
		t.Fatalf("Status on a missing row = %v, want unknown", status)
	}
}

func TestWaiterBestEffortDoesNotSwallowCancellation(t *testing.T) {
	t.Run("canceled mid-read", func(t *testing.T) {
		store := newCtxCancelStore()
		w := newTestWaiter(t, store, WithBestEffort())

		ctx, cancel := context.WithCancel(context.Background())
		ch := startWait(ctx, w, "ws", "users", token("ws", 5), 2*time.Second)
		select {
		case <-store.started:
		case <-time.After(time.Second):
			t.Fatalf("LoadProgress was not called")
		}
		cancel()

		o := await(t, ch, time.Second)
		if !errors.Is(o.err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled even with best effort", o.err)
		}
		if o.result.Reached {
			t.Fatalf("Reached = true, want false")
		}
	})

	t.Run("deadline exceeded from the store", func(t *testing.T) {
		store := newCountingProgressStore()
		store.setLoadErr(context.DeadlineExceeded)
		w := newTestWaiter(t, store, WithBestEffort())

		res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 5), 20*time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want context.DeadlineExceeded even with best effort", err)
		}
		if res.Reached {
			t.Fatalf("Reached = true, want false")
		}
	})
}

func TestWaiterHubBounded(t *testing.T) {
	const hubCap = 8192
	store := newAlwaysFoundStore(1)
	w := newTestWaiter(t, store, WithFallback(time.Hour))

	projection := func(i int) string { return fmt.Sprintf("p%d", i) }
	for i := 0; i < hubCap; i++ {
		res, err := w.WaitFor(context.Background(), "ws", projection(i), token("ws", 1), 20*time.Millisecond)
		if err != nil || !res.Reached {
			t.Fatalf("fill key %d = (%+v, %v), want reached", i, res, err)
		}
	}

	w.mu.Lock()
	n := len(w.keys)
	w.mu.Unlock()
	if n > hubCap {
		t.Fatalf("hub size = %d, want at most %d", n, hubCap)
	}

	// Touch key 0 so it is no longer the most idle; the next insert must evict
	// the colder key 1 instead.
	if _, found, err := w.Checkpoint(context.Background(), "ws", projection(0)); err != nil || !found {
		t.Fatalf("touch key 0 = (found %v, err %v)", found, err)
	}
	res, err := w.WaitFor(context.Background(), "ws", projection(hubCap), token("ws", 1), 20*time.Millisecond)
	if err != nil || !res.Reached {
		t.Fatalf("overflow key = (%+v, %v), want reached", res, err)
	}

	w.mu.Lock()
	_, has0 := w.keys[progressKey{scope: "ws", projection: projection(0)}]
	_, has1 := w.keys[progressKey{scope: "ws", projection: projection(1)}]
	n = len(w.keys)
	w.mu.Unlock()
	if !has0 {
		t.Fatalf("hot key 0 was evicted")
	}
	if has1 {
		t.Fatalf("cold key 1 survived the cap")
	}
	if n > hubCap {
		t.Fatalf("hub size after overflow = %d, want at most %d", n, hubCap)
	}

	// The evicted key must re-read the store on its next wait.
	before := store.loadsFor(projection(1))
	res, err = w.WaitFor(context.Background(), "ws", projection(1), token("ws", 1), 20*time.Millisecond)
	if err != nil || !res.Reached {
		t.Fatalf("evicted key = (%+v, %v), want reached", res, err)
	}
	if got := store.loadsFor(projection(1)); got <= before {
		t.Fatalf("evicted key loads = %d, want more than %d", got, before)
	}

	// The hot key stays warm: a cache hit must not read the store.
	before = store.loadsFor(projection(0))
	if _, found, err := w.Checkpoint(context.Background(), "ws", projection(0)); err != nil || !found {
		t.Fatalf("hot key checkpoint = (found %v, err %v)", found, err)
	}
	if got := store.loadsFor(projection(0)); got != before {
		t.Fatalf("hot key loads = %d, want unchanged %d", got, before)
	}
}

func TestWaiterColdReadMergeFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		results []Checkpoint
	}{
		{
			name: "live then rebuilding",
			results: []Checkpoint{
				{Position: 10, State: StatusLive},
				{Position: 0, State: StatusRebuilding},
			},
		},
		{
			name: "rebuilding then live",
			results: []Checkpoint{
				{Position: 0, State: StatusRebuilding},
				{Position: 10, State: StatusLive},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newScriptedLoadStore(tc.results...)
			w := newTestWaiter(t, store, WithFallback(time.Hour))

			type callResult struct {
				cp    Checkpoint
				found bool
				err   error
			}
			out := make(chan callResult, 2)
			call := func() {
				cp, found, err := w.Checkpoint(context.Background(), "ws", "users")
				out <- callResult{cp: cp, found: found, err: err}
			}

			go call()
			if i := <-store.started; i != 0 {
				t.Fatalf("first LoadProgress index = %d, want 0", i)
			}
			// The second call starts while the first read is still held open,
			// so both observe a cold cache.
			go call()
			if i := <-store.started; i != 1 {
				t.Fatalf("second LoadProgress index = %d, want 1", i)
			}

			close(store.releases[0])
			r0 := <-out
			if r0.err != nil || !r0.found {
				t.Fatalf("first cold read = (%+v, err %v), want found", r0.cp, r0.err)
			}
			close(store.releases[1])
			r1 := <-out
			if r1.err != nil || !r1.found {
				t.Fatalf("second cold read = (%+v, err %v), want found", r1.cp, r1.err)
			}

			cp, found, err := w.Checkpoint(context.Background(), "ws", "users")
			if err != nil || !found {
				t.Fatalf("merged checkpoint = (found %v, err %v)", found, err)
			}
			if cp.State != StatusRebuilding || cp.Position != 0 {
				t.Fatalf("merged checkpoint = {pos %d, %v}, want {pos 0, rebuilding}", cp.Position, cp.State)
			}

			res, err := w.WaitFor(context.Background(), "ws", "users", token("ws", 10), 50*time.Millisecond)
			if err != nil {
				t.Fatalf("WaitFor error = %v", err)
			}
			if res.Reached {
				t.Fatalf("Reached = true during a rebuild, want false")
			}
			if res.Status != StatusRebuilding {
				t.Fatalf("Status = %v, want rebuilding", res.Status)
			}
		})
	}
}

func TestWaiterNoSpuriousWakeOrIdleRead(t *testing.T) {
	store := newCountingProgressStore()
	mustSave(t, store, "ws", "users", 0, StatusLive)
	w := newTestWaiter(t, store, WithFallback(5*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	ch := startWait(ctx, w, "ws", "users", token("ws", 5), 2*time.Second)
	waitRegistered(t, w, "ws", "users", 1)

	key := progressKey{scope: "ws", projection: "users"}
	_, _, wake := w.snapshot(key)
	before := store.count()

	// Wait for at least three fallback ticks. They read the store, but an
	// unchanged checkpoint must not broadcast to the blocked waiter.
	deadline := time.Now().Add(time.Second)
	for store.count()-before < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("fallback ticks = %d, want at least 3", store.count()-before)
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, after := w.snapshot(key); after != wake {
		t.Fatalf("an unchanged checkpoint woke the waiter")
	}

	cancel()
	o := await(t, ch, time.Second)
	if !errors.Is(o.err, context.Canceled) {
		t.Fatalf("waiter error = %v, want context.Canceled", o.err)
	}

	// Once the last waiter leaves, the ticker must skip the idle key without a
	// store read. Let any read already in flight settle first.
	time.Sleep(20 * time.Millisecond)
	idle := store.count()
	time.Sleep(30 * time.Millisecond)
	if got := store.count() - idle; got != 0 {
		t.Fatalf("idle hub performed %d store reads", got)
	}
}
