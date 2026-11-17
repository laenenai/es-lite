package feed

// Hub tests (ADR 0039 §3.2). This file is package feed on purpose: the
// lifecycle assertions read h.scopes directly, which only an internal test
// can. The invariants under test are the ones the obvious implementation
// gets wrong — close-and-remove on Wake (not replace-and-leave), the
// deferred decrement guarded on the same generation, no goroutine per
// waiter, and the zero-value panic.

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// waitOutcome carries one Wait/WaitScope result across a goroutine.
type waitOutcome struct {
	woke bool
	err  error
}

// startScopedWaiter parks one WaitScope in its own goroutine and returns a
// buffered channel for its single result.
func startScopedWaiter(h *Hub, scope string, timeout time.Duration) <-chan waitOutcome {
	res := make(chan waitOutcome, 1)
	go func() {
		woke, err := h.WaitScope(context.Background(), scope, timeout)
		res <- waitOutcome{woke, err}
	}()
	return res
}

// startUnscopedWaiter parks one unscoped Wait in its own goroutine.
func startUnscopedWaiter(h *Hub, timeout time.Duration) <-chan waitOutcome {
	res := make(chan waitOutcome, 1)
	go func() {
		woke, err := h.Wait(context.Background(), timeout)
		res <- waitOutcome{woke, err}
	}()
	return res
}

// awaitOutcome receives a parked waiter's result within d, failing the test
// if it never arrives.
func awaitOutcome(t *testing.T, res <-chan waitOutcome, d time.Duration) waitOutcome {
	t.Helper()
	select {
	case got := <-res:
		return got
	case <-time.After(d):
		t.Fatalf("waiter did not return within %v", d)
		return waitOutcome{}
	}
}

// assertWoke requires a waiter's result to be (true, nil): a genuine wake.
func assertWoke(t *testing.T, got waitOutcome) {
	t.Helper()
	if !got.woke || got.err != nil {
		t.Fatalf("waiter result = (woke %v, err %v), want (true, nil)", got.woke, got.err)
	}
}

// waitScopeRegistered polls until a parked WaitScope has created the scope's
// generation, so a Wake that follows is guaranteed to land on a live
// registrant rather than racing the park.
func waitScopeRegistered(t *testing.T, h *Hub, scope string) {
	t.Helper()
	end := time.Now().Add(2 * time.Second)
	for {
		h.mu.Lock()
		_, ok := h.scopes[scope]
		h.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("scope %q was never registered by a parked waiter", scope)
		}
		time.Sleep(time.Millisecond)
	}
}

// scopeCount reads len(h.scopes) under the hub's mutex.
func scopeCount(h *Hub) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.scopes)
}

// assertPanics requires fn to panic with exactly want as its message.
func assertPanics(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("fn did not panic, want %q", want)
		}
		if got, ok := r.(string); !ok || got != want {
			t.Fatalf("panic = %v, want %q", r, want)
		}
	}()
	fn()
}

// TestHubWakeIsScoped pins the routing itself: a wake for one scope wakes
// only that scope's waiters.
func TestHubWakeIsScoped(t *testing.T) {
	h := NewHub()
	a := startScopedWaiter(h, "a", 2*time.Second)
	b := startScopedWaiter(h, "b", 2*time.Second)
	waitScopeRegistered(t, h, "a")
	waitScopeRegistered(t, h, "b")

	h.Wake("a")

	assertWoke(t, awaitOutcome(t, a, 100*time.Millisecond))

	// b has no reason to return: no wake for its scope, its timeout far
	// away. The grace keeps the check meaningful against a Wake that woke
	// the wrong scopes.
	time.Sleep(20 * time.Millisecond)
	select {
	case got := <-b:
		t.Fatalf("waiter on b returned (woke %v, err %v) after Wake(a): wakes are not scoped", got.woke, got.err)
	default:
	}

	h.Wake("b")
	assertWoke(t, awaitOutcome(t, b, 100*time.Millisecond))
}

// TestHubWakeAll pins the blanket: every scope entry and the unscoped
// generation close together.
func TestHubWakeAll(t *testing.T) {
	h := NewHub()
	a := startScopedWaiter(h, "a", 2*time.Second)
	b := startScopedWaiter(h, "b", 2*time.Second)
	u := startUnscopedWaiter(h, 2*time.Second)
	waitScopeRegistered(t, h, "a")
	waitScopeRegistered(t, h, "b")
	// The unscoped waiter registers nothing observable in the map; give it
	// a grace period to reach its select.
	time.Sleep(20 * time.Millisecond)

	h.WakeAll()

	assertWoke(t, awaitOutcome(t, a, 100*time.Millisecond))
	assertWoke(t, awaitOutcome(t, b, 100*time.Millisecond))
	assertWoke(t, awaitOutcome(t, u, 100*time.Millisecond))
}

// TestHubWakeWakesUnscopedWaiter pins the compatibility path: an unscoped
// Wait returns on any scoped Wake, preserving "any append may wake me".
func TestHubWakeWakesUnscopedWaiter(t *testing.T) {
	h := NewHub()
	u := startUnscopedWaiter(h, 2*time.Second)
	time.Sleep(20 * time.Millisecond) // nothing observable to poll

	h.Wake("a")

	assertWoke(t, awaitOutcome(t, u, 100*time.Millisecond))
}

// TestHubWaitScopeTimeout pins the timeout return: (false, nil), plus the
// deferred cleanup — a timed-out waiter leaves no entry behind.
func TestHubWaitScopeTimeout(t *testing.T) {
	h := NewHub()
	woke, err := h.WaitScope(context.Background(), "a", 10*time.Millisecond)
	if woke || err != nil {
		t.Fatalf("WaitScope = (woke %v, err %v), want (false, nil)", woke, err)
	}
	if n := scopeCount(h); n != 0 {
		t.Fatalf("len(h.scopes) = %d after a timed-out WaitScope, want 0", n)
	}
}

// TestHubWaitScopeCancel pins the cancellation return: (false,
// context.Canceled), exercised through a waiter that is parked first.
func TestHubWaitScopeCancel(t *testing.T) {
	h := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := make(chan waitOutcome, 1)
	go func() {
		woke, err := h.WaitScope(ctx, "a", 2*time.Second)
		res <- waitOutcome{woke, err}
	}()
	waitScopeRegistered(t, h, "a")

	cancel()

	got := awaitOutcome(t, res, 100*time.Millisecond)
	if got.woke || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("WaitScope = (woke %v, err %v), want (false, context.Canceled)", got.woke, got.err)
	}
}

// TestHubWakeOnAbsentScopeLeavesMapEmpty pins invariant 3: a wake for a
// scope nobody parked on must not grow the map — "appends but no live
// subscriber" is the normal case for a workspace.
func TestHubWakeOnAbsentScopeLeavesMapEmpty(t *testing.T) {
	h := NewHub()
	h.Wake("never-registered")
	if n := scopeCount(h); n != 0 {
		t.Fatalf("len(h.scopes) = %d after Wake on a never-registered scope, want 0", n)
	}
}

// TestHubWakeDuringParkLeavesNoEntry pins the stale-generation interleaving
// (review focus 2): a wake that lands between a waiter parking and exiting.
// Wake must close AND remove; a replace-and-leave implementation exits the
// waiter on the closed old generation and leaves the fresh one in the map
// with no registrant and no one to delete it.
func TestHubWakeDuringParkLeavesNoEntry(t *testing.T) {
	h := NewHub()
	res := startScopedWaiter(h, "a", 2*time.Second)
	waitScopeRegistered(t, h, "a")

	h.Wake("a") // lands while the waiter is still parked

	assertWoke(t, awaitOutcome(t, res, 100*time.Millisecond))
	if n := scopeCount(h); n != 0 {
		t.Fatalf("len(h.scopes) = %d after a wake between park and exit, want 0: a replaced generation leaked", n)
	}
}

// TestHubTenThousandScopesLeaveNoEntries pins the leak bound (ADR 0039 §8):
// 10k scopes that come and go leave no map entries and no goroutines. Every
// tenth scope has a Wake racing its park/exit, so both cleanup paths — the
// deferred delete after a timeout/cancel, and the close-and-remove under a
// wake — run under contention.
func TestHubTenThousandScopesLeaveNoEntries(t *testing.T) {
	h := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // every WaitScope below returns through ctx.Done immediately

	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		scope := fmt.Sprintf("scope-%d", i)
		if i%10 == 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				h.Wake(scope) // races the waiter's park and exit
			}()
		}
		// The return value is deliberately not asserted: with the wake
		// racing, both (true, nil) on the closed generation and (false,
		// context.Canceled) on the cancelled context are valid outcomes.
		_, _ = h.WaitScope(ctx, scope, 100*time.Millisecond)
	}
	wg.Wait()

	if n := scopeCount(h); n != 0 {
		t.Fatalf("len(h.scopes) = %d after 10000 scopes came and went, want 0", n)
	}

	// The hub itself must create no goroutines; the only goroutines this
	// test spawned are its own Wakes, joined above. Give the runtime a
	// moment to reap them, then hold the count to the baseline.
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines grew from %d to %d, want at most %d: the hub leaks goroutines", before, after, before+2)
	}
}

// TestHubZeroValuePanics pins the construction contract: the zero value is
// not usable, and the panic message is the contract.
func TestHubZeroValuePanics(t *testing.T) {
	const want = "feed: Hub must be created with NewHub"
	ctx := context.Background()

	var nilHub *Hub
	assertPanics(t, want, func() { nilHub.Wake("a") })
	assertPanics(t, want, func() { nilHub.WakeAll() })
	assertPanics(t, want, func() { nilHub.Wait(ctx, time.Second) })
	assertPanics(t, want, func() { nilHub.WaitScope(ctx, "a", time.Second) })

	var zero Hub
	assertPanics(t, want, func() { zero.Wake("a") })
	assertPanics(t, want, func() { zero.WakeAll() })
	assertPanics(t, want, func() { zero.Wait(ctx, time.Second) })
	assertPanics(t, want, func() { zero.WaitScope(ctx, "a", time.Second) })
}

// TestHubStats pins the wake diagnostic: Wake counts calls, not scopes, and
// WakeAll counts its own calls (preflight ruling).
func TestHubStats(t *testing.T) {
	h := NewHub()
	h.Wake("a")
	h.Wake("a") // the same scope twice still counts
	h.Wake("b")
	h.WakeAll()

	st := h.Stats()
	if st.Wakes != 3 || st.WakeAlls != 1 {
		t.Fatalf("Stats() = {Wakes: %d, WakeAlls: %d}, want {Wakes: 3, WakeAlls: 1}", st.Wakes, st.WakeAlls)
	}
}

// TestHubConcurrentWakeAndWait runs the hub's full interleaving matrix under
// the race detector: many waiters parking, timing out and being woken on a
// shared scope set, while wakers fire Wake and WakeAll concurrently. Every
// waiter must return without an error, and once everything has joined the
// map must be empty — no generation outlives its last waiter.
func TestHubConcurrentWakeAndWait(t *testing.T) {
	h := NewHub()
	const (
		waiters    = 32
		iterations = 200
		numScopes  = 8
		wakers     = 4
	)

	stop := make(chan struct{})
	var wakersWG, waitersWG sync.WaitGroup

	for i := 0; i < wakers; i++ {
		wakersWG.Add(1)
		go func() {
			defer wakersWG.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				h.Wake(fmt.Sprintf("scope-%d", n%numScopes))
				if n%2 == 0 {
					h.WakeAll()
				}
			}
		}()
	}

	for i := 0; i < waiters; i++ {
		waitersWG.Add(1)
		go func() {
			defer waitersWG.Done()
			for n := 0; n < iterations; n++ {
				if _, err := h.WaitScope(context.Background(), fmt.Sprintf("scope-%d", n%numScopes), time.Millisecond); err != nil {
					t.Errorf("WaitScope: unexpected error: %v", err)
				}
			}
		}()
	}

	waitersWG.Wait()
	close(stop)
	wakersWG.Wait()

	if n := scopeCount(h); n != 0 {
		t.Fatalf("len(h.scopes) = %d after concurrent wake/wait, want 0: a generation outlived its last waiter", n)
	}
}
