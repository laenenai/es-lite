package feed

import (
	"context"
	"sync"
	"time"
)

// Hub is the in-memory wake hub of ADR 0039 §3.2: a Waiter and ScopedWaiter
// the server waits through, which producers wake after a commit. A wake is
// only a latency hint — correctness stays with the feed's horizon fence
// (ADR 0037): a missed, late or duplicated wake costs at most one poll of
// delay, never loss, and a woken stream reads the same fenced page a poll
// would have read.
//
// Producers call Wake(scope) with the appended workspace's scope, and the
// server's streams wake through WaitScope with their Request.Scope, so one
// append wakes only that workspace's streams plus the unscoped waiters.
// WakeAll wakes everyone and is the reconnect repair for a wake source that
// was down while hints were emitted.
//
// The hub is in-process and trusted: Wake must never be exposed over a
// network, and a producer calling it in a tight loop spins every stream of
// the scope. The hub creates no goroutines — each wait parks on a channel
// copy and a time.NewTimer created inside the call — and no entry or
// goroutine outlives its last waiter (ADR 0039 §8's leak bound).
//
// The zero value is not usable: Wake, WakeAll, Wait, WaitScope and Stats
// panic with "feed: Hub must be created with NewHub". Construct with NewHub.
type Hub struct {
	mu      sync.Mutex
	scopes  map[string]*scopeGen
	all     *scopeGen
	wakes   uint64
	wakeAll uint64
}

// scopeGen is one broadcast generation: the channel waiters copy while
// parking and a wake closes, plus the number of waiters currently parked on
// it. refs is only touched under the hub's mutex.
type scopeGen struct {
	ch   chan struct{}
	refs int
}

var (
	_ Waiter       = (*Hub)(nil)
	_ ScopedWaiter = (*Hub)(nil)
)

// NewHub returns a ready-to-use Hub.
func NewHub() *Hub {
	return &Hub{
		scopes: make(map[string]*scopeGen),
		all:    &scopeGen{ch: make(chan struct{})},
	}
}

// check enforces the construction contract; the panic is the contract, and
// a test pins it.
func (h *Hub) check() {
	if h == nil || h.scopes == nil || h.all == nil {
		panic("feed: Hub must be created with NewHub")
	}
}

// Wake wakes every waiter currently parked on scope, and every unscoped
// waiter: the hub-wide generation is closed and replaced on every wake, so a
// legacy Wait keeps "any append may wake me".
//
// The scope's entry is closed and REMOVED, not replaced. Removing on wake is
// load-bearing: a wake that lands between a waiter parking and exiting would
// otherwise leave a fresh generation with no registrant and no one to delete
// it — a permanent map entry per workspace, which is the normal "appends but
// no live subscriber" case. Removal also coalesces repeated wakes while no
// stream is parked. A wake for a scope with no entry is a no-op for the map:
// there is no waiter, and creating an entry would grow the map with every
// appended workspace that has no subscriber. Stats.Wakes counts the call
// either way.
//
// Wake only closes channels and swaps the hub-wide generation: it never
// blocks on a channel, and it holds the lock for none of it.
func (h *Hub) Wake(scope string) {
	h.check()
	h.mu.Lock()
	h.wakes++
	if g := h.scopes[scope]; g != nil {
		close(g.ch)
		delete(h.scopes, scope)
	}
	close(h.all.ch)
	h.all.ch = make(chan struct{})
	h.mu.Unlock()
}

// WakeAll wakes everyone: every live scope entry is closed and removed, and
// the hub-wide generation is closed and replaced. A wake source calls it
// after reconnecting, because hints emitted while it was down may have been
// missed — a latency repair, never a correctness one (ADR 0039 §4).
func (h *Hub) WakeAll() {
	h.check()
	h.mu.Lock()
	h.wakeAll++
	for scope, g := range h.scopes {
		close(g.ch)
		delete(h.scopes, scope)
	}
	close(h.all.ch)
	h.all.ch = make(chan struct{})
	h.mu.Unlock()
}

// WaitScope parks one scoped wait (the ScopedWaiter path). It registers
// under the mutex — get-or-create the scope's generation, increment refs,
// copy the channel — and only then blocks on the copy, a timer created
// inside this call, and the context; no lock is held while blocking, and no
// goroutine is spawned. The deferred cleanup decrements refs under the same
// mutex and deletes the entry only when refs reaches zero and the map still
// holds that same generation — a Wake may have removed or replaced it in
// between, and deleting a newer generation would strand its waiters.
//
// It returns (true, nil) when the generation closed (a wake), (false, nil)
// on the timer — a normal timeout, not an error — and (false, ctx.Err())
// when the context ends.
func (h *Hub) WaitScope(ctx context.Context, scope string, timeout time.Duration) (bool, error) {
	h.check()
	h.mu.Lock()
	g := h.scopes[scope]
	if g == nil {
		g = &scopeGen{ch: make(chan struct{})}
		h.scopes[scope] = g
	}
	g.refs++
	ch := g.ch
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		g.refs--
		if g.refs == 0 && h.scopes[scope] == g {
			delete(h.scopes, scope)
		}
		h.mu.Unlock()
	}()

	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ch:
		return true, nil
	case <-t.C:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// Wait parks one unscoped wait (the Waiter compatibility path): it copies
// the hub-wide generation's channel and blocks on it, a timer created
// inside this call, and the context, holding no lock. It never touches
// refs — the hub-wide generation is closed and replaced by every Wake and
// WakeAll and is never deleted, so an unscoped waiter preserves "any
// append may wake me" without per-waiter bookkeeping.
//
// The returns match WaitScope: (true, nil) on a wake, (false, nil) on the
// timer, (false, ctx.Err()) when the context ends.
func (h *Hub) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	h.check()
	h.mu.Lock()
	ch := h.all.ch
	h.mu.Unlock()

	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ch:
		return true, nil
	case <-t.C:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// HubStats is the hub's wake diagnostic (ADR 0039 §3.3): cheap counters an
// operator reads to answer "are wakes arriving for this deployment?" — a
// service that forgets to wake after its own commit fails silently into
// polling, and no other signal distinguishes that from a blocked fence.
type HubStats struct {
	// Wakes counts Wake calls, not scopes: a wake nobody was parked for
	// still counts.
	Wakes uint64
	// WakeAlls counts WakeAll calls.
	WakeAlls uint64
}

// Stats returns the wake counters. It is safe for concurrent use.
func (h *Hub) Stats() HubStats {
	h.check()
	h.mu.Lock()
	defer h.mu.Unlock()
	return HubStats{Wakes: h.wakes, WakeAlls: h.wakeAll}
}
