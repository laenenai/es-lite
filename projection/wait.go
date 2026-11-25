package projection

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/laenenai/es-lite/es"
)

const (
	// defaultWaitTimeout is used when WaitFor is called with a zero (or
	// negative) timeout.
	defaultWaitTimeout = 500 * time.Millisecond
	// maxWaitTimeout caps a caller-supplied timeout.
	maxWaitTimeout = 5 * time.Second
	// defaultFallback is the default refresh interval: how long a waiter may
	// go without a notification before the refresher re-reads the store.
	defaultFallback = 100 * time.Millisecond
	// defaultMaxWaitersPerKey and defaultMaxWaitersTotal bound the registry.
	// Above either cap WaitFor fails open instead of queueing an unbounded
	// number of waiters.
	defaultMaxWaitersPerKey = 1024
	defaultMaxWaitersTotal  = 16384
	// maxWaitScope matches es's token scope limit. A token scope over this
	// size is ErrInvalidToken.
	maxWaitScope = 512
	// scopeHintQueue is the bounded size of the non-blocking scope-hint
	// queue. An overflow drops the hint; the ticker repairs it.
	scopeHintQueue = 64
)

// WaitResult is the outcome of WaitFor. Reached reports whether the projection
// had applied every event at or below the token's position when the wait
// ended; Status is the checkpoint's state at that moment. A timeout is
// Reached:false with a nil error.
type WaitResult struct {
	Reached    bool
	Checkpoint Checkpoint
	Waited     time.Duration
	Status     Status
}

type waiterOptions struct {
	fallback   time.Duration
	bestEffort bool
	maxPerKey  int
	maxTotal   int
}

// WaiterOption configures a Waiter.
type WaiterOption func(*waiterOptions)

// WithFallback sets the refresh interval: the longest a waiter may go without
// a notification before the single refresher re-reads the store. The interval
// is jittered by ±20%. Zero (or negative) keeps the 100ms default.
func WithFallback(d time.Duration) WaiterOption {
	return func(o *waiterOptions) { o.fallback = d }
}

// WithBestEffort converts store errors into Reached:false with a nil error, so
// a query endpoint can fail open to stale data. Malformed tokens, scope
// mismatches and context cancellation still fail.
func WithBestEffort() WaiterOption {
	return func(o *waiterOptions) { o.bestEffort = true }
}

// WithMaxWaiters bounds the registry: at most perKey waiters on one
// (scope, projection) key and at most total waiters overall. A non-positive
// value keeps that default (1024 per key, 16384 total). Above either cap
// WaitFor returns Reached:false, nil immediately.
func WithMaxWaiters(perKey, total int) WaiterOption {
	return func(o *waiterOptions) {
		o.maxPerKey = perKey
		o.maxTotal = total
	}
}

// Waiter is the read side of read-your-writes (ADR 0040): WaitFor waits,
// bounded by a deadline, until a projection has applied a token's position.
//
// One Waiter is shared by every request in a process. It caches checkpoints in
// a hub keyed by (scope, projection) and runs a single lazy refresh worker
// that owns every fallback store read. Advance and SetStatus feed it from the
// projection runner in this process; EnqueueScopeRefresh and
// EnqueueRefreshAll feed it hints from a cross-process listener. Hints carry
// no position, so a forged hint can cost a store read but can never produce a
// false "reached".
//
// A Waiter is safe for concurrent use. Call Close when the process is done
// with it to stop the refresh worker.
type Waiter struct {
	store ProgressStore
	opts  waiterOptions

	mu      sync.Mutex
	keys    map[progressKey]*waiterKey
	total   int
	closed  bool
	started bool

	closeOnce   sync.Once
	closeCh     chan struct{}
	doneCh      chan struct{}
	workerCtx   context.Context
	workerStop  context.CancelFunc
	scopeHints  chan string
	refreshAll  chan struct{}
	refreshErrs atomic.Uint64
}

// waiterKey is one (scope, projection)'s cached checkpoint, its registered
// waiter count and its broadcast channel. A wake closes the channel and
// installs a fresh one, so every waiter sees the broadcast. gen changes on
// every Advance/SetStatus; the refresher uses it to discard a store read that
// overlapped a newer publisher value.
type waiterKey struct {
	cp      Checkpoint
	found   bool
	gen     uint64
	waiters int
	wake    chan struct{}
}

// NewWaiter returns a Waiter over store. The refresh worker starts on the
// first WaitFor that actually has to wait. A nil store is treated as an empty
// store, which is useful for Advance-only tests.
func NewWaiter(store ProgressStore, opts ...WaiterOption) *Waiter {
	o := waiterOptions{
		fallback:  defaultFallback,
		maxPerKey: defaultMaxWaitersPerKey,
		maxTotal:  defaultMaxWaitersTotal,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.fallback <= 0 {
		o.fallback = defaultFallback
	}
	if o.maxPerKey <= 0 {
		o.maxPerKey = defaultMaxWaitersPerKey
	}
	if o.maxTotal <= 0 {
		o.maxTotal = defaultMaxWaitersTotal
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Waiter{
		store:      store,
		opts:       o,
		keys:       make(map[progressKey]*waiterKey),
		closeCh:    make(chan struct{}),
		doneCh:     make(chan struct{}),
		workerCtx:  ctx,
		workerStop: cancel,
		scopeHints: make(chan string, scopeHintQueue),
		refreshAll: make(chan struct{}, 1),
	}
}

// WaitFor waits until the projection has applied tok.Position in scope, or the
// deadline (default 500ms, clamped at 5s) or ctx ends. A timeout is
// Reached:false with a nil error; store errors are returned unless the Waiter
// was created WithBestEffort. A zero token is reached immediately without any
// store access.
func (w *Waiter) WaitFor(ctx context.Context, scope, projection string, tok es.Token, timeout time.Duration) (WaitResult, error) {
	start := time.Now()

	if tok == (es.Token{}) {
		return WaitResult{Reached: true, Waited: time.Since(start)}, nil
	}
	if len(tok.Scope) > maxWaitScope {
		return WaitResult{Waited: time.Since(start)}, fmt.Errorf("%w: scope length %d exceeds %d", es.ErrInvalidToken, len(tok.Scope), maxWaitScope)
	}
	if tok.Scope != scope {
		return WaitResult{Waited: time.Since(start)}, fmt.Errorf("%w: token scope %q, caller scope %q", es.ErrScopeMismatch, tok.Scope, scope)
	}
	if err := ctx.Err(); err != nil {
		return WaitResult{Waited: time.Since(start)}, err
	}

	timeout = normalizeWaitTimeout(timeout)
	deadline := start.Add(timeout)
	key := progressKey{scope: scope, projection: projection}

	// The cap check comes before any store access: an overloaded hub fails
	// open immediately rather than paying for a read it will throw away.
	if res, over := w.capResult(key, start); over {
		return res, nil
	}

	cp, found, err := w.Checkpoint(ctx, scope, projection)
	if err != nil {
		if w.opts.bestEffort {
			return WaitResult{Waited: time.Since(start)}, nil
		}
		return WaitResult{Waited: time.Since(start)}, err
	}
	if res, decided := waitDecision(start, cp, found, tok); decided {
		return res, nil
	}

	// Register, then re-check: an Advance may have landed between the fast
	// path and registration.
	w.mu.Lock()
	k, ok := w.keys[key]
	perKey := 0
	if ok {
		perKey = k.waiters
	}
	if w.total >= w.opts.maxTotal || perKey >= w.opts.maxPerKey {
		var cached Checkpoint
		if ok && k.found {
			cached = k.cp
		}
		w.mu.Unlock()
		return WaitResult{Reached: false, Checkpoint: cached, Waited: time.Since(start), Status: cached.State}, nil
	}
	if !ok {
		k = &waiterKey{wake: make(chan struct{})}
		w.keys[key] = k
	} else if found && !k.found {
		// Seed a key another path created while our store read was in flight.
		k.cp, k.found = cp, true
	}
	k.waiters++
	w.total++
	w.mu.Unlock()

	w.start()
	defer w.unregister(key)

	remaining := time.Until(deadline)
	if remaining <= 0 {
		cp, found, _ = w.snapshot(key)
		if res, decided := waitDecision(start, cp, found, tok); decided {
			return res, nil
		}
		return timeoutResult(start, cp, timeout), nil
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()

	for {
		cp, found, wake := w.snapshot(key)
		if res, decided := waitDecision(start, cp, found, tok); decided {
			return res, nil
		}
		select {
		case <-wake:
			// The refresher or a publisher woke us; compare again.
		case <-ctx.Done():
			return WaitResult{Reached: false, Checkpoint: cp, Waited: time.Since(start), Status: cp.State}, ctx.Err()
		case <-timer.C:
			// A publisher may have won the race with the timer; prefer the
			// checkpoint it installed over a bare timeout.
			cp, found, _ = w.snapshot(key)
			if res, decided := waitDecision(start, cp, found, tok); decided {
				return res, nil
			}
			return timeoutResult(start, cp, timeout), nil
		}
	}
}

// Checkpoint returns the cached checkpoint for (scope, projection). On a cache
// miss it reads the store once and caches a found row. found is false when no
// row exists.
func (w *Waiter) Checkpoint(ctx context.Context, scope, projection string) (Checkpoint, bool, error) {
	key := progressKey{scope: scope, projection: projection}

	w.mu.Lock()
	if k := w.keys[key]; k != nil {
		cp, found := k.cp, k.found
		w.mu.Unlock()
		return cp, found, nil
	}
	w.mu.Unlock()

	cp, found, err := w.load(ctx, scope, projection)
	if err != nil {
		return Checkpoint{}, false, err
	}
	if found {
		cp.Scope, cp.Projection = scope, projection
	} else {
		cp = Checkpoint{}
	}

	w.mu.Lock()
	if k := w.keys[key]; k != nil {
		// Another goroutine cached while our read was in flight; its value is
		// at least as fresh as ours.
		cp, found = k.cp, k.found
		w.mu.Unlock()
		return cp, found, nil
	}
	if found {
		w.keys[key] = &waiterKey{cp: cp, found: true, wake: make(chan struct{})}
	}
	w.mu.Unlock()
	return cp, found, nil
}

// Status returns the projection's cached state, reading the store when the
// cache is cold. An absent row is StatusUnknown with a nil error.
func (w *Waiter) Status(ctx context.Context, scope, projection string) (Status, error) {
	cp, found, err := w.Checkpoint(ctx, scope, projection)
	if err != nil {
		return StatusUnknown, err
	}
	if !found {
		return StatusUnknown, nil
	}
	return cp.State, nil
}

// Advance implements ProgressPublisher. The runner calls it only after a
// durable SaveProgress, so cp is authoritative. A lower position in the same
// state is ignored (the store itself never moves backwards within a state); a
// state change replaces the position, because a rebuild legitimately restarts
// it. Waiters on the key are woken.
func (w *Waiter) Advance(cp Checkpoint) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := progressKey{scope: cp.Scope, projection: cp.Projection}
	k := w.keys[key]
	if k == nil {
		k = &waiterKey{wake: make(chan struct{})}
		w.keys[key] = k
	} else if k.found && k.cp.State == cp.State && cp.Position < k.cp.Position {
		return
	}
	k.cp, k.found = cp, true
	k.gen++
	w.broadcastLocked(k)
}

// SetStatus implements ProgressPublisher. It changes the cached state without
// moving the position and wakes that key's waiters.
func (w *Waiter) SetStatus(scope, projection string, state Status) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := progressKey{scope: scope, projection: projection}
	k := w.keys[key]
	if k == nil {
		k = &waiterKey{wake: make(chan struct{})}
		w.keys[key] = k
	}
	k.cp.Scope, k.cp.Projection = scope, projection
	k.cp.State = state
	k.found = true
	k.gen++
	w.broadcastLocked(k)
}

// EnqueueScopeRefresh reports that a scope may have new progress. It is a
// non-blocking hint from a Postgres listener; it carries no position and is
// dropped when nothing local is waiting for that scope. Any dropped hint is
// repaired by the jittered ticker.
func (w *Waiter) EnqueueScopeRefresh(scope string) {
	w.mu.Lock()
	closed := w.closed
	active := false
	if !closed {
		for key, k := range w.keys {
			if key.scope == scope && k.waiters > 0 {
				active = true
				break
			}
		}
	}
	w.mu.Unlock()
	if closed || !active {
		return
	}
	select {
	case w.scopeHints <- scope:
	default:
		// Queue full: drop the hint; the ticker repairs every active key.
	}
}

// EnqueueRefreshAll reports that the listener reconnected and every active key
// may have been missed. It is non-blocking and bounded; the ticker is the
// repair path if the signal is coalesced away.
func (w *Waiter) EnqueueRefreshAll() {
	w.mu.Lock()
	closed, active := w.closed, w.total > 0
	w.mu.Unlock()
	if closed || !active {
		return
	}
	select {
	case w.refreshAll <- struct{}{}:
	default:
	}
}

// Close stops the refresh worker and waits for it to exit when it was started.
// It does not cancel in-flight WaitFor calls: they remain bounded by their own
// deadlines and contexts.
func (w *Waiter) Close() {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		started := w.started
		w.mu.Unlock()

		close(w.closeCh)
		w.workerStop()
		if started {
			<-w.doneCh
		}
	})
}

// start launches the one refresh worker, at most once and never after Close.
func (w *Waiter) start() {
	w.mu.Lock()
	if w.closed || w.started {
		w.mu.Unlock()
		return
	}
	w.started = true
	w.mu.Unlock()
	go w.run()
}

// run is the refresh worker: it is the only place that reads the store for
// blocked waiters. It refreshes the keys that currently have waiters on every
// hint, reconnect signal and jittered tick.
func (w *Waiter) run() {
	defer close(w.doneCh)

	timer := time.NewTimer(w.tickInterval())
	defer timer.Stop()
	for {
		select {
		case <-w.closeCh:
			return
		case scope := <-w.scopeHints:
			w.refreshKeys(w.activeKeys(scope, false))
		case <-w.refreshAll:
			w.refreshKeys(w.activeKeys("", true))
		case <-timer.C:
			w.refreshKeys(w.activeKeys("", true))
			timer.Reset(w.tickInterval())
		}
	}
}

// tickInterval returns the next fallback delay, jittered by ±20%.
func (w *Waiter) tickInterval() time.Duration {
	d := time.Duration(float64(w.opts.fallback) * (0.8 + 0.4*rand.Float64()))
	if d <= 0 {
		// A multiplication that overflows time.Duration lands here.
		d = w.opts.fallback
	}
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

// activeKeys snapshots the keys that currently have registered waiters. all
// selects every key; otherwise only keys in scope are returned. Snapshotting
// keeps store reads outside the hub lock.
func (w *Waiter) activeKeys(scope string, all bool) []progressKey {
	w.mu.Lock()
	defer w.mu.Unlock()
	keys := make([]progressKey, 0, len(w.keys))
	for key, k := range w.keys {
		if k.waiters == 0 {
			continue
		}
		if !all && key.scope != scope {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

// refreshKeys reads each key's authoritative checkpoint once. A read error is
// counted and ignored: the next hint or tick retries.
func (w *Waiter) refreshKeys(keys []progressKey) {
	for _, key := range keys {
		w.refreshKey(key)
	}
}

// refreshKey re-reads one key and installs the store's checkpoint. Unlike
// Advance it can move the cached position backwards: the store is the source
// of truth, so a forged or stale publisher value is corrected here. A read
// that overlapped a newer Advance/SetStatus is discarded, because that
// publisher value is newer than the snapshot the read returned.
func (w *Waiter) refreshKey(key progressKey) {
	w.mu.Lock()
	var gen uint64
	if k := w.keys[key]; k != nil {
		gen = k.gen
	}
	w.mu.Unlock()

	cp, found, err := w.load(w.workerCtx, key.scope, key.projection)
	if err != nil {
		w.refreshErrs.Add(1)
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	k := w.keys[key]
	if k == nil || k.waiters == 0 || k.gen != gen {
		return
	}
	if found {
		cp.Scope, cp.Projection = key.scope, key.projection
		k.cp, k.found = cp, true
	}
	// A store miss keeps the cached checkpoint. Rows are not deleted on the
	// normal path, and keeping it avoids erasing a just-published advance when
	// a replica or a lagging read returns "no row".
	w.broadcastLocked(k)
}

// load reads the store, treating a nil store as empty.
func (w *Waiter) load(ctx context.Context, scope, projection string) (Checkpoint, bool, error) {
	if w.store == nil {
		return Checkpoint{}, false, nil
	}
	return w.store.LoadProgress(ctx, scope, projection)
}

// capResult reports whether registering one more waiter would exceed a cap,
// and if so the fail-open result. It never touches the store.
func (w *Waiter) capResult(key progressKey, start time.Time) (WaitResult, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	k := w.keys[key]
	perKey := 0
	if k != nil {
		perKey = k.waiters
	}
	if w.total < w.opts.maxTotal && perKey < w.opts.maxPerKey {
		return WaitResult{}, false
	}
	var cached Checkpoint
	if k != nil && k.found {
		cached = k.cp
	}
	return WaitResult{Reached: false, Checkpoint: cached, Waited: time.Since(start), Status: cached.State}, true
}

// snapshot returns the current cached checkpoint and wake channel for a key.
func (w *Waiter) snapshot(key progressKey) (Checkpoint, bool, <-chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	k := w.keys[key]
	if k == nil {
		return Checkpoint{}, false, nil
	}
	return k.cp, k.found, k.wake
}

// unregister decrements the waiter counts. A key that never had a row is
// forgotten with its last waiter, so a later WaitFor re-reads the store rather
// than trusting a stale "absent" result.
func (w *Waiter) unregister(key progressKey) {
	w.mu.Lock()
	defer w.mu.Unlock()
	k := w.keys[key]
	if k == nil || k.waiters == 0 {
		return
	}
	k.waiters--
	w.total--
	if k.waiters == 0 && !k.found {
		delete(w.keys, key)
	}
}

// broadcastLocked wakes every waiter on k by closing its channel and
// installing a fresh one for the next generation.
func (w *Waiter) broadcastLocked(k *waiterKey) {
	close(k.wake)
	k.wake = make(chan struct{})
}

// waitDecision evaluates a checkpoint against a token. The state gate comes
// first: a rebuilding or stalled projection is never reported reached, even if
// its stale position is at or above the token.
func waitDecision(start time.Time, cp Checkpoint, found bool, tok es.Token) (WaitResult, bool) {
	if !found {
		return WaitResult{}, false
	}
	switch cp.State {
	case StatusRebuilding, StatusStalled:
		return WaitResult{Reached: false, Checkpoint: cp, Waited: time.Since(start), Status: cp.State}, true
	}
	if cp.Position >= tok.Position {
		return WaitResult{Reached: true, Checkpoint: cp, Waited: time.Since(start), Status: cp.State}, true
	}
	return WaitResult{}, false
}

// normalizeWaitTimeout applies the default and the 5s clamp.
func normalizeWaitTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultWaitTimeout
	}
	if d > maxWaitTimeout {
		return maxWaitTimeout
	}
	return d
}

// timeoutResult builds a timeout result whose Waited is never less than the
// timeout, even if a coarse timer fires marginally early.
func timeoutResult(start time.Time, cp Checkpoint, timeout time.Duration) WaitResult {
	waited := time.Since(start)
	if waited < timeout {
		waited = timeout
	}
	return WaitResult{Reached: false, Checkpoint: cp, Waited: waited, Status: cp.State}
}

var _ ProgressPublisher = (*Waiter)(nil)
