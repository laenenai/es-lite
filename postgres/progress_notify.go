package postgres

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// The checkpoint hint (ADR 0040, read-your-writes) reuses the C2 wake
// shape but not its transaction discipline. Two decisions are load-
// bearing:
//
//   - The hint is emitted after the save commits, never inside its
//     transaction. A hint can therefore never fail or roll back a
//     checkpoint write; a lost hint only costs the waiter one poll.
//   - The payload is the scope alone — no position, no state. A forged
//     or spoofed notification can trigger a store read but can never
//     make a waiter claim progress the store does not hold.
const (
	// progressNotifyInterval is the coalescing window: one flush per
	// window at most, so a projector's burst of saves (or a batch save's
	// many advances) becomes one notification per scope.
	progressNotifyInterval = 10 * time.Millisecond

	// progressNotifyBatchMax bounds a single flush: reaching it flushes
	// immediately instead of waiting out the window. It keeps the
	// statement and its text[] argument modest without a second queue.
	progressNotifyBatchMax = 512

	// progressNotifyFlushTimeout bounds the final best-effort flush
	// Close runs, so a database that stopped answering cannot hang a
	// shutdown on a hint nobody needs.
	progressNotifyFlushTimeout = 5 * time.Second

	// progressNotifySQL is the whole flush: one statement, one round
	// trip, one array parameter. Postgres expands the array into a row
	// per scope for pg_notify; the channel is bound, not interpolated.
	progressNotifySQL = `SELECT pg_notify($1, s) FROM unnest($2::text[]) AS s`
)

// ScopeRefresher is the receiving side of a cross-process checkpoint
// hint. It is projection.Waiter's two wiring methods, expressed here so
// postgres stays independent of the projection package's concrete type:
// both calls are bounded, non-blocking queue writes — the store read
// happens on the waiter's own worker, never on the listener goroutine
// (NotifyHandler's contract).
type ScopeRefresher interface {
	// EnqueueScopeRefresh reports that scope may have new progress. The
	// payload is a hint only; the position always comes from the store.
	EnqueueScopeRefresh(scope string)
	// EnqueueRefreshAll reports that the listener reconnected and every
	// active scope may have missed a hint.
	EnqueueRefreshAll()
}

// NewProgressListener returns a NotifyListener that forwards checkpoint
// hints to r: OnNotify passes the raw payload — the scope — to
// EnqueueScopeRefresh, and a reconnect calls EnqueueRefreshAll to
// repair every hint missed while the connection was down.
//
// It is a thin wrapper: channel validation, the dedicated LISTEN
// connection, reconnection backoff and the per-scope throttle are all
// NotifyListener's. The handler must not block, so both refresher calls
// must be non-blocking — projection.Waiter's are. A nil refresher is a
// wiring bug and panics here rather than as a nil dereference on the
// listener goroutine.
func NewProgressListener(dsn, channel string, r ScopeRefresher) *NotifyListener {
	if r == nil {
		panic("postgres: NewProgressListener: refresher is nil")
	}
	return NewNotifyListener(dsn, channel, scopeRefreshHandler{r: r})
}

// scopeRefreshHandler adapts a ScopeRefresher to NotifyHandler.
type scopeRefreshHandler struct{ r ScopeRefresher }

func (h scopeRefreshHandler) OnNotify(scope string) { h.r.EnqueueScopeRefresh(scope) }
func (h scopeRefreshHandler) OnReconnected()        { h.r.EnqueueRefreshAll() }

// progressNotifyExec is the notifier's one database seam. Production
// binds it to the store's pool; tests inject a recorder or a failing
// executor, which is how the "a hint never fails a save" contract is
// observed without breaking a real database.
type progressNotifyExec func(ctx context.Context, sql string, args ...any) error

// progressNotifier coalesces checkpoint scopes and emits them as
// pg_notify hints. Mark is safe for concurrent use and never blocks on
// the database: it records the scope in a map and, at most, wakes the
// single flusher goroutine. The goroutine owns every statement.
type progressNotifier struct {
	channel  string
	exec     progressNotifyExec
	interval time.Duration
	maxBatch int

	// mu guards pending and closed. Mark runs on every successful save, so
	// it must stay cheap; the flusher swaps the map out under the same
	// lock, so a Mark during a flush lands in the next window instead of
	// being lost.
	mu      sync.Mutex
	pending map[string]struct{}
	closed  bool

	// wake is a capacity-1 doorbell: a mark that opens a window or fills
	// the batch signals it; concurrent signals coalesce.
	wake chan struct{}

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	// dropped counts hints that will never be delivered: an oversized
	// scope (Mark refuses to widen the payload) and a failed flush (the
	// batch is already gone; there is no retry). It is observability for
	// the runbook metric, not a recovery path — ProgressStore.NotifyDropped
	// exposes it.
	dropped atomic.Uint64
}

// newProgressNotifier starts the flusher goroutine. Callers must Close
// it; an empty channel never reaches here, because NewProgressStore
// disables the notifier entirely.
func newProgressNotifier(channel string, exec progressNotifyExec, interval time.Duration, maxBatch int) *progressNotifier {
	n := &progressNotifier{
		channel:  channel,
		exec:     exec,
		interval: interval,
		maxBatch: maxBatch,
		pending:  make(map[string]struct{}),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	n.wg.Add(1)
	go n.run()
	return n
}

// Mark records scope for the next flush. It never blocks and never
// touches the database: the flush is entirely the goroutine's. A nil
// notifier (the channel-disabled case) is a no-op.
//
// A scope over maxNotifyWorkspaceBytes would overflow the NOTIFY payload
// bound. Enabling a hint must not change the save contract, so the hint
// is skipped and counted as dropped instead of the save failing. Mark
// after Close is a no-op: the goroutine is gone and nothing may queue
// behind it.
func (n *progressNotifier) Mark(scope string) {
	if n == nil {
		return
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	if len(scope) > maxNotifyWorkspaceBytes {
		n.dropped.Add(1)
		n.mu.Unlock()
		return
	}
	opened := len(n.pending) == 0
	n.pending[scope] = struct{}{}
	full := len(n.pending) >= n.maxBatch
	n.mu.Unlock()

	if opened || full {
		select {
		case n.wake <- struct{}{}:
		default:
		}
	}
}

// Close stops the flusher and joins it. The goroutine flushes whatever
// is still pending once, best-effort, so a save racing shutdown is not
// guaranteed lost (and a hint nobody needs cannot hang the join: the
// flush is bounded). Close is idempotent, and a Mark after it is a
// no-op.
func (n *progressNotifier) Close() error {
	if n == nil {
		return nil
	}
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.closed = true
		n.mu.Unlock()
		close(n.done)
	})
	n.wg.Wait()
	return nil
}

// run owns the flush schedule: the first mark opens a window, the window
// expires after interval, and a full batch flushes immediately. A mark
// during a flush lands in the fresh map and opens the next window.
func (n *progressNotifier) run() {
	defer n.wg.Done()

	var (
		timer *time.Timer
		fire  <-chan time.Time
	)
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer, fire = nil, nil
		}
	}
	defer stopTimer()

	for {
		select {
		case <-n.done:
			stopTimer()
			n.flush()
			return
		case <-n.wake:
			if n.full() {
				stopTimer()
				n.flush()
				continue
			}
			if n.empty() {
				// A doorbell whose batch was already flushed by the
				// window; nothing to schedule.
				continue
			}
			if timer == nil {
				timer = time.NewTimer(n.interval)
				fire = timer.C
			}
		case <-fire:
			timer, fire = nil, nil
			n.flush()
		}
	}
}

// flush emits the pending set in one statement and drops it. The set is
// swapped out before the statement runs, so a Mark during the round trip
// is kept for the next window. An error increments dropped and the batch
// is gone: the checkpoint row is already committed and a hint is
// optional, so retrying a failed wake would only repeat a failure.
func (n *progressNotifier) flush() {
	n.mu.Lock()
	if len(n.pending) == 0 {
		n.mu.Unlock()
		return
	}
	scopes := make([]string, 0, len(n.pending))
	for scope := range n.pending {
		scopes = append(scopes, scope)
	}
	n.pending = make(map[string]struct{}, len(scopes))
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), progressNotifyFlushTimeout)
	defer cancel()
	if err := n.exec(ctx, progressNotifySQL, n.channel, scopes); err != nil {
		n.dropped.Add(1)
	}
}

func (n *progressNotifier) full() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.pending) >= n.maxBatch
}

func (n *progressNotifier) empty() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.pending) == 0
}
