package delivery

// Signal is a coalescing wake-up primitive. Producers call Notify after
// committing events; a Poller selects on C() to drain immediately
// instead of waiting for its next fallback tick.
//
// It is the seam that lets Postgres LISTEN/NOTIFY (or any push source)
// eliminate polling latency: a pg listener goroutine calls Notify on
// each NOTIFY, and the same Poller code that works on SQLite reacts with
// no changes. On SQLite the application simply calls Notify itself after
// a successful append.
//
// Notify never blocks: if a wake is already pending it is a no-op, so N
// rapid appends collapse into at most one extra drain. Delivery
// correctness never depends on a signal arriving — a missed Notify only
// delays events to the next fallback poll; the checkpoint guarantees
// nothing is skipped.
type Signal struct {
	ch chan struct{}
}

// NewSignal creates a Signal with a depth-1 coalescing buffer.
func NewSignal() *Signal {
	return &Signal{ch: make(chan struct{}, 1)}
}

// Notify requests a drain. Non-blocking and idempotent between drains.
func (s *Signal) Notify() {
	select {
	case s.ch <- struct{}{}:
	default:
	}
}

// C returns the channel a Poller waits on.
func (s *Signal) C() <-chan struct{} { return s.ch }
