package feed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

// The stubs exist only to satisfy NewServer's dependency checks; no method is
// called by the tests in this file.
type stubCheckpoints struct{}

func (stubCheckpoints) Load(context.Context, string, string, string) (uint64, error) {
	return 0, nil
}
func (stubCheckpoints) Save(context.Context, string, string, string, string, uint64) error {
	return nil
}
func (stubCheckpoints) Reset(context.Context, string, string, string, string, uint64) error {
	return nil
}
func (stubCheckpoints) Delete(context.Context, string, string, string) error { return nil }
func (stubCheckpoints) Checkpoint(context.Context, string, string, string) (Checkpoint, bool, error) {
	return Checkpoint{}, false, nil
}

type stubLeases struct{}

func (stubLeases) Acquire(context.Context, string, string, string, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (stubLeases) Renew(context.Context, string, string, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (stubLeases) Release(context.Context, string, string, string, string) error { return nil }
func (stubLeases) Holder(context.Context, string, string, string) (Lease, bool, error) {
	return Lease{}, false, nil
}

type stubLogs struct{ log Log }

func (s stubLogs) Open(context.Context, string) (Log, error) { return s.log, nil }

func TestNewServerNormalizesRenewEvery(t *testing.T) {
	base := Config{Checkpoints: stubCheckpoints{}, Leases: stubLeases{}, Logs: stubLogs{}}
	cases := []struct {
		name       string
		ttl, renew time.Duration
		want       time.Duration
	}{
		{"defaults leave renew below ttl", 0, 0, defaultRenewEvery},
		{"equal is reduced to a third", 30 * time.Second, 30 * time.Second, 10 * time.Second},
		{"greater is reduced to a third", 9 * time.Second, 20 * time.Second, 3 * time.Second},
		{"valid explicit interval is kept", 30 * time.Second, 5 * time.Second, 5 * time.Second},
		{"sub-nanosecond third falls back to a half", 2 * time.Nanosecond, 2 * time.Nanosecond, time.Nanosecond},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := base
			cfg.LeaseTTL, cfg.RenewEvery = c.ttl, c.renew
			srv, err := NewServer(cfg)
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			if srv.cfg.RenewEvery != c.want {
				t.Fatalf("RenewEvery = %v, want %v", srv.cfg.RenewEvery, c.want)
			}
			if srv.cfg.RenewEvery >= srv.cfg.LeaseTTL {
				t.Fatalf("RenewEvery = %v is not below LeaseTTL = %v", srv.cfg.RenewEvery, srv.cfg.LeaseTTL)
			}
		})
	}
}

func TestNewServerRejectsAnUnrenewableLeaseTTL(t *testing.T) {
	cfg := Config{
		Checkpoints: stubCheckpoints{}, Leases: stubLeases{}, Logs: stubLogs{},
		LeaseTTL: time.Nanosecond, RenewEvery: time.Nanosecond,
	}
	if _, err := NewServer(cfg); err == nil {
		t.Fatal("NewServer accepted a 1ns lease TTL it cannot renew")
	}
}

// Without entropy no safe owner can be minted: a constant prefix would
// collide across processes. NewServer must propagate the failure, not mask it.
func TestNewPrefixPropagatesEntropyError(t *testing.T) {
	cause := errors.New("entropy source failed")
	orig := randRead
	randRead = func([]byte) (int, error) { return 0, cause }
	t.Cleanup(func() { randRead = orig })

	_, err := NewServer(Config{Checkpoints: stubCheckpoints{}, Leases: stubLeases{}, Logs: stubLogs{}})
	if err == nil {
		t.Fatal("NewServer minted a prefix without entropy; want an error")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want the entropy cause", err)
	}
}

// Negative MaxPayloadBytes is kept as-is (the stream treats it as "check
// disabled"); negative MaxBatchBytes is replaced by the default, because the
// Log contract requires a positive budget.
func TestNewServerNegativeConfigSemantics(t *testing.T) {
	base := Config{Checkpoints: stubCheckpoints{}, Leases: stubLeases{}, Logs: stubLogs{}}

	cfg := base
	cfg.MaxPayloadBytes = -1
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv.cfg.MaxPayloadBytes != -1 {
		t.Fatalf("MaxPayloadBytes = %d, want -1 kept (negative disables the check)", srv.cfg.MaxPayloadBytes)
	}

	cfg = base
	cfg.MaxBatchBytes = -1
	srv, err = NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv.cfg.MaxBatchBytes != defaultMaxBatchBytes {
		t.Fatalf("MaxBatchBytes = %d, want the default %d", srv.cfg.MaxBatchBytes, defaultMaxBatchBytes)
	}
}

// ---------------------------------------------------------------------------
// Idle-wait jitter and Waiter floor (spec §5.12).
// ---------------------------------------------------------------------------

// idlePageLog serves empty pages and timestamps every ReadAllBounded call, so
// internal tests can observe the read loop's cadence: the gap between two
// consecutive reads spans exactly one loop iteration — the page read plus the
// idle wait. Recording is capped so a spinning (unfloored) loop cannot grow
// the slice without bound.
type idlePageLog struct {
	mu       sync.Mutex
	maxReads int
	reads    []time.Time
}

func newIdlePageLog(maxReads int) *idlePageLog {
	return &idlePageLog{maxReads: maxReads}
}

func (l *idlePageLog) ReadAllBounded(context.Context, uint64, int, int, []string) ([]es.Envelope, bool, error) {
	l.mu.Lock()
	if len(l.reads) < l.maxReads {
		l.reads = append(l.reads, time.Now())
	}
	l.mu.Unlock()
	return nil, false, nil
}

func (l *idlePageLog) Head(context.Context) (uint64, error) { return 0, nil }

func (l *idlePageLog) RawHead(context.Context) (uint64, error) { return 0, nil }

func (l *idlePageLog) MatchingHead(context.Context, []string) (uint64, error) { return 0, nil }

// MatchingRawHead serves the empty log's matching raw head: 0, like
// every other head view on this fake — it has no events and no fence.
func (l *idlePageLog) MatchingRawHead(context.Context, []string) (uint64, error) { return 0, nil }

func (l *idlePageLog) snapshot() []time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]time.Time(nil), l.reads...)
}

// instantWaiter is the degenerate Waiter the floor defends against: Wait
// records its timeout and returns immediately, never reporting a genuine
// wake. (false, nil) means "timed out without a wakeup" by the Waiter
// contract, so a stream that trusts it alone would spin the read loop
// against the log as fast as pages can be read.
type instantWaiter struct {
	mu   sync.Mutex
	args []time.Duration
}

func (w *instantWaiter) Wait(_ context.Context, timeout time.Duration) (bool, error) {
	w.mu.Lock()
	w.args = append(w.args, timeout)
	w.mu.Unlock()
	return false, nil
}

func (w *instantWaiter) snapshot() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Duration(nil), w.args...)
}

// nullSink accepts every send; the idle tests never deliver an event.
type nullSink struct{}

func (nullSink) Send(context.Context, es.Envelope) error { return nil }

// startIdleStream subscribes one stream over the stub stores and returns a
// stop function that cancels the stream and waits for Subscribe to return, so
// results read after stop are race-free.
func startIdleStream(t *testing.T, srv *Server) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	req := Request{
		Scope:      "tenant",
		Projection: "counter.view",
		Instance:   "idle",
		Consumer:   "internal-test",
		Definition: projection.Definition{Name: "counter.view", Consumes: []string{"counter.v1.Incremented"}},
	}
	go func() { done <- srv.Subscribe(ctx, req, nullSink{}) }()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Subscribe did not return after cancellation")
		}
	}
}

// waitForReads blocks until the log has recorded at least n reads or the
// deadline passes (returning false then).
func waitForReads(t *testing.T, log *idlePageLog, n int, deadline time.Duration) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		if got := len(log.snapshot()); got >= n {
			return true
		}
		if time.Now().After(end) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// TestWaitJittersIdlePoll pins the ±20% jitter on idle waits (spec §5.12): a
// fleet of streams started together must not poll in lockstep and micro-burst
// the pool. The jitter indirection is pinned to fixed factors so both paths
// are asserted exactly: the timer path (the fleet's — no Waiter configured)
// must actually WAIT the jittered duration, and the Waiter path must at least
// receive it as its timeout.
func TestWaitJittersIdlePoll(t *testing.T) {
	// Fixed factors at the ±20% bounds, alternating, drawn per idle wait.
	factors := []float64{0.8, 1.2}

	// pinJitter replaces the server's jitter with the pinned sequence and
	// returns snapshots of the inputs it received and outputs it produced.
	// jitter runs on the stream goroutine; the snapshots are read only after
	// stop() joined Subscribe, so plain slices are race-free.
	pinJitter := func(srv *Server) (inputs, outputs func() []time.Duration) {
		var in, out []time.Duration
		srv.jitter = func(d time.Duration) time.Duration {
			f := factors[len(out)%len(factors)]
			in = append(in, d)
			j := time.Duration(f * float64(d))
			out = append(out, j)
			return j
		}
		return func() []time.Duration { return append([]time.Duration(nil), in...) },
			func() []time.Duration { return append([]time.Duration(nil), out...) }
	}

	t.Run("timer path waits the jittered poll", func(t *testing.T) {
		const poll = 400 * time.Millisecond
		log := newIdlePageLog(64)
		srv, err := NewServer(Config{
			Checkpoints: stubCheckpoints{}, Leases: stubLeases{},
			Logs: stubLogs{log}, Poll: poll,
		})
		if err != nil {
			t.Fatal(err)
		}
		inputs, outputs := pinJitter(srv)

		stop := startIdleStream(t, srv)
		if !waitForReads(t, log, 5, 5*time.Second) {
			stop()
			t.Fatalf("only %d page reads in 5s, want at least 5", len(log.snapshot()))
		}
		stop()

		ins, outs := inputs(), outputs()
		if len(ins) == 0 {
			t.Fatal("jitter was never called: the idle wait ignores the jitter source")
		}
		for i, d := range ins {
			if d != poll {
				t.Fatalf("jitter input #%d = %v, want the raw Poll %v", i, d, poll)
			}
		}
		for i, j := range outs {
			lo, hi := time.Duration(0.8*float64(poll)), time.Duration(1.2*float64(poll))
			if j < lo || j > hi {
				t.Fatalf("jitter output #%d = %v, want within [0.8*Poll, 1.2*Poll] = [%v, %v]", i, j, lo, hi)
			}
			if want := time.Duration(factors[i%len(factors)] * float64(poll)); j != want {
				t.Fatalf("jitter output #%d = %v, want the pinned factor %v * Poll = %v", i, j, factors[i%len(factors)], want)
			}
		}

		// The wait spent the jittered duration. The gap between consecutive
		// page reads is one whole loop iteration; on the timer path the
		// timer is the idle wait, so the gap is the jittered poll plus the
		// µs-scale page read and scheduler overshoot (timers never fire
		// early). The slop stays far below Poll so an unjittered wait
		// (exactly Poll) fails the 0.8-factor windows.
		const slop = 55 * time.Millisecond
		reads := log.snapshot()
		for i := 0; i+1 < len(reads); i++ {
			gap, want := reads[i+1].Sub(reads[i]), outs[i]
			if gap < want || gap > want+slop {
				t.Fatalf("idle wait #%d waited %v, want the jittered poll %v (±%v): the idle poll is not jittered",
					i, gap, want, slop)
			}
		}
	})

	t.Run("waiter path receives the jittered poll", func(t *testing.T) {
		const poll = 200 * time.Millisecond
		log := newIdlePageLog(64)
		waiter := &instantWaiter{}
		srv, err := NewServer(Config{
			Checkpoints: stubCheckpoints{}, Leases: stubLeases{},
			Logs: stubLogs{log}, Waiter: waiter, Poll: poll,
		})
		if err != nil {
			t.Fatal(err)
		}
		inputs, _ := pinJitter(srv)

		stop := startIdleStream(t, srv)
		end := time.Now().Add(5 * time.Second)
		for {
			if got := len(waiter.snapshot()); got >= 5 {
				break
			}
			if time.Now().After(end) {
				stop()
				t.Fatalf("only %d Wait calls in 5s, want at least 5", len(waiter.snapshot()))
			}
			time.Sleep(time.Millisecond)
		}
		stop()

		ins := inputs()
		if len(ins) == 0 {
			t.Fatal("jitter was never called: the idle wait ignores the jitter source")
		}
		for i, d := range ins {
			if d != poll {
				t.Fatalf("jitter input #%d = %v, want the raw Poll %v", i, d, poll)
			}
		}
		// Every timeout the Waiter received is its pinned jittered poll —
		// exactly, so an unjittered Poll (inside [0.8, 1.2]·Poll only at
		// its own value) fails.
		args := waiter.snapshot()
		for i, got := range args {
			want := time.Duration(factors[i%len(factors)] * float64(poll))
			if got != want {
				t.Fatalf("Waiter timeout #%d = %v, want the jittered poll %v (%v * Poll)", i, got, want, factors[i%len(factors)])
			}
		}
	})
}

// TestWaiterFloorPreventsSpin pins the idle wait's floor: a Waiter returning
// nil immediately must not turn the read loop into a spin against the log.
// Whatever the Waiter did, one loop iteration — one page read to the next —
// has spent at least Poll by the time the next page is read: the server
// sleeps the remainder (spec §5.12). The upper bound pins that the floor
// sleeps ONLY the remainder — no second full poll on top of the wait.
func TestWaiterFloorPreventsSpin(t *testing.T) {
	const poll = 100 * time.Millisecond
	log := newIdlePageLog(64)
	waiter := &instantWaiter{}
	srv, err := NewServer(Config{
		Checkpoints: stubCheckpoints{}, Leases: stubLeases{},
		Logs: stubLogs{log}, Waiter: waiter, Poll: poll,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The floor is the subject: keep the jitter at the identity so the
	// bound asserts the floor, not the default random source.
	srv.jitter = func(d time.Duration) time.Duration { return d }

	stop := startIdleStream(t, srv)
	if !waitForReads(t, log, 6, 5*time.Second) {
		stop()
		t.Fatalf("only %d page reads in 5s, want at least 6 (the floor paces the idle loop at one read per poll)", len(log.snapshot()))
	}
	stop()

	// Every completed iteration spans at least Poll since it began (the
	// slack covers only the scheduler, never a missing floor: an unfloored
	// instant Waiter spins iterations orders of magnitude faster), and at
	// most Poll plus overshoot: sleeping a second full poll on top of the
	// wait would fail the upper bound.
	const (
		lowerSlack = 25 * time.Millisecond
		upperSlack = 75 * time.Millisecond
	)
	reads := log.snapshot()
	for i := 0; i+1 < len(reads); i++ {
		gap := reads[i+1].Sub(reads[i])
		if gap < poll-lowerSlack {
			t.Fatalf("idle iteration #%d spanned %v, want at least Poll %v (−%v slack): an instant Waiter spins the loop",
				i, gap, poll, lowerSlack)
		}
		if gap > poll+upperSlack {
			t.Fatalf("idle iteration #%d spanned %v, want at most Poll %v (+%v overshoot): the floor must sleep only the remainder",
				i, gap, poll, upperSlack)
		}
	}
	// The Waiter really is the instant one — the floor, not a blocked
	// Waiter, is what paces the loop.
	if n := len(waiter.snapshot()); n < 5 {
		t.Fatalf("Waiter was called %d times, want at least 5 (it returns instantly, so the loop keeps reaching it)", n)
	}
}
