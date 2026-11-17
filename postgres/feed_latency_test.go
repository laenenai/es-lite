// End-to-end C1 latency and read-amplification evidence for ADR 0039's
// scoped wake-ups: the gate numbers Task 9 records in the ADR and Phase C
// (Postgres NOTIFY) is gated on. Everything here needs a live Postgres and
// skips without PG_DSN, like the rest of the suite:
//
//	task test:pg
//
// One test drives three shapes over the REAL feed adapters, 50 iterations
// each, one store per subtest:
//
//   - poll: Waiter nil. The baseline: an idle append waits out the 250ms
//     poll, so p50 must sit near Poll (>= 100ms keeps the baseline honest
//     on a loaded runner while a wake masquerading as a poll fails it).
//   - hub: Waiter is a fresh feed.Hub and the append goes through
//     es.WithAfterAppend, which wakes the hub with the workspace scope
//     after the append transaction commits. A clean-fence append must
//     deliver in single-digit milliseconds — the gate number: Phase C does
//     not start unless this p50 < 10ms.
//   - contended: like hub, but each measured append is preceded by an open
//     transaction that has drawn an xid — a pinned fence — committed 50ms
//     after the append. The wake fires, but the fence holds the page empty
//     until the pinner settles, so delivery falls back to the poll: p50
//     must show the fence engaged (>= 50ms) while p99 stays bounded by the
//     poll (<= 2*Poll + 150ms) — a pinned fence costs ~Poll, never a
//     stall.
//
// Two subscriptions run on the scope: one matching the appended type, and
// one consuming a type nothing appends (a null sink). The second never
// delivers — it exists so the wake's read amplification is measurable:
// every append wakes it too, and it draws a fence for an empty page. The
// hub subtest's draws/append minus the poll subtest's is that number.
//
// The draw counts come from countingLogs, which sums FenceDraws() (the F9
// counter, diagnostic only) across the logs the feed opened, so the logged
// amplification counts real fence transactions, not estimates.
package postgres_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/projection"
)

// The harness knobs, brief-verbatim: 50 iterations, a 250ms poll, and the
// 30ms park between iterations so the streams are idle-waiting when the
// append lands.
const (
	latIterations = 50
	latPoll       = 250 * time.Millisecond
	latPark       = 30 * time.Millisecond
	// The contended shape's pinned fence: the pinner commits this long
	// after the append.
	latPinSettle = 50 * time.Millisecond
	// The appended event's type URL; the non-matching subscription
	// consumes a type nothing ever appends.
	latType  = "latency.v1.Event"
	latOther = "latency.v1.Other"
	// waitDelivery's bound: generous over the poll-shaped subtests
	// (~350ms worst case) and over the hub's single-digit delivery.
	latDeliveryTimeout = 10 * time.Second
)

// percentile returns the p-th percentile of ds by the nearest-rank method:
// the smallest value whose rank is at least ceil(p/100 * n). p50 of 50
// sorted samples is the 25th; p99 rounds up to the last — the maximum.
func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), ds...)
	slices.Sort(sorted)
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// countingLogs wraps the feed's Logs and records every Log Open returns,
// so the test can sum FenceDraws() (the F9 counter) across the
// per-subscription logs the feed opened. Open's result is returned
// verbatim; nothing else changes.
type countingLogs struct {
	inner feed.Logs

	mu   sync.Mutex
	logs []feed.Log
}

// Open implements feed.Logs: record, then return.
func (c *countingLogs) Open(ctx context.Context, scope string) (feed.Log, error) {
	lg, err := c.inner.Open(ctx, scope)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.logs = append(c.logs, lg)
	c.mu.Unlock()
	return lg, nil
}

// fenceDraws sums FenceDraws across the recorded logs, via the diagnostic
// accessor the Postgres log (*Workspace) carries. The interface assertion
// keeps the sum honest if the adapter's concrete type ever changes: a log
// that cannot report contributes zero, and the harness fails loudly on a
// missing counter instead of logging a fabricated amplification.
func (c *countingLogs) fenceDraws() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total uint64
	for _, lg := range c.logs {
		if fd, ok := lg.(interface{ FenceDraws() uint64 }); ok {
			total += fd.FenceDraws()
		}
	}
	return total
}

// delivered is one delivery's receipt: when the sink saw the event, and
// the event's global position.
type delivered struct {
	at       time.Time
	position uint64
}

// envelopeSink is the matching subscription's sink: it timestamps each
// delivery and the position it carries, then hands both to the test loop.
type envelopeSink struct {
	ch chan delivered
}

// Send implements feed.Sink.
func (s envelopeSink) Send(ctx context.Context, env es.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d := delivered{at: time.Now(), position: env.GlobalPosition}
	select {
	case s.ch <- d:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// nullSink is the non-matching subscription's sink. Its type filter means
// the server never Sends; should it ever receive one anyway, the delivery
// is dropped on the floor — this stream exists to draw fence rounds, not
// to observe events.
type nullSink struct{}

// Send implements feed.Sink.
func (nullSink) Send(ctx context.Context, _ es.Envelope) error {
	return ctx.Err()
}

// subResult reports one subscription goroutine's exit, so the test loop
// can fail fast on a stream that died early instead of hanging on a
// delivery that will never arrive.
type subResult struct {
	projection string
	err        error
}

// fencePin is the contended shape's fence pinner: a transaction that has
// drawn an xid and holds it open, so every fence round a reader draws
// while it lives sees an xid in flight below its R — the page stays empty
// until the pinner settles. Exactly one settle runs: the delayed commit on
// the happy path, the rollback when the test ends early. Both take the
// mutex, so a cleanup racing the timer cannot double-settle a transaction.
type fencePin struct {
	mu        sync.Mutex
	tx        pgx.Tx
	settled   bool
	commitErr error
}

// openFencePin begins the pinner and draws its xid — the same statement
// the append path's P1 draw and the fence use, forcing xid assignment.
func openFencePin(t *testing.T, ctx context.Context, store *postgres.Store) *fencePin {
	t.Helper()
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin fence pinner: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_current_xact_id()`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("draw fence pinner xid: %v", err)
	}
	return &fencePin{tx: tx}
}

// settle ends the pinner exactly once: commit=true commits it (the delayed
// commit, 50ms after the append), commit=false rolls it back (the
// early-exit path). The second settle is a no-op.
func (p *fencePin) settle(commit bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.settled {
		return
	}
	p.settled = true
	if commit {
		p.commitErr = p.tx.Commit(context.Background())
		return
	}
	_ = p.tx.Rollback(context.Background())
}

// commitAfter arms the delayed commit that releases the fence.
func (p *fencePin) commitAfter(d time.Duration) {
	time.AfterFunc(d, func() { p.settle(true) })
}

// commitError reports the delayed commit's outcome once it has settled.
func (p *fencePin) commitError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.settled {
		return fmt.Errorf("fence pinner has not settled")
	}
	return p.commitErr
}

// runWakeLatency runs one shape: a fresh store, workspace, hub and server;
// the two subscriptions; one warm-up append; then latIterations measured
// append→delivery latencies, acking each delivery under the matching
// subscription's owner. It returns the samples' p50/p99 and the fence draws
// per append — countingLogs's total across both subscriptions divided by
// the iteration count — and logs the same numbers the ADR records.
//
// wake wires the ADR 0039 shape: the hub is the Waiter and the append
// goes through es.WithAfterAppend, so hub.Wake fires with the workspace
// scope after the append transaction commits. pinFence adds the contended
// shape's pinner: one open xact drawn before each append, committed 50ms
// after it.
func runWakeLatency(t *testing.T, name string, wake, pinFence bool) (p50, p99 time.Duration, drawsPerAppend float64) {
	t.Helper()
	ctx := context.Background()

	store := openStore(t)
	wsID := "lat-" + name
	ws := store.WorkspaceStore(wsID)
	hub := feed.NewHub()

	logs := &countingLogs{inner: postgres.NewLogs(store)}
	var waiter feed.Waiter
	appendStore := es.Store(ws)
	if wake {
		waiter = hub
		appendStore = es.WithAfterAppend(ws, func(context.Context, es.AppendResult) { hub.Wake(wsID) })
	}
	srv, err := feed.NewServer(feed.Config{
		Checkpoints: postgres.NewCheckpointStore(store),
		Leases:      postgres.NewLeaseStore(store),
		Logs:        logs,
		Waiter:      waiter,
		Poll:        latPoll,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	def := projection.Definition{Name: "latency.view", Consumes: []string{latType}}
	otherDef := projection.Definition{Name: "latency.other", Consumes: []string{latOther}}
	ackKey := feed.Ack{Scope: wsID, Projection: "latency.view", Instance: "i", Owner: "latency-owner", Definition: def.Hash()}
	matchReq := feed.Request{
		Scope:      wsID,
		Projection: "latency.view",
		Instance:   "i",
		Owner:      "latency-owner",
		Definition: def,
	}
	otherReq := feed.Request{
		Scope:      wsID,
		Projection: "latency.other",
		Instance:   "i",
		Definition: otherDef,
	}

	// Subscribe both in goroutines; the cleanup cancels the streams and
	// waits for Subscribe to return (it releases both leases) before the
	// openStore cleanup closes the pool.
	var subWg sync.WaitGroup
	subResults := make(chan subResult, 2)
	deliveries := make(chan delivered, latIterations+8)
	subCtx, cancelSubs := context.WithCancel(ctx)
	subscribe := func(req feed.Request, sink feed.Sink) {
		subWg.Add(1)
		go func() {
			defer subWg.Done()
			subResults <- subResult{projection: req.Projection, err: srv.Subscribe(subCtx, req, sink)}
		}()
	}
	subscribe(matchReq, envelopeSink{ch: deliveries})
	subscribe(otherReq, nullSink{})
	t.Cleanup(func() {
		cancelSubs()
		subWg.Wait()
	})

	sid := feedStream(t, "wake-"+name)

	// waitDelivery waits for the event at want and fails fast on a dead
	// stream or a stalled delivery rather than hanging the subtest.
	waitDelivery := func(want uint64) delivered {
		t.Helper()
		timeout := time.NewTimer(latDeliveryTimeout)
		defer timeout.Stop()
		for {
			select {
			case d := <-deliveries:
				if d.position != want {
					t.Fatalf("delivered position %d, want %d (the just-appended event)", d.position, want)
				}
				return d
			case r := <-subResults:
				t.Fatalf("subscription %s ended early: %v", r.projection, r.err)
			case <-timeout.C:
				t.Fatalf("no delivery of position %d within %v", want, latDeliveryTimeout)
			}
		}
	}

	appendOne := func(version uint64) es.AppendResult {
		t.Helper()
		res, err := appendStore.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: version,
			Events:          []es.EventData{{TypeURL: latType, SchemaVersion: 1, Payload: []byte("x")}},
		})
		if err != nil {
			t.Fatalf("append at v%d: %v", version, err)
		}
		return res
	}

	ack := func(position uint64) {
		t.Helper()
		a := ackKey
		a.Position = position
		if err := srv.Checkpoint(ctx, a); err != nil {
			t.Fatalf("ack at %d: %v", position, err)
		}
	}

	// Warm up: one append/delivery (subscription establishment, pool and
	// planner warm-up) before anything is measured.
	warm := appendOne(0)
	version := warm.ToVersion
	d := waitDelivery(warm.Envelopes[0].GlobalPosition)
	ack(d.position)

	// The measurement's own wiring check: the delivered warm-up event
	// crossed a fence round, so the opened logs must be able to report at
	// least one draw. A log that cannot (the adapter's concrete type lost
	// FenceDraws) would log a fabricated draws/append of zero.
	if logs.fenceDraws() == 0 {
		t.Fatalf("the opened feed logs report no fence draws: the warm-up delivery crossed a fence round, so countingLogs must see at least one")
	}
	drawsBefore := logs.fenceDraws()

	samples := make([]time.Duration, 0, latIterations)
	for i := 0; i < latIterations; i++ {
		// Park the streams first: the wake must be what delivers, not a
		// poll that happens to be due.
		time.Sleep(latPark)

		var pin *fencePin
		if pinFence {
			pin = openFencePin(t, ctx, store)
			// Roll the pinner back if the test ends before the delayed
			// commit fires; a settled pinner makes this a no-op.
			t.Cleanup(func() { pin.settle(false) })
		}

		appendAt := time.Now()
		res := appendOne(version)
		version = res.ToVersion
		want := res.Envelopes[0].GlobalPosition
		if pin != nil {
			pin.commitAfter(latPinSettle)
		}

		d = waitDelivery(want)
		if pin != nil {
			if err := pin.commitError(); err != nil {
				t.Fatalf("fence pinner commit: %v", err)
			}
		}
		samples = append(samples, d.at.Sub(appendAt))
		ack(d.position)
	}

	drawsAfter := logs.fenceDraws()
	p50 = percentile(samples, 50)
	p99 = percentile(samples, 99)
	drawsPerAppend = float64(drawsAfter) / float64(latIterations)
	steady := float64(drawsAfter-drawsBefore) / float64(latIterations)
	t.Logf("wake-latency/%s: p50=%v p99=%v fence draws/append=%.2f (%d draws over %d appends; steady-state %.2f/append)",
		name, p50, p99, drawsPerAppend, drawsAfter, latIterations, steady)
	return p50, p99, drawsPerAppend
}

// TestFeedWakeLatency is ADR 0039's gate evidence: the poll baseline is
// real, a scoped wake delivers an idle append in single-digit
// milliseconds on a clean fence, a pinned fence pushes delivery back to
// the poll without stalling past it, and the wake's read amplification is
// measured as draws per append.
func TestFeedWakeLatency(t *testing.T) {
	if os.Getenv("PG_DSN") == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests (task test:pg starts a throwaway Postgres)")
	}

	t.Run("poll", func(t *testing.T) {
		p50, _, _ := runWakeLatency(t, "poll", false, false)
		if p50 < 100*time.Millisecond {
			t.Fatalf("poll: p50=%v, want >= 100ms — the poll baseline must be real", p50)
		}
	})

	t.Run("hub", func(t *testing.T) {
		p50, _, _ := runWakeLatency(t, "hub", true, false)
		if p50 >= 10*time.Millisecond {
			t.Fatalf("hub: p50=%v, want < 10ms — the ADR 0039 gate: a scoped wake must deliver an idle append in single-digit milliseconds on a clean fence", p50)
		}
	})

	t.Run("contended", func(t *testing.T) {
		p50, p99, _ := runWakeLatency(t, "contended", true, true)
		if p50 < latPinSettle {
			t.Fatalf("contended: p50=%v, want >= %v — the pinned fence must engage (delivery only after the pinner settles)", p50, latPinSettle)
		}
		if max := 2*latPoll + 150*time.Millisecond; p99 > max {
			t.Fatalf("contended: p99=%v, want <= %v — a pinned fence costs ~Poll, never a stall", p99, max)
		}
	})
}
