//go:build feedgate

package postgres_test

// The notify gate (ADR 0039 Phase C): the chaos reconnect test and the
// append-cost benchmark that prove the opt-in Postgres NOTIFY wake (C2)
// is safe under a listener outage and free when off. Everything here
// needs a live Postgres: the file is build-tagged AND each test skips
// without PG_DSN, like feedgate_test.go, so a `-tags feedgate` run
// without a database skips rather than fails.
//
//	task test:feedgate
//
// The chaos test's premise is the ADR's failure model: the wake is a
// latency hint only — a missed, late or duplicated hint costs at most one
// poll of delay, never loss; the feed's fence is untouched. It kills the
// listener's real backend with pg_terminate_backend mid-run, keeps
// appending through the outage (so hints are genuinely missed, not just
// delayed), and demands the delivered set still equal the appended set
// exactly — the same set-equality oracle the no-loss chaos test uses.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/projection"
)

// ---------------------------------------------------------------------------
// TestFeedGateNotifyReconnect: the chaos scenario (brief Step 2).
// ---------------------------------------------------------------------------

const (
	// Fixed test fixtures, never logged; production channels are
	// high-entropy (docs/deploy/README.md).
	fgNotifyChaosChannel = "es_lite_gate_notify_chaos"
	fgNotifyChaosWS      = "ws_notify_chaos"
	// ~200 events with a ~5ms gap: long enough that the ~250ms reconnect
	// backoff swallows ~40 appends' hints mid-run — the outage must cost
	// delay only — short enough to keep the test under a few seconds.
	fgNotifyChaosEvents = 200
	fgNotifyChaosGap    = 5 * time.Millisecond
	fgNotifyChaosType   = "gate.v1.NotifyChaos"
	// The kill fires once this many events have been appended — mid-run,
	// with the rest of the producer's appends landing through the outage.
	fgNotifyChaosKillAt = 60
)

// fgNotifyWaker wires the NotifyListener to the chaos test's hub and
// records what it was called with — the gateway wiring ADR 0039 names
// (OnNotify → hub.Wake, OnReconnected → hub.WakeAll) plus the counters
// the oracle reads. The handler runs on the listener's goroutine and
// must not block; both hub calls are non-blocking.
type fgNotifyWaker struct {
	hub *feed.Hub

	mu         sync.Mutex
	notifies   int
	reconnects int
}

func (w *fgNotifyWaker) OnNotify(scope string) {
	w.mu.Lock()
	w.notifies++
	w.mu.Unlock()
	w.hub.Wake(scope)
}

func (w *fgNotifyWaker) OnReconnected() {
	w.mu.Lock()
	w.reconnects++
	w.mu.Unlock()
	// Hints emitted while the connection was down were missed: the
	// repair is a wake of every scope.
	w.hub.WakeAll()
}

func (w *fgNotifyWaker) notifyCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.notifies
}

func (w *fgNotifyWaker) reconnectCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.reconnects
}

// fgNotifySink is the chaos test's one subscriber: it records every
// delivery's global position and acks each one with a lease-fenced
// feed.Ack under its stable owner token, so the feed advances and the
// delivered set converges to the appended set.
type fgNotifySink struct {
	srv *feed.Server
	ack feed.Ack

	mu    sync.Mutex
	seen  map[uint64]struct{}
	dupes int
}

func (s *fgNotifySink) Send(ctx context.Context, env es.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if _, ok := s.seen[env.GlobalPosition]; ok {
		s.dupes++
	} else {
		s.seen[env.GlobalPosition] = struct{}{}
	}
	s.mu.Unlock()
	a := s.ack
	a.Position = env.GlobalPosition
	if err := s.srv.Checkpoint(ctx, a); err != nil {
		return fmt.Errorf("feedgate: notify chaos ack at %d: %w", env.GlobalPosition, err)
	}
	return nil
}

// matches reports whether the sink has delivered exactly the positions in
// want — set equality: no omission, no invention.
func (s *fgNotifySink) matches(want map[uint64]struct{}) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) != len(want) {
		return false
	}
	for p := range want {
		if _, ok := s.seen[p]; !ok {
			return false
		}
	}
	return true
}

func (s *fgNotifySink) snapshot() (seen map[uint64]struct{}, dupes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uint64]struct{}, len(s.seen))
	for p := range s.seen {
		out[p] = struct{}{}
	}
	return out, s.dupes
}

// killNotifyListener terminates the listener's current backend from a
// separate connection, exactly as a failover or an operator would — the
// real kill, never a fake listener.
func killNotifyListener(ctx context.Context, dsn string, pid uint32) error {
	if pid == 0 {
		return errors.New("listener PID is 0 (disconnected)")
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("kill connect: %w", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `SELECT pg_terminate_backend($1)`, int32(pid)); err != nil {
		return fmt.Errorf("terminate backend %d: %w", pid, err)
	}
	return nil
}

func TestFeedGateNotifyReconnect(t *testing.T) {
	dsn := fgDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// The store's append path emits the C2 hint on the chaos channel;
	// openStoreWithOptions truncates, so the run starts from a clean log.
	store := openStoreWithOptions(t, postgres.WithAppendNotify(fgNotifyChaosChannel))
	ws := store.WorkspaceStore(fgNotifyChaosWS)
	hub := feed.NewHub()

	srv, err := feed.NewServer(feed.Config{
		Checkpoints: postgres.NewCheckpointStore(store),
		Leases:      postgres.NewLeaseStore(store),
		Logs:        postgres.NewLogs(store),
		Waiter:      hub,
		// The poll is the safety net: 250ms, the production default. The
		// listener outage must cost delay only — everything the wake
		// misses reaches the subscriber through the poll or the
		// reconnect's WakeAll.
		Poll: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("feedgate: new server: %v", err)
	}

	def := projection.Definition{Name: "gate.notify.chaos", Consumes: []string{fgNotifyChaosType}}
	req := feed.Request{
		Scope:      fgNotifyChaosWS,
		Projection: "gate.notify.chaos",
		Instance:   "i",
		Consumer:   "feedgate-notify-chaos",
		// A stable owner token, so the acks are fenced to the lease the
		// one live subscription holds (spec §5.4).
		Owner:      "feedgate-notify-chaos-owner",
		Definition: def,
	}
	sink := &fgNotifySink{
		srv:  srv,
		ack:  feed.Ack{Scope: req.Scope, Projection: req.Projection, Instance: req.Instance, Owner: req.Owner, Definition: def.Hash()},
		seen: map[uint64]struct{}{},
	}

	// The subscription runs until the test ends; the cleanup cancels the
	// stream and joins it (releasing the lease) before the store closes.
	subCtx, cancelSub := context.WithCancel(ctx)
	subDone := make(chan error, 1)
	go func() { subDone <- srv.Subscribe(subCtx, req, sink) }()
	t.Cleanup(func() {
		cancelSub()
		select {
		case err := <-subDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("feedgate: notify chaos subscription ended with %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("feedgate: notify chaos subscription did not return within 10s of cancellation")
		}
	})

	// The listener: OnNotify → hub.Wake, OnReconnected → hub.WakeAll.
	waker := &fgNotifyWaker{hub: hub}
	listener := postgres.NewNotifyListener(dsn, fgNotifyChaosChannel, waker)
	lctx, cancelListener := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() { listenerDone <- listener.Run(lctx) }()
	t.Cleanup(func() {
		cancelListener()
		select {
		case <-listenerDone:
		case <-time.After(10 * time.Second):
			t.Error("feedgate: notify listener Run did not return within 10s of cancellation")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for listener.PID() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("feedgate: notify listener did not connect and LISTEN within 10s (PID still 0)")
		}
		time.Sleep(time.Millisecond)
	}

	// Wiring check before the chaos starts: one append must reach the
	// subscriber through the notify wake path itself. Without this, a
	// dead wake path would pass the whole test on polls alone.
	sid, err := es.NewStreamID("gate", "notify-chaos")
	if err != nil {
		t.Fatal(err)
	}
	// The oracle's want set: every event this test appends, warm-up
	// included — the subscription sees the whole log from position 0.
	appended := map[uint64]struct{}{}
	warm, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: fgNotifyChaosType, SchemaVersion: 1, Payload: fgPayload(sid, 1)}},
	})
	if err != nil {
		t.Fatalf("feedgate: warm-up append: %v", err)
	}
	appended[warm.Envelopes[0].GlobalPosition] = struct{}{}
	warmDeadline := time.Now().Add(10 * time.Second)
	for {
		if waker.notifyCount() >= 1 {
			break
		}
		if time.Now().After(warmDeadline) {
			t.Fatalf("feedgate: no OnNotify within 10s of the warm-up append — the notify wake path is dead; the chaos test would pass on polls alone")
		}
		time.Sleep(time.Millisecond)
	}
	for !sink.matches(map[uint64]struct{}{warm.Envelopes[0].GlobalPosition: {}}) {
		if time.Now().After(warmDeadline) {
			t.Fatalf("feedgate: warm-up event %d not delivered within 10s", warm.Envelopes[0].GlobalPosition)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// The killer: waits for the kill point (mid-run), then terminates the
	// listener's real backend from a separate connection and waits for
	// OnReconnected. The producer keeps appending through the whole
	// outage — that is what makes the outage cost MISSED hints, not just
	// delayed ones.
	killerDone := make(chan struct{})
	killerErr := make(chan error, 1)
	produced := &atomic.Int64{}
	go func() {
		defer close(killerDone)
		deadline := time.Now().Add(60 * time.Second)
		for produced.Load() < fgNotifyChaosKillAt {
			if time.Now().After(deadline) {
				killerErr <- errors.New("producer never reached the kill point within 60s")
				return
			}
			time.Sleep(time.Millisecond)
		}
		pre := waker.reconnectCount()
		if err := killNotifyListener(ctx, dsn, listener.PID()); err != nil {
			killerErr <- fmt.Errorf("kill listener backend: %w", err)
			return
		}
		dl := time.Now().Add(15 * time.Second)
		for waker.reconnectCount() <= pre {
			if time.Now().After(dl) {
				killerErr <- fmt.Errorf("no OnReconnected within 15s of pg_terminate_backend (reconnects before kill: %d)", pre)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// The producer: fgNotifyChaosEvents single-event appends, ~5ms apart,
	// through the outage and after the reconnect. The appended positions
	// are the oracle's want set — read from the results, never assumed.
	var version uint64 = warm.ToVersion
	start := time.Now()
	for i := 0; i < fgNotifyChaosEvents; i++ {
		res, err := ws.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: version,
			Events:          []es.EventData{{TypeURL: fgNotifyChaosType, SchemaVersion: 1, Payload: fgPayload(sid, version+1)}},
		})
		if err != nil {
			cancel()
			t.Fatalf("feedgate: producer append %d: %v", i, err)
		}
		version = res.ToVersion
		appended[res.Envelopes[0].GlobalPosition] = struct{}{}
		produced.Store(int64(i + 1))
		time.Sleep(fgNotifyChaosGap)
	}
	appendElapsed := time.Since(start)

	<-killerDone
	select {
	case err := <-killerErr:
		cancel()
		t.Fatalf("feedgate: notify chaos killer: %v", err)
	default:
	}
	if got := waker.reconnectCount(); got < 1 {
		cancel()
		t.Fatalf("feedgate: OnReconnected never fired")
	}

	// The oracle: the delivered set must equal the appended set exactly —
	// an omission is a lost message, an invention a phantom. The wake is
	// a hint only, so the outage may cost delay (polls + the reconnect's
	// WakeAll) and duplicates are tolerated (an unacked in-flight page at
	// any moment), but the set must still converge exactly.
	dl := time.Now().Add(30 * time.Second)
	for !sink.matches(appended) {
		if time.Now().After(dl) {
			seen, _ := sink.snapshot()
			var missing, invented []uint64
			for p := range appended {
				if _, ok := seen[p]; !ok {
					missing = append(missing, p)
				}
			}
			for p := range seen {
				if _, ok := appended[p]; !ok {
					invented = append(invented, p)
				}
			}
			slices.Sort(missing)
			slices.Sort(invented)
			cancel()
			t.Fatalf("feedgate: delivered set never matched the appended set within 30s: %d appended, %d delivered, %d missing (first: %v), %d invented (first: %v)",
				len(appended), len(seen), len(missing), fgFirst(missing, 10), len(invented), fgFirst(invented, 10))
		}
		time.Sleep(2 * time.Millisecond)
	}
	elapsed := time.Since(start)

	_, dupes := sink.snapshot()
	t.Logf("feedgate: notify chaos done in %v (appends %v): %d events appended and delivered exactly (set equality), %d duplicate deliveries, %d reconnect(s)",
		elapsed, appendElapsed, len(appended), dupes, waker.reconnectCount())
}

// ---------------------------------------------------------------------------
// BenchmarkWorkspaceAppendNotify: the append-cost evidence (brief Step 3).
// ---------------------------------------------------------------------------

// nopNotifyHandler drains the notify=on benchmark's notifications without
// doing anything: the wake is a hint and the benchmark measures the append
// path's cost, not the delivery. A no-op OnNotify is the correct shape —
// the listener's deliver() applies its throttle and returns immediately.
type nopNotifyHandler struct{}

func (nopNotifyHandler) OnNotify(string) {}
func (nopNotifyHandler) OnReconnected()  {}

// fgBenchNotifyChannel is the benchmark's notify channel fixture.
const fgBenchNotifyChannel = "es_lite_bench_notify"

// fgOpenNotifyStore is fgOpenStore applying WithAppendNotify(channel); an
// empty channel opens without the option — the notify=off rows.
func fgOpenNotifyStore(ctx context.Context, tb testing.TB, dsn string, maxConns int32, truncate bool, channel string) (*postgres.Store, *pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("feedgate: parse dsn: %w", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("feedgate: new pool: %w", err)
	}
	opts := []postgres.Option{}
	if channel != "" {
		opts = append(opts, postgres.WithAppendNotify(channel))
	}
	s, err := postgres.OpenWithPool(ctx, pool, opts...)
	if err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("feedgate: open: %w", err)
	}
	if truncate {
		if _, err := pool.Exec(ctx, `TRUNCATE events, unique_claims, anchors, feed_checkpoints, feed_leases, feed_dead_letters`); err != nil {
			s.Close()
			pool.Close()
			return nil, nil, fmt.Errorf("feedgate: truncate: %w", err)
		}
	}
	tb.Cleanup(func() {
		s.Close()
		pool.Close()
	})
	return s, pool, nil
}

// BenchmarkWorkspaceAppendNotify measures the single-event append under
// the C2 notify hint off and on, at 1, 8 and 32 concurrent appenders:
// the no-regression evidence that the opt-in hint costs nothing when off,
// and what it costs when on (one pg_notify statement per append, drained
// by a real listener with a no-op handler).
func BenchmarkWorkspaceAppendNotify(b *testing.B) {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		b.Skip("set PG_DSN to run the feedgate benchmarks (task test:feedgate starts a throwaway Postgres)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	for _, mode := range []struct {
		tag     string
		channel string
	}{
		{tag: "off", channel: ""},
		{tag: "on", channel: fgBenchNotifyChannel},
	} {
		b.Run("notify="+mode.tag, func(b *testing.B) {
			// The pool hosts the G appender connections plus headroom; the
			// notify listener's connection is dedicated, never pooled.
			s, _, err := fgOpenNotifyStore(ctx, b, dsn, 48, true, mode.channel)
			if err != nil {
				b.Fatal(err)
			}
			ws := s.WorkspaceStore("ws_bench_notify_" + mode.tag)

			// The framework re-invokes each sub-benchmark with growing b.N,
			// and the store persists across those probes: streams get a
			// fresh run number per probe, so no probe ever appends at v0 to
			// a stream another probe advanced.
			var probe atomic.Int64

			if mode.channel != "" {
				l := postgres.NewNotifyListener(dsn, mode.channel, nopNotifyHandler{})
				lctx, lcancel := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- l.Run(lctx) }()
				b.Cleanup(func() {
					lcancel()
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						b.Error("notify listener Run did not return within 10s of cancellation")
					}
				})
				deadline := time.Now().Add(10 * time.Second)
				for l.PID() == 0 {
					if time.Now().After(deadline) {
						b.Fatal("notify listener did not connect and LISTEN within 10s (PID still 0)")
					}
					time.Sleep(time.Millisecond)
				}
			}

			for _, g := range []int{1, 8, 32} {
				b.Run(fmt.Sprintf("appenders=%d", g), func(b *testing.B) {
					// The framework's first probe runs b.N=1; a floor of one
					// round per appender keeps every G measured (after the
					// ramp b.N >> g, per is exactly b.N/g).
					per := max(b.N/g, 1)
					total := per * g // b.N need not divide evenly: measure what actually ran
					sids := make([]es.StreamID, g)
					run := probe.Add(1)
					for i := range sids {
						sid, err := es.NewStreamID("gate", fmt.Sprintf("bench-notify-%s-r%d-%d-%d", mode.tag, run, g, i))
						if err != nil {
							b.Fatal(err)
						}
						sids[i] = sid
					}
					events := []es.EventData{{TypeURL: "gate.v1.BenchNotify", SchemaVersion: 1, Payload: []byte("bench-notify-payload-0123456789abcdef")}}
					var wg sync.WaitGroup
					b.ResetTimer()
					for i := 0; i < g; i++ {
						wg.Add(1)
						go func(sid es.StreamID) {
							defer wg.Done()
							var v uint64
							for j := 0; j < per; j++ {
								res, err := ws.Append(ctx, es.AppendParams{
									StreamID:        sid,
									ExpectedVersion: v,
									Events:          events,
								})
								if err != nil {
									b.Error(err) // not Fatal: Fatal Goexits the wrong goroutine
									return
								}
								v = res.ToVersion
							}
						}(sids[i])
					}
					wg.Wait()
					b.StopTimer()
					secs := b.Elapsed().Seconds()
					b.ReportMetric(float64(total)/secs, "appends/s")
					// Single-event batches: events/s equals appends/s,
					// reported so the ADR table's two columns line up.
					b.ReportMetric(float64(total)/secs, "events/s")
				})
			}
		})
	}
}
