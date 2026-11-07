//go:build feedgate

package postgres_test

// The feed gate (Phase 1 / Gate 0): the chaos, plan and fleet scenarios that
// prove no message loss over the horizon fence (spec §5.1), the composite
// page indexes (§5.2), the append payload cap and the two-step bounded
// pages. Everything here needs a live Postgres: the file is build-tagged
// AND each test skips without PG_DSN, so a `-tags feedgate` run without a
// database skips rather than fails.
//
//	task test:feedgate
//
// which runs, against a throwaway Docker Postgres:
//
//	PG_DSN='postgres://postgres:pw@127.0.0.1:5433/eslite?sslmode=disable' \
//		go test -race -shuffle=on -tags feedgate -count=1 -timeout 45m \
//		./postgres/ -run TestFeedGate
//
// The benchmarks (BenchmarkWorkspaceAppend, BenchmarkWorkspaceAppendXidDraw)
// are not part of the gate target; run them with -bench for the write-path
// baseline and record both numbers in the Gate 0 exit review.
//
// The feed.Servers here run on the REAL Postgres feed adapters (Task 13:
// postgres.NewLogs/NewCheckpointStore/NewLeaseStore; DeadLetters stays nil,
// the gate has no poison path), so the gate exercises the log reads —
// *postgres.Workspace, fence and indexes included — AND the store
// close/reopen kill window exercises real durable checkpoints: a killed
// generation's cursor survives the pool death in feed_checkpoints, the
// same database the next generation reconnects to.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/projection"
)

// ---------------------------------------------------------------------------
// Harness plumbing shared by the gate tests.
// ---------------------------------------------------------------------------

// fgDSN returns PG_DSN or skips: the gate runs against a real Postgres.
func fgDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the feedgate tests (task test:feedgate starts a throwaway Postgres)")
	}
	return dsn
}

// fgOpenStore opens a store over a caller-built pool with MaxConns capped, so
// a test controls its connection budget (the fleet test runs 100
// subscriptions on 50; the chaos test isolates its consumers). It returns
// both because Store.Close never closes a borrowed pool (the OpenWithPool
// contract) — the cleanup closes each exactly once, idempotently.
//
// The helper only registers tb.Cleanup (safe from any goroutine); it never
// Skips or Fatals, so consumer harness goroutines may call it too.
func fgOpenStore(ctx context.Context, tb testing.TB, dsn string, maxConns int32, truncate bool) (*postgres.Store, *pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("feedgate: parse dsn: %w", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("feedgate: new pool: %w", err)
	}
	s, err := postgres.OpenWithPool(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("feedgate: open: %w", err)
	}
	if truncate {
		// Each gate test owns the database for its duration (the gate runs
		// the package sequentially, like the other integration tests).
		// The feed tables are included now that the Servers run on the
		// real adapters: stale checkpoint or lease rows would seed a
		// resume or fence a fresh subscription.
		// The bare checkpoints table is gone (0008: dead on Postgres —
		// only SQLite implements delivery.Checkpoints), so it is not
		// truncated here.
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

// fgPayload is the deterministic event content: the chain check verifies
// integrity cryptographically, and this re-computation verifies the delivered
// bytes are the appended bytes (a fabricated-but-consistent event fails it).
func fgPayload(sid es.StreamID, version uint64) []byte {
	return []byte(fmt.Sprintf("feedgate:%s:v%06d", sid.Canonical(), version))
}

// ---------------------------------------------------------------------------
// TestFeedGateNoLoss: the chaos scenario (spec §5.10.2, brief Step 2).
// ---------------------------------------------------------------------------

// Chaos shape:
//
//   - 8 appenders, each appending 625 batches of 20 events (12,500 events
//     each, 100,000 total) through caller-owned transactions:
//     Begin → AppendTx → sleep rand(0–500ms) → Commit. Positions are drawn
//     at INSERT while the commit lands up to half a second later, so every
//     consumer page must fence around in-flight transactions. The last 20
//     batches per appender are held back and released only after the
//     consumers have fully drained the in-load phase — the post-append
//     regime where no transaction is in flight.
//   - 3 feed.Server consumers with disjoint type filters over the real log.
//     Appender i uses type i%3, so each consumer owns whole streams and the
//     per-stream chain oracle (version contiguity + PrevHash links over the
//     DELIVERED envelopes) is meaningful.
//   - Kill windows per consumer: two mid-page cancels (after a Send, before
//     the Checkpoint call) and one full store close/reopen (close pool,
//     re-Open, re-create Server + adapters, resume from the durable
//     checkpoint) while appenders are in flight; plus one more mid-page
//     cancel AFTER every append has committed.
//
// Oracle: SET EQUALITY. Each consumer's delivered-position set (deduplicated
// for the comparison; duplicates themselves allowed) must equal the appended
// positions filtered by that consumer's type set — an omission is a lost
// message, an invention is a phantom. Plus per-stream hash-chain verification
// over the delivered envelopes, a content re-computation, and per-kill
// redelivery bounds:
//
//   - Every kill — in-load and post-append alike — is bounded by the
//     resume-from-checkpoint envelope: the unacked window plus one
//     in-flight page. Task 16 made the resume clamp fence-aware (it
//     consults RawHead before clamping, so a fence-blocked Head 0 no
//     longer replays the log from 0), so a kill whose fence is still
//     blocked makes the reconnect wait and re-head instead of replaying.
//     The measured cost after the fix is a few dozen redeliveries per
//     kill; a reintroduced clamp regression replays everything the
//     consumer had seen at the kill — thousands of events — and fails
//     the bound by an order of magnitude.
//   - The post-append kill is the resume WITNESS: with every append
//     committed, no transaction is in flight, Head settles to the full max
//     on its first fence round and the clamp cannot fire. That kill must
//     resume from the checkpoint — a handful of redeliveries, never a
//     replay. If the checkpoint were lost or ignored, this kill redelivers
//     ~96,800 events and fails the gate.

const (
	fgChaosAppenders        = 8
	fgChaosBatchesPerStream = 625
	fgChaosTailBatches      = 20 // released after the in-load catch-up
	fgChaosInLoadBatches    = fgChaosBatchesPerStream - fgChaosTailBatches
	fgChaosEventsPerBatch   = 20
	fgChaosEventsPerStream  = fgChaosBatchesPerStream * fgChaosEventsPerBatch // 12,500
	fgChaosPageSize         = 100                                             // feed page size
	fgChaosAckEvery         = 30                                              // consumer acks every N delivered events
)

var fgChaosTypes = [3]string{"gate.v1.TypeA", "gate.v1.TypeB", "gate.v1.TypeC"}

// fgKillKind selects which way a consumer's stream dies.
type fgKillKind uint8

const (
	fgKillCancel fgKillKind = iota // cancel the subscription ctx mid-page
	fgKillReopen                   // close the store pool; re-Open on reconnect
)

// fgKill is one scheduled kill window. With signal nil it fires once the
// consumer's GROSS delivered count (redeliveries included) reaches
// atDelivered; with signal set it fires after the channel closes (the
// post-append kill).
type fgKill struct {
	atDelivered int64
	kind        fgKillKind
	signal      <-chan struct{}
}

// The in-load plan per consumer: two mid-page cancels and one full store
// close/reopen, at gross-delivery thresholds.
var fgChaosInLoadKills = []fgKill{
	{atDelivered: 400, kind: fgKillCancel},
	{atDelivered: 3500, kind: fgKillReopen},
	{atDelivered: 9000, kind: fgKillCancel},
}

// fgWindow is the deterministic kill window between the killer and the sink.
// The killer arms it (via fgConsumer.win); the sink, on its next Send —
// mid-page, AFTER the event is recorded but BEFORE any ack — signals now and
// blocks on release. The killer observes now, kills (cancel or pool close),
// then releases. At the moment of the kill there is therefore always at
// least one delivered-but-unacked event, so resume-from-checkpoint must
// redeliver at least one event: the window cannot silently no-op.
type fgWindow struct {
	fired   atomic.Bool
	now     chan struct{}
	release chan struct{}
}

// fgGen is one consumer subscription attempt. The generation index advances
// on EVERY reconnect (the killer waits on it between kills). A cancel kill
// reuses the generation's server and store with a fresh subscription ctx;
// a reopen kill rebuilds everything over a fresh pool to the SAME
// database — the killed generation's checkpoints and leases are still
// there, durable in Postgres, which is exactly what the reopen kill
// exists to exercise.
type fgGen struct {
	idx    int
	srv    *feed.Server
	store  *postgres.Store
	pool   *pgxpool.Pool
	cancel context.CancelFunc
	killed bool
	kind   fgKillKind
}

// fgKillMark records one fired kill: the generation it killed and whether it
// was the post-append (quiescent log) kill.
type fgKillMark struct {
	gen        int
	postAppend bool
}

// fgConsumer is the chaos consumer harness: a feed.Sink that records every
// delivery and acks every fgChaosAckEvery events with a lease-fenced
// feed.Ack under its Request.Owner token, plus the reconnect loop that
// survives kills.
type fgConsumer struct {
	tb testing.TB

	scope, projection, instance string
	typeURL                     string
	req                         feed.Request

	mu        sync.Mutex
	delivered []es.Envelope // every Send, in order, duplicates included
	seen      map[uint64]struct{}
	unacked   int
	gen       *fgGen

	// Per-generation duplicate counts: a delivery whose position was
	// already seen counts as a duplicate in the generation that re-sent it,
	// which attributes each kill's redelivery cost to the reconnect it
	// caused (gen g's duplicates are kill g-1's cost).
	dupByGen map[int]int64
	killSeen map[int]int64 // kill's gen -> unique positions seen at the kill
	killList []fgKillMark

	total    atomic.Int64 // gross deliveries; the killer's trigger
	win      atomic.Pointer[fgWindow]
	holdAcks atomic.Bool // set while a reopen window is open: no ack may land

	killsFired    atomic.Int64
	unexpected    atomic.Int64
	unexpectedMu  sync.Mutex
	unexpectedMsg []string
}

func newFGConsumer(tb testing.TB, idx int) *fgConsumer {
	c := &fgConsumer{
		tb:       tb,
		scope:    "ws_chaos",
		instance: fmt.Sprintf("chaos-%d", idx),
		typeURL:  fgChaosTypes[idx],
		seen:     map[uint64]struct{}{},
		dupByGen: map[int]int64{},
		killSeen: map[int]int64{},
	}
	c.projection = fmt.Sprintf("gate.chaos.%s", string(rune('A'+idx)))
	c.req = feed.Request{
		Scope:      c.scope,
		Projection: c.projection,
		Instance:   c.instance,
		Consumer:   "feedgate-chaos",
		// A stable owner token: every generation of this consumer's
		// reconnect loop re-subscribes under it, so its acks are fenced to
		// the lease its own live stream holds (spec §5.4: a stable token
		// gives self-stealing reconnects).
		Owner:      fmt.Sprintf("feedgate-chaos-%d", idx),
		Definition: projection.Definition{Name: c.projection, Consumes: []string{c.typeURL}},
	}
	return c
}

// Send implements feed.Sink: record the delivery (and its duplicate
// attribution), open the armed kill window, then ack every fgChaosAckEvery
// events.
func (c *fgConsumer) Send(ctx context.Context, env es.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if _, ok := c.seen[env.GlobalPosition]; ok {
		c.dupByGen[c.gen.idx]++
	} else {
		c.seen[env.GlobalPosition] = struct{}{}
	}
	c.delivered = append(c.delivered, env)
	c.unacked++
	c.mu.Unlock()
	c.total.Add(1)

	// Kill window: mid-page, after this Send, before the ack below.
	if w := c.win.Load(); w != nil && w.fired.CompareAndSwap(false, true) {
		close(w.now)
		<-w.release
	}

	c.mu.Lock()
	needAck := c.unacked >= fgChaosAckEvery && !c.holdAcks.Load()
	pos := env.GlobalPosition
	gen := c.gen
	c.mu.Unlock()
	if needAck {
		// The ack carries the consumer's definition fingerprint, so a
		// generation's reconnect is checked against the identity its
		// predecessor recorded (a stable definition: never drifts here).
		if err := gen.srv.Checkpoint(ctx, feed.Ack{
			Scope:      c.scope,
			Projection: c.projection,
			Instance:   c.instance,
			Owner:      c.req.Owner,
			Position:   pos,
			Definition: c.req.Definition.Hash(),
		}); err != nil {
			return fmt.Errorf("feedgate: ack %s/%s/%s at %d: %w", c.scope, c.projection, c.instance, pos, err)
		}
		c.mu.Lock()
		c.unacked = 0
		c.mu.Unlock()
	}
	return nil
}

// recordUnexpected logs a stream death nobody scheduled. The gate treats any
// such death as a finding: with a healthy database only the kills may end a
// stream.
func (c *fgConsumer) recordUnexpected(msg string) {
	c.unexpected.Add(1)
	c.unexpectedMu.Lock()
	if len(c.unexpectedMsg) < 8 {
		c.unexpectedMsg = append(c.unexpectedMsg, msg)
	}
	c.unexpectedMu.Unlock()
}

// buildGeneration opens a fresh store (its own pool) and Server over the
// REAL Postgres feed adapters. No checkpoint carry: the rows live in
// feed_checkpoints in the same database the new pool connects to, so the
// previous generation's cursor survives the store death — the real
// durability the in-memory fakes used to simulate.
func (c *fgConsumer) buildGeneration(ctx context.Context, dsn string, prev *fgGen) (*fgGen, error) {
	idx := 0
	if prev != nil {
		idx = prev.idx + 1
	}
	store, pool, err := fgOpenStore(ctx, c.tb, dsn, 8, false)
	if err != nil {
		return nil, err
	}
	srv, err := feed.NewServer(feed.Config{
		Checkpoints: postgres.NewCheckpointStore(store),
		Leases:      postgres.NewLeaseStore(store),
		Logs:        postgres.NewLogs(store),
		// DeadLetters stays nil: the gate has no poison path.
		LeaseTTL:   100 * time.Millisecond, // the brief's chaos TTL
		RenewEvery: 0,                      // derived: TTL/3
		Batch:      fgChaosPageSize,
		MaxBatch:   fgChaosPageSize * 5,
		Poll:       50 * time.Millisecond,
	})
	if err != nil {
		return nil, fmt.Errorf("feedgate: new server: %w", err)
	}
	return &fgGen{idx: idx, srv: srv, store: store, pool: pool}, nil
}

// run is the consumer's reconnect loop: subscribe, classify the death,
// rebuild after a reopen kill, repeat until ctx ends.
func (c *fgConsumer) run(ctx context.Context, dsn string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		c.mu.Lock()
		prev := c.gen
		rebuild := prev == nil || (prev.killed && prev.kind == fgKillReopen)
		c.mu.Unlock()

		var gen *fgGen
		if rebuild {
			g, err := c.buildGeneration(ctx, dsn, prev)
			if err != nil {
				c.recordUnexpected(fmt.Sprintf("rebuild generation: %v", err))
				return // no store to subscribe with; the oracle fails loudly
			}
			gen = g
		} else {
			// Cancel kill: same server and store, fresh subscription.
			gen = &fgGen{
				idx:   prev.idx + 1,
				srv:   prev.srv,
				store: prev.store,
				pool:  prev.pool,
			}
		}

		subCtx, cancel := context.WithCancel(ctx)
		c.mu.Lock()
		gen.cancel = cancel
		c.gen = gen
		c.unacked = 0 // redeliveries restart the ack cadence
		c.mu.Unlock()
		c.holdAcks.Store(false)

		err := gen.srv.Subscribe(subCtx, c.req, c)
		cancel()

		if ctx.Err() != nil {
			return // the test's own cancellation: expected
		}
		c.mu.Lock()
		killed := gen.killed
		c.mu.Unlock()
		if killed {
			continue
		}
		if err == nil {
			err = errors.New("Subscribe returned nil with a live context")
		}
		c.recordUnexpected(err.Error())
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// runKiller executes the kill plan: wait for the trigger (threshold or
// signal), arm the window, let the sink signal mid-page, kill, release, then
// wait for the harness to advance to a fresh generation before the next
// kill. For signal-triggered kills it reports the armed window on armed, so
// the test can order the release of the fence pinner strictly after the
// window is in place.
func (c *fgConsumer) runKiller(ctx context.Context, plan []fgKill, armed chan<- struct{}) {
	for _, k := range plan {
		if k.signal == nil {
			for c.total.Load() < k.atDelivered {
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Millisecond):
				}
			}
		} else {
			select {
			case <-k.signal:
			case <-ctx.Done():
				return
			}
		}
		w := &fgWindow{now: make(chan struct{}), release: make(chan struct{})}
		if k.kind == fgKillReopen {
			// While the store dies, no ack may land for the windowed
			// events: an ack racing the pool close would fail on the
			// dead connection, but one could still land just before the
			// close and ack away the kill's own redelivery window. The
			// gate wants the worst case pinned: hold the acks until the
			// release, so the window must bite. Landed acks stay
			// durable in feed_checkpoints either way.
			c.holdAcks.Store(true)
		}
		c.win.Store(w)
		if k.signal != nil {
			select {
			case armed <- struct{}{}:
			case <-ctx.Done():
				close(w.release) // never leave the sink blocked
				return
			}
		}
		select {
		case <-w.now:
		case <-ctx.Done():
			close(w.release) // never leave the sink blocked
			return
		}
		c.mu.Lock()
		gen := c.gen
		gen.killed = true
		gen.kind = k.kind
		c.killSeen[gen.idx] = int64(len(c.seen))
		c.killList = append(c.killList, fgKillMark{gen: gen.idx, postAppend: k.signal != nil})
		c.mu.Unlock()
		switch k.kind {
		case fgKillCancel:
			gen.cancel()
		case fgKillReopen:
			// Store.Close would not close the borrowed pool; close the pool
			// itself — the live stream fails on its next read.
			gen.pool.Close()
		}
		c.killsFired.Add(1)
		close(w.release)

		// Wait for the harness to advance past this generation.
		deadline := time.Now().Add(15 * time.Second)
		for {
			c.mu.Lock()
			idx := c.gen.idx
			c.mu.Unlock()
			if idx > gen.idx {
				break
			}
			if time.Now().After(deadline) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond):
			}
		}
	}
}

// fgChaosRecord is one committed event as an appender saw it.
type fgChaosRecord struct {
	pos uint64
	typ string
}

// fgRunAppender is one chaos appender: batches of 20 through caller-owned
// transactions with a rand(0–500ms) sleep between INSERT and COMMIT, so pages
// must fence around positions that are drawn but not yet committed. The
// final fgChaosTailBatches batches are held behind release: the test lets the
// consumers drain the in-load log first, then releases the tail so the
// post-append kill can fire against a quiescent log. inLoad.Done is wrapped
// in a sync.Once and deferred, so the WaitGroup is released exactly once on
// EVERY exit path — an in-load failure must not hang inLoadWg.Wait (and
// swallow the appender error to the test timeout); the success path fires
// it early, before the appender parks on release.
func fgRunAppender(ctx context.Context, ws *postgres.Workspace, pool *pgxpool.Pool, idx int, typeURL string,
	release <-chan struct{}, inLoad *sync.WaitGroup, recs, tailRecs *[]fgChaosRecord) error {

	inLoaded := sync.OnceFunc(inLoad.Done)
	defer inLoaded()

	sid, err := es.NewStreamID("gate", fmt.Sprintf("app-%02d", idx))
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewPCG(uint64(idx)+1, 0xC0FFEE)) // per-appender, reproducible
	var expected uint64

	appendBatch := func(b int, out *[]fgChaosRecord) error {
		events := make([]es.EventData, fgChaosEventsPerBatch)
		for j := range events {
			v := expected + uint64(j) + 1
			events[j] = es.EventData{
				TypeURL:       typeURL,
				SchemaVersion: 1,
				Payload:       fgPayload(sid, v),
			}
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin batch %d: %w", b, err)
		}
		res, err := ws.AppendTx(ctx, tx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: expected,
			Events:          events,
		})
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("append batch %d: %w", b, err)
		}
		// The positions are drawn; hold the commit for rand(0–500ms).
		sleep := time.Duration(rng.IntN(500)) * time.Millisecond
		select {
		case <-ctx.Done():
			tx.Rollback(ctx)
			return ctx.Err()
		case <-time.After(sleep):
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit batch %d: %w", b, err)
		}
		expected = res.ToVersion
		for _, env := range res.Envelopes {
			*out = append(*out, fgChaosRecord{pos: env.GlobalPosition, typ: typeURL})
		}
		return nil
	}

	for b := 0; b < fgChaosInLoadBatches; b++ {
		if err := appendBatch(b, recs); err != nil {
			return err // the deferred inLoaded() releases the WaitGroup
		}
	}

	// In-load batches committed. Fire the Once here — before parking on
	// release — so inLoadWg.Wait observes phase completion on time; the
	// deferred call above is then a no-op, and on any earlier error return
	// it is the one that releases the WaitGroup.
	inLoaded()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-release:
	}
	for b := fgChaosInLoadBatches; b < fgChaosBatchesPerStream; b++ {
		if err := appendBatch(b, tailRecs); err != nil {
			return err
		}
	}
	return nil
}

// fgFirst returns at most n samples of positions for a failure message.
func fgFirst(pos []uint64, n int) []uint64 {
	if len(pos) <= n {
		return pos
	}
	return pos[:n]
}

// fgRouteExpected builds per-consumer expected position sets from committed
// appender records.
func fgRouteExpected(records [][]fgChaosRecord) []map[uint64]struct{} {
	expected := make([]map[uint64]struct{}, len(fgChaosTypes))
	for i := range expected {
		expected[i] = map[uint64]struct{}{}
	}
	for _, recs := range records {
		for _, r := range recs {
			for ci, typ := range fgChaosTypes {
				if r.typ == typ {
					expected[ci][r.pos] = struct{}{}
				}
			}
		}
	}
	return expected
}

func TestFeedGateNoLoss(t *testing.T) {
	dsn := fgDSN(t)
	// Generous but bounded: the append phase alone runs ~3 minutes on a dev
	// laptop (625 batches × E[rand(0,500ms)] per appender, in parallel).
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	start := time.Now()

	// The appenders' store: also truncates, so the gate starts from a clean
	// log. Its pool (16 conns) hosts the 8 in-flight appender transactions.
	appStore, appPool, err := fgOpenStore(ctx, t, dsn, 16, true)
	if err != nil {
		t.Fatal(err)
	}

	// The post-append kill's trigger: closed once every tail batch has
	// committed, so the kill's reconnect faces a quiescent log. The killers
	// confirm on killArmed that the window is in place before the test
	// releases the fence pinner, so the kill strictly precedes the drain.
	tailCommitted := make(chan struct{})
	killArmed := make(chan struct{}, len(fgChaosTypes))
	// The appenders' tail gate: closed when the in-load log is drained.
	tailRelease := make(chan struct{})

	// Three consumers over the real log, one per type; disjoint filters.
	// The kill plan is shared (read-only); the windows are per consumer.
	killPlan := append(slices.Clone(fgChaosInLoadKills), fgKill{kind: fgKillCancel, signal: tailCommitted})
	consumers := make([]*fgConsumer, len(fgChaosTypes))
	var consWg sync.WaitGroup
	for i := range consumers {
		c := newFGConsumer(t, i)
		consumers[i] = c
		consWg.Add(2)
		go func(c *fgConsumer) {
			defer consWg.Done()
			c.run(ctx, dsn)
		}(c)
		go func(c *fgConsumer) {
			defer consWg.Done()
			c.runKiller(ctx, killPlan, killArmed)
		}(c)
	}

	// Eight appenders: 8 × 12,500 = 100,000 committed events. The last 20
	// batches per appender wait for tailRelease.
	records := make([][]fgChaosRecord, fgChaosAppenders)     // in-load phase
	tailRecords := make([][]fgChaosRecord, fgChaosAppenders) // released tail
	var appWg, inLoadWg sync.WaitGroup
	appErrs := make(chan error, fgChaosAppenders)
	drainAppErrs := func() []error {
		var out []error
		for {
			select {
			case err, ok := <-appErrs:
				if !ok {
					return out
				}
				out = append(out, err)
			default:
				return out
			}
		}
	}
	ws := appStore.WorkspaceStore("ws_chaos")
	for i := 0; i < fgChaosAppenders; i++ {
		appWg.Add(1)
		inLoadWg.Add(1)
		go func(i int) {
			defer appWg.Done()
			if err := fgRunAppender(ctx, ws, appPool, i, fgChaosTypes[i%len(fgChaosTypes)],
				tailRelease, &inLoadWg, &records[i], &tailRecords[i]); err != nil {
				appErrs <- err
			}
		}(i)
	}

	// A shared wait helper: consumer i must have delivered `want` unique
	// positions, or the gate fails with the number it stalled at.
	waitCaughtUp := func(i int, c *fgConsumer, want int, what string) {
		deadline := time.Now().Add(120 * time.Second)
		for {
			c.mu.Lock()
			got := len(c.seen)
			c.mu.Unlock()
			if got >= want {
				return
			}
			if time.Now().After(deadline) {
				cancel()
				consWg.Wait()
				t.Fatalf("feedgate: consumer %d delivered %d unique positions, want %d (%s) within 120s",
					i, got, want, what)
			}
			select {
			case <-ctx.Done():
				cancel()
				consWg.Wait()
				t.Fatalf("feedgate: context ended while consumer %d was at %d/%d (%s)", i, got, want, what)
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	failOnAppErrs := func(errs []error, phase string) {
		if len(errs) == 0 {
			return
		}
		cancel()
		consWg.Wait()
		for _, err := range errs {
			t.Errorf("feedgate: appender failed: %v", err)
		}
		t.Fatalf("feedgate: %d appender failures in the %s phase; cannot run the oracle", len(errs), phase)
	}

	// Phase 1 — in-load: 605 batches per appender with commits held open
	// mid-flight; the consumers fence around them while the kills land.
	inLoadWg.Wait()
	failOnAppErrs(drainAppErrs(), "in-load")
	inLoadElapsed := time.Since(start)
	inLoadExpected := fgRouteExpected(records)
	inLoadCount := 0
	for _, m := range inLoadExpected {
		inLoadCount += len(m)
	}
	for i, c := range consumers {
		waitCaughtUp(i, c, len(inLoadExpected[i]), "in-load backlog")
	}
	t.Logf("feedgate: in-load phase done: %d events in %v; all consumers drained",
		inLoadCount, inLoadElapsed)

	// Phase 2 — the tail. First, pin the fence: one transaction that has
	// drawn an xid and holds it open for the whole tail. Every fence round a
	// consumer draws during the tail sees that xid in flight below its R, so
	// no page clears while the tail commits — the tail becomes a single
	// backlog that can only start flowing after the pinner aborts. That is
	// what makes the post-append kill deterministic rather than a race
	// against a 300-event straggler drain.
	pinTx, err := appPool.Begin(ctx)
	if err != nil {
		cancel()
		consWg.Wait()
		t.Fatalf("feedgate: fence pinner begin: %v", err)
	}
	defer pinTx.Rollback(context.Background()) // settle the xid on any exit
	if _, err := pinTx.Exec(ctx, `SELECT pg_current_xact_id()`); err != nil {
		cancel()
		consWg.Wait()
		t.Fatalf("feedgate: fence pinner xid: %v", err)
	}

	close(tailRelease)
	appWg.Wait()
	close(appErrs)
	failOnAppErrs(drainAppErrs(), "tail")
	appendElapsed := time.Since(start)
	t.Logf("feedgate: append phase done: %d events in %v",
		fgChaosAppenders*fgChaosEventsPerStream, appendElapsed)

	expected := fgRouteExpected(append(slices.Clone(records), tailRecords...))
	// committed = unique positions across all consumers (each event has
	// exactly one type, so the per-consumer sets are disjoint).
	committed := 0
	for ci := range expected {
		committed += len(expected[ci])
	}

	// Arm the post-append kill windows while the fence is still pinned (the
	// consumers are provably idle: their fences cannot clear), then abort
	// the pinner and let the drain begin into armed windows.
	close(tailCommitted)
	armed, armedDeadline := 0, time.Now().Add(10*time.Second)
	for armed < len(consumers) {
		select {
		case <-killArmed:
			armed++
		case <-ctx.Done():
			cancel()
			consWg.Wait()
			t.Fatal("feedgate: context ended while arming the post-append kill")
		default:
			if time.Now().After(armedDeadline) {
				cancel()
				consWg.Wait()
				t.Fatal("feedgate: post-append kill windows did not arm within 10s")
			}
			time.Sleep(time.Millisecond)
		}
	}
	if err := pinTx.Rollback(ctx); err != nil {
		cancel()
		consWg.Wait()
		t.Fatalf("feedgate: release fence pinner: %v", err)
	}

	for i, c := range consumers {
		waitCaughtUp(i, c, len(expected[i]), "full log")
	}

	// End the streams and let the harnesses finish before the oracle reads.
	cancel()
	consWg.Wait()
	chaosElapsed := time.Since(start)

	for i, c := range consumers {
		c.mu.Lock()
		seen := c.seen
		delivered := c.delivered
		dupByGen := c.dupByGen
		killList := c.killList
		c.mu.Unlock()

		// ---- Oracle 1: set equality (no omission, no invention).
		var omitted, invented []uint64
		for p := range expected[i] {
			if _, ok := seen[p]; !ok {
				omitted = append(omitted, p)
			}
		}
		for p := range seen {
			if _, ok := expected[i][p]; !ok {
				invented = append(invented, p)
			}
		}
		slices.Sort(omitted)
		slices.Sort(invented)
		if len(omitted) > 0 || len(invented) > 0 {
			t.Errorf("feedgate: consumer %d (%s): LOST %d events (first: %v), INVENTED %d (first: %v)",
				i, c.typeURL, len(omitted), fgFirst(omitted, 10), len(invented), fgFirst(invented, 10))
		}

		// ---- Kill accounting: the windows fired, and only the windows killed.
		if kills := c.killsFired.Load(); kills != int64(len(killList)) || int(kills) != len(killPlan) {
			t.Errorf("feedgate: consumer %d: %d kills fired, want %d", i, kills, len(killPlan))
		}
		if u := c.unexpected.Load(); u > 0 {
			c.unexpectedMu.Lock()
			msgs := append([]string(nil), c.unexpectedMsg...)
			c.unexpectedMu.Unlock()
			t.Errorf("feedgate: consumer %d: %d unexpected stream deaths: %v", i, u, msgs)
		}

		// ---- Oracle 3: per-kill redelivery bounds.
		// Generation g's duplicates are the cost of the kill that ended
		// generation g-1. Every kill is bounded by the same
		// resume-from-checkpoint envelope — the unacked window plus one
		// in-flight page — because the resume clamp is fence-aware
		// (Task 16): an in-load kill whose fence is blocked makes the
		// reconnect wait and re-head instead of replaying from the
		// blocked Head 0. A reintroduced clamp regression redelivers
		// everything seen at the kill and fails the bound.
		var totalDup int64
		clampReplays := 0
		for ki, mark := range killList {
			dupK := dupByGen[mark.gen+1]
			totalDup += dupK
			bound := int64(fgChaosAckEvery + fgChaosPageSize + 64)
			if !mark.postAppend && dupK > bound {
				// In-load kills exceeding the resume envelope are
				// clamp-style replays; the bound below is what fails
				// them, this counter keeps them visible in the log.
				clampReplays++
			}
			if dupK < 1 {
				t.Errorf("feedgate: consumer %d: kill %d (gen %d) redelivered nothing — the window must bite",
					i, ki, mark.gen)
			}
			if dupK > bound {
				t.Errorf("feedgate: consumer %d: kill %d (gen %d, post-append=%v) redelivered %d events, want <= %d (resume-from-checkpoint bound)",
					i, ki, mark.gen, mark.postAppend, dupK, bound)
			}
		}
		t.Logf("feedgate: consumer %d (%s): %d unique positions, %d redeliveries over %d kills (%d clamp replays)",
			i, c.typeURL, len(seen), totalDup, len(killList), clampReplays)

		// ---- Oracle 4: per-stream hash chain over the DELIVERED envelopes
		// (version-contiguous, PrevHash links, recomputed hashes), and the
		// content re-computation: the delivered bytes are the appended bytes.
		byStream := map[es.StreamID]map[uint64]es.Envelope{}
		for _, env := range delivered {
			m := byStream[env.StreamID]
			if m == nil {
				m = map[uint64]es.Envelope{}
				byStream[env.StreamID] = m
			}
			if _, ok := m[env.Version]; !ok {
				m[env.Version] = env
			}
		}
		for sid, m := range byStream {
			versions := make([]uint64, 0, len(m))
			for v := range m {
				versions = append(versions, v)
			}
			slices.Sort(versions)
			checker := es.NewChainChecker(sid)
			for _, v := range versions {
				if err := checker.Next(m[v]); err != nil {
					t.Errorf("feedgate: consumer %d: stream %s chain broken at v%d: %v", i, sid.Canonical(), v, err)
					break
				}
			}
			rep := checker.Report()
			if !rep.Intact() || rep.ChainedFrom != 1 || rep.Head != fgChaosEventsPerStream {
				t.Errorf("feedgate: consumer %d: stream %s chain report = %+v (want intact, chained from 1, head %d)",
					i, sid.Canonical(), rep, fgChaosEventsPerStream)
			}
			for _, v := range versions {
				if want := fgPayload(sid, v); !bytes.Equal(m[v].Payload, want) {
					t.Errorf("feedgate: consumer %d: stream %s v%d payload %q, want %q",
						i, sid.Canonical(), v, m[v].Payload, want)
				}
				if m[v].TypeURL != c.typeURL {
					t.Errorf("feedgate: consumer %d: stream %s v%d delivered type %q, want %q (filter leak)",
						i, sid.Canonical(), v, m[v].TypeURL, c.typeURL)
				}
			}
		}
	}

	t.Logf("feedgate: chaos done in %v: %d committed events, set equality over %d+%d+%d delivered positions",
		chaosElapsed, committed,
		len(expected[0]), len(expected[1]), len(expected[2]))
}

// ---------------------------------------------------------------------------
// TestFeedGatePageQueryPlan: the 1M-row planner gate (brief Step 3).
// ---------------------------------------------------------------------------

// The exact page query ReadAllBounded runs as step 1 (keep in sync with
// Workspace.ReadAllBounded): server-side type filter, fence bound ($5) and
// page limit ($3) included. The planner must serve it from the composite
// (workspace_id, type_url, global_position) index — the one Task 4 added —
// never from a sequential scan over an events partition.
const fgPageQuery = `SELECT global_position, type_url, octet_length(payload) FROM events
      WHERE workspace_id = $1 AND global_position > $2 AND type_url = ANY($4)
          AND global_position <= $5
      ORDER BY global_position ASC LIMIT $3`

// The exact step-2 fetch: full rows for the chosen positions only.
const fgFetchQuery = `SELECT global_position, event_id, stream_type, stream_id, version,
	type_url, schema_version, occurred_at, recorded_at, correlation_id, causation_id, command_id,
	actor_type, actor_id, payload, workspace_id, prev_hash, hash FROM events
	  WHERE workspace_id = $1 AND global_position = ANY($2)
	  ORDER BY global_position ASC`

// fgSeedChunk inserts rows [lo, hi] of the 1M-row planner fixture: 512-byte
// payloads, schema-valid dummy columns (this fixture exists for the planner,
// not for consumption), every 500th row of the consumed type so the typed
// index is decisively cheaper than scanning positions and filtering.
const fgSeedChunk = `
INSERT INTO events (
    workspace_id, global_position, event_id, stream_type, stream_id, version,
    type_url, schema_version, occurred_at, recorded_at,
    correlation_id, causation_id, command_id,
    actor_type, actor_id, actor_principal, payload
)
SELECT
    'ws_plan',
    nextval('events_global_position_seq'),
    gen_random_uuid()::text,
    'gate',
    'gate:seed-' || (g % 1000)::text,
    (g / 1000) + 1,
    CASE WHEN g % 500 = 0 THEN 'gate.v1.Seed'
         ELSE 'gate.v1.Noise' || ((g % 3) + 1)::text END,
    1, now(), now(),
    '', '', '',
    'gate', 'seed', 'gate:seed',
    repeat('p', 512)::bytea
FROM generate_series($1::bigint, $2::bigint) AS g`

// fgPlanScan is what fgWalkPlan collects from an EXPLAIN (FORMAT JSON) plan.
type fgPlanScan struct {
	nodeTypes  []string
	indexNames []string
	seqScans   []string // Relation Names of Seq Scan nodes
}

// fgWalkPlan parses an EXPLAIN (FORMAT JSON) document and walks the plan
// tree, collecting node types, index names and Seq Scan relations.
func fgWalkPlan(raw []byte) (fgPlanScan, error) {
	var scan fgPlanScan
	var doc []map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return scan, fmt.Errorf("feedgate: parse EXPLAIN json: %w", err)
	}
	if len(doc) == 0 || doc[0]["Plan"] == nil {
		return scan, errors.New("feedgate: EXPLAIN json has no Plan node")
	}
	root, ok := doc[0]["Plan"].(map[string]any)
	if !ok {
		return scan, errors.New("feedgate: EXPLAIN Plan node is not an object")
	}
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		nt, _ := node["Node Type"].(string)
		scan.nodeTypes = append(scan.nodeTypes, nt)
		if name, ok := node["Index Name"].(string); ok {
			scan.indexNames = append(scan.indexNames, name)
		}
		if nt == "Seq Scan" {
			if rel, ok := node["Relation Name"].(string); ok {
				scan.seqScans = append(scan.seqScans, rel)
			}
		}
		if plans, ok := node["Plans"].([]any); ok {
			for _, p := range plans {
				if m, ok := p.(map[string]any); ok {
					walk(m)
				}
			}
		}
	}
	walk(root)
	return scan, nil
}

// fgIndexChildren returns the names of the partition-child indexes that
// belong to the named partitioned parent index (pg_inherits links them).
// CREATE INDEX on the hash-partitioned parent gives every partition a child
// with a Postgres-generated name, so "events_ws_type_pos_idx appears in the
// plan" is asserted as: the plan's Index Name is one of this parent's
// children.
func fgIndexChildren(ctx context.Context, pool *pgxpool.Pool, parent string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class p ON p.oid = i.inhparent
		JOIN pg_class c ON c.oid = i.inhrelid
		WHERE p.relname = $1`, parent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// fgAssertNoSeqScan fails the test if the plan scans any events partition
// sequentially (the parent included).
func fgAssertNoSeqScan(t *testing.T, scan fgPlanScan, what string) {
	t.Helper()
	for _, rel := range scan.seqScans {
		if rel == "events" || strings.HasPrefix(rel, "events_p") {
			t.Errorf("feedgate: %s plan Seq Scans %s (want index-served over every events partition)", what, rel)
		}
	}
}

func TestFeedGatePageQueryPlan(t *testing.T) {
	dsn := fgDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	_, pool, err := fgOpenStore(ctx, t, dsn, 4, true)
	if err != nil {
		t.Fatal(err)
	}

	const (
		ws       = "ws_plan"
		rows     = 1_000_000
		chunk    = 100_000
		seedType = "gate.v1.Seed"
	)

	// Seed >= 1M rows in batched INSERT..SELECT chunks (never row-by-row).
	seedStart := time.Now()
	for lo := int64(1); lo <= int64(rows); lo += chunk {
		hi := lo + chunk - 1
		if _, err := pool.Exec(ctx, fgSeedChunk, lo, hi); err != nil {
			t.Fatalf("feedgate: seed chunk %d..%d: %v", lo, hi, err)
		}
	}
	// Raise type_url's statistics target BEFORE ANALYZE (ALTER on the
	// partitioned parent recurses to every partition, verified on PG 17).
	// The Seed type is 0.2%-selectitive and ANALYZE samples randomly: with
	// the default target (100) the estimate for a type that rare swings
	// enough that the position index occasionally looks cheaper than the
	// composite — a first-run-fail / identical-tree-green planner toss-up
	// (Task 18 carry). Target 1000 lifts the sample on the fixture's
	// populated partition to ~300k of its 1M rows (the other partitions
	// are empty), which stabilizes the rare-type estimate enough to make
	// the composite index win deterministically.
	if _, err := pool.Exec(ctx, `ALTER TABLE events ALTER COLUMN type_url SET STATISTICS 1000`); err != nil {
		t.Fatalf("feedgate: raise type_url statistics target: %v", err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE events`); err != nil {
		t.Fatalf("feedgate: analyze: %v", err)
	}
	var n uint64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE workspace_id = $1`, ws).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Fatalf("feedgate: seeded %d rows in %s, want %d", n, ws, rows)
	}
	t.Logf("feedgate: seeded %d rows (512-byte payloads) in %v", n, time.Since(seedStart))

	// The fence bound a live page would carry: the workspace's max position.
	var bound uint64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(global_position), 0) FROM events WHERE workspace_id = $1`, ws).Scan(&bound); err != nil {
		t.Fatal(err)
	}
	if bound == 0 {
		t.Fatal("feedgate: empty planner fixture")
	}

	// Step 1 — the page query, fence bound included.
	var planRaw []byte
	if err := pool.QueryRow(ctx, `EXPLAIN (FORMAT JSON) `+fgPageQuery,
		ws, uint64(0), 100, []string{seedType}, bound).Scan(&planRaw); err != nil {
		t.Fatalf("feedgate: explain page query: %v", err)
	}
	scan, err := fgWalkPlan(planRaw)
	if err != nil {
		t.Fatal(err)
	}

	// The composite index (parent events_ws_type_pos_idx) must serve the page.
	const composite = "events_ws_type_pos_idx"
	children, err := fgIndexChildren(ctx, pool, composite)
	if err != nil {
		t.Fatalf("feedgate: resolve %s children: %v", composite, err)
	}
	if len(children) == 0 {
		t.Fatalf("feedgate: %s has no partition children", composite)
	}
	served := false
	for _, name := range scan.indexNames {
		if slices.Contains(children, name) {
			served = true
		}
	}
	if !served {
		t.Errorf("feedgate: page plan indexes = %v, want one of %s's children %v (the composite type index must serve the page)",
			scan.indexNames, composite, children)
	}

	// The fence bound must be part of the plan (it is an index qual), and no
	// events partition may be sequentially scanned.
	if !strings.Contains(string(planRaw), fmt.Sprintf("global_position <= '%d'", bound)) {
		t.Errorf("feedgate: page plan does not carry the fence bound %d:\n%s", bound, planRaw)
	}
	fgAssertNoSeqScan(t, scan, "page query")

	// Step 2 — the position fetch must be index-served too.
	seedRows, err := pool.Query(ctx,
		`SELECT global_position FROM events WHERE workspace_id = $1 AND type_url = $2 ORDER BY global_position LIMIT 100`,
		ws, seedType)
	if err != nil {
		t.Fatal(err)
	}
	positions := make([]int64, 0, 100)
	for seedRows.Next() {
		var p int64
		if err := seedRows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		positions = append(positions, p)
	}
	seedRows.Close()
	if err := seedRows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(positions) != 100 {
		t.Fatalf("feedgate: fetched %d seed positions, want 100", len(positions))
	}
	var fetchRaw []byte
	if err := pool.QueryRow(ctx, `EXPLAIN (FORMAT JSON) `+fgFetchQuery, ws, positions).Scan(&fetchRaw); err != nil {
		t.Fatalf("feedgate: explain fetch query: %v", err)
	}
	fetchScan, err := fgWalkPlan(fetchRaw)
	if err != nil {
		t.Fatal(err)
	}
	fgAssertNoSeqScan(t, fetchScan, "fetch query")

	t.Logf("feedgate: page plan %s served by %v; fetch plan node types %v",
		strings.Join(scan.nodeTypes, " → "), scan.indexNames, fetchScan.nodeTypes)
}

// ---------------------------------------------------------------------------
// TestFeedGateHundredIdleSubscribers: the fleet scenario (brief Step 4).
// ---------------------------------------------------------------------------

// Fleet shape: 100 subscriptions (distinct instances, so distinct
// checkpoint/lease keys) on a feed.Server over the real log, with the pool
// capped at MaxConns=50 — half the subscription count. After a quiet start,
// one matching event must reach all 100 within a second, and the pool must
// be healthy after the burst. The quiet start also asserts the idle-poll
// jitter spread (Task 18): with the production ±20% jitter the fleet's
// polls must de-synchronize across the whole poll interval.

const (
	fleetSubs    = 100
	fleetMaxConn = 50
	fleetType    = "gate.v1.Fleet"
)

// fleetPollClock timestamps the fleet's idle polls: OnPage with zero events
// is one empty-page read, and the fleet test asserts the spread of those
// timestamps. The server's ±20% per-wait idle jitter random-walks each
// subscriber's phase off the shared start; without it a fleet started
// together polls in lockstep and clusters into a handful of 100ms buckets.
type fleetPollClock struct {
	mu    sync.Mutex
	polls []time.Time
}

func (c *fleetPollClock) OnPage(events, _ int) {
	if events != 0 {
		return // only empty pages timestamp the idle cadence
	}
	c.mu.Lock()
	c.polls = append(c.polls, time.Now())
	c.mu.Unlock()
}

func (c *fleetPollClock) OnCheckpoint(uint64)                        {}
func (c *fleetPollClock) OnLeaseLost(string, string, string, string) {}
func (c *fleetPollClock) OnPoison(*feed.EventTooLargeError)          {}
func (c *fleetPollClock) OnStall(uint64, uint64)                     {}

func (c *fleetPollClock) snapshot() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.polls...)
}

// fgFleetSink counts deliveries and signals each one.
type fgFleetSink struct {
	mu     sync.Mutex
	events []uint64
	signal chan struct{}
}

func newFGFleetSink() *fgFleetSink {
	return &fgFleetSink{signal: make(chan struct{}, 1)}
}

func (s *fgFleetSink) Send(ctx context.Context, env es.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.events = append(s.events, env.GlobalPosition)
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
	return nil
}

func (s *fgFleetSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// waitFor blocks until the sink has delivered pos, or the deadline passes.
func (s *fgFleetSink) waitFor(pos uint64, deadline time.Time) bool {
	for {
		s.mu.Lock()
		got := slices.Contains(s.events, pos)
		s.mu.Unlock()
		if got {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-s.signal:
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func TestFeedGateHundredIdleSubscribers(t *testing.T) {
	dsn := fgDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	s, _, err := fgOpenStore(ctx, t, dsn, fleetMaxConn, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Pool().Stat().MaxConns(); got != fleetMaxConn {
		t.Fatalf("feedgate: fleet pool MaxConns = %d, want %d", got, fleetMaxConn)
	}

	polls := &fleetPollClock{}
	srv, err := feed.NewServer(feed.Config{
		Checkpoints: postgres.NewCheckpointStore(s),
		Leases:      postgres.NewLeaseStore(s),
		Logs:        postgres.NewLogs(s),
		Observer:    polls,
		// DeadLetters stays nil: the fleet has no poison path.
		// Everything else at the defaults: poll 250ms (with the
		// production ±20% idle jitter the default wires), lease TTL 30s,
		// batch 500 — the fleet runs production-shaped knobs.
	})
	if err != nil {
		t.Fatalf("feedgate: new server: %v", err)
	}

	// heldLeases counts the lease rows the fleet's subscriptions hold:
	// one row per acquired key, scoped to the fleet's workspace.
	heldLeases := func() int {
		var n int
		if err := s.Pool().QueryRow(ctx,
			`SELECT COUNT(*) FROM feed_leases WHERE workspace_id = $1`, "ws_fleet",
		).Scan(&n); err != nil {
			t.Fatalf("feedgate: count fleet leases: %v", err)
		}
		return n
	}

	type sub struct {
		sink   *fgFleetSink
		cancel context.CancelFunc
		done   chan error
	}
	subs := make([]*sub, fleetSubs)
	for i := range subs {
		req := feed.Request{
			Scope:      "ws_fleet",
			Projection: "gate.fleet",
			Instance:   fmt.Sprintf("fleet-%03d", i),
			Consumer:   "feedgate-fleet",
			Definition: projection.Definition{Name: "gate.fleet", Consumes: []string{fleetType}},
		}
		sink := newFGFleetSink()
		subCtx, subCancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- srv.Subscribe(subCtx, req, sink) }()
		subs[i] = &sub{sink: sink, cancel: subCancel, done: done}
	}

	// Quiet start: every subscription holds its lease and has polled the
	// empty log, and not one has delivered anything.
	deadline := time.Now().Add(10 * time.Second)
	for heldLeases() < fleetSubs {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("feedgate: only %d of %d subscriptions acquired their lease", heldLeases(), fleetSubs)
		}
		time.Sleep(5 * time.Millisecond)
	}
	quietEnd := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(quietEnd) {
		for i, sub := range subs {
			if n := sub.sink.count(); n != 0 {
				cancel()
				t.Fatalf("feedgate: subscription %d delivered %d events during the quiet start, want 0", i, n)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Idle jitter spread (Task 18, spec §5.12): over the quiet start, the
	// fleet's poll timestamps must spread across at least 12 of the 100ms
	// buckets in the 1.5s quiet window — the plan's "≥10 distinct 100ms
	// buckets" bar, measured over the full quiet start and tuned into the
	// measured gap between the two fleets. The ±20% per-wait jitter
	// random-walks every subscriber's poll phase off the shared start, so
	// the merged fleet covers the whole poll interval: a jittered fleet
	// reaches 14/15 in every measured run (the one empty bucket is
	// structural: the first jittered gap is ≥ 0.8*Poll, so the window's
	// second 100ms bucket can only be reached by late drift). A fleet
	// without the jitter polls the empty log in lockstep — herds of 100
	// on the poll lattice, 6-8 buckets in every measured run — the
	// thundering herd the jitter exists to break.
	//
	// The window is the full quiet start rather than a 1s slice of it:
	// page-read costs stretch the lattice and open dead zones between
	// its peaks, and on a slower runner a dead zone can fully swallow one
	// 100ms bucket of a 1s slice (measured on this runner: 13 real 1s
	// runs always hit 10/10, but one bucket held as few as 3 polls).
	// Requiring 12 of the quiet window's 15 buckets keeps a ≥2-bucket
	// margin on both sides of the gap across read-cost regimes, instead
	// of gambling the nightly gate on dead-zone alignment.
	const bucket = 100 * time.Millisecond
	windowEnd := time.Now()
	windowStart := windowEnd.Add(-1500 * time.Millisecond)
	var perBucket [15]int
	for _, ts := range polls.snapshot() {
		if ts.After(windowStart) && ts.Before(windowEnd) {
			perBucket[int(ts.Sub(windowStart)/bucket)]++
		}
	}
	spread := 0
	for _, n := range perBucket {
		if n > 0 {
			spread++
		}
	}
	if spread < 12 {
		cancel()
		t.Fatalf("feedgate: fleet idle polls spread across %d of 15 %v buckets over the quiet start, want at least 12 (per-bucket polls: %v) — the idle jitter is missing or ineffective",
			spread, bucket, perBucket)
	}
	t.Logf("feedgate: fleet idle polls spread across %d/15 %v buckets over the quiet start (per bucket: %v)",
		spread, bucket, perBucket)

	// One matching event; all 100 must deliver it within 1 second.
	ws := s.WorkspaceStore("ws_fleet")
	sid, err := es.NewStreamID("gate", "fleet-burst")
	if err != nil {
		t.Fatal(err)
	}
	res, err := ws.Append(ctx, es.AppendParams{
		StreamID: sid,
		Events:   []es.EventData{{TypeURL: fleetType, SchemaVersion: 1, Payload: []byte("fleet-burst-payload")}},
	})
	if err != nil {
		t.Fatalf("feedgate: fleet burst append: %v", err)
	}
	pos := res.Envelopes[0].GlobalPosition

	burstStart := time.Now()
	perSubDeadline := burstStart.Add(1 * time.Second)
	var late []int
	for i, sub := range subs {
		if !sub.sink.waitFor(pos, perSubDeadline) {
			late = append(late, i)
		}
	}
	if len(late) > 0 {
		cancel()
		if len(late) > 10 {
			late = late[:10]
		}
		t.Fatalf("feedgate: %d of %d subscriptions did not deliver position %d within 1s (first late: %v)",
			len(late), fleetSubs, pos, late)
	}
	fleetElapsed := time.Since(burstStart)
	for i, sub := range subs {
		if n := sub.sink.count(); n != 1 {
			t.Errorf("feedgate: subscription %d delivered %d events, want exactly 1", i, n)
		}
	}

	// Pool stats healthy: end every stream, then the pool must show zero
	// acquired connections (nothing hoarded or leaked) and stay within its
	// 50-connection budget.
	for _, sub := range subs {
		sub.cancel()
	}
	for i, sub := range subs {
		select {
		case err := <-sub.done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("feedgate: subscription %d ended with %v, want context.Canceled", i, err)
			}
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("feedgate: subscription %d did not return after cancellation", i)
		}
	}
	settle := time.Now().Add(3 * time.Second)
	for {
		acquired := s.Pool().Stat().AcquiredConns()
		if acquired == 0 {
			break
		}
		if time.Now().After(settle) {
			t.Errorf("feedgate: pool still holds %d acquired connections after every stream ended", acquired)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if total := s.Pool().Stat().TotalConns(); total > fleetMaxConn {
		t.Errorf("feedgate: pool TotalConns = %d, want <= %d", total, fleetMaxConn)
	}

	t.Logf("feedgate: fleet: %d/%d subscriptions delivered position %d in %v (quiet start: 0 deliveries)",
		fleetSubs, fleetSubs, pos, fleetElapsed)
}

// ---------------------------------------------------------------------------
// Benchmarks: the write-path evidence for the Gate 0 exit review (brief
// Step 5). The only append-path change of Phase 1 is the extra bare xid-draw
// statement (Task 2), so both halves are measured: absolute throughput of
// the 20-event batch append, and the per-call round-trip cost of
// `SELECT pg_current_xact_id()` on the same pool.
// ---------------------------------------------------------------------------

// BenchmarkWorkspaceAppend measures the batch-of-20 append the chaos
// appenders drive: one transaction with the xid draw, the head read, the
// commit clock and 20 INSERT..RETURNING statements.
func BenchmarkWorkspaceAppend(b *testing.B) {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		b.Skip("set PG_DSN to run the feedgate benchmarks (task test:feedgate starts a throwaway Postgres)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s, _, err := fgOpenStore(ctx, b, dsn, 8, true)
	if err != nil {
		b.Fatal(err)
	}
	ws := s.WorkspaceStore("ws_bench")
	sid, err := es.NewStreamID("gate", "bench")
	if err != nil {
		b.Fatal(err)
	}
	events := make([]es.EventData, 20)
	for i := range events {
		events[i] = es.EventData{
			TypeURL:       "gate.v1.Bench",
			SchemaVersion: 1,
			Payload:       []byte(fmt.Sprintf("bench-payload-%02d-0123456789abcdef", i)),
		}
	}

	var v uint64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := ws.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: v,
			Events:          events,
		})
		if err != nil {
			b.Fatal(err)
		}
		v = res.ToVersion
	}
	b.StopTimer()
	secs := b.Elapsed().Seconds()
	b.ReportMetric(float64(b.N)/secs, "appends/s")
	b.ReportMetric(float64(b.N*20)/secs, "events/s")
}

// BenchmarkWorkspaceAppendXidDraw isolates the one extra statement the fence
// work added to the append path (Task 2): a bare read-only
// SELECT pg_current_xact_id() in its own round trip on the same pool, in a
// tight loop — the per-call latency Phase 1 paid for commit-safe reads.
func BenchmarkWorkspaceAppendXidDraw(b *testing.B) {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		b.Skip("set PG_DSN to run the feedgate benchmarks (task test:feedgate starts a throwaway Postgres)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	_, pool, err := fgOpenStore(ctx, b, dsn, 8, true)
	if err != nil {
		b.Fatal(err)
	}
	var x uint64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.QueryRow(ctx, `SELECT pg_current_xact_id()`).Scan(&x); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "µs/call")
}
