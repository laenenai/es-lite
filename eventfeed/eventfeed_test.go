package eventfeed_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/eventfeed"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/projection"
	"github.com/laenenai/es-lite/sqlite"
)

// The eventfeed consumer adapter's tests: the binding that makes the
// fenced feed protocol load-bearing (review R8). Everything runs against
// the real feed core over the real SQLite adapters — the e2e test drives
// the adapter through cancellation and reconnect, the ack test pins the
// page-coalescing policy:
//
//   - at-least-once across reconnects: set equality after the marker
//     dedups redelivery, and each reconnect's first delivery within one
//     ack window of the last acked position;
//   - exactly one fenced ack per full page (never per event), trailing
//     sub-pages acking on the AckInterval tick while the stream is live
//     and at stream end — while the lease is still held, so the final
//     ack passes the fence.

// typConsumed is the single event type the test projection consumes. The
// test log holds only this type, so global positions are contiguous
// matching events and position arithmetic is event arithmetic.
const typConsumed = "eventfeed.v1.Happened"

// testDefinition is the projection under consumption: its name keys the
// marker, the checkpoint and the lease.
var testDefinition = projection.Definition{
	Name:     "eventfeed.TestProjection",
	Consumes: []string{typConsumed},
}

// The subscription identity every test acks under.
const (
	testScope    = "ws_eventfeed"
	testInstance = "live"
)

// openStore opens a fresh in-memory SQLite store named for the test, so
// every test owns one database.
func openStore(t testing.TB) *sqlite.Store {
	t.Helper()
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared"
	s, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// appendEvent appends one consumed event at the stream's next version and
// returns the committed envelope.
func appendEvent(t testing.TB, s *sqlite.Store, sid es.StreamID, version uint64) es.Envelope {
	t.Helper()
	res, err := s.Append(context.Background(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events: []es.EventData{{
			TypeURL:       typConsumed,
			SchemaVersion: 1,
			Payload:       []byte("x"),
		}},
	})
	if err != nil {
		t.Fatalf("append at version %d: %v", version+1, err)
	}
	return res.Envelopes[0]
}

func mustStreamID(t testing.TB, typ, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID(typ, id)
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	return sid
}

// newServer wires the real feed core over the SQLite adapters, tuned for
// tests: a short poll so catch-up is fast.
func newServer(t testing.TB, s *sqlite.Store, checkpoints feed.CheckpointStore) *feed.Server {
	t.Helper()
	srv, err := feed.NewServer(feed.Config{
		Logs:        sqlite.NewLogs(s),
		Checkpoints: checkpoints,
		Leases:      sqlite.NewLeaseStore(s),
		Poll:        10 * time.Millisecond,
		LeaseTTL:    time.Minute,
		RenewEvery:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return srv
}

// waitFor polls cond until it holds, failing the test on timeout.
func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// checkpointPosition reads the durable position the fenced acks advanced
// to, 0 when no checkpoint exists yet.
func checkpointPosition(t testing.TB, cp feed.CheckpointStore) uint64 {
	t.Helper()
	c, found, err := cp.Checkpoint(context.Background(), testScope, testDefinition.Name, testInstance)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if !found {
		return 0
	}
	return c.Position
}

// recordingHandle records every delivered position, in delivery order —
// the raw deliveries, before any marker dedup.
type recordingHandle struct {
	mu   sync.Mutex
	seen []uint64
}

func (h *recordingHandle) handle(_ context.Context, e es.Envelope) error {
	h.mu.Lock()
	h.seen = append(h.seen, e.GlobalPosition)
	h.mu.Unlock()
	return nil
}

func (h *recordingHandle) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.seen)
}

func (h *recordingHandle) positions() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint64(nil), h.seen...)
}

// savedAck is one ack that landed: Server.Checkpoint calls Save only after
// the holder check passes, so these are exactly the successful fenced
// acks — the call counts and positions the coalescing policy produces.
type savedAck struct {
	scope, projection, instance string
	definition                  string
	position                    uint64
}

// recordingCheckpoints wraps a CheckpointStore and records every Save.
type recordingCheckpoints struct {
	feed.CheckpointStore
	mu    sync.Mutex
	saves []savedAck
}

func (c *recordingCheckpoints) Save(ctx context.Context, scope, projection, instance, definition string, position uint64) error {
	c.mu.Lock()
	c.saves = append(c.saves, savedAck{scope, projection, instance, definition, position})
	c.mu.Unlock()
	return c.CheckpointStore.Save(ctx, scope, projection, instance, definition, position)
}

func (c *recordingCheckpoints) acks() []savedAck {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]savedAck(nil), c.saves...)
}

// assertAckShape pins the full Ack the adapter must always send: the
// subscription key and the definition fingerprint, never a partial one.
func assertAckShape(t testing.TB, acks ...savedAck) {
	t.Helper()
	for _, a := range acks {
		if a.scope != testScope || a.projection != testDefinition.Name || a.instance != testInstance {
			t.Fatalf("ack at %d named %s/%s/%s, want %s/%s/%s",
				a.position, a.scope, a.projection, a.instance, testScope, testDefinition.Name, testInstance)
		}
		if a.definition != testDefinition.Hash() {
			t.Fatalf("ack at %d carried definition %q, want the definition hash %q",
				a.position, a.definition, testDefinition.Hash())
		}
	}
}

// poisonObserver captures OnPoison reports without blocking: the
// Observer contract forbids blocking, and the reconnect loop may report
// the same poison more than once before the test cancels.
type poisonObserver struct {
	feed.NoopObserver
	poisons chan *feed.EventTooLargeError
}

func (o *poisonObserver) OnPoison(err *feed.EventTooLargeError) {
	select {
	case o.poisons <- err:
	default:
	}
}

// plantOverCap inserts one events row whose payload exceeds the store's
// cap, bypassing the append-time cap — after it exists, raw SQL is the
// only way such a row can be stored — and returns its global position.
func plantOverCap(t testing.TB, s *sqlite.Store, sid es.StreamID, version uint64, payload []byte) uint64 {
	t.Helper()
	var gp uint64
	err := s.DB().QueryRowContext(context.Background(),
		`INSERT INTO events (
			event_id, stream_type, stream_id, version,
			type_url, schema_version, occurred_at, recorded_at,
			correlation_id, causation_id, command_id,
			actor_type, actor_id, actor_principal, payload
		) VALUES (?,?,?,?, ?,?,?, ?,?,?, ?,?,?,?, ?)
		RETURNING global_position`,
		"11111111-1111-7111-8111-111111111111", sid.Type, sid.Canonical(), version,
		typConsumed, 1, "2026-01-01T00:00:00.000000000Z", "2026-01-01T00:00:00.000000000Z",
		"", "", "",
		sid.Type, "poison", "eventfeed:poison", payload,
	).Scan(&gp)
	if err != nil {
		t.Fatalf("plant over-cap row: %v", err)
	}
	return gp
}

// TestEventfeedDeliversAtLeastOnceAcrossReconnect runs the adapter over a
// real feed.Server and a real SQLite log, cancels the consumer mid-stream
// and reconnects it with appends in between, and asserts the at-least-once
// invariant the adapter exists to enforce (review R8):
//
//   - set equality: every appended position is applied exactly once after
//     the marker dedups redelivery, and nothing is invented;
//   - bounded redelivery: each reconnect's first delivery is within one
//     ack window of the last acked position — the cursor never strays more
//     than one window from what was delivered — and no round redelivers
//     more than one window.
func TestEventfeedDeliversAtLeastOnceAcrossReconnect(t *testing.T) {
	store := openStore(t)
	checkpoints := sqlite.NewCheckpointStore(store)
	server := newServer(t, store, checkpoints)

	const (
		batchSize = 4
		perRound  = 6 // a full page plus a trailing sub-page
		rounds    = 4 // the initial run plus three reconnects
	)
	ef := eventfeed.New(eventfeed.Config{
		Server:     server,
		Definition: testDefinition,
		Scope:      testScope,
		Instance:   testInstance,
		Consumer:   "eventfeed-e2e",
		BatchSize:  batchSize,
		// Ticks stay out of the picture: the reconnect-window assertions
		// below see only page-boundary and stream-end acks.
		AckInterval: time.Hour,
	})

	sid := mustStreamID(t, "eventfeed", "reconnect")
	marker := projection.NewMemoryMarker()

	var (
		mu        sync.Mutex
		round     int
		applied   = map[uint64]bool{}
		delivered = make([][]uint64, rounds)
	)
	// handle mirrors the runner's apply path: a position at or below the
	// marker is skipped but still acknowledged with nil; a new position
	// is applied and advances the marker. The marker is what dedups
	// redelivery across reconnects.
	handle := func(ctx context.Context, e es.Envelope) error {
		mu.Lock()
		delivered[round] = append(delivered[round], e.GlobalPosition)
		mu.Unlock()
		cur, err := marker.Load(ctx, testDefinition.Name)
		if err != nil {
			return err
		}
		if e.GlobalPosition <= cur {
			return nil
		}
		if err := marker.Save(ctx, testDefinition.Name, e.GlobalPosition); err != nil {
			return err
		}
		mu.Lock()
		applied[e.GlobalPosition] = true
		mu.Unlock()
		return nil
	}

	var (
		appended  []uint64
		lastAcked []uint64
	)
	for r := 0; r < rounds; r++ {
		ctx, cancel := context.WithCancel(context.Background())
		mu.Lock()
		round = r
		mu.Unlock()
		done := make(chan error, 1)
		go func() { done <- ef.Feed(ctx, handle) }()

		// Append mid-stream: the stream is live while these land.
		for k := 0; k < perRound; k++ {
			env := appendEvent(t, store, sid, uint64(len(appended)))
			appended = append(appended, env.GlobalPosition)
		}
		// Caught up: every appended position is applied.
		waitFor(t, "round catch-up", func() bool {
			mu.Lock()
			defer mu.Unlock()
			for _, p := range appended {
				if !applied[p] {
					return false
				}
			}
			return true
		})

		// Cancel mid-stream: the stream is live and caught up. The
		// stream-end ack fires before the lease is released, so the
		// checkpoint position is final once Feed returns.
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("round %d feed: %v", r, err)
		}
		lastAcked = append(lastAcked, checkpointPosition(t, checkpoints))
	}

	// Set equality after marker dedup: every appended position applied,
	// nothing lost, nothing invented.
	mu.Lock()
	defer mu.Unlock()
	if len(applied) != len(appended) {
		t.Fatalf("applied %d positions, want %d", len(applied), len(appended))
	}
	for _, p := range appended {
		if !applied[p] {
			t.Fatalf("position %d was never applied — a lost delivery", p)
		}
	}
	for _, rd := range delivered {
		for _, p := range rd {
			if !slices.Contains(appended, p) {
				t.Fatalf("position %d was delivered but never appended — invented", p)
			}
		}
	}

	// Every reconnect resumes within one ack window of the last acked
	// position, and redelivers no more than one window.
	seen := map[uint64]bool{}
	for r := 0; r < rounds; r++ {
		var dupes int
		for _, p := range delivered[r] {
			if seen[p] {
				dupes++
			}
			seen[p] = true
		}
		if dupes > batchSize {
			t.Fatalf("round %d redelivered %d events, want at most %d (one ack window)", r, dupes, batchSize)
		}
		if r == 0 {
			continue
		}
		if len(delivered[r]) == 0 {
			t.Fatalf("round %d delivered nothing, want the events appended since round %d", r, r-1)
		}
		first, acked := delivered[r][0], lastAcked[r-1]
		if first <= acked || first-acked > batchSize {
			t.Fatalf("round %d first delivery %d is not within one ack window of the last acked position %d",
				r, first, acked)
		}
	}
}

// TestEventfeedAcksAtPageBoundary pins the ack coalescing: a full page
// acks exactly once (never per event), a trailing sub-page acks on the
// AckInterval tick while the stream is live, and the last sub-page acks
// at stream end — before the lease is released, so it passes the fence
// and a clean shutdown leaves nothing to redeliver. The phases share one
// SQLite store, so the durable checkpoint carries the whole story.
func TestEventfeedAcksAtPageBoundary(t *testing.T) {
	store := openStore(t)
	sid := mustStreamID(t, "eventfeed", "acks")
	checkpoints := sqlite.NewCheckpointStore(store)

	const batchSize = 4
	appended := 0 // events appended so far; also the stream's next version

	// runPhase starts one feed over its own server (so each phase counts
	// its own acks), appends events, and returns the delivery recorder,
	// the ack recorder, and stop, which cancels and waits for the feed.
	runPhase := func(ackInterval time.Duration, events int) (*recordingHandle, *recordingCheckpoints, func()) {
		rec := &recordingCheckpoints{CheckpointStore: checkpoints}
		server := newServer(t, store, rec)
		ef := eventfeed.New(eventfeed.Config{
			Server:      server,
			Definition:  testDefinition,
			Scope:       testScope,
			Instance:    testInstance,
			Consumer:    "eventfeed-acks",
			BatchSize:   batchSize,
			AckInterval: ackInterval,
		})
		h := &recordingHandle{}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- ef.Feed(ctx, h.handle) }()
		for k := 0; k < events; k++ {
			appendEvent(t, store, sid, uint64(appended))
			appended++
		}
		stop := func() {
			cancel()
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("feed: %v", err)
			}
		}
		return h, rec, stop
	}

	// A full page acks once per page: 2×BatchSize events in pages of
	// BatchSize, exactly two fenced acks — not one per event.
	h1, rec1, stop := runPhase(time.Hour, 2*batchSize)
	waitFor(t, "the two page acks", func() bool {
		return checkpointPosition(t, checkpoints) == uint64(2*batchSize)
	})
	stop()
	acks := rec1.acks()
	if len(acks) != 2 {
		t.Fatalf("got %d acks for %d events, want exactly 2 (one per page, not one per event)", len(acks), 2*batchSize)
	}
	for i, want := range []uint64{batchSize, 2 * batchSize} {
		if acks[i].position != want {
			t.Fatalf("ack %d = position %d, want %d (the page boundary)", i, acks[i].position, want)
		}
	}
	if got := h1.count(); got != 2*batchSize {
		t.Fatalf("phase delivered %d events, want %d", got, 2*batchSize)
	}
	assertAckShape(t, acks...)

	// A trailing sub-page acks on the tick while the stream is live.
	_, rec2, stop := runPhase(30*time.Millisecond, 1)
	waitFor(t, "the tick ack", func() bool {
		return checkpointPosition(t, checkpoints) == uint64(2*batchSize+1)
	})
	// Further ticks are no-ops: the window was claimed.
	time.Sleep(90 * time.Millisecond)
	stop()
	acks = rec2.acks()
	if len(acks) != 1 || acks[0].position != uint64(2*batchSize+1) {
		t.Fatalf("tick acks = %+v, want exactly one at position %d", acks, 2*batchSize+1)
	}
	assertAckShape(t, acks...)

	// The final trailing sub-page acks at stream end. The tick is silent
	// and the page boundary is unreached, so no ack can fire before the
	// cancel.
	h3, rec3, stop := runPhase(time.Hour, batchSize-1)
	waitFor(t, "the sub-page delivered", func() bool {
		return h3.count() >= batchSize-1
	})
	// Let one poll cycle pass: the bookkeeping of the last delivery is
	// ordered before the server's next page read, so after a cycle the
	// window is settled — and still unacked.
	time.Sleep(100 * time.Millisecond)
	if got := checkpointPosition(t, checkpoints); got != uint64(2*batchSize+1) {
		t.Fatalf("checkpoint = %d before cancel, want %d — an ack fired below the page boundary", got, 2*batchSize+1)
	}
	stop()
	// The stream-end ack already completed before Feed returned: it
	// fires while the lease is still held, so it passes the fence.
	acks = rec3.acks()
	if len(acks) != 1 || acks[0].position != uint64(3*batchSize) {
		t.Fatalf("stream-end acks = %+v, want exactly one at position %d", acks, 3*batchSize)
	}
	assertAckShape(t, acks...)
}

// TestEventfeedReportsPoisonThroughObserver pins the observability
// contract: the oversized event that kills the stream surfaces through
// Config.Observer.OnPoison — named by position, type, size and limit,
// never by payload — while the reconnect loop retries with backoff, so
// alerting sees the wedge until an operator skips past the position.
func TestEventfeedReportsPoisonThroughObserver(t *testing.T) {
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared"
	store, err := sqlite.Open(context.Background(), dsn, sqlite.WithMaxPayloadBytes(64))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	server := newServer(t, store, sqlite.NewCheckpointStore(store))
	obs := &poisonObserver{poisons: make(chan *feed.EventTooLargeError, 8)}
	ef := eventfeed.New(eventfeed.Config{
		Server:      server,
		Definition:  testDefinition,
		Scope:       testScope,
		Instance:    testInstance,
		Consumer:    "eventfeed-poison",
		BatchSize:   4,
		AckInterval: time.Hour,
		Observer:    obs,
	})

	// One healthy event first, so the poison row is a later row: the page
	// stops before it and the next page makes it the first row. The
	// healthy append commits as version 1, so the planted row is version 2.
	sid := mustStreamID(t, "eventfeed", "poison")
	appendEvent(t, store, sid, 0)
	oversized := bytes.Repeat([]byte("p"), 64*64)
	poisonGP := plantOverCap(t, store, sid, 2, oversized)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	h := &recordingHandle{}
	go func() { done <- ef.Feed(ctx, h.handle) }()

	select {
	case got := <-obs.poisons:
		if got.Position != poisonGP || got.TypeURL != typConsumed ||
			got.Size != len(oversized) || got.Limit != 64 {
			t.Fatalf("OnPoison = (pos %d, type %q, size %d, limit %d), want (pos %d, type %q, size %d, limit %d)",
				got.Position, got.TypeURL, got.Size, got.Limit, poisonGP, typConsumed, len(oversized), 64)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no OnPoison report — the poison error was swallowed into the reconnect loop")
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("feed: %v", err)
	}
	// Only events below the poison row were ever delivered — the poison
	// payload never materialized — and the healthy one did arrive before
	// the first report. A reconnect may have redelivered it; the marker
	// dedups, so the count itself is not the contract.
	got := h.positions()
	if len(got) == 0 {
		t.Fatal("the event before the poison row was never delivered")
	}
	for _, p := range got {
		if p >= poisonGP {
			t.Fatalf("position %d was delivered, want only events below the poison row at %d", p, poisonGP)
		}
	}
}
