package feed_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/projection"
)

// ---------------------------------------------------------------------------
// Fakes: checkpoint, lease, log, sink, waiter and clock. No database.
// ---------------------------------------------------------------------------

// memCheckpoints is a CheckpointStore keyed by (scope, projection, instance).
type memCheckpoints struct {
	mu   sync.Mutex
	rows map[string]memCheckpoint
	now  time.Time
}

type memCheckpoint struct {
	position uint64
	at       time.Time
}

func newMemCheckpoints(now time.Time) *memCheckpoints {
	return &memCheckpoints{rows: map[string]memCheckpoint{}, now: now}
}

func (m *memCheckpoints) key(scope, projection, instance string) string {
	return scope + "|" + projection + "|" + instance
}

func (m *memCheckpoints) Load(_ context.Context, scope, projection, instance string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rows[m.key(scope, projection, instance)].position, nil
}

func (m *memCheckpoints) Save(_ context.Context, scope, projection, instance string, position uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[m.key(scope, projection, instance)] = memCheckpoint{position: position, at: m.now}
	return nil
}

func (m *memCheckpoints) Reset(_ context.Context, scope, projection, instance string, position uint64) error {
	return m.Save(context.Background(), scope, projection, instance, position)
}

func (m *memCheckpoints) Delete(_ context.Context, scope, projection, instance string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, m.key(scope, projection, instance))
	return nil
}

func (m *memCheckpoints) Checkpoint(_ context.Context, scope, projection, instance string) (feed.Checkpoint, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.rows[m.key(scope, projection, instance)]
	if !ok {
		return feed.Checkpoint{}, false, nil
	}
	return feed.Checkpoint{Position: row.position, UpdatedAt: row.at}, true, nil
}

func (m *memCheckpoints) set(scope, projection, instance string, position uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[m.key(scope, projection, instance)] = memCheckpoint{position: position, at: m.now}
}

// memLeases is a LeaseStore. failAfter is the number of successful Renew
// calls before every later Renew fails; -1 never fails.
type memLeases struct {
	mu        sync.Mutex
	rows      map[string]memLease
	renewals  int
	failAfter int
	renewErr  error
	acquired  []time.Duration // TTLs Acquire saw, newest last
}

type memLease struct {
	owner, consumer string
	expiresAt       time.Time
}

func newMemLeases() *memLeases {
	return &memLeases{rows: map[string]memLease{}, failAfter: -1}
}

func (m *memLeases) Acquire(_ context.Context, key, owner, consumer string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acquired = append(m.acquired, ttl)
	if row, held := m.rows[key]; held && time.Now().Before(row.expiresAt) && row.owner != owner {
		return false, nil
	}
	m.rows[key] = memLease{owner: owner, consumer: consumer, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

func (m *memLeases) Renew(_ context.Context, key, owner string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renewals++
	if m.failAfter >= 0 && m.renewals > m.failAfter {
		return false, m.renewErr
	}
	row, held := m.rows[key]
	if !held || row.owner != owner {
		return false, nil
	}
	row.expiresAt = time.Now().Add(ttl)
	m.rows[key] = row
	return true, nil
}

func (m *memLeases) Release(_ context.Context, key, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if row, held := m.rows[key]; held && row.owner == owner {
		delete(m.rows, key)
	}
	return nil
}

func (m *memLeases) Holder(_ context.Context, key string) (feed.Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, held := m.rows[key]
	if !held || !time.Now().Before(row.expiresAt) {
		return feed.Lease{}, false, nil
	}
	return feed.Lease{Owner: row.owner, Consumer: row.consumer, ExpiresAt: row.expiresAt}, true, nil
}

func (m *memLeases) renewCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.renewals
}

// memLogs is a Logs over in-memory per-scope event slices. It records every
// ReadAllBounded call so tests can pin the exact arguments the loop used.
type memLogs struct {
	mu      sync.Mutex
	events  map[string][]es.Envelope
	head    uint64
	headSet bool
	openErr error
	calls   []readCall
	opens   int
}

type readCall struct {
	scope    string
	from     uint64
	limit    int
	maxBytes int
	types    []string
}

func newMemLogs(events ...es.Envelope) *memLogs {
	return &memLogs{events: map[string][]es.Envelope{"tenant": events}}
}

func (l *memLogs) Open(_ context.Context, scope string) (feed.Log, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.opens++
	if l.openErr != nil {
		return nil, l.openErr
	}
	return &memLog{parent: l, scope: scope}, nil
}

func (l *memLogs) append(scope string, envs ...es.Envelope) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events[scope] = append(l.events[scope], envs...)
}

func (l *memLogs) setHead(h uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.head, l.headSet = h, true
}

func (l *memLogs) callsSnapshot() []readCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]readCall(nil), l.calls...)
}

func (l *memLogs) headOf(scope string) uint64 {
	if l.headSet {
		return l.head
	}
	var h uint64
	for _, e := range l.events[scope] {
		if e.GlobalPosition > h {
			h = e.GlobalPosition
		}
	}
	return h
}

type memLog struct {
	parent *memLogs
	scope  string
}

func (l *memLog) Head(context.Context) (uint64, error) {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()
	return l.parent.headOf(l.scope), nil
}

func (l *memLog) ReadAllBounded(ctx context.Context, from uint64, limit, maxBytes int, types []string) ([]es.Envelope, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()
	l.parent.calls = append(l.parent.calls, readCall{
		scope: l.scope, from: from, limit: limit, maxBytes: maxBytes,
		types: append([]string(nil), types...),
	})
	want := map[string]bool{}
	for _, t := range types {
		want[t] = true
	}
	var (
		out       []es.Envelope
		total     int
		truncated bool
	)
	for _, e := range l.parent.events[l.scope] {
		if e.GlobalPosition <= from || !want[e.TypeURL] {
			continue
		}
		if len(out) == limit {
			break
		}
		// The store always includes the first row of a page; after that it
		// stops before a row that would cross the byte budget.
		if len(out) > 0 && maxBytes > 0 && total+len(e.Payload) > maxBytes {
			truncated = true
			break
		}
		total += len(e.Payload)
		out = append(out, e)
	}
	return out, truncated, nil
}

// memSink records sent envelopes. Send fails with sendErr when set.
type memSink struct {
	mu      sync.Mutex
	events  []es.Envelope
	sendErr error
	signal  chan struct{}
}

func newMemSink() *memSink {
	return &memSink{signal: make(chan struct{}, 1)}
}

func (s *memSink) Send(ctx context.Context, env es.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.sendErr != nil {
		err := s.sendErr
		s.mu.Unlock()
		return err
	}
	s.events = append(s.events, env)
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
	return nil
}

func (s *memSink) snapshot() []es.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]es.Envelope(nil), s.events...)
}

// waitFor blocks until at least n events were sent (or fails the test).
func (s *memSink) waitFor(t *testing.T, n int) []es.Envelope {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := s.snapshot()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d events, got %d: %+v", n, len(got), got)
		}
		select {
		case <-s.signal:
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// blockingWaiter blocks until the stream is cancelled; it never busy-polls.
type blockingWaiter struct {
	mu      sync.Mutex
	waits   int
	err     error
	release chan struct{}
}

func (w *blockingWaiter) Wait(ctx context.Context, _ time.Duration) error {
	w.mu.Lock()
	w.waits++
	err := w.err
	w.mu.Unlock()
	if err != nil {
		return err
	}
	if w.release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.release:
			return nil
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (w *blockingWaiter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.waits
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func ev(position uint64, typeURL, payload string) es.Envelope {
	return es.Envelope{GlobalPosition: position, TypeURL: typeURL, Payload: []byte(payload)}
}

func testRequest(instance string) feed.Request {
	return feed.Request{
		Scope: "tenant", Projection: "counter.view", Instance: instance,
		Consumer: "worker-1",
		Definition: projection.Definition{
			Name:     "counter.view",
			Consumes: []string{"counter.v1.Incremented"},
		},
	}
}

// start launches Subscribe and returns a cancel func and its result channel.
func start(t *testing.T, srv *feed.Server, req feed.Request, sink feed.Sink) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(ctx, req, sink) }()
	t.Cleanup(cancel)
	return cancel, done
}

func await(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe did not return")
		return nil
	}
}

func waitHeld(t *testing.T, leases *memLeases, key string, want bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, held, err := leases.Holder(context.Background(), key)
		if err != nil {
			t.Fatalf("Holder: %v", err)
		}
		if held == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease %q held = %v, want %v", key, !want, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitCalls(t *testing.T, logs *memLogs, n int) []readCall {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		calls := logs.callsSnapshot()
		if len(calls) >= n {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d log reads, got %d", n, len(calls))
		}
		time.Sleep(time.Millisecond)
	}
}

func newServer(t *testing.T, cfg feed.Config) *feed.Server {
	t.Helper()
	srv, err := feed.NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestLeaseKeyIsNULSeparated(t *testing.T) {
	if got, want := feed.LeaseKey("t", "a/b", "c"), "t\x00a/b\x00c"; got != want {
		t.Fatalf("LeaseKey = %q, want %q", got, want)
	}
	if feed.LeaseKey("t", "a/b", "c") == feed.LeaseKey("t", "a", "b/c") {
		t.Fatal("LeaseKey collides across segment boundaries")
	}
}

func TestNewServerRequiresStores(t *testing.T) {
	base := feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()),
		Leases:      newMemLeases(),
		Logs:        newMemLogs(),
	}
	if _, err := feed.NewServer(feed.Config{}); err == nil {
		t.Fatal("NewServer accepted an empty config")
	}
	cases := map[string]feed.Config{
		"checkpoints": {Leases: base.Leases, Logs: base.Logs},
		"leases":      {Checkpoints: base.Checkpoints, Logs: base.Logs},
		"logs":        {Checkpoints: base.Checkpoints, Leases: base.Leases},
	}
	for name, cfg := range cases {
		if _, err := feed.NewServer(cfg); err == nil {
			t.Fatalf("NewServer accepted a config without %s", name)
		}
	}
}

func TestSubscribeRejectsInvalidRequests(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	leases := newMemLeases()
	srv := newServer(t, feed.Config{Checkpoints: checkpoints, Leases: leases, Logs: newMemLogs()})
	sink := newMemSink()

	cases := map[string]feed.Request{
		"no scope":      {Projection: "p", Definition: projection.Definition{Name: "p", Consumes: []string{"t"}}},
		"no projection": {Scope: "tenant", Definition: projection.Definition{Name: "p", Consumes: []string{"t"}}},
		"no definition name": {
			Scope: "tenant", Projection: "p",
			Definition: projection.Definition{Consumes: []string{"t"}},
		},
		"no consumes": {
			Scope: "tenant", Projection: "p",
			Definition: projection.Definition{Name: "p"},
		},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			err := srv.Subscribe(context.Background(), req, sink)
			if !errors.Is(err, feed.ErrInvalidRequest) {
				t.Fatalf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}
	if err := srv.Subscribe(context.Background(), testRequest("live"), nil); !errors.Is(err, feed.ErrInvalidRequest) {
		t.Fatalf("nil sink error = %v, want ErrInvalidRequest", err)
	}
	if _, held, _ := leases.Holder(context.Background(), feed.LeaseKey("tenant", "p", "")); held {
		t.Fatal("invalid request acquired a lease")
	}
}

func TestSubscribeAllowsOneSubscriberPerKey(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: leases, Logs: newMemLogs(),
		Waiter: &blockingWaiter{},
	})
	req := testRequest("live")

	_, done := start(t, srv, req, newMemSink())
	waitHeld(t, leases, feed.LeaseKey(req.Scope, req.Projection, req.Instance), true)

	err := srv.Subscribe(context.Background(), req, newMemSink())
	if !errors.Is(err, feed.ErrLeaseHeld) {
		t.Fatalf("second Subscribe error = %v, want ErrLeaseHeld", err)
	}
	var held *feed.LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("second Subscribe error = %T, want *LeaseHeldError", err)
	}
	if held.Lease.Consumer != "worker-1" {
		t.Fatalf("held lease = %+v, want the active consumer", held.Lease)
	}

	// Another instance is a different key, so it is allowed.
	other := testRequest("shadow")
	_, otherDone := start(t, srv, other, newMemSink())
	waitHeld(t, leases, feed.LeaseKey(other.Scope, other.Projection, other.Instance), true)

	if err := srv.Subscribe(context.Background(), req, newMemSink()); !errors.Is(err, feed.ErrLeaseHeld) {
		t.Fatalf("first stream lost its key: %v", err)
	}
	_ = otherDone
	_ = done
}

func TestSubscribeResumesFromCheckpoint(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	checkpoints.set("tenant", "counter.view", "live", 2)
	logs := newMemLogs(
		ev(1, "counter.v1.Incremented", "one"),
		ev(2, "counter.v1.Incremented", "two"),
		ev(3, "counter.v1.Incremented", "three"),
		ev(4, "counter.v1.Incremented", "four"),
	)
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: newMemLeases(), Logs: logs,
		Waiter: &blockingWaiter{},
	})
	sink := newMemSink()
	cancel, done := start(t, srv, testRequest("live"), sink)

	got := sink.waitFor(t, 2)
	if positions := positionsOf(got); !reflect.DeepEqual(positions, []uint64{3, 4}) {
		t.Fatalf("positions = %v, want [3 4] (resume after the checkpoint)", positions)
	}
	if calls := logs.callsSnapshot(); calls[0].from != 2 {
		t.Fatalf("first read from = %d, want 2", calls[0].from)
	}
	cancel()
	_ = await(t, done)
}

// The consumer owns the checkpoint: Subscribe streams events but never saves
// on send, so redelivery after a crash starts at the consumer's last
// acknowledged batch (the adapter's Checkpoint call is the only writer).
func TestSubscribeDoesNotSaveCheckpoints(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: newMemLeases(),
		Logs:   newMemLogs(ev(1, "counter.v1.Incremented", "one")),
		Waiter: &blockingWaiter{},
	})
	sink := newMemSink()
	cancel, done := start(t, srv, testRequest("live"), sink)
	sink.waitFor(t, 1)

	pos, err := checkpoints.Load(context.Background(), "tenant", "counter.view", "live")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if pos != 0 {
		t.Fatalf("checkpoint = %d after a send, want 0 (Subscribe never saves)", pos)
	}
	cancel()
	_ = await(t, done)
}

func TestSubscribeClampsCheckpointAheadOfHead(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	checkpoints.set("tenant", "counter.view", "live", 9)
	logs := newMemLogs(
		ev(1, "counter.v1.Incremented", "one"),
		ev(4, "counter.v1.Incremented", "four"),
	)
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: newMemLeases(), Logs: logs,
		Waiter: &blockingWaiter{},
	})
	cancel, done := start(t, srv, testRequest("live"), newMemSink())

	calls := waitCalls(t, logs, 1)
	if calls[0].from != 4 {
		t.Fatalf("first read from = %d, want the head 4 (checkpoint was 9)", calls[0].from)
	}
	// The stream is idle on its waiter, not failed: cancel must be what ends it.
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe error = %v, want context.Canceled", err)
	}
}

func TestSubscribeRequestsOnlyTheGivenEventTypes(t *testing.T) {
	logs := newMemLogs(
		ev(1, "a.v1.One", "a1"),
		ev(2, "b.v1.Two", "b1"),
		ev(3, "a.v1.One", "a2"),
	)
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: newMemLeases(), Logs: logs,
		Waiter: &blockingWaiter{},
	})
	req := testRequest("live")
	req.Definition.Consumes = []string{"a.v1.One"}
	sink := newMemSink()
	cancel, done := start(t, srv, req, sink)

	got := sink.waitFor(t, 2)
	if positions := positionsOf(got); !reflect.DeepEqual(positions, []uint64{1, 3}) {
		t.Fatalf("positions = %v, want [1 3]", positions)
	}
	for _, call := range logs.callsSnapshot() {
		if !reflect.DeepEqual(call.types, req.Definition.Consumes) {
			t.Fatalf("log types = %v, want the definition's %v", call.types, req.Definition.Consumes)
		}
	}
	cancel()
	_ = await(t, done)
}

func TestSubscribeRejectsAnOversizedEventWithoutLeakingThePayload(t *testing.T) {
	const payload = "supersecret-payload"
	logs := newMemLogs(ev(7, "counter.v1.Incremented", payload))
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: logs,
		Waiter:          &blockingWaiter{},
		MaxPayloadBytes: 4,
	})
	sink := newMemSink()
	_, done := start(t, srv, testRequest("live"), sink)

	err := await(t, done)
	var tooLarge *feed.EventTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("error = %v (%T), want *EventTooLargeError", err, err)
	}
	if tooLarge.Position != 7 || tooLarge.TypeURL != "counter.v1.Incremented" || tooLarge.Size != len(payload) || tooLarge.Limit != 4 {
		t.Fatalf("oversized error = %+v", tooLarge)
	}
	msg := err.Error()
	for _, want := range []string{"7", "counter.v1.Incremented", fmt.Sprint(len(payload))} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q does not name %q", msg, want)
		}
	}
	if strings.Contains(msg, payload) {
		t.Fatalf("error leaks the payload: %q", msg)
	}
	if len(sink.snapshot()) != 0 {
		t.Fatal("oversized event was sent")
	}
	waitHeld(t, leases, feed.LeaseKey("tenant", "counter.view", "live"), false)
}

func TestSubscribeAllowsAnEventExactlyAtTheLimit(t *testing.T) {
	logs := newMemLogs(ev(1, "counter.v1.Incremented", "12345678"))
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: newMemLeases(), Logs: logs,
		Waiter:          &blockingWaiter{},
		MaxPayloadBytes: 8,
	})
	sink := newMemSink()
	cancel, done := start(t, srv, testRequest("live"), sink)
	got := sink.waitFor(t, 1)
	if positions := positionsOf(got); !reflect.DeepEqual(positions, []uint64{1}) {
		t.Fatalf("positions = %v, want [1]", positions)
	}
	cancel()
	_ = await(t, done)
}

// A page cut by the byte budget must continue immediately: with a waiter that
// blocks until cancellation, the events after the truncation point can only
// arrive if the loop did not wait between pages.
func TestSubscribeContinuesImmediatelyAfterTruncation(t *testing.T) {
	logs := newMemLogs(
		ev(1, "counter.v1.Incremented", "12345"),
		ev(2, "counter.v1.Incremented", "67890"),
		ev(3, "counter.v1.Incremented", "abcde"),
	)
	waiter := &blockingWaiter{}
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: newMemLeases(), Logs: logs,
		Waiter:        waiter,
		MaxBatchBytes: 5,
	})
	sink := newMemSink()
	cancel, done := start(t, srv, testRequest("live"), sink)

	got := sink.waitFor(t, 3)
	if positions := positionsOf(got); !reflect.DeepEqual(positions, []uint64{1, 2, 3}) {
		t.Fatalf("positions = %v, want [1 2 3]", positions)
	}
	if waiter.count() == 0 {
		t.Fatal("expected the loop to reach the waiter after the log was drained")
	}
	cancel()
	_ = await(t, done)
}

func TestStatusReportsPositionHeadLagAndHolder(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	checkpoints := newMemCheckpoints(at)
	checkpoints.set("tenant", "counter.view", "live", 2)
	logs := newMemLogs()
	logs.setHead(5)
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: leases, Logs: logs,
		Waiter: &blockingWaiter{},
	})

	// No subscriber: position and head, no active consumer.
	got, err := srv.Status(context.Background(), "tenant", "counter.view", "live")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got.Position != 2 || got.Head != 5 || got.Lag != 3 {
		t.Fatalf("Status = %+v, want position 2 head 5 lag 3", got)
	}
	if got.ActiveConsumer != "" {
		t.Fatalf("ActiveConsumer = %q, want empty", got.ActiveConsumer)
	}
	if !got.LastCheckpointAt.Equal(at) {
		t.Fatalf("LastCheckpointAt = %v, want %v", got.LastCheckpointAt, at)
	}

	req := testRequest("live")
	_, done := start(t, srv, req, newMemSink())
	waitHeld(t, leases, feed.LeaseKey(req.Scope, req.Projection, req.Instance), true)

	got, err = srv.Status(context.Background(), "tenant", "counter.view", "live")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got.ActiveConsumer != req.Consumer {
		t.Fatalf("ActiveConsumer = %q, want %q", got.ActiveConsumer, req.Consumer)
	}
	_ = done
}

func TestStatusWithoutACheckpoint(t *testing.T) {
	logs := newMemLogs(ev(3, "counter.v1.Incremented", "three"))
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: newMemLeases(), Logs: logs,
	})
	got, err := srv.Status(context.Background(), "tenant", "counter.view", "live")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got.Position != 0 || got.Head != 3 || got.Lag != 3 {
		t.Fatalf("Status = %+v, want position 0 head 3 lag 3", got)
	}
	if !got.LastCheckpointAt.IsZero() || got.ActiveConsumer != "" {
		t.Fatalf("Status = %+v, want zero checkpoint time and no consumer", got)
	}
}

func TestSubscribeCancellationReleasesTheLease(t *testing.T) {
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: newMemLogs(),
		Waiter: &blockingWaiter{},
	})
	req := testRequest("live")
	key := feed.LeaseKey(req.Scope, req.Projection, req.Instance)
	cancel, done := start(t, srv, req, newMemSink())
	waitHeld(t, leases, key, true)

	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe error = %v, want context.Canceled", err)
	}
	waitHeld(t, leases, key, false)
}

func TestSubscribeAbortsWhenRenewFails(t *testing.T) {
	leases := newMemLeases()
	leases.failAfter = 0
	leases.renewErr = errors.New("lease store unavailable")
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: newMemLogs(),
		Waiter:     &blockingWaiter{},
		LeaseTTL:   50 * time.Millisecond,
		RenewEvery: 2 * time.Millisecond,
	})
	req := testRequest("live")
	_, done := start(t, srv, req, newMemSink())

	err := await(t, done)
	var lost *feed.LeaseLostError
	if !errors.As(err, &lost) {
		t.Fatalf("error = %v (%T), want *LeaseLostError", err, err)
	}
	if !errors.Is(err, feed.ErrLeaseLost) {
		t.Fatalf("error = %v, want ErrLeaseLost", err)
	}
	if !errors.Is(err, leases.renewErr) {
		t.Fatalf("error = %v, want the renew cause", err)
	}
	waitHeld(t, leases, feed.LeaseKey(req.Scope, req.Projection, req.Instance), false)
}

func TestSubscribeReleasesTheLeaseWhenTheLogFails(t *testing.T) {
	leases := newMemLeases()
	logs := newMemLogs()
	logs.openErr = errors.New("no log for tenant")
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: logs,
	})
	req := testRequest("live")
	_, done := start(t, srv, req, newMemSink())
	if err := await(t, done); !errors.Is(err, logs.openErr) {
		t.Fatalf("error = %v, want the Open error", err)
	}
	waitHeld(t, leases, feed.LeaseKey(req.Scope, req.Projection, req.Instance), false)
}

func TestSubscribeReturnsASinkErrorAndReleasesTheLease(t *testing.T) {
	leases := newMemLeases()
	logs := newMemLogs(ev(1, "counter.v1.Incremented", "one"))
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: logs,
		Waiter: &blockingWaiter{},
	})
	req := testRequest("live")
	sink := newMemSink()
	sendErr := errors.New("client went away")
	sink.sendErr = sendErr
	_, done := start(t, srv, req, sink)
	if err := await(t, done); !errors.Is(err, sendErr) {
		t.Fatalf("error = %v, want the sink error", err)
	}
	waitHeld(t, leases, feed.LeaseKey(req.Scope, req.Projection, req.Instance), false)
}

func TestSubscribeReturnsAWaitErrorAndReleasesTheLease(t *testing.T) {
	leases := newMemLeases()
	waitErr := errors.New("wakeup source failed")
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: newMemLogs(),
		Waiter: &blockingWaiter{err: waitErr},
	})
	req := testRequest("live")
	_, done := start(t, srv, req, newMemSink())
	if err := await(t, done); !errors.Is(err, waitErr) {
		t.Fatalf("error = %v, want the wait error", err)
	}
	waitHeld(t, leases, feed.LeaseKey(req.Scope, req.Projection, req.Instance), false)
}

func TestSubscribeBatchSizeDefaultsAndClamps(t *testing.T) {
	cases := []struct {
		name    string
		request int
		want    int
	}{
		{"zero uses Batch", 0, 500},
		{"over MaxBatch is clamped", 5000, 1000},
		{"negative uses the default", -1, 500},
		{"in range is kept", 7, 7},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			logs := newMemLogs()
			srv := newServer(t, feed.Config{
				Checkpoints: newMemCheckpoints(time.Now()), Leases: newMemLeases(), Logs: logs,
				Waiter: &blockingWaiter{},
			})
			req := testRequest(fmt.Sprintf("instance-%d", i))
			req.BatchSize = c.request
			cancel, done := start(t, srv, req, newMemSink())
			calls := waitCalls(t, logs, 1)
			if calls[0].limit != c.want {
				t.Fatalf("log limit = %d, want %d", calls[0].limit, c.want)
			}
			cancel()
			_ = await(t, done)
		})
	}
}

func TestCheckpointSavesThePosition(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: newMemLeases(), Logs: newMemLogs(),
	})
	if err := srv.Checkpoint(context.Background(), "tenant", "counter.view", "live", 42); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	pos, err := checkpoints.Load(context.Background(), "tenant", "counter.view", "live")
	if err != nil || pos != 42 {
		t.Fatalf("Load = %d, %v; want 42", pos, err)
	}
}

func TestResetCheckpointRefusesWhileHeldUnlessForced(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	checkpoints.set("tenant", "counter.view", "live", 5)
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: leases, Logs: newMemLogs(),
		Waiter: &blockingWaiter{},
	})
	_, done := start(t, srv, testRequest("live"), newMemSink())
	waitHeld(t, leases, feed.LeaseKey("tenant", "counter.view", "live"), true)

	err := srv.ResetCheckpoint(context.Background(), "tenant", "counter.view", "live", 0, false)
	if !errors.Is(err, feed.ErrLeaseHeld) {
		t.Fatalf("ResetCheckpoint while held = %v, want ErrLeaseHeld", err)
	}
	if got, _ := checkpoints.Load(context.Background(), "tenant", "counter.view", "live"); got != 5 {
		t.Fatalf("checkpoint changed to %d without force", got)
	}

	if err := srv.ResetCheckpoint(context.Background(), "tenant", "counter.view", "live", 0, true); err != nil {
		t.Fatalf("forced ResetCheckpoint: %v", err)
	}
	if got, _ := checkpoints.Load(context.Background(), "tenant", "counter.view", "live"); got != 0 {
		t.Fatalf("forced checkpoint = %d, want 0", got)
	}
	_ = done
}

func TestDeleteSubscriptionRefusesWhileHeld(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	checkpoints.set("tenant", "counter.view", "live", 5)
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: leases, Logs: newMemLogs(),
		Waiter: &blockingWaiter{},
	})
	cancel, done := start(t, srv, testRequest("live"), newMemSink())
	waitHeld(t, leases, feed.LeaseKey("tenant", "counter.view", "live"), true)

	if err := srv.DeleteSubscription(context.Background(), "tenant", "counter.view", "live"); !errors.Is(err, feed.ErrLeaseHeld) {
		t.Fatalf("DeleteSubscription while held = %v, want ErrLeaseHeld", err)
	}
	if _, found, _ := checkpoints.Checkpoint(context.Background(), "tenant", "counter.view", "live"); !found {
		t.Fatal("checkpoint deleted while the lease was held")
	}

	cancel()
	_ = await(t, done)
	if err := srv.DeleteSubscription(context.Background(), "tenant", "counter.view", "live"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	if _, found, _ := checkpoints.Checkpoint(context.Background(), "tenant", "counter.view", "live"); found {
		t.Fatal("checkpoint survived DeleteSubscription")
	}
}

func positionsOf(envs []es.Envelope) []uint64 {
	out := make([]uint64, len(envs))
	for i, e := range envs {
		out[i] = e.GlobalPosition
	}
	return out
}
