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

	// acquireErr/acquireTrueOnErr simulate a store that took the lease but
	// still returned an error; releaseErr simulates a failed release.
	acquireErr       error
	acquireTrueOnErr bool
	releaseErr       error
	releases         int
}

type memLease struct {
	owner, consumer string
	expiresAt       time.Time
}

func newMemLeases() *memLeases {
	return &memLeases{rows: map[string]memLease{}, failAfter: -1}
}

// key composes the fake's internal map key from the triple. The separator
// cannot occur inside a part: validateKey refuses control bytes, and the
// tests never put "|" in a part.
func (m *memLeases) key(scope, projection, instance string) string {
	return scope + "|" + projection + "|" + instance
}

func (m *memLeases) Acquire(_ context.Context, scope, projection, instance, owner, consumer string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acquired = append(m.acquired, ttl)
	key := m.key(scope, projection, instance)
	if m.acquireErr != nil {
		if m.acquireTrueOnErr {
			// The lease was taken as a side effect even though the call
			// reported an error.
			m.rows[key] = memLease{owner: owner, consumer: consumer, expiresAt: time.Now().Add(ttl)}
		}
		return m.acquireTrueOnErr, m.acquireErr
	}
	if row, held := m.rows[key]; held && time.Now().Before(row.expiresAt) && row.owner != owner {
		return false, nil
	}
	m.rows[key] = memLease{owner: owner, consumer: consumer, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

func (m *memLeases) Renew(_ context.Context, scope, projection, instance, owner string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renewals++
	if m.failAfter >= 0 && m.renewals > m.failAfter {
		return false, m.renewErr
	}
	key := m.key(scope, projection, instance)
	row, held := m.rows[key]
	if !held || row.owner != owner {
		return false, nil
	}
	row.expiresAt = time.Now().Add(ttl)
	m.rows[key] = row
	return true, nil
}

func (m *memLeases) Release(_ context.Context, scope, projection, instance, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releases++
	key := m.key(scope, projection, instance)
	if row, held := m.rows[key]; held && row.owner == owner {
		delete(m.rows, key)
	}
	return m.releaseErr
}

func (m *memLeases) Holder(_ context.Context, scope, projection, instance string) (feed.Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, held := m.rows[m.key(scope, projection, instance)]
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

func (m *memLeases) releaseCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.releases
}

// failRenewalsFromNow makes the next renewal fail and returns the renewal
// count to wait for. Callers use it after the stream is known to be running,
// so the failure does not race startup.
func (m *memLeases) failRenewalsFromNow() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failAfter = m.renewals
	return m.renewals + 1
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
	// emptyTruncated makes every page report truncated with zero rows, the
	// pathological adapter the loop must not spin on.
	emptyTruncated bool
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
	if l.parent.emptyTruncated {
		return nil, true, nil
	}
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

// blockingSendSink blocks in Send until the test releases it, then returns a
// real send error. It lets a lease loss and a stream error race deterministically.
type blockingSendSink struct {
	sendErr error
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingSendSink) Send(ctx context.Context, _ es.Envelope) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
	case <-ctx.Done():
		// The renewer may cancel first; wait for the test to let Send finish
		// so the real error, not a derived cancellation, reaches Subscribe.
		<-s.release
	}
	return s.sendErr
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

func waitHeld(t *testing.T, leases *memLeases, scope, projection, instance string, want bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, held, err := leases.Holder(context.Background(), scope, projection, instance)
		if err != nil {
			t.Fatalf("Holder: %v", err)
		}
		if held == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease %s/%s/%s held = %v, want %v", scope, projection, instance, !want, want)
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

func waitWaits(t *testing.T, w *blockingWaiter, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for w.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d waits, got %d", n, w.count())
		}
		time.Sleep(time.Millisecond)
	}
}

func waitRenewals(t *testing.T, leases *memLeases, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for leases.renewCount() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d renewals, got %d", n, leases.renewCount())
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
	if _, held, _ := leases.Holder(context.Background(), "tenant", "p", ""); held {
		t.Fatal("invalid request acquired a lease")
	}
}

// Control bytes anywhere in a subscription identity are refused: adapters may
// join the parts into one storage key with NUL, so a NUL inside a part would
// collide distinct triples into one lease/checkpoint key — ("a\x00b","c","i")
// equals ("a","b\x00c","i") under a NUL join. The empty instance stays legal:
// it is the documented default instance.
func TestValidateKeyRejectsControlBytes(t *testing.T) {
	leases := newMemLeases()
	// The waiter fails fast so that, if validation regresses and accepts the
	// key, the stream errors out instead of idling and hanging the test.
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: newMemLogs(),
		Waiter: &blockingWaiter{err: errors.New("wakeup source failed")},
	})
	sink := newMemSink()

	for _, bad := range []string{"\x00", "\x01", "\x7f"} {
		for _, field := range []string{"scope", "projection", "instance"} {
			req := testRequest("live")
			switch field {
			case "scope":
				req.Scope = "ten" + bad + "ant"
			case "projection":
				req.Projection = "counter." + bad + "view"
				req.Definition.Name = req.Projection
			case "instance":
				req.Instance = "live" + bad
			}
			t.Run(fmt.Sprintf("%s %q", field, bad), func(t *testing.T) {
				err := srv.Subscribe(context.Background(), req, sink)
				if !errors.Is(err, feed.ErrInvalidRequest) {
					t.Fatalf("Subscribe with %s %q: error = %v, want ErrInvalidRequest", field, bad, err)
				}
			})
		}
	}
	if len(leases.acquired) != 0 {
		t.Fatalf("rejected requests acquired %d lease(s); validation must fail closed before the lease store", len(leases.acquired))
	}

	// The empty instance is the documented default instance and stays valid:
	// a fenced ack on the default key has to get past validateKey, so hold
	// the key's lease first and checkpoint through the full path.
	if _, err := leases.Acquire(context.Background(), "tenant", "counter.view", "", "owner-empty", "worker-1", time.Minute); err != nil {
		t.Fatalf("acquire the default-instance lease: %v", err)
	}
	ack := feed.Ack{Scope: "tenant", Projection: "counter.view", Instance: "", Owner: "owner-empty", Position: 1}
	if err := srv.Checkpoint(context.Background(), ack); err != nil {
		t.Fatalf("Checkpoint with the default (empty) instance: %v", err)
	}
}

// Request.Projection and Definition.Name name the same projection. A silent
// mismatch would stream one projection's type set but file its checkpoints
// under another projection's name.
func TestSubscribeRejectsProjectionNameMismatch(t *testing.T) {
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: newMemLogs(),
		// A failing waiter keeps a regressed (accepted) request from idling
		// forever: the stream errors out immediately instead.
		Waiter: &blockingWaiter{err: errors.New("wakeup source failed")},
	})
	req := testRequest("live")
	req.Projection = "counter-view"
	req.Definition.Name = "counterview"

	err := srv.Subscribe(context.Background(), req, newMemSink())
	if !errors.Is(err, feed.ErrInvalidRequest) {
		t.Fatalf("Subscribe with projection %q but definition %q: error = %v, want ErrInvalidRequest",
			req.Projection, req.Definition.Name, err)
	}
	if len(leases.acquired) != 0 {
		t.Fatal("a mismatched request acquired a lease")
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
	waitHeld(t, leases, req.Scope, req.Projection, req.Instance, true)

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
	waitHeld(t, leases, other.Scope, other.Projection, other.Instance, true)

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
	waitHeld(t, leases, "tenant", "counter.view", "live", false)
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

// An adapter that reports truncated with zero rows is misbehaving. The loop
// must wait (not spin) and must not call the log in a tight loop.
func TestSubscribeWaitsOnAZeroRowTruncatedPage(t *testing.T) {
	logs := newMemLogs()
	logs.emptyTruncated = true
	waiter := &blockingWaiter{}
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: newMemLeases(), Logs: logs,
		Waiter: waiter,
	})
	cancel, done := start(t, srv, testRequest("live"), newMemSink())

	waitWaits(t, waiter, 1)
	if calls := len(logs.callsSnapshot()); calls != 1 {
		t.Fatalf("log reads = %d before the first wait, want 1 (no spin)", calls)
	}
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe error = %v, want context.Canceled", err)
	}
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
	waitHeld(t, leases, req.Scope, req.Projection, req.Instance, true)

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

// A checkpoint ahead of the head (a log restored to an earlier point) is
// reported as negative lag, not clamped away: the operator needs to see the
// inconsistency.
func TestStatusReportsNegativeLagWhenCheckpointIsAhead(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	checkpoints.set("tenant", "counter.view", "live", 9)
	logs := newMemLogs()
	logs.setHead(4)
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: newMemLeases(), Logs: logs,
	})
	got, err := srv.Status(context.Background(), "tenant", "counter.view", "live")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got.Position != 9 || got.Head != 4 || got.Lag != -5 {
		t.Fatalf("Status = %+v, want position 9 head 4 lag -5", got)
	}
}

func TestSubscribeCancellationReleasesTheLease(t *testing.T) {
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: newMemLogs(),
		Waiter: &blockingWaiter{},
	})
	req := testRequest("live")
	cancel, done := start(t, srv, req, newMemSink())
	waitHeld(t, leases, req.Scope, req.Projection, req.Instance, true)

	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe error = %v, want context.Canceled", err)
	}
	waitHeld(t, leases, req.Scope, req.Projection, req.Instance, false)
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
	if strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("error = %v, want only the lease loss (the cancellation is derived)", err)
	}
	waitHeld(t, leases, req.Scope, req.Projection, req.Instance, false)
}

// A real stream error that races a lease loss must keep both diagnoses: the
// lease loss is terminal, but the send failure explains why the stream was in
// trouble and must not be silently replaced.
func TestSubscribeJoinsAStreamErrorRacingLeaseLoss(t *testing.T) {
	leases := newMemLeases()
	sendErr := errors.New("client write failed")
	sink := &blockingSendSink{
		sendErr: sendErr,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases,
		Logs:       newMemLogs(ev(1, "counter.v1.Incremented", "one")),
		LeaseTTL:   50 * time.Millisecond,
		RenewEvery: 5 * time.Millisecond,
	})
	_, done := start(t, srv, testRequest("live"), sink)

	select {
	case <-sink.started:
	case <-time.After(3 * time.Second):
		t.Fatal("Send was not called")
	}
	// Fail the next renewal only after the stream is inside Send, so the send
	// failure and the lease loss are genuinely concurrent.
	target := leases.failRenewalsFromNow()
	waitRenewals(t, leases, target)
	close(sink.release)

	err := await(t, done)
	if !errors.Is(err, feed.ErrLeaseLost) {
		t.Fatalf("error = %v, want ErrLeaseLost", err)
	}
	if !errors.Is(err, sendErr) {
		t.Fatalf("error = %v, want the racing send error preserved", err)
	}
	var lost *feed.LeaseLostError
	if !errors.As(err, &lost) {
		t.Fatalf("error = %v (%T), want *LeaseLostError", err, err)
	}
}

// The stream runs under a child of the caller's ctx, so a caller deadline is
// echoed by the stream's own context error. Joining both would report the
// same deadline twice.
func TestSubscribeJoinsDeadlineExceededOnce(t *testing.T) {
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: newMemLogs(),
		Waiter: &blockingWaiter{},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := srv.Subscribe(ctx, testRequest("live"), newMemSink())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if n := strings.Count(err.Error(), "deadline exceeded"); n != 1 {
		t.Fatalf("error mentions the deadline %d times, want exactly once: %q", n, err)
	}
	waitHeld(t, leases, "tenant", "counter.view", "live", false)
}

// A store may report an error after it took the lease. (true, err) means the
// key was acquired, so Subscribe must release it before returning.
func TestSubscribeReleasesALeaseAcquiredWithAnError(t *testing.T) {
	leases := newMemLeases()
	acquireErr := errors.New("acquire commit uncertain")
	releaseErr := errors.New("release store down")
	leases.acquireErr = acquireErr
	leases.acquireTrueOnErr = true
	leases.releaseErr = releaseErr
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases, Logs: newMemLogs(),
	})
	req := testRequest("live")

	err := srv.Subscribe(context.Background(), req, newMemSink())
	if !errors.Is(err, acquireErr) {
		t.Fatalf("error = %v, want the acquire error", err)
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("error = %v, want the release error joined", err)
	}
	if releases := leases.releaseCount(); releases != 1 {
		t.Fatalf("releases = %d, want exactly 1", releases)
	}
	waitHeld(t, leases, req.Scope, req.Projection, req.Instance, false)
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
	waitHeld(t, leases, req.Scope, req.Projection, req.Instance, false)
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
	waitHeld(t, leases, req.Scope, req.Projection, req.Instance, false)
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
	waitHeld(t, leases, req.Scope, req.Projection, req.Instance, false)
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

// Negative size/budget values have deliberate, opposite semantics: a negative
// MaxPayloadBytes disables the payload check entirely, while a negative
// MaxBatchBytes falls back to the default budget — the log always receives a
// positive budget.
func TestConfigNegativeValueSemantics(t *testing.T) {
	t.Run("negative MaxPayloadBytes disables the size check", func(t *testing.T) {
		// One byte past the documented 8 MiB default: the default would
		// reject this event, the disabled check must deliver it.
		const overDefault = 8<<20 + 1
		logs := newMemLogs(ev(1, "counter.v1.Incremented", strings.Repeat("x", overDefault)))
		srv := newServer(t, feed.Config{
			Checkpoints: newMemCheckpoints(time.Now()), Leases: newMemLeases(), Logs: logs,
			Waiter:          &blockingWaiter{},
			MaxPayloadBytes: -1,
		})
		sink := newMemSink()
		cancel, done := start(t, srv, testRequest("live"), sink)

		got := sink.waitFor(t, 1)
		if len(got[0].Payload) != overDefault {
			t.Fatalf("payload sent = %d bytes, want %d (the negative limit must not reject)", len(got[0].Payload), overDefault)
		}
		cancel()
		_ = await(t, done)
	})

	t.Run("negative MaxBatchBytes uses the default budget", func(t *testing.T) {
		logs := newMemLogs()
		srv := newServer(t, feed.Config{
			Checkpoints: newMemCheckpoints(time.Now()), Leases: newMemLeases(), Logs: logs,
			Waiter:        &blockingWaiter{},
			MaxBatchBytes: -1,
		})
		cancel, done := start(t, srv, testRequest("live"), newMemSink())

		calls := waitCalls(t, logs, 1)
		if calls[0].maxBytes != 16<<20 {
			t.Fatalf("ReadAllBounded maxBytes = %d, want the documented 16 MiB default", calls[0].maxBytes)
		}
		cancel()
		_ = await(t, done)
	})
}

func TestCheckpointSavesThePosition(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: leases, Logs: newMemLogs(),
	})
	// Checkpoint is fenced on the lease: the ack's owner must hold the key.
	if _, err := leases.Acquire(context.Background(), "tenant", "counter.view", "live", "owner-a", "worker-1", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	ack := feed.Ack{
		Scope: "tenant", Projection: "counter.view", Instance: "live",
		Owner: "owner-a", Position: 42,
	}
	if err := srv.Checkpoint(context.Background(), ack); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	pos, err := checkpoints.Load(context.Background(), "tenant", "counter.view", "live")
	if err != nil || pos != 42 {
		t.Fatalf("Load = %d, %v; want 42", pos, err)
	}
}

// Review finding R2's zombie, end to end. Subscriber A delivers and acks
// while it holds the lease; then A's lease expires and B takes the
// subscription over, acks its own progress and leaves. The late ack from A —
// an in-flight receipt from the dead subscription — must be rejected with
// ErrLeaseLost, and the stored cursor must stay exactly where B's last ack
// put it. The position assertion is the point: a rejected ack that still
// moved the cursor would advance the shared subscription past events nobody
// delivered, which is precisely the review's harm.
func TestCheckpointRejectsStaleOwner(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	leases := newMemLeases()
	logs := newMemLogs(
		ev(1, "counter.v1.Incremented", "one"),
		ev(2, "counter.v1.Incremented", "two"),
	)
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: leases, Logs: logs,
		Waiter:     &blockingWaiter{},
		LeaseTTL:   40 * time.Millisecond,
		RenewEvery: 4 * time.Millisecond,
	})

	// A subscribes with a caller-supplied owner, delivers both events and
	// acks the delivered position while it holds the lease.
	reqA := testRequest("live")
	reqA.Owner = "owner-a"
	sinkA := newMemSink()
	_, doneA := start(t, srv, reqA, sinkA)
	sinkA.waitFor(t, 2)
	if err := srv.Checkpoint(context.Background(), feed.Ack{
		Scope: "tenant", Projection: "counter.view", Instance: "live",
		Owner: "owner-a", Position: 2,
	}); err != nil {
		t.Fatalf("A's ack while it holds the lease: %v", err)
	}

	// A's lease expires: its next renewal fails, the stream dies with
	// ErrLeaseLost and the key is free again.
	target := leases.failRenewalsFromNow()
	waitRenewals(t, leases, target)
	if err := await(t, doneA); !errors.Is(err, feed.ErrLeaseLost) {
		t.Fatalf("stream A error = %v, want ErrLeaseLost (its lease expired)", err)
	}
	waitHeld(t, leases, "tenant", "counter.view", "live", false)
	leases.failAfter = -1 // re-arm the store so B's stream renews normally

	// B takes the key over, delivers the event appended after A's ack and
	// acks its own progress. B's stream ends too, leaving the key free.
	logs.append("tenant", ev(3, "counter.v1.Incremented", "three"))
	reqB := testRequest("live")
	reqB.Owner = "owner-b"
	sinkB := newMemSink()
	cancelB, doneB := start(t, srv, reqB, sinkB)
	sinkB.waitFor(t, 1)
	if err := srv.Checkpoint(context.Background(), feed.Ack{
		Scope: "tenant", Projection: "counter.view", Instance: "live",
		Owner: "owner-b", Position: 3,
	}); err != nil {
		t.Fatalf("B's ack while it holds the lease: %v", err)
	}
	cancelB()
	_ = await(t, doneB)
	waitHeld(t, leases, "tenant", "counter.view", "live", false)

	// A's zombie ack claims a position far past everything delivered.
	// Nothing holds the key, so it is rejected with ErrLeaseLost ...
	before, err := checkpoints.Load(context.Background(), "tenant", "counter.view", "live")
	if err != nil {
		t.Fatalf("Load before the stale ack: %v", err)
	}
	err = srv.Checkpoint(context.Background(), feed.Ack{
		Scope: "tenant", Projection: "counter.view", Instance: "live",
		Owner: "owner-a", Position: 99,
	})
	if !errors.Is(err, feed.ErrLeaseLost) {
		t.Fatalf("stale ack error = %v, want ErrLeaseLost", err)
	}
	var lost *feed.LeaseLostError
	if !errors.As(err, &lost) || lost.Owner != "owner-a" {
		t.Fatalf("stale ack error = %v (%T), want *LeaseLostError naming owner-a", err, err)
	}
	// ... and the stored cursor is unchanged: B's position, never A's claim.
	after, err := checkpoints.Load(context.Background(), "tenant", "counter.view", "live")
	if err != nil {
		t.Fatalf("Load after the stale ack: %v", err)
	}
	if after != before || after != 3 {
		t.Fatalf("stored position = %d after the stale ack, want %d (unchanged, never advanced past the delivered 3)", after, before)
	}
}

// An ack whose owner is not the live holder is rejected with
// *LeaseHeldError and never touches the cursor: the live subscriber's
// redelivery position is the truth, not a foreign claim.
func TestCheckpointRejectsForeignOwner(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	checkpoints.set("tenant", "counter.view", "live", 5)
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: leases, Logs: newMemLogs(),
		Waiter: &blockingWaiter{},
	})
	req := testRequest("live")
	req.Owner = "owner-b"
	cancel, done := start(t, srv, req, newMemSink())
	waitHeld(t, leases, "tenant", "counter.view", "live", true)

	err := srv.Checkpoint(context.Background(), feed.Ack{
		Scope: "tenant", Projection: "counter.view", Instance: "live",
		Owner: "owner-a", Position: 99,
	})
	if !errors.Is(err, feed.ErrLeaseHeld) {
		t.Fatalf("foreign-owner ack error = %v, want ErrLeaseHeld", err)
	}
	var held *feed.LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("foreign-owner ack error = %T, want *LeaseHeldError", err)
	}
	if !held.Held || held.Lease.Owner != "owner-b" {
		t.Fatalf("held error = %+v, want the live holder owner-b", held)
	}
	if got, _ := checkpoints.Load(context.Background(), "tenant", "counter.view", "live"); got != 5 {
		t.Fatalf("stored position = %d, want 5 (a rejected ack never touches the cursor)", got)
	}
	cancel()
	_ = await(t, done)
}

// An ack with no lease behind it at all — the key was never held, or its
// holder is gone — is rejected with ErrLeaseLost and leaves the cursor
// untouched.
func TestCheckpointRejectsWithoutLease(t *testing.T) {
	checkpoints := newMemCheckpoints(time.Now())
	srv := newServer(t, feed.Config{
		Checkpoints: checkpoints, Leases: newMemLeases(), Logs: newMemLogs(),
	})
	err := srv.Checkpoint(context.Background(), feed.Ack{
		Scope: "tenant", Projection: "counter.view", Instance: "live",
		Owner: "owner-a", Position: 7,
	})
	if !errors.Is(err, feed.ErrLeaseLost) {
		t.Fatalf("ack without a lease error = %v, want ErrLeaseLost", err)
	}
	var lost *feed.LeaseLostError
	if !errors.As(err, &lost) || lost.Owner != "owner-a" {
		t.Fatalf("ack without a lease error = %v (%T), want *LeaseLostError naming owner-a", err, err)
	}
	if got, _ := checkpoints.Load(context.Background(), "tenant", "counter.view", "live"); got != 0 {
		t.Fatalf("stored position = %d, want 0 (a rejected ack never touches the cursor)", got)
	}
}

// Request.Owner becomes the lease owner verbatim: it is what Holder reports,
// what a second subscriber is refused against, and what Ack.Owner must
// match. A reconnect reusing the same token re-acquires its own key
// (self-steal) and streams; while both same-token streams are live an
// owner-only lease port cannot tell them apart, but the steal fences the
// first stream out as soon as the reconnect's stream ends and releases the
// key: its next renewal fails and it dies with *LeaseLostError instead of
// silently double-holding the subscription.
func TestSubscribeUsesRequestOwnerAsLeaseOwner(t *testing.T) {
	leases := newMemLeases()
	srv := newServer(t, feed.Config{
		Checkpoints: newMemCheckpoints(time.Now()), Leases: leases,
		Logs:       newMemLogs(ev(1, "counter.v1.Incremented", "one")),
		Waiter:     &blockingWaiter{},
		LeaseTTL:   50 * time.Millisecond,
		RenewEvery: 5 * time.Millisecond,
	})
	reqA := testRequest("live")
	reqA.Owner = "owner-a"

	// The first subscription acquires with the request's token, verbatim.
	_, doneA := start(t, srv, reqA, newMemSink())
	waitHeld(t, leases, "tenant", "counter.view", "live", true)
	lease, held, err := leases.Holder(context.Background(), "tenant", "counter.view", "live")
	if err != nil || !held {
		t.Fatalf("Holder: held = %v, err = %v", held, err)
	}
	if lease.Owner != "owner-a" {
		t.Fatalf("holder owner = %q, want the request's owner %q", lease.Owner, "owner-a")
	}

	// A second subscriber under a different owner is refused.
	reqB := testRequest("live")
	reqB.Owner = "owner-b"
	err = srv.Subscribe(context.Background(), reqB, newMemSink())
	if !errors.Is(err, feed.ErrLeaseHeld) {
		t.Fatalf("second Subscribe with a different owner: error = %v, want ErrLeaseHeld", err)
	}
	var heldErr *feed.LeaseHeldError
	if !errors.As(err, &heldErr) || heldErr.Lease.Owner != "owner-a" {
		t.Fatalf("second Subscribe error = %v, want *LeaseHeldError naming the holder owner-a", err)
	}

	// Re-subscribing with the same token re-acquires the key (self-steal):
	// the reconnect streams alongside the stolen-from stream.
	sinkC := newMemSink()
	cancelC, doneC := start(t, srv, reqA, sinkC)
	sinkC.waitFor(t, 1) // the reconnect's stream is live and delivering

	// The reconnect's stream ends and releases the key (as every Subscribe
	// does before returning). The stolen-from stream's next renewal then
	// fails and it dies with *LeaseLostError.
	cancelC()
	_ = await(t, doneC)
	err = await(t, doneA)
	if !errors.Is(err, feed.ErrLeaseLost) {
		t.Fatalf("first stream error = %v, want ErrLeaseLost after the self-steal's release", err)
	}
	var lost *feed.LeaseLostError
	if !errors.As(err, &lost) {
		t.Fatalf("first stream error = %T, want *LeaseLostError", err)
	}
	waitHeld(t, leases, "tenant", "counter.view", "live", false)
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
	waitHeld(t, leases, "tenant", "counter.view", "live", true)

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
	waitHeld(t, leases, "tenant", "counter.view", "live", true)

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
