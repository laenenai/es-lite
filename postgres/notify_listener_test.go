package postgres

// Integration tests for NotifyListener (ADR 0039 C2, the receiving half
// of the opt-in Postgres wake hint) against a real Postgres. Internal —
// package postgres — because the throttle test sets the unexported
// window before Run starts, and the reconnect test reads the listener's
// backend PID to kill it. Every test that needs a live server skips
// without PG_DSN, like the rest of the Postgres suite; `task test:pg`
// starts a throwaway container.
//
// The contract under test is the ADR's: one dedicated connection (never
// a pooled one), LISTEN quoted with pgx.Identifier.Sanitize,
// notifications delivered as OnNotify(scope) with the workspace ID as
// the payload, a reconnect that re-LISTENs and reports OnReconnected, a
// per-scope throttle that bounds duplicate and spoofed amplification,
// and a Run that returns ctx.Err() when cancelled. The channel names
// here are fixed test fixtures, not capabilities; production channels
// are high-entropy and never logged (docs/deploy/README.md).

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
)

// listenerDSN returns PG_DSN or skips the test.
func listenerDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	return dsn
}

// openNotifyStore opens a Store whose append path emits the C2 hint on
// channel. Unlike the external suite's openStoreWithOptions it does not
// truncate — these tests append to per-test workspaces and read nothing
// back — and it needs no es_relay setup: the relay gate covers Drain and
// the anchor bypass, not plain workspace appends.
func openNotifyStore(t *testing.T, channel string) *Store {
	t.Helper()
	s, err := Open(context.Background(), listenerDSN(t), WithAppendNotify(channel))
	if err != nil {
		t.Fatalf("open store with append notify: %v", err)
	}
	t.Cleanup(s.Close)
	// Isolate each test from earlier runs and from each other: the
	// fixed workspace and stream IDs would otherwise collide across
	// invocations — a second `go test -run` or the full suite
	// re-appends at v0 to a stream that already sits at v1. Tests run
	// sequentially, so a truncate is safe; this is the same isolation
	// the external suite's openStoreWithOptions applies.
	if _, err := s.Pool().Exec(context.Background(),
		`TRUNCATE events, log_order, unique_claims, anchors, feed_checkpoints, feed_leases, feed_dead_letters, command_dedup`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

// listenerEvents mirrors the external suite's eventData: one opaque
// event, enough for a non-empty — therefore notifying — append.
func listenerEvents(payload string) []es.EventData {
	return []es.EventData{{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: []byte(payload)}}
}

func listenerStreamID(t *testing.T) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("counter", "main")
	if err != nil {
		t.Fatal(err)
	}
	return sid
}

// recordingNotifyHandler records OnNotify and OnReconnected calls behind
// a mutex. OnNotify must not block (the listener's contract), so it
// appends and returns immediately; the tests poll the recorded state
// instead of receiving on a channel Run's goroutine could park on.
type recordingNotifyHandler struct {
	mu         sync.Mutex
	notifies   []string
	reconnects int
}

func (h *recordingNotifyHandler) OnNotify(scope string) {
	h.mu.Lock()
	h.notifies = append(h.notifies, scope)
	h.mu.Unlock()
}

func (h *recordingNotifyHandler) OnReconnected() {
	h.mu.Lock()
	h.reconnects++
	h.mu.Unlock()
}

func (h *recordingNotifyHandler) notifyCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.notifies)
}

// scopes snapshots the delivered scopes in delivery order.
func (h *recordingNotifyHandler) scopes() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.notifies...)
}

func (h *recordingNotifyHandler) reconnectCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reconnects
}

// waitUntil polls ok every millisecond until it holds or within
// elapses, reporting whether ok ever held. One millisecond keeps the
// measurement error of the 50 ms latency assertion small.
func waitUntil(within time.Duration, ok func() bool) bool {
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// runListener starts l.Run on its own goroutine and returns stop, which
// cancels Run's context and waits (bounded) for Run to return,
// surfacing its error. stop is idempotent and registered as a cleanup,
// so every test joins its listener — which means every test also checks
// that Run honours cancellation, not just the cancel test.
func runListener(t *testing.T, l *NotifyListener) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	var (
		once sync.Once
		err  error
	)
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				// Run outliving a cancelled context is a real defect,
				// but it must not hang the suite on top of it.
				t.Error("postgres: notify listener Run did not return within 10s of cancellation")
				err = ctx.Err()
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// waitListenerPID polls until the listener reports its backend PID —
// nonzero means connected AND LISTENing, because Run stores the PID
// only after LISTEN succeeds — and fails the test at the deadline.
func waitListenerPID(t *testing.T, l *NotifyListener, within time.Duration) uint32 {
	t.Helper()
	if !waitUntil(within, func() bool { return l.PID() != 0 }) {
		t.Fatalf("notify listener did not connect and LISTEN within %v (PID still 0)", within)
	}
	return l.PID()
}

// appendNotify pins one non-empty — therefore notifying — append to ws
// at the given expected version.
func appendNotify(t *testing.T, ws *Workspace, sid es.StreamID, version uint64, payload string) {
	t.Helper()
	if _, err := ws.Append(context.Background(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events:          listenerEvents(payload),
	}); err != nil {
		t.Fatalf("append v%d: %v", version, err)
	}
}

// TestNotifyListenerDelivers is the C2 correctness core from the
// receiving side: with the store emitting the hint and the listener
// LISTENing, a committed append reaches OnNotify — with the workspace
// ID as the scope — within 50 ms, the ADR's wake bound (single-digit on
// a clean local fence, 50 ms the accepted ceiling). It also pins the
// dedicated connection's shape: the listener's backend announces itself
// as es-lite-notify in pg_stat_activity — never a pooled connection.
func TestNotifyListenerDelivers(t *testing.T) {
	const channel = "es_lite_listener_delivers"
	const wsID = "ws_listener_delivers"
	ctx := context.Background()
	s := openNotifyStore(t, channel)
	h := &recordingNotifyHandler{}
	l := NewNotifyListener(listenerDSN(t), channel, h)
	runListener(t, l)
	pid := waitListenerPID(t, l, 10*time.Second)

	// The listener's connection is its own backend with the documented
	// application_name — pg_stat_activity triage, panel review CISO-4.
	// (The pid column is Postgres' pid type, not int4, so cast it before
	// binding the parameter.)
	var app string
	if err := s.Pool().QueryRow(ctx,
		`SELECT application_name FROM pg_stat_activity WHERE pid::integer = $1`, int32(pid),
	).Scan(&app); err != nil {
		t.Fatalf("read application_name of listener pid %d: %v", pid, err)
	}
	if app != "es-lite-notify" {
		t.Fatalf("listener connection application_name = %q, want %q", app, "es-lite-notify")
	}

	appendNotify(t, s.WorkspaceStore(wsID), listenerStreamID(t), 0, "listener-delivers")
	if !waitUntil(50*time.Millisecond, func() bool { return h.notifyCount() >= 1 }) {
		t.Fatalf("no OnNotify within 50ms of the append (got %d)", h.notifyCount())
	}
	if got := h.scopes(); len(got) != 1 || got[0] != wsID {
		t.Fatalf("OnNotify scopes = %q, want exactly [%q]", got, wsID)
	}
}

// TestNotifyListenerReconnectsAndWakesAll pins Review Focus 5: a killed
// listener reconnects, re-issues LISTEN, and reports OnReconnected — and
// a notification committed after the reconnect is delivered again. The
// re-LISTEN is the load-bearing half: without it the listener would be
// "connected" but deaf, and the second append's wake would never arrive.
func TestNotifyListenerReconnectsAndWakesAll(t *testing.T) {
	const channel = "es_lite_listener_reconnect"
	const wsID = "ws_listener_reconnect"
	ctx := context.Background()
	s := openNotifyStore(t, channel)
	h := &recordingNotifyHandler{}
	l := NewNotifyListener(listenerDSN(t), channel, h)
	runListener(t, l)
	oldPID := waitListenerPID(t, l, 10*time.Second)
	ws := s.WorkspaceStore(wsID)
	sid := listenerStreamID(t)

	appendNotify(t, ws, sid, 0, "before-kill")
	if !waitUntil(5*time.Second, func() bool { return h.notifyCount() >= 1 }) {
		t.Fatalf("first append did not reach OnNotify (got %d)", h.notifyCount())
	}

	// Kill the listener's backend exactly as a failover or an operator
	// would, from a different connection.
	if _, err := s.Pool().Exec(ctx, `SELECT pg_terminate_backend($1)`, int32(oldPID)); err != nil {
		t.Fatalf("terminate listener backend %d: %v", oldPID, err)
	}
	if !waitUntil(15*time.Second, func() bool { return h.reconnectCount() >= 1 }) {
		t.Fatalf("no OnReconnected within 15s of the backend being killed (got %d)", h.reconnectCount())
	}
	if newPID := waitListenerPID(t, l, 10*time.Second); newPID == oldPID {
		t.Fatalf("listener still on backend %d after reconnect", newPID)
	}

	// Hints emitted while the listener was down were missed; the
	// gateway's response to OnReconnected is hub.WakeAll. What the
	// listener itself must guarantee is liveness going forward: this
	// append commits after the re-LISTEN and must be delivered.
	appendNotify(t, ws, sid, 1, "after-reconnect")
	if !waitUntil(5*time.Second, func() bool { return h.notifyCount() >= 2 }) {
		t.Fatalf("post-reconnect append did not reach OnNotify (got %d)", h.notifyCount())
	}
	if got := h.scopes(); len(got) != 2 || got[0] != wsID || got[1] != wsID {
		t.Fatalf("OnNotify scopes = %q, want two deliveries of %q", got, wsID)
	}
}

// TestNotifyListenerThrottles pins the abuse bound (panel review
// CISO-1c): a notification for a scope already notified within the
// window is dropped, so a spoofed or duplicated burst cannot amplify
// into a wake per notification. The window is widened to 200 ms before
// Run starts — the field is unexported on purpose, and the default 10 ms
// is too fast to observe deterministically — and two appends land
// within that window, so exactly one OnNotify may come out.
func TestNotifyListenerThrottles(t *testing.T) {
	const channel = "es_lite_listener_throttle"
	const wsID = "ws_listener_throttle"
	s := openNotifyStore(t, channel)
	h := &recordingNotifyHandler{}
	l := NewNotifyListener(listenerDSN(t), channel, h)
	l.throttle = 200 * time.Millisecond // before Run starts: no race on the field
	runListener(t, l)
	waitListenerPID(t, l, 10*time.Second)
	ws := s.WorkspaceStore(wsID)
	sid := listenerStreamID(t)

	start := time.Now()
	appendNotify(t, ws, sid, 0, "throttle-first")
	appendNotify(t, ws, sid, 1, "throttle-second")
	elapsed := time.Since(start)

	if !waitUntil(5*time.Second, func() bool { return h.notifyCount() >= 1 }) {
		t.Fatalf("first notification never arrived (got %d)", h.notifyCount())
	}
	// A second delivery — if the throttle were broken — would show up
	// within milliseconds like the first; watch for it for 300 ms, six
	// times the 50 ms wake bound.
	if waitUntil(300*time.Millisecond, func() bool { return h.notifyCount() > 1 }) {
		t.Fatalf("OnNotify called %d times, want exactly 1: the second append (two appends %v apart, inside the 200ms window) must be throttled", h.notifyCount(), elapsed)
	}
	if got := h.scopes(); len(got) != 1 || got[0] != wsID {
		t.Fatalf("OnNotify scopes = %q, want exactly [%q]", got, wsID)
	}
}

// TestNotifyListenerQuotesChannel pins the CISO-3 injection concern:
// LISTEN takes no bind parameter, so the channel is interpolated into
// the statement — quoted with pgx.Identifier.Sanitize. A keyword channel
// ("select": a valid identifier, a syntax error bare) both constructs
// and receives the wake.
func TestNotifyListenerQuotesChannel(t *testing.T) {
	const channel = "select" // reserved word: valid quoted, syntax error bare
	const wsID = "ws_listener_quotes"
	s := openNotifyStore(t, channel)
	h := &recordingNotifyHandler{}
	l := NewNotifyListener(listenerDSN(t), channel, h)
	runListener(t, l)
	waitListenerPID(t, l, 10*time.Second)

	appendNotify(t, s.WorkspaceStore(wsID), listenerStreamID(t), 0, "quoted-channel")
	if !waitUntil(5*time.Second, func() bool { return h.notifyCount() >= 1 }) {
		t.Fatalf("no OnNotify on the quoted channel (got %d)", h.notifyCount())
	}
	if got := h.scopes(); len(got) != 1 || got[0] != wsID {
		t.Fatalf("OnNotify scopes = %q, want exactly [%q]", got, wsID)
	}
}

// TestNotifyListenerStopsOnCancel pins the shutdown contract: Run
// returns ctx.Err() when its context ends (delivery.Run's convention),
// and the connection is closed — PID() reports 0 and the backend
// disappears from pg_stat_activity.
func TestNotifyListenerStopsOnCancel(t *testing.T) {
	const channel = "es_lite_listener_cancel"
	ctx := context.Background()
	s := openNotifyStore(t, channel)
	h := &recordingNotifyHandler{}
	l := NewNotifyListener(listenerDSN(t), channel, h)
	stop := runListener(t, l)
	pid := waitListenerPID(t, l, 10*time.Second)

	err := stop()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
	if got := l.PID(); got != 0 {
		t.Fatalf("PID() = %d after Run returned, want 0 (disconnected)", got)
	}
	if got := h.notifyCount(); got != 0 {
		t.Fatalf("OnNotify called %d times on an idle listener, want 0", got)
	}

	// The connection is really closed: the backend disappears from
	// pg_stat_activity. Poll — the stats view trails the socket close by
	// a moment.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var n int
		if err := s.Pool().QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE pid::integer = $1`, int32(pid),
		).Scan(&n); err != nil {
			t.Fatalf("poll pg_stat_activity for pid %d: %v", pid, err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backend %d still listed in pg_stat_activity 5s after cancel", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestNewNotifyListenerPanicsOnBadInput pins the construction contract:
// a nil handler or an invalid channel is a wiring bug that surfaces as a
// panic at construction — never as a wedged or deaf listener at run
// time. No server is needed: construction never connects.
func TestNewNotifyListenerPanicsOnBadInput(t *testing.T) {
	const dsn = "postgres://example.invalid/eslite?sslmode=disable"
	assertPanics(t, "nil handler", func() {
		NewNotifyListener(dsn, "es_lite_x", nil)
	})
	assertPanics(t, "invalid channel", func() {
		NewNotifyListener(dsn, "bad-name", &recordingNotifyHandler{})
	})
}

func assertPanics(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: NewNotifyListener did not panic", what)
		}
	}()
	fn()
}
