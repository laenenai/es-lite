package postgres

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laenenai/es-lite/projection"
)

// Unit and integration tests for the checkpoint advance hint (ADR 0040,
// Task 7). Task 5's ProgressStore accepted a channel but emitted
// nothing; these tests pin the notifier that now does: it marks only
// after a save commits, dedupes scopes in a bounded window, flushes one
// statement per window, and drops a flush error because the checkpoint
// row is already durable. The listener tests pin the other side —
// NewProgressListener wrapping NotifyListener so a hint becomes a
// non-blocking refresher call and a reconnect becomes WakeAll.
//
// Internal (package postgres) on purpose: the failing-executor case and
// the short coalescing window touch unexported seams, and the listeners
// reuse the notify_listener_test helpers (runListener, waitListenerPID).
// Tests needing a live server skip without PG_DSN, like the rest of the
// suite.

// notifierCall is one flush captured by notifierRecorder.
type notifierCall struct {
	sql  string
	args []any
}

// notifierRecorder is the notifier's executor seam: it records each
// flush and succeeds. A non-flushing notifier records nothing, so the
// window, dedupe and Close assertions read directly off the calls.
type notifierRecorder struct {
	mu    sync.Mutex
	calls []notifierCall
}

func (r *notifierRecorder) exec(ctx context.Context, sql string, args ...any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, notifierCall{sql: sql, args: append([]any(nil), args...)})
	return nil
}

func (r *notifierRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *notifierRecorder) snapshot() []notifierCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]notifierCall(nil), r.calls...)
}

// notifyScopes unpacks a flush's (channel, scopes) arguments.
func notifyScopes(t *testing.T, call notifierCall) (string, []string) {
	t.Helper()
	if !strings.Contains(call.sql, "pg_notify") || !strings.Contains(call.sql, "unnest") {
		t.Fatalf("flush SQL %q, want the one-statement pg_notify(unnest(...)) form", call.sql)
	}
	if len(call.args) != 2 {
		t.Fatalf("flush passed %d args, want 2 (channel, scopes)", len(call.args))
	}
	channel, ok := call.args[0].(string)
	if !ok {
		t.Fatalf("flush arg[0] = %#v, want the channel string", call.args[0])
	}
	scopes, ok := call.args[1].([]string)
	if !ok {
		t.Fatalf("flush arg[1] = %#v, want a []string of scopes", call.args[1])
	}
	return channel, scopes
}

// TestProgressNotifyCoalescesScopes pins the window: repeated marks of
// one scope and a second scope land in a single flush, with the channel
// and the scope set — never one notification per save.
func TestProgressNotifyCoalescesScopes(t *testing.T) {
	const channel = "es_lite_progress_coalesce"
	rec := &notifierRecorder{}
	n := newProgressNotifier(channel, rec.exec, 100*time.Millisecond, 1024)
	defer n.Close()

	n.Mark("ws_a")
	n.Mark("ws_a")
	n.Mark("ws_b")

	if !waitUntil(2*time.Second, func() bool { return rec.count() == 1 }) {
		t.Fatalf("flushes = %d, want 1 for one coalescing window", rec.count())
	}
	gotChannel, scopes := notifyScopes(t, rec.snapshot()[0])
	if gotChannel != channel {
		t.Fatalf("flush channel = %q, want %q", gotChannel, channel)
	}
	sort.Strings(scopes)
	if len(scopes) != 2 || scopes[0] != "ws_a" || scopes[1] != "ws_b" {
		t.Fatalf("flush scopes = %q, want exactly [ws_a ws_b] — duplicates collapse", scopes)
	}

	// Nothing is pending after the flush: an idle notifier issues no
	// further statements.
	time.Sleep(150 * time.Millisecond)
	if got := rec.count(); got != 1 {
		t.Fatalf("flushes = %d after an idle window, want 1", got)
	}
}

// TestProgressNotifyFlushesWhenFull pins the other flush trigger: a
// batch that reaches the bound flushes immediately, without waiting out
// the interval.
func TestProgressNotifyFlushesWhenFull(t *testing.T) {
	rec := &notifierRecorder{}
	n := newProgressNotifier("es_lite_progress_full", rec.exec, time.Hour, 2)
	defer n.Close()

	n.Mark("ws_a")
	n.Mark("ws_b")

	if !waitUntil(time.Second, func() bool { return rec.count() == 1 }) {
		t.Fatalf("full batch did not flush (flushes = %d), want an immediate flush", rec.count())
	}
	_, scopes := notifyScopes(t, rec.snapshot()[0])
	if len(scopes) != 2 {
		t.Fatalf("full flush carried %d scopes, want 2", len(scopes))
	}
}

// TestProgressNotifyCloseFlushes pins Close: a mark that never reached
// its window is flushed once, best-effort, and the goroutine joins — the
// path the waiter's goleak test relies on.
func TestProgressNotifyCloseFlushes(t *testing.T) {
	rec := &notifierRecorder{}
	n := newProgressNotifier("es_lite_progress_close", rec.exec, time.Hour, 64)
	n.Mark("ws_close")

	if err := n.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := rec.count(); got != 1 {
		t.Fatalf("flushes = %d after Close, want exactly 1 (the final best-effort flush)", got)
	}
	_, scopes := notifyScopes(t, rec.snapshot()[0])
	if len(scopes) != 1 || scopes[0] != "ws_close" {
		t.Fatalf("final flush scopes = %q, want [ws_close]", scopes)
	}
}

// TestProgressNotifyFlushErrorIsCountedAndDropped pins the failure
// contract: a flush error increments the counter, drops the batch, and
// never surfaces to the saver. Close after the failure issues no new
// statement, because the failed batch must not be retried.
func TestProgressNotifyFlushErrorIsCountedAndDropped(t *testing.T) {
	var calls atomic.Int64
	n := newProgressNotifier("es_lite_progress_flush_error", func(context.Context, string, ...any) error {
		calls.Add(1)
		return errors.New("flush unavailable")
	}, 10*time.Millisecond, 64)
	defer n.Close()

	n.Mark("ws_broken")

	if !waitUntil(2*time.Second, func() bool { return calls.Load() == 1 }) {
		t.Fatalf("flush attempts = %d, want 1", calls.Load())
	}
	if !waitUntil(time.Second, func() bool { return n.failures.Load() == 1 }) {
		t.Fatalf("failure count = %d, want 1", n.failures.Load())
	}
	if err := n.Close(); err != nil {
		t.Fatalf("close after a failed flush: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("flush attempts = %d after Close, want 1 — a failed batch is dropped, not retried", got)
	}
	if got := n.failures.Load(); got != 1 {
		t.Fatalf("failure count = %d after Close, want 1", got)
	}
}

// TestProgressNotifyEmptyChannelDisables pins the opt-in default: an
// empty channel starts no goroutine and holds no notifier, so the save
// path issues no statement and Close is a no-op.
func TestProgressNotifyEmptyChannelDisables(t *testing.T) {
	ps := NewProgressStore(&Store{}, "")
	if ps.notifier != nil {
		t.Fatal("NewProgressStore(\"\") started a notifier, want none")
	}
	if err := ps.Close(); err != nil {
		t.Fatalf("close on a disabled notifier: %v", err)
	}
}

// TestProgressNotifyOversizedScopeRejected pins the payload bound on
// the save path: a scope over maxNotifyWorkspaceBytes fails the save
// before any statement and leaves no row, exactly like the marker
// channel's Save.
func TestProgressNotifyOversizedScopeRejected(t *testing.T) {
	s := openStore(t)
	ps := NewProgressStore(s, "es_lite_progress_oversized")
	defer ps.Close()

	ctx := context.Background()
	const projectionName = "orders"
	scope := strings.Repeat("s", maxNotifyWorkspaceBytes+1)

	err := ps.SaveProgress(ctx, scope, projectionName, projection.Checkpoint{
		Position: 1, State: projection.StatusLive,
	})
	if err == nil || !strings.Contains(err.Error(), "notify bound") {
		t.Fatalf("oversized scope save = %v, want a notify-bound error", err)
	}
	if _, found, lerr := ps.LoadProgress(ctx, scope, projectionName); lerr != nil || found {
		t.Fatalf("oversized save left a row (found=%v err=%v), want none", found, lerr)
	}
}

// TestProgressNotifyFlushFailureDoesNotFailSave pins the ordering that
// makes a hint harmless: the checkpoint commits first, then the notifier
// marks the scope. A failing flush is counted and dropped; SaveProgress
// still succeeds and the committed row is readable.
func TestProgressNotifyFlushFailureDoesNotFailSave(t *testing.T) {
	s := openStore(t)
	ps := NewProgressStore(s, "es_lite_progress_save_survives")
	defer ps.Close()

	var failures atomic.Int64
	ps.notifier.exec = func(context.Context, string, ...any) error {
		failures.Add(1)
		return errors.New("flush unavailable")
	}

	ctx := context.Background()
	const (
		scope          = "ws_progress_save_survives"
		projectionName = "orders"
	)
	if err := ps.SaveProgress(ctx, scope, projectionName, projection.Checkpoint{
		Position: 9, State: projection.StatusLive,
	}); err != nil {
		t.Fatalf("save failed because a hint flush failed: %v", err)
	}
	if !waitUntil(2*time.Second, func() bool { return failures.Load() >= 1 }) {
		t.Fatal("notifier never attempted the failing flush")
	}

	cp, found, err := ps.LoadProgress(ctx, scope, projectionName)
	if err != nil {
		t.Fatalf("load after a failed hint: %v", err)
	}
	if !found || cp.Position != 9 || cp.State != projection.StatusLive {
		t.Fatalf("stored row = found %v %+v, want the committed position 9/live", found, cp)
	}
}

// fakeScopeRefresher records the listener's refresher calls. Its
// channels are buffered so a test refresher honours the handler's
// non-blocking contract.
type fakeScopeRefresher struct {
	scopes chan string
	all    chan struct{}
}

func (f *fakeScopeRefresher) EnqueueScopeRefresh(scope string) { f.scopes <- scope }
func (f *fakeScopeRefresher) EnqueueRefreshAll()               { f.all <- struct{}{} }

func newFakeScopeRefresher() *fakeScopeRefresher {
	return &fakeScopeRefresher{scopes: make(chan string, 8), all: make(chan struct{}, 8)}
}

// TestProgressListenerForwardsRawPayload pins the adapter: the handler
// hands OnNotify's payload through untouched as the scope — no parsing,
// no position — and OnReconnected asks for a full refresh. A payload
// that looks like data still only triggers a scope refresh; the waiter
// re-reads the store for the authoritative position (see projection's
// TestWaiterHintIsNotData).
func TestProgressListenerForwardsRawPayload(t *testing.T) {
	r := newFakeScopeRefresher()
	l := NewProgressListener("postgres://unused.invalid:1/eslite?sslmode=disable", "es_lite_progress_listener", r)

	const forged = `{"scope":"ws","position":42}`
	l.handler.OnNotify(forged)

	select {
	case got := <-r.scopes:
		if got != forged {
			t.Fatalf("EnqueueScopeRefresh(%q), want the raw payload %q", got, forged)
		}
	case <-time.After(time.Second):
		t.Fatal("OnNotify did not enqueue a scope refresh")
	}
	select {
	case <-r.all:
		t.Fatal("OnNotify triggered a full refresh, want only the scope")
	default:
	}

	l.handler.OnReconnected()
	select {
	case <-r.all:
	case <-time.After(time.Second):
		t.Fatal("OnReconnected did not enqueue a full refresh")
	}
	select {
	case got := <-r.scopes:
		t.Fatalf("OnReconnected enqueued scope %q, want only the full refresh", got)
	default:
	}
}

// TestProgressNotifyListenerWakesRefresher is the end-to-end listener
// path: NewProgressListener LISTENs on the checkpoint channel, a
// committed SaveProgress reaches OnNotify as the scope, and killing the
// listener's backend reconnects and calls EnqueueRefreshAll — the repair
// for hints missed while it was down.
func TestProgressNotifyListenerWakesRefresher(t *testing.T) {
	const (
		channel = "es_lite_progress_wakes"
		scope   = "ws_progress_wakes"
	)
	ctx := context.Background()
	s := openStore(t)
	r := newFakeScopeRefresher()
	l := NewProgressListener(listenerDSN(t), channel, r)
	runListener(t, l)
	pid := waitListenerPID(t, l, 10*time.Second)

	ps := NewProgressStore(s, channel)
	defer ps.Close()
	if err := ps.SaveProgress(ctx, scope, "orders", projection.Checkpoint{
		Position: 3, State: projection.StatusLive,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	select {
	case got := <-r.scopes:
		if got != scope {
			t.Fatalf("EnqueueScopeRefresh(%q), want the saved scope %q", got, scope)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("committed save did not wake the refresher")
	}

	// A reconnect must repair every scope: hints emitted while the
	// connection was down were missed.
	if _, err := s.Pool().Exec(ctx, `SELECT pg_terminate_backend($1)`, int32(pid)); err != nil {
		t.Fatalf("terminate listener backend %d: %v", pid, err)
	}
	select {
	case <-r.all:
	case <-time.After(20 * time.Second):
		t.Fatal("listener reconnect did not enqueue a full refresh")
	}
}
