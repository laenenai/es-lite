package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

// countingProgressStore wraps a ProgressStore and counts every LoadProgress
// call, so a test can assert that a tokenless request never touches the
// checkpoint store.
type countingProgressStore struct {
	projection.ProgressStore
	loads atomic.Int64
	err   error
}

func (c *countingProgressStore) LoadProgress(ctx context.Context, scope, name string) (projection.Checkpoint, bool, error) {
	c.loads.Add(1)
	if c.err != nil {
		return projection.Checkpoint{}, false, c.err
	}
	return c.ProgressStore.LoadProgress(ctx, scope, name)
}

// nextSpy records whether next ran and the ES-Consistent value it observed
// before writing its response.
type nextSpy struct {
	called     bool
	consistent string
}

func (s *nextSpy) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.called = true
	s.consistent = w.Header().Get(ConsistentHeader)
	w.WriteHeader(http.StatusOK)
}

// fixedScope returns a scopeOf that always resolves scope or fails with err.
func fixedScope(scope string, err error) func(*http.Request) (string, error) {
	return func(*http.Request) (string, error) { return scope, err }
}

// newTestWaiter returns a Waiter that the test always closes.
func newTestWaiter(t *testing.T, store projection.ProgressStore, opts ...projection.WaiterOption) *projection.Waiter {
	t.Helper()
	w := projection.NewWaiter(store, opts...)
	t.Cleanup(w.Close)
	return w
}

// seedProgress saves one live checkpoint for the example scope.
func seedProgress(t *testing.T, store projection.ProgressStore, position uint64) {
	t.Helper()
	err := store.SaveProgress(context.Background(), "ws", testProjection, projection.Checkpoint{
		Position: position,
		State:    projection.StatusLive,
	})
	if err != nil {
		t.Fatalf("seed progress: %v", err)
	}
}

const testProjection = "users"

// TestWaitForMiddlewareNoHeader copies the response through without a wait,
// without a store read and without an ES-Consistent claim.
func TestWaitForMiddlewareNoHeader(t *testing.T) {
	store := &countingProgressStore{ProgressStore: projection.NewMemoryProgressStore()}
	waiter := newTestWaiter(t, store)
	spy := &nextSpy{}

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	rec := httptest.NewRecorder()
	WaitFor(waiter, testProjection, fixedScope("ws", nil), 50*time.Millisecond, spy).ServeHTTP(rec, req)

	if !spy.called {
		t.Fatal("next did not run without ES-Consistency")
	}
	if spy.consistent != "" || rec.Header().Get(ConsistentHeader) != "" {
		t.Fatalf("ES-Consistent = %q, want unset for a tokenless request", spy.consistent)
	}
	if got := store.loads.Load(); got != 0 {
		t.Fatalf("LoadProgress calls = %d, want 0 for a tokenless request", got)
	}
}

// TestWaitForMiddlewareReached asserts that a satisfied token reaches next with
// ES-Consistent: true when the projection is live.
func TestWaitForMiddlewareReached(t *testing.T) {
	mem := projection.NewMemoryProgressStore()
	seedProgress(t, mem, 10)
	waiter := newTestWaiter(t, mem)
	spy := &nextSpy{}

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	req.Header.Set(ConsistencyHeader, (es.Token{Scope: "ws", Position: 10}).String())
	rec := httptest.NewRecorder()
	WaitFor(waiter, testProjection, fixedScope("ws", nil), 50*time.Millisecond, spy).ServeHTTP(rec, req)

	if !spy.called {
		t.Fatal("next did not run for a reached token")
	}
	if spy.consistent != "true" {
		t.Fatalf("ES-Consistent = %q, want true", spy.consistent)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// TestWaitForMiddlewareTimeout asserts that an unmet token still reaches next,
// with ES-Consistent: false, when the deadline passes.
func TestWaitForMiddlewareTimeout(t *testing.T) {
	mem := projection.NewMemoryProgressStore()
	seedProgress(t, mem, 9)
	waiter := newTestWaiter(t, mem)
	spy := &nextSpy{}

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	req.Header.Set(ConsistencyHeader, (es.Token{Scope: "ws", Position: 10}).String())
	rec := httptest.NewRecorder()
	const timeout = 20 * time.Millisecond
	WaitFor(waiter, testProjection, fixedScope("ws", nil), timeout, spy).ServeHTTP(rec, req)

	if !spy.called {
		t.Fatal("next did not run after a wait timeout")
	}
	if spy.consistent != "false" {
		t.Fatalf("ES-Consistent = %q, want false", spy.consistent)
	}
}

// TestWaitForMiddlewareRebuilding asserts that a rebuilding projection is
// never claimed consistent, even at a position at or above the token.
func TestWaitForMiddlewareRebuilding(t *testing.T) {
	mem := projection.NewMemoryProgressStore()
	err := mem.SaveProgress(context.Background(), "ws", testProjection, projection.Checkpoint{
		Position: 10,
		State:    projection.StatusRebuilding,
	})
	if err != nil {
		t.Fatalf("seed rebuilding progress: %v", err)
	}
	waiter := newTestWaiter(t, mem)
	spy := &nextSpy{}

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	req.Header.Set(ConsistencyHeader, (es.Token{Scope: "ws", Position: 10}).String())
	rec := httptest.NewRecorder()
	WaitFor(waiter, testProjection, fixedScope("ws", nil), 50*time.Millisecond, spy).ServeHTTP(rec, req)

	if !spy.called {
		t.Fatal("next did not run for a rebuilding projection")
	}
	if spy.consistent != "false" {
		t.Fatalf("ES-Consistent = %q, want false while rebuilding", spy.consistent)
	}
}

// TestWaitForMiddlewareMalformedToken asserts a 400 and no next call before any
// store access.
func TestWaitForMiddlewareMalformedToken(t *testing.T) {
	store := &countingProgressStore{ProgressStore: projection.NewMemoryProgressStore()}
	waiter := newTestWaiter(t, store)
	spy := &nextSpy{}

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	req.Header.Set(ConsistencyHeader, "not-a-token")
	rec := httptest.NewRecorder()
	WaitFor(waiter, testProjection, fixedScope("ws", nil), 50*time.Millisecond, spy).ServeHTTP(rec, req)

	if spy.called {
		t.Fatal("next ran for a malformed token")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got := store.loads.Load(); got != 0 {
		t.Fatalf("LoadProgress calls = %d, want 0 for a malformed token", got)
	}
}

// TestWaitForMiddlewareScopeMismatch asserts that a token minted for another
// workspace is rejected with 403 and never reaches the store or next.
func TestWaitForMiddlewareScopeMismatch(t *testing.T) {
	store := &countingProgressStore{ProgressStore: projection.NewMemoryProgressStore()}
	waiter := newTestWaiter(t, store)
	spy := &nextSpy{}

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	req.Header.Set(ConsistencyHeader, (es.Token{Scope: "other", Position: 10}).String())
	rec := httptest.NewRecorder()
	WaitFor(waiter, testProjection, fixedScope("ws", nil), 50*time.Millisecond, spy).ServeHTTP(rec, req)

	if spy.called {
		t.Fatal("next ran for a token from another workspace")
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if got := store.loads.Load(); got != 0 {
		t.Fatalf("LoadProgress calls = %d, want 0 for a scope mismatch", got)
	}
}

// TestWaitForMiddlewareScopeError asserts the two scopeOf failure statuses:
// a malformed request is 400 and an authorization failure is 403.
func TestWaitForMiddlewareScopeError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "bad request", err: errors.New("no workspace"), want: http.StatusBadRequest},
		{name: "forbidden", err: ErrScopeForbidden, want: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := projection.NewMemoryProgressStore()
			seedProgress(t, mem, 10)
			waiter := newTestWaiter(t, mem)
			spy := &nextSpy{}

			req := httptest.NewRequest(http.MethodGet, "/users", nil)
			req.Header.Set(ConsistencyHeader, (es.Token{Scope: "ws", Position: 10}).String())
			rec := httptest.NewRecorder()
			WaitFor(waiter, testProjection, fixedScope("", tt.err), 50*time.Millisecond, spy).ServeHTTP(rec, req)

			if spy.called {
				t.Fatal("next ran when scopeOf failed")
			}
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

// TestWaitForMiddlewareBestEffortStoreFailure asserts that a store failure
// under WithBestEffort still calls next, with ES-Consistent: false.
func TestWaitForMiddlewareBestEffortStoreFailure(t *testing.T) {
	store := &countingProgressStore{
		ProgressStore: projection.NewMemoryProgressStore(),
		err:           errors.New("checkpoint store down"),
	}
	waiter := newTestWaiter(t, store, projection.WithBestEffort())
	spy := &nextSpy{}

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	req.Header.Set(ConsistencyHeader, (es.Token{Scope: "ws", Position: 10}).String())
	rec := httptest.NewRecorder()
	WaitFor(waiter, testProjection, fixedScope("ws", nil), 50*time.Millisecond, spy).ServeHTTP(rec, req)

	if !spy.called {
		t.Fatal("next did not run under WithBestEffort")
	}
	if spy.consistent != "false" {
		t.Fatalf("ES-Consistent = %q, want false", spy.consistent)
	}
}

// TestWaitForMiddlewareStrictStoreFailure asserts that without WithBestEffort a
// store failure is a 503, not a stale 200.
func TestWaitForMiddlewareStrictStoreFailure(t *testing.T) {
	store := &countingProgressStore{
		ProgressStore: projection.NewMemoryProgressStore(),
		err:           errors.New("checkpoint store down"),
	}
	waiter := newTestWaiter(t, store)
	spy := &nextSpy{}

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	req.Header.Set(ConsistencyHeader, (es.Token{Scope: "ws", Position: 10}).String())
	rec := httptest.NewRecorder()
	WaitFor(waiter, testProjection, fixedScope("ws", nil), 50*time.Millisecond, spy).ServeHTTP(rec, req)

	if spy.called {
		t.Fatal("next ran after a strict store failure")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
