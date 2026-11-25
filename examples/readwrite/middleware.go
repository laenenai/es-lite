package main

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
)

const (
	// ConsistencyHeader carries the read-your-writes token from a command
	// response to the next query.
	ConsistencyHeader = "ES-Consistency"
	// ConsistentHeader reports whether a token was satisfied and the
	// projection was live when the query read its read model.
	ConsistentHeader = "ES-Consistent"
)

// ErrScopeForbidden marks a scopeOf failure that is an authorization failure
// (403) rather than a malformed request (400).
var ErrScopeForbidden = errors.New("readwrite: scope forbidden")

// WaitFor wraps next: it parses the token, resolves the authenticated scope
// with scopeOf (the example reads it from the request context), checks the
// token's scope, waits on the named projection and sets ES-Consistent.
// Missing header = no wait, no store access and no ES-Consistent claim.
// Invalid token = 400. Scope mismatch = 403. With WithBestEffort a store
// failure still calls next with ES-Consistent:false.
//
// The same Waiter is shared by every request in the process; create it once at
// startup (for example projection.NewWaiter(progressStore,
// projection.WithBestEffort())) and pass it here.
//
// A strict (non-best-effort) store failure is a 503. If the client cancels,
// next does not run: there is no response to write and no useful request
// context to serve it with.
func WaitFor(w *projection.Waiter, projectionName string, scopeOf func(*http.Request) (string, error), timeout time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		raw := r.Header.Get(ConsistencyHeader)
		if raw == "" {
			// No token: there is nothing to wait for and no freshness to
			// claim. Do not touch the checkpoint store.
			next.ServeHTTP(rw, r)
			return
		}

		tok, err := es.ParseToken(raw)
		if err != nil {
			http.Error(rw, "invalid consistency token", http.StatusBadRequest)
			return
		}

		scope, err := scopeOf(r)
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, ErrScopeForbidden) {
				status = http.StatusForbidden
			}
			http.Error(rw, "cannot resolve workspace", status)
			return
		}

		// Never follow the scope in the token: it is a position hint, not an
		// authorization claim. The authenticated scope is the only authority.
		if tok.Scope != scope {
			http.Error(rw, "consistency token is for another workspace", http.StatusForbidden)
			return
		}

		res, err := w.WaitFor(r.Context(), scope, projectionName, tok, timeout)
		if err != nil {
			if r.Context().Err() != nil {
				// The client is gone; there is no response to write and next
				// must not run with a dead request context.
				return
			}
			writeWaitError(rw, err)
			return
		}

		// Only a reached, live projection is claimed consistent. A timeout,
		// a rebuilding or stalled projection, and a best-effort store failure
		// all serve stale data with ES-Consistent:false.
		consistent := res.Reached && res.Status == projection.StatusLive
		rw.Header().Set(ConsistentHeader, strconv.FormatBool(consistent))

		next.ServeHTTP(rw, r)
	})
}

// writeWaitError maps a strict WaitFor error to an HTTP status. A timeout is
// not an error, so it never reaches here.
func writeWaitError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, es.ErrInvalidToken):
		http.Error(w, "invalid consistency token", http.StatusBadRequest)
	case errors.Is(err, es.ErrScopeMismatch):
		http.Error(w, "consistency token is for another workspace", http.StatusForbidden)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// No response is useful to a cancelled client, but errors from the
		// store's own deadline are not the request's cancellation.
		http.Error(w, "consistency check cancelled", http.StatusServiceUnavailable)
	default:
		http.Error(w, "consistency check unavailable", http.StatusServiceUnavailable)
	}
}
