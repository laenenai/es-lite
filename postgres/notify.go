package postgres

import (
	"context"
	"fmt"
	mathrand "math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// NotifyHandler receives the notify listener's callbacks (ADR 0039 C2).
// It is a two-method interface instead of *feed.Hub on purpose: the
// coupling stays one-way — postgres never imports feed — and a gateway
// wires the hub itself, OnNotify → hub.Wake(scope) and OnReconnected →
// hub.WakeAll(), while tests record calls instead.
type NotifyHandler interface {
	// OnNotify reports that a notification arrived carrying scope —
	// the appended workspace's ID, the payload of the store's
	// pg_notify. It is called on Run's single goroutine and MUST NOT
	// block: the intended wiring, hub.Wake, is non-blocking, and a
	// blocking handler wedges the listener — no reconnect, no further
	// delivery — for as long as it blocks. A panic in the handler is
	// not recovered; like the hub, this is a trusted in-process seam.
	OnNotify(scope string)

	// OnReconnected reports that a lost connection was re-established
	// and LISTEN re-issued. Notifications emitted while the listener
	// was down were missed, so the handler must repair with a wake of
	// every scope (the gateway wires this to hub.WakeAll). It is never
	// called for the first connect, and — like OnNotify — must not
	// block.
	OnReconnected()
}

// Listener defaults (ADR 0039 §3.3).
const (
	// notifyAppName labels the listener's dedicated connection in
	// pg_stat_activity so an operator can tell it apart from pool
	// traffic (panel review CISO-4). It is only an application_name —
	// the channel is the capability, and it never appears in a log
	// line or an error message.
	notifyAppName = "es-lite-notify"

	// defaultNotifyThrottle is the per-scope coalescing window: a
	// notification for a scope already notified within the window is
	// dropped — parked waiters already received the close. 10 ms is
	// far above a notification's own delivery latency (sub-ms) and
	// merges only true duplicates or floods, so it bounds spoofed and
	// duplicated notification amplification (panel review CISO-1c)
	// without touching the single-digit latency claim.
	defaultNotifyThrottle = 10 * time.Millisecond

	// defaultReconnectMin is where the reconnect backoff schedule
	// starts, and it is the ADR's floor: 250 ms, the feed's default
	// Poll — a flapping listener never reconnects faster than the poll
	// cadence it repairs (panel review CTO-11).
	defaultReconnectMin = 250 * time.Millisecond

	// defaultReconnectMax caps the backoff schedule: a listener whose
	// database is down settles to one wake attempt per ~30 s instead of
	// a hot loop.
	defaultReconnectMax = 30 * time.Second

	// notifyPruneAt is the soft bound at which the throttle map is
	// pruned: past this many scopes the listener drops entries that can
	// no longer throttle anything. It is soft on purpose — entries
	// younger than 2×throttle are kept even past the bound, so a burst
	// of distinct scopes inside the window can exceed it briefly; the
	// first delivery outside their window prunes them.
	notifyPruneAt = 4096
)

// NotifyListener turns the opt-in append hint into handler calls: it
// owns one dedicated Postgres connection that LISTENs on a channel
// (ADR 0039 C2), and every notification it receives — the append path
// emits them via WithAppendNotify — is delivered as OnNotify(scope). A
// wake is a latency hint only: a missed, late or duplicated one costs
// at most one poll of delay, never loss; the feed's fence is untouched.
//
// The connection is dedicated for the listener's whole life:
// pgx.ConnectConfig, never a pooled connection that could be returned
// and silently lose LISTEN. It is one connection per listener — a
// deployment runs one listener per process per channel, not per
// workspace. PgBouncer transaction pooling breaks LISTEN; see
// docs/deploy/README.md for the operating model (trusted-CONNECT
// precondition, channel entropy, queue-usage alerting, 2PC exclusion).
//
// channel is a capability: NewNotifyListener validates it with the
// WithAppendNotify validator (a strict PostgreSQL identifier) and
// panics on anything else; production names are high-entropy, and the
// channel is never logged or echoed in an error. A valid channel never
// appears in any error: only a rejected candidate is ever named, by the
// construction panic or the Open-time option validation.
type NotifyListener struct {
	dsn     string
	channel string
	handler NotifyHandler

	// reconnectMin/reconnectMax bound the reconnect backoff schedule;
	// throttle is the per-scope coalescing window. Unexported: the
	// defaults live here, and in-package tests tighten them.
	reconnectMin time.Duration
	reconnectMax time.Duration
	throttle     time.Duration

	// pid is the current listener backend's PID, 0 while disconnected.
	// Atomic because PID() is read from other goroutines while Run's
	// goroutine stores it; every field below is Run-goroutine-only.
	pid atomic.Uint32

	// backoff is the base of the next reconnect delay: it doubles
	// after each failed attempt and resets after a successful LISTEN.
	backoff time.Duration

	// last maps a scope to when it was last handed to OnNotify; the
	// per-scope throttle reads and writes it. Run-goroutine-only, so
	// it needs no mutex — Run's single-goroutine discipline, the same
	// contract as delivery.Poller's cached checkpoint.
	last map[string]time.Time
}

// NewNotifyListener returns a listener for channel on the database at
// dsn. It never connects — Run does — so construction is safe in any
// order. It panics if handler is nil or channel is not a strict
// PostgreSQL identifier: both are wiring bugs that must surface at
// construction, not as a wedged or deaf listener at run time. The
// channel panic value is the shared validNotifyChannel verdict, which
// names the channel for the constructor's caller (Open-time-style
// feedback; no other path ever puts it in a message or a log).
func NewNotifyListener(dsn, channel string, handler NotifyHandler) *NotifyListener {
	if handler == nil {
		panic("postgres: NewNotifyListener: handler is nil")
	}
	if err := validNotifyChannel(channel); err != nil {
		panic(err)
	}
	return &NotifyListener{
		dsn:          dsn,
		channel:      channel,
		handler:      handler,
		reconnectMin: defaultReconnectMin,
		reconnectMax: defaultReconnectMax,
		throttle:     defaultNotifyThrottle,
		last:         make(map[string]time.Time),
	}
}

// Run listens until ctx is cancelled. It returns ctx.Err() when ctx
// ends — delivery.Run's convention — with the connection closed and
// PID() reporting 0. It never returns a connection error: a listener
// that cannot connect — at startup, after a loss, after a failover —
// backs off and keeps trying, because the wake is optional and the
// poll is the safety net. The one non-cancellation exit is a DSN that
// cannot even be parsed: that is a deterministic misconfiguration, and
// retrying it forever would be a silent no-op listener.
//
// Call Run at most once per listener, from one goroutine: the backoff
// and the throttle map are guarded only by Run's own single-goroutine
// discipline, so concurrent Runs race on them (delivery.Poller.Run's
// contract). PID() is the only safe concurrent accessor.
func (l *NotifyListener) Run(ctx context.Context) error {
	cfg, err := pgx.ParseConfig(l.dsn)
	if err != nil {
		return fmt.Errorf("postgres: notify listener: %w", err)
	}
	// The dedicated connection announces itself for pg_stat_activity
	// triage. The channel never reaches this layer's logs or errors.
	cfg.RuntimeParams["application_name"] = notifyAppName

	l.backoff = l.reconnectMin
	connected := false
	for {
		conn, err := l.connectListening(ctx, cfg)
		if err != nil {
			// Cancellation wins over retry; anything else is the
			// server being unreachable — try again on the schedule.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if serr := notifySleep(ctx, l.nextBackoff()); serr != nil {
				return serr
			}
			continue
		}
		l.pid.Store(conn.PgConn().PID())
		if connected {
			// Hints emitted while the connection was down were missed;
			// this is the repair signal. It fires only now that the new
			// connection is LISTENing, so a commit racing this call is
			// caught by LISTEN rather than lost in the gap.
			l.handler.OnReconnected()
		}
		connected = true
		l.backoff = l.reconnectMin // a connection that LISTENed resets the schedule

		for {
			n, err := conn.WaitForNotification(ctx)
			if err == nil && n != nil {
				l.deliver(n.Payload)
				continue
			}
			// A dead connection, or ctx ended — either way this
			// connection is finished. Close it on a detached context,
			// so a cancelled ctx cannot block the socket close, report
			// disconnected, and either return or reconnect.
			conn.Close(context.WithoutCancel(ctx))
			l.pid.Store(0)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if serr := notifySleep(ctx, l.nextBackoff()); serr != nil {
				return serr
			}
			break // reconnect
		}
	}
}

// connectListening opens the listener's next connection and LISTENs on
// the channel. LISTEN takes no bind parameter, so the channel —
// already a validated identifier — is interpolated quoted, with
// pgx.Identifier.Sanitize: a keyword channel like "select" is valid
// only quoted, and anything that could smuggle syntax through the
// interpolation was rejected by validNotifyChannel at construction. On
// a LISTEN failure the connection is closed here, so Run's retry path
// never leaks one.
func (l *NotifyListener) connectListening(ctx context.Context, cfg *pgx.ConnConfig) (*pgx.Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{l.channel}.Sanitize()); err != nil {
		conn.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return conn, nil
}

// deliver applies the per-scope throttle and hands the notification's
// payload — the appended workspace's ID — to the handler. Called only
// on Run's goroutine, so last needs no mutex: a notification for a
// scope already notified within the window is dropped, because parked
// waiters already received the close and the throttle's job is done
// (the abuse bound on spoofed and duplicated notification
// amplification, panel review CISO-1c). Duplicates outside the window
// are harmless — a wake is a hint.
//
// last grows with the number of distinct scopes that notify; past
// notifyPruneAt it is pruned by dropping entries older than
// 2×throttle — ones that cannot throttle a future notification anyway —
// so a long-lived listener's map stays bounded without a background
// goroutine. The prune runs after recording, never between the check
// and the record, so this notification's own throttle verdict is exact.
func (l *NotifyListener) deliver(scope string) {
	now := time.Now()
	if now.Sub(l.last[scope]) < l.throttle {
		return
	}
	l.last[scope] = now
	if len(l.last) > notifyPruneAt {
		for s, at := range l.last {
			if now.Sub(at) > 2*l.throttle {
				delete(l.last, s)
			}
		}
	}
	l.handler.OnNotify(scope)
}

// PID returns the backend PID of the listener's current connection, or
// 0 while disconnected — including after Run returns. It is diagnostic
// — pg_stat_activity triage, pg_terminate_backend in tests and chaos
// runs — and never blocks: one atomic load. By design it can observe a
// reconnect mid-flight (a momentary 0 between connections); a caller
// polling it sees either the old or the new backend, both real answers.
func (l *NotifyListener) PID() uint32 {
	return l.pid.Load()
}

// nextBackoff returns the next reconnect delay and advances the
// schedule: the current base scaled by the ±20% uniform jitter (the
// same [0.8, 1.2) factor as the feed's idle poll, so a fleet of
// listeners that lost their connections together random-walks out of
// lockstep instead of reconnecting as one herd), then the base
// doubles, capped at reconnectMax. The floor is the schedule's — it
// starts at reconnectMin, 250 ms, the feed's default Poll — not each
// jittered draw's: clamping every draw at the floor would pile exactly
// the re-herding the jitter exists to prevent onto the first retry.
// Run resets the base after a successful LISTEN.
func (l *NotifyListener) nextBackoff() time.Duration {
	d := time.Duration(float64(l.backoff) * (0.8 + 0.4*mathrand.Float64()))
	l.backoff *= 2
	if l.backoff > l.reconnectMax {
		l.backoff = l.reconnectMax
	}
	return d
}

// notifySleep waits for d, returning ctx.Err() if ctx ends first —
// cancellation always wins over a backoff schedule.
func notifySleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
