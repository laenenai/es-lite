package postgres_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// Integration tests for the opt-in append notify hint (ADR 0039 C2).
// A dedicated pgx.Conn — never a pooled connection — LISTENs on the
// channel and observes, or fails to observe, the pg_notify a committed
// append emits. Each test skips without PG_DSN, like the rest of the
// Postgres suite; `task test:pg` starts a throwaway Postgres.
//
// Notifications are visible only to sessions already LISTENing when the
// notifying transaction commits, so every test listens before it
// appends, and per-test channels keep runs independent of each other.
// The channel names here are fixed test fixtures, not capabilities;
// production channels are high-entropy and never logged
// (docs/deploy/README.md).

// notifyDSN returns PG_DSN or skips the test.
func notifyDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	return dsn
}

// listenConn opens a dedicated connection and LISTENs on channel: the
// listener shape ADR 0039 requires (a session-pooled connection would
// die with the pool; PgBouncer transaction pooling breaks LISTEN). The
// channel is quoted with pgx.Identifier.Sanitize exactly as the
// NotifyListener will quote it.
func listenConn(t *testing.T, channel string) *pgx.Conn {
	t.Helper()
	dsn := notifyDSN(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("listen connect: %v", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		conn.Close(ctx)
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// assertNoNotification fails if any notification arrives within the
// window. The expected outcome is the wait expiring: a deadline error,
// not a connection error.
func assertNoNotification(t *testing.T, conn *pgx.Conn, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	n, err := conn.WaitForNotification(ctx)
	if n != nil {
		t.Fatalf("notification arrived within %v, want none (payload %q)", within, n.Payload)
	}
	if err == nil {
		t.Fatalf("WaitForNotification returned nil error and nil notification within %v", within)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait for notification: %v", err)
	}
}

// TestAppendNotifyFiresOnCommit is the C2 correctness core: with
// WithAppendNotify set, a committed append reaches a LISTENing
// connection within 50 ms — single-digit on a clean local fence — with
// the channel and the workspace ID as payload.
func TestAppendNotifyFiresOnCommit(t *testing.T) {
	const channel = "es_lite_notify_fires"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStoreWithOptions(t, postgres.WithAppendNotify(channel))
	const wsID = "ws_notify_fires"
	ws := s.WorkspaceStore(wsID)
	sid := streamID(t)

	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          eventData("fires-on-commit"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	n, err := lc.WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("wait for notification: %v", err)
	}
	if n.Channel != channel {
		t.Fatalf("notification channel = %q, want %q", n.Channel, channel)
	}
	if n.Payload != wsID {
		t.Fatalf("notification payload = %q, want the workspace id %q", n.Payload, wsID)
	}
}

// TestAppendNotifyRollbackDoesNotFire pins the transactional half of the
// contract: NOTIFY sent inside a caller-owned transaction (AppendTx)
// fires only if that transaction commits. A rolled-back append notifies
// nobody — the hint can never claim events that never became durable.
func TestAppendNotifyRollbackDoesNotFire(t *testing.T) {
	const channel = "es_lite_notify_rollback"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStoreWithOptions(t, postgres.WithAppendNotify(channel))
	ws := s.WorkspaceStore("ws_notify_rollback")
	sid := streamID(t)

	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := ws.AppendTx(ctx, tx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          eventData("rolled-back"),
	}); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("append tx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	assertNoNotification(t, lc, 100*time.Millisecond)
}

// TestAppendNotifyZeroEventDoesNotFire pins the zero-event boundary: an
// append with no events succeeds (a no-op command) but notifies nobody — a
// wake for it would only cost every subscriber of the workspace a fenced read
// that delivers nothing. A constraint-bearing zero-event append is refused
// before the notify path (ErrConstraintWithoutEvent, ADR 0043).
func TestAppendNotifyZeroEventDoesNotFire(t *testing.T) {
	const channel = "es_lite_notify_claim_only"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStoreWithOptions(t, postgres.WithAppendNotify(channel))
	ws := s.WorkspaceStore("ws_notify_claim_only")
	sid := streamID(t)

	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
	}); err != nil {
		t.Fatalf("zero-event append: %v", err)
	}

	assertNoNotification(t, lc, 100*time.Millisecond)
}

// TestAppendNotifyOffDoesNotFire pins the default: opened without
// WithAppendNotify, an append commits and no notification is issued —
// the option is opt-in and the default append path is unchanged.
func TestAppendNotifyOffDoesNotFire(t *testing.T) {
	const channel = "es_lite_notify_off"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStore(t) // no WithAppendNotify
	ws := s.WorkspaceStore("ws_notify_off")
	sid := streamID(t)

	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          eventData("no-hint"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	assertNoNotification(t, lc, 100*time.Millisecond)
}

// TestWithAppendNotifyRejectsInvalidChannel pins the fail-fast contract:
// Open rejects a bad channel with an error that names it, and does so
// without connecting — an unreachable DSN changes nothing, because the
// verdict is the option's, not the server's.
func TestWithAppendNotifyRejectsInvalidChannel(t *testing.T) {
	dsn := notifyDSN(t)
	ctx := context.Background()

	_, err := postgres.Open(ctx, dsn, postgres.WithAppendNotify("bad-name"))
	if err == nil {
		t.Fatal("open with an invalid notify channel succeeded, want error")
	}
	if !strings.Contains(err.Error(), "bad-name") {
		t.Fatalf("error %q does not name the channel", err)
	}

	// The verdict needs no server: the same bad channel on an
	// unconnectable DSN still fails with the channel error, proving the
	// validation runs before any connection attempt.
	_, err = postgres.Open(ctx, "postgres://nonexistent.invalid:1/eslite?sslmode=disable", postgres.WithAppendNotify("bad-name"))
	if err == nil {
		t.Fatal("open with an invalid notify channel succeeded against an unreachable DSN, want error")
	}
	if !strings.Contains(err.Error(), "bad-name") {
		t.Fatalf("error %q does not name the channel", err)
	}
}
