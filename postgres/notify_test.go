package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/laenenai/es-lite/es"
)

// Unit tests for the opt-in append notify hint (ADR 0039 C2). No
// database: a recording DBTX pins the statement discipline the
// transactional contract rests on — the option off issues no statement
// at all, a zero-event batch issues none, an oversized workspace ID is
// refused before any statement — and the channel validator is checked
// rule by rule. The end-to-end behaviour against a real LISTEN
// connection lives in notify_integration_test.go, gated by PG_DSN.

// recordedExec is one Exec call captured by recordingDBTX.
type recordedExec struct {
	sql  string
	args []any
}

// recordingDBTX implements DBTX for notifyAppend's unit tests: Exec
// records the call and succeeds, while Query and QueryRow panic —
// notifyAppend must reach the database as exactly one Exec and nothing
// else, so any other statement is a bug loud enough to fail the test
// run outright.
type recordingDBTX struct {
	execs []recordedExec
}

func (r *recordingDBTX) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.execs = append(r.execs, recordedExec{sql: sql, args: args})
	return pgconn.CommandTag{}, nil
}

func (r *recordingDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	panic("notifyAppend must not Query")
}

func (r *recordingDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	panic("notifyAppend must not QueryRow")
}

// notifyWorkspace builds the Workspace notifyAppend is called on,
// directly: the unit tests stay inside the package so no database, no
// pool and no migration is needed to observe the statement discipline.
func notifyWorkspace(channel, ws string) *Workspace {
	return &Workspace{store: &Store{notifyChannel: channel}, ws: ws}
}

// TestNotifyAppendOffIssuesNoStatement pins the default: with
// WithAppendNotify unset the append hint is a no-op — not a skipped
// statement, no round trip, nothing. The default append path must be
// byte-for-byte unchanged (ADR 0039 §2 anti-goal).
func TestNotifyAppendOffIssuesNoStatement(t *testing.T) {
	db := &recordingDBTX{}
	w := notifyWorkspace("", "ws_notify")
	if err := w.notifyAppend(context.Background(), db, es.AppendParams{
		Events: []es.EventData{{TypeURL: "counter.v1.Initialized", Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("notifyAppend with channel unset: %v", err)
	}
	if len(db.execs) != 0 {
		t.Fatalf("channel unset issued %d Exec calls, want 0 — the default append path adds no statement", len(db.execs))
	}
}

// TestNotifyAppendFiresOnceForNonEmptyBatch pins the on path's shape:
// exactly one Exec, its SQL is the pg_notify statement, and its
// parameters are the channel and the workspace ID — the payload is the
// workspace ID and nothing else.
func TestNotifyAppendFiresOnceForNonEmptyBatch(t *testing.T) {
	const (
		channel = "es_lite_appended"
		wsID    = "ws_notify"
	)
	db := &recordingDBTX{}
	w := notifyWorkspace(channel, wsID)
	if err := w.notifyAppend(context.Background(), db, es.AppendParams{
		Events: []es.EventData{{TypeURL: "counter.v1.Initialized", Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("notifyAppend: %v", err)
	}
	if len(db.execs) != 1 {
		t.Fatalf("issued %d Exec calls, want exactly 1", len(db.execs))
	}
	e := db.execs[0]
	if !strings.Contains(e.sql, "pg_notify") {
		t.Fatalf("sql %q does not contain pg_notify", e.sql)
	}
	if len(e.args) != 2 {
		t.Fatalf("Exec passed %d args, want 2 (channel, workspace id)", len(e.args))
	}
	if got, ok := e.args[0].(string); !ok || got != channel {
		t.Fatalf("arg[0] = %#v, want channel", e.args[0])
	}
	if got, ok := e.args[1].(string); !ok || got != wsID {
		t.Fatalf("arg[1] = %#v, want workspace id %q", e.args[1], wsID)
	}
}

// TestNotifyAppendZeroEventBatchDoesNotNotify pins the zero-event
// boundary: an append whose Events are empty does no feed work, so it must
// not notify — waking a subscriber would deliver nothing but a wasted fenced
// read. A constraint-bearing zero-event append is rejected before this point
// (ErrConstraintWithoutEvent, ADR 0043).
func TestNotifyAppendZeroEventBatchDoesNotNotify(t *testing.T) {
	db := &recordingDBTX{}
	w := notifyWorkspace("es_lite_appended", "ws_notify")
	if err := w.notifyAppend(context.Background(), db, es.AppendParams{}); err != nil {
		t.Fatalf("notifyAppend on a zero-event batch: %v", err)
	}
	if len(db.execs) != 0 {
		t.Fatalf("zero-event batch issued %d Exec calls, want 0", len(db.execs))
	}
}

// TestNotifyAppendRejectsOversizedWorkspaceBeforeExec pins the payload
// bound: a workspace ID over maxNotifyWorkspaceBytes is rejected before
// any statement runs, so the 8000-byte NOTIFY limit is unreachable
// rather than a live database error. The bound itself is pinned at both
// edges — 512 bytes still notifies, 513 is refused.
func TestNotifyAppendRejectsOversizedWorkspaceBeforeExec(t *testing.T) {
	db := &recordingDBTX{}
	oversized := notifyWorkspace("es_lite_appended", strings.Repeat("w", maxNotifyWorkspaceBytes+1))
	err := oversized.notifyAppend(context.Background(), db, es.AppendParams{
		Events: []es.EventData{{TypeURL: "counter.v1.Initialized", Payload: []byte("x")}},
	})
	if err == nil {
		t.Fatalf("workspace id of %d bytes was accepted, want the notify bound to reject it", maxNotifyWorkspaceBytes+1)
	}
	if !strings.Contains(err.Error(), "notify bound") {
		t.Fatalf("error %q does not name the notify bound", err)
	}
	if len(db.execs) != 0 {
		t.Fatalf("oversized workspace issued %d Exec calls, want the error before any statement", len(db.execs))
	}

	// The boundary itself: exactly maxNotifyWorkspaceBytes bytes is fine.
	bound := &recordingDBTX{}
	atBound := notifyWorkspace("es_lite_appended", strings.Repeat("w", maxNotifyWorkspaceBytes))
	if err := atBound.notifyAppend(context.Background(), bound, es.AppendParams{
		Events: []es.EventData{{TypeURL: "counter.v1.Initialized", Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("workspace id of exactly %d bytes rejected: %v", maxNotifyWorkspaceBytes, err)
	}
	if len(bound.execs) != 1 {
		t.Fatalf("boundary workspace issued %d Exec calls, want 1", len(bound.execs))
	}
}

// TestNotifyChannelValidator checks validNotifyChannel rule by rule: a
// PostgreSQL keyword like "select" is a fine identifier (LISTEN quotes
// it), while an empty name, an over-long one, a hyphen or a leading
// digit is not.
func TestNotifyChannelValidator(t *testing.T) {
	for _, tc := range []struct {
		name    string
		channel string
		wantErr bool
	}{
		{"canonical example", "es_lite_appended", false},
		{"empty", "", true},
		{"63 bytes is the limit", strings.Repeat("c", 63), false},
		{"64 bytes is over", strings.Repeat("c", 64), true},
		{"hyphen", "bad-name", true},
		{"keyword select", "select", false},
		{"leading digit", "1channel", true},
		{"leading underscore", "_es_lite", false},
		{"digit inside", "es_lite_2", false},
	} {
		err := validNotifyChannel("WithAppendNotify", tc.channel)
		if tc.wantErr && err == nil {
			t.Errorf("%s: channel %q accepted, want error", tc.name, tc.channel)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("%s: channel %q rejected: %v", tc.name, tc.channel, err)
		}
		if tc.wantErr && err != nil && !strings.Contains(err.Error(), tc.channel) {
			// Only channels the error is allowed to name: the empty
			// string has no bytes to quote.
			if tc.channel != "" {
				t.Errorf("%s: error %q does not name the channel", tc.name, err)
			}
		}
	}
}

// TestOpenConfigRejectsInvalidNotifyChannel pins the fail-fast contract:
// a bad channel fails validate() — which both constructors run before
// connecting — a good one validates, and so does the default (option
// off): an empty channel means "off", not "misconfigured", or every
// existing Open call would start failing.
func TestOpenConfigRejectsInvalidNotifyChannel(t *testing.T) {
	bad := newOpenConfig([]Option{WithAppendNotify("bad-name")})
	if err := bad.validate(); err == nil {
		t.Fatal("validate accepted an invalid notify channel, want error")
	}
	badMarker := newOpenConfig([]Option{WithCheckpointNotify("bad-name")})
	if err := badMarker.validate(); err == nil || !strings.Contains(err.Error(), "WithCheckpointNotify") {
		t.Fatalf("validate(marker) = %v, want an error naming WithCheckpointNotify", err)
	}
	same := newOpenConfig([]Option{WithAppendNotify("es_lite_same"), WithCheckpointNotify("es_lite_same")})
	if err := same.validate(); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("validate(same channel) = %v, want a must-differ error", err)
	}
	good := newOpenConfig([]Option{WithAppendNotify("es_lite_appended")})
	if err := good.validate(); err != nil {
		t.Fatalf("validate rejected a valid channel: %v", err)
	}
	def := newOpenConfig(nil)
	if err := def.validate(); err != nil {
		t.Fatalf("validate rejected the default config (option off): %v", err)
	}
}
