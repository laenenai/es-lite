package sqlite_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/laenenai/es-lite/projection"
	"github.com/laenenai/es-lite/sqlite"
)

// TestSQLiteProjectionCheckpoints pins migration 0015's table shape: the
// five columns of the proposal, the (scope, projection) primary key and a
// state CHECK that admits only the three persisted statuses. SQLite has no
// workspace column or RLS — one database is one workspace — so the row key
// is (scope, projection) with the scope usually the empty string.
func TestSQLiteProjectionCheckpoints(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	rows, err := s.DB().QueryContext(ctx, `PRAGMA table_info(projection_checkpoints)`)
	if err != nil {
		t.Fatalf("table_info(projection_checkpoints): %v", err)
	}
	defer rows.Close()

	type col struct {
		name    string
		typ     string
		notNull int
		pk      int
	}
	var got []col
	for rows.Next() {
		var (
			cid  int
			c    col
			dflt any // NULL or a DEFAULT expression; must not scan into a typed value
		)
		if err := rows.Scan(&cid, &c.name, &c.typ, &c.notNull, &dflt, &c.pk); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		got = append(got, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info: %v", err)
	}

	want := []col{
		{"scope", "TEXT", 1, 1},
		{"projection", "TEXT", 1, 2},
		{"position", "INTEGER", 1, 0},
		{"state", "TEXT", 1, 0},
		{"updated_at", "TEXT", 1, 0},
	}
	if len(got) != len(want) {
		t.Fatalf("projection_checkpoints columns = %+v, want %+v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("projection_checkpoints column %d = %+v, want %+v", i, got[i], w)
		}
	}

	// The CHECK admits exactly live/rebuilding/stalled: each accepted value
	// inserts, and "unknown" (the zero Status, never written by the stores)
	// is refused. The rows are rolled back, so the test leaves nothing.
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin schema probe: %v", err)
	}
	defer tx.Rollback()
	for _, state := range []string{"live", "rebuilding", "stalled"} {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO projection_checkpoints (scope, projection, position, state, updated_at)
			VALUES ('', ?, 0, ?, '2026-01-01T00:00:00.000000000Z')`, state, state); err != nil {
			t.Fatalf("insert state %q: %v — the CHECK must admit it", state, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO projection_checkpoints (scope, projection, position, state, updated_at)
		VALUES ('', 'unknown_row', 0, 'unknown', '2026-01-01T00:00:00.000000000Z')`); err == nil {
		t.Fatal("insert state 'unknown' succeeded — the CHECK must admit only live/rebuilding/stalled")
	} else if !strings.Contains(strings.ToLower(err.Error()), "check") {
		t.Fatalf("insert state 'unknown': error %q does not name the state CHECK", err)
	}
}

// TestSQLiteProgressStore pins the adapter's ProgressStore semantics: absent
// rows read as found=false, saves are monotonic within a state, a state
// change replaces the position, ResetProgress rewinds to zero, SetStatus
// never moves the position, scopes are isolated, and unknown state text on
// read is rejected.
func TestSQLiteProgressStore(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.Open(ctx, "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ps := sqlite.NewProgressStore(s)

	const (
		scope     = "" // the in-process backend's scope
		other     = "ws_other"
		projector = "orders"
	)

	// Absent: found=false and a zero Checkpoint, no error.
	cp, found, err := ps.LoadProgress(ctx, scope, projector)
	if err != nil {
		t.Fatalf("load absent: %v", err)
	}
	if found {
		t.Fatalf("load absent: found=true, want false")
	}
	if cp != (projection.Checkpoint{}) {
		t.Fatalf("load absent: checkpoint = %+v, want the zero value", cp)
	}

	// Save creates the row and the args override cp's own key.
	if err := ps.SaveProgress(ctx, scope, projector, projection.Checkpoint{
		Scope: "ignored", Projection: "ignored", Position: 10, State: projection.StatusLive,
	}); err != nil {
		t.Fatalf("save create: %v", err)
	}
	cp, found, err = ps.LoadProgress(ctx, scope, projector)
	if err != nil || !found {
		t.Fatalf("load created = found %v err %v, want found", found, err)
	}
	if cp.Scope != scope || cp.Projection != projector {
		t.Fatalf("loaded key = (%q,%q), want (%q,%q)", cp.Scope, cp.Projection, scope, projector)
	}
	if cp.Position != 10 || cp.State != projection.StatusLive {
		t.Fatalf("loaded = position %d state %s, want 10/live", cp.Position, cp.State)
	}
	if cp.UpdatedAt.IsZero() {
		t.Fatal("loaded UpdatedAt is zero — the row must carry a timestamp")
	}
	// updated_at is the store's fixed-width RFC3339 UTC layout (tsLayout),
	// not an opaque driver default.
	var stored string
	if err := s.DB().QueryRowContext(ctx,
		`SELECT updated_at FROM projection_checkpoints WHERE scope = ? AND projection = ?`,
		scope, projector).Scan(&stored); err != nil {
		t.Fatalf("read stored updated_at: %v", err)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000000000Z", stored); err != nil {
		t.Fatalf("stored updated_at %q is not the store's timestamp layout: %v", stored, err)
	}

	// Monotonic within a state: a lower position leaves the stored one.
	if err := ps.SaveProgress(ctx, scope, projector, projection.Checkpoint{
		Position: 4, State: projection.StatusLive,
	}); err != nil {
		t.Fatalf("save lower: %v", err)
	}
	cp, _, err = ps.LoadProgress(ctx, scope, projector)
	if err != nil {
		t.Fatalf("load after lower save: %v", err)
	}
	if cp.Position != 10 {
		t.Fatalf("position after save at 4 = %d, want 10 (monotonic within a state)", cp.Position)
	}

	// An equal save also leaves the position alone but refreshes UpdatedAt.
	prev := cp.UpdatedAt
	if err := ps.SaveProgress(ctx, scope, projector, projection.Checkpoint{
		Position: 10, State: projection.StatusLive,
	}); err != nil {
		t.Fatalf("save equal: %v", err)
	}
	cp, _, err = ps.LoadProgress(ctx, scope, projector)
	if err != nil {
		t.Fatalf("load after equal save: %v", err)
	}
	if cp.Position != 10 {
		t.Fatalf("position after save at same position = %d, want 10", cp.Position)
	}
	if cp.UpdatedAt.Before(prev) {
		t.Fatalf("UpdatedAt moved backwards: %v before %v", cp.UpdatedAt, prev)
	}

	// A higher position advances.
	if err := ps.SaveProgress(ctx, scope, projector, projection.Checkpoint{
		Position: 12, State: projection.StatusLive,
	}); err != nil {
		t.Fatalf("save higher: %v", err)
	}
	if cp, _, err = ps.LoadProgress(ctx, scope, projector); err != nil || cp.Position != 12 {
		t.Fatalf("position after save at 12 = %d (err %v), want 12", cp.Position, err)
	}

	// A different state replaces the position: a rebuild restarts it.
	if err := ps.SaveProgress(ctx, scope, projector, projection.Checkpoint{
		Position: 3, State: projection.StatusRebuilding,
	}); err != nil {
		t.Fatalf("save rebuilding: %v", err)
	}
	cp, _, err = ps.LoadProgress(ctx, scope, projector)
	if err != nil {
		t.Fatalf("load rebuilding: %v", err)
	}
	if cp.Position != 3 || cp.State != projection.StatusRebuilding {
		t.Fatalf("after state change = position %d state %s, want 3/rebuilding", cp.Position, cp.State)
	}
	if err := ps.SaveProgress(ctx, scope, projector, projection.Checkpoint{
		Position: 2, State: projection.StatusRebuilding,
	}); err != nil {
		t.Fatalf("save lower rebuilding: %v", err)
	}
	if cp, _, err = ps.LoadProgress(ctx, scope, projector); err != nil || cp.Position != 3 {
		t.Fatalf("position after lower rebuilding save = %d (err %v), want 3", cp.Position, err)
	}

	// Reset rewinds to zero and writes the requested state, returning the
	// stored checkpoint.
	cp, err = ps.ResetProgress(ctx, scope, projector, projection.StatusRebuilding)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if cp.Scope != scope || cp.Projection != projector {
		t.Fatalf("reset key = (%q,%q), want (%q,%q)", cp.Scope, cp.Projection, scope, projector)
	}
	if cp.Position != 0 || cp.State != projection.StatusRebuilding || cp.UpdatedAt.IsZero() {
		t.Fatalf("reset returned %+v, want position 0, state rebuilding, nonzero UpdatedAt", cp)
	}
	if cp, _, err = ps.LoadProgress(ctx, scope, projector); err != nil {
		t.Fatalf("load after reset: %v", err)
	}
	if cp.Position != 0 || cp.State != projection.StatusRebuilding {
		t.Fatalf("after reset = position %d state %s, want 0/rebuilding", cp.Position, cp.State)
	}

	// SetStatus never moves the position.
	if err := ps.SaveProgress(ctx, scope, projector, projection.Checkpoint{
		Position: 42, State: projection.StatusLive,
	}); err != nil {
		t.Fatalf("save before set-status: %v", err)
	}
	if err := ps.SetStatus(ctx, scope, projector, projection.StatusStalled); err != nil {
		t.Fatalf("set status: %v", err)
	}
	cp, _, err = ps.LoadProgress(ctx, scope, projector)
	if err != nil {
		t.Fatalf("load after set-status: %v", err)
	}
	if cp.Position != 42 || cp.State != projection.StatusStalled {
		t.Fatalf("after set-status = position %d state %s, want 42/stalled", cp.Position, cp.State)
	}

	// SetStatus on an absent row creates a zero-position row.
	if err := ps.SetStatus(ctx, scope, "fresh", projection.StatusRebuilding); err != nil {
		t.Fatalf("set status on absent row: %v", err)
	}
	cp, found, err = ps.LoadProgress(ctx, scope, "fresh")
	if err != nil {
		t.Fatalf("load fresh: %v", err)
	}
	if !found || cp.Position != 0 || cp.State != projection.StatusRebuilding {
		t.Fatalf("fresh row = found %v %+v, want found position 0 rebuilding", found, cp)
	}

	// Scope isolation: the same projection key in another scope is its own
	// row, and a scope with no row stays absent.
	if err := ps.SaveProgress(ctx, other, projector, projection.Checkpoint{
		Position: 99, State: projection.StatusLive,
	}); err != nil {
		t.Fatalf("save other scope: %v", err)
	}
	cp, _, err = ps.LoadProgress(ctx, other, projector)
	if err != nil {
		t.Fatalf("load other scope: %v", err)
	}
	if cp.Scope != other || cp.Position != 99 || cp.State != projection.StatusLive {
		t.Fatalf("other scope = %+v, want (%q, 99, live)", cp, other)
	}
	cp, _, err = ps.LoadProgress(ctx, scope, projector)
	if err != nil {
		t.Fatalf("load original after other-scope write: %v", err)
	}
	if cp.Position != 42 || cp.State != projection.StatusStalled {
		t.Fatalf("original scope = %+v, want position 42 stalled — a foreign scope's write must not leak", cp)
	}
	if _, found, err = ps.LoadProgress(ctx, other, "fresh"); err != nil || found {
		t.Fatalf("load absent in other scope = found %v err %v, want absent", found, err)
	}

	// StatusUnknown and out-of-range statuses are never written.
	for name, state := range map[string]projection.Status{
		"unknown":      projection.StatusUnknown,
		"out-of-range": projection.Status(9),
	} {
		if err := ps.SaveProgress(ctx, scope, "invalid_save_"+name, projection.Checkpoint{State: state}); err == nil {
			t.Errorf("SaveProgress state %s: err = nil, want refusal", name)
		}
		if err := ps.SetStatus(ctx, scope, "invalid_status_"+name, state); err == nil {
			t.Errorf("SetStatus state %s: err = nil, want refusal", name)
		}
		if _, err := ps.ResetProgress(ctx, scope, "invalid_reset_"+name, state); err == nil {
			t.Errorf("ResetProgress state %s: err = nil, want refusal", name)
		}
	}

	// Unknown state text on read is rejected: last writer wins through the
	// CHECK normally, so disable it to plant the corrupt row the reader must
	// defend against.
	if err := ps.SaveProgress(ctx, scope, "corrupt", projection.Checkpoint{Position: 1, State: projection.StatusLive}); err != nil {
		t.Fatalf("save corrupt probe: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("ignore_check_constraints on: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE projection_checkpoints SET state = 'bogus' WHERE scope = ? AND projection = 'corrupt'`, scope); err != nil {
		t.Fatalf("plant corrupt state: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatalf("ignore_check_constraints off: %v", err)
	}
	if _, _, err := ps.LoadProgress(ctx, scope, "corrupt"); err == nil {
		t.Fatal("LoadProgress of an unknown state text: err = nil, want rejection")
	}
}
