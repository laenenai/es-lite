package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/projection"
)

// TestProjectionCheckpointsSchema pins migration 0017's table shape: the
// five columns of the proposal, the (workspace_id, projection) primary key
// and a state CHECK that admits only the three persisted statuses. No extra
// columns: the table is derived progress, not state.
func TestProjectionCheckpointsSchema(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	rows, err := s.Pool().Query(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'projection_checkpoints'
		ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("read projection_checkpoints columns: %v", err)
	}
	type column struct {
		name, dataType, nullable string
	}
	var got []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.dataType, &c.nullable); err != nil {
			rows.Close()
			t.Fatalf("scan projection_checkpoints column: %v", err)
		}
		got = append(got, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("projection_checkpoints columns: %v", err)
	}
	want := []column{
		{"workspace_id", "text", "NO"},
		{"projection", "text", "NO"},
		{"position", "bigint", "NO"},
		{"state", "text", "NO"},
		{"updated_at", "timestamp with time zone", "NO"},
	}
	if len(got) != len(want) {
		t.Fatalf("projection_checkpoints has %d columns (%v), want %d", len(got), got, len(want))
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("projection_checkpoints column %d = %+v, want %+v", i, got[i], w)
		}
	}

	var pk int
	if err := s.Pool().QueryRow(ctx, `
		SELECT count(*) FROM pg_constraint
		WHERE conrelid = 'public.projection_checkpoints'::regclass
		  AND contype = 'p'
		  AND pg_get_constraintdef(oid) = 'PRIMARY KEY (workspace_id, projection)'`).Scan(&pk); err != nil {
		t.Fatalf("read projection_checkpoints primary key: %v", err)
	}
	if pk != 1 {
		t.Fatalf("projection_checkpoints: %d constraints match PRIMARY KEY (workspace_id, projection), want 1", pk)
	}

	// The CHECK admits exactly live/rebuilding/stalled: each accepted value
	// inserts, and "unknown" (the zero Status, never written by the stores)
	// is refused. The rows are rolled back, so the test leaves nothing.
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin schema probe: %v", err)
	}
	defer tx.Rollback(ctx)
	for _, state := range []string{"live", "rebuilding", "stalled"} {
		if _, err := tx.Exec(ctx, `
			INSERT INTO projection_checkpoints (workspace_id, projection, state)
			VALUES ('ws_progress_schema', $1, $1)`, state); err != nil {
			t.Fatalf("insert state %q: %v — the CHECK must admit it", state, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO projection_checkpoints (workspace_id, projection, state)
		VALUES ('ws_progress_schema', 'unknown_row', 'unknown')`); err == nil {
		t.Fatal("insert state 'unknown' succeeded — the CHECK must admit only live/rebuilding/stalled")
	} else if !strings.Contains(err.Error(), "projection_checkpoints_state_check") &&
		!strings.Contains(err.Error(), "check constraint") {
		t.Fatalf("insert state 'unknown': error %q does not name the state CHECK", err)
	}
}

// TestProjectionCheckpointsRLS pins the table's tenant isolation: ENABLE +
// FORCE and the standard workspace policy (0016's shape), with both the
// workspace arm and the role-gated es_relay bypass arm.
func TestProjectionCheckpointsRLS(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	var rls, force bool
	if err := s.Pool().QueryRow(ctx, `
		SELECT relrowsecurity, relforcerowsecurity
		FROM pg_class WHERE oid = 'public.projection_checkpoints'::regclass`,
	).Scan(&rls, &force); err != nil {
		t.Fatalf("projection_checkpoints rls flags: %v", err)
	}
	if !rls || !force {
		t.Fatalf("projection_checkpoints rls=%v force=%v, want both true", rls, force)
	}

	var qual, withCheck string
	if err := s.Pool().QueryRow(ctx, `
		SELECT qual, with_check FROM pg_policies
		WHERE schemaname = 'public' AND tablename = 'projection_checkpoints'
		  AND policyname = 'projection_checkpoints_workspace_isolation'`,
	).Scan(&qual, &withCheck); err != nil {
		t.Fatalf("projection_checkpoints policy: %v", err)
	}
	for _, clause := range []struct{ name, definition string }{
		{"USING", qual},
		{"WITH CHECK", withCheck},
	} {
		if !strings.Contains(clause.definition, "app.workspace_id") {
			t.Errorf("policy %s lacks the workspace arm: %s", clause.name, clause.definition)
		}
		if !strings.Contains(clause.definition, "es_relay") {
			t.Errorf("policy %s lacks the role-gated es_relay arm: %s", clause.name, clause.definition)
		}
	}
}

// TestPostgresProgressStore pins the adapter's ProgressStore semantics:
// absent rows read as found=false, saves are monotonic within a state,
// a state change replaces the position, ResetProgress rewinds to zero,
// SetStatus never moves the position, and scopes are isolated.
func TestPostgresProgressStore(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ps := postgres.NewProgressStore(s, "")

	const (
		scope     = "ws_progress_a"
		other     = "ws_progress_b"
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
		t.Fatal("loaded UpdatedAt is zero — the row must carry the database clock")
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
}
