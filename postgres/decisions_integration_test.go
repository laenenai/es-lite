package postgres_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// decisionRow is one append_decisions row, as the raw queries below read it.
// commandID is COALESCEd to "" so a NULL (the adopt backfill, Task 4) scans
// without a nullable pointer; a live append always writes the UUID string.
type decisionRow struct {
	streamID    string
	fromVersion uint64
	toVersion   uint64
	commandID   string
	ops         []byte
}

// decisionStream returns a canonical stream id for the decision tests.
func decisionStream(t *testing.T, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("counter", id)
	if err != nil {
		t.Fatal(err)
	}
	return sid
}

// pinnedConn acquires a pooled connection with app.workspace_id pinned on the
// session (is_local=false, unlike the store's per-transaction set_config).
// append_decisions is FORCE ROW LEVEL SECURITY and the pool's login user is
// the table owner, so a bare pooled query sees rows the way the login user's
// RLS standing decides rather than the way a workspace principal reads them;
// pinning here makes the raw assertions workspace-scoped. The setting dies
// with the test's own pool, which openStore closes in cleanup.
func pinnedConn(t *testing.T, s *postgres.Store, ws string) *pgxpool.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := s.Pool().Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	t.Cleanup(conn.Release)
	if _, err := conn.Exec(ctx, `SELECT set_config('app.workspace_id', $1, false)`, ws); err != nil {
		t.Fatalf("pin workspace %s: %v", ws, err)
	}
	return conn
}

// decisionRows reads every append_decisions row under ws in id order.
func decisionRows(t *testing.T, s *postgres.Store, ws string) []decisionRow {
	t.Helper()
	ctx := context.Background()
	rows, err := pinnedConn(t, s, ws).Query(ctx,
		`SELECT stream_id, from_version, to_version, COALESCE(command_id, ''), ops
		   FROM append_decisions ORDER BY id`)
	if err != nil {
		t.Fatalf("read append_decisions under %s: %v", ws, err)
	}
	defer rows.Close()
	var out []decisionRow
	for rows.Next() {
		var r decisionRow
		if err := rows.Scan(&r.streamID, &r.fromVersion, &r.toVersion, &r.commandID, &r.ops); err != nil {
			t.Fatalf("scan append_decisions row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("append_decisions rows under %s: %v", ws, err)
	}
	return out
}

// countPinned runs one count over ws with the workspace pinned like
// decisionRows.
func countPinned(t *testing.T, s *postgres.Store, ws, query string) int64 {
	t.Helper()
	var n int64
	if err := pinnedConn(t, s, ws).QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("count under %s: %v", ws, err)
	}
	return n
}

// TestAppendWritesDecisionRecord pins the append-side write: one append with
// a single Claim records exactly one decision row for its whole span, with
// the command id the events store and the encoded ops the caller supplied.
func TestAppendWritesDecisionRecord(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_dec_write"
	sid := streamID(t)
	cmd := uuid.MustParse("0192f3a0-0000-7000-8000-0000000000d1")

	params := es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          eventData("decision"),
		Constraints:     []es.ConstraintOp{es.Claim("email", "a@b")},
		CommandID:       cmd,
	}
	if _, err := s.Workspace(ws).Append(ctx, params); err != nil {
		t.Fatalf("append: %v", err)
	}

	rows := decisionRows(t, s, ws)
	if len(rows) != 1 {
		t.Fatalf("append_decisions rows = %d, want 1", len(rows))
	}
	r := rows[0]
	if r.streamID != sid.Canonical() {
		t.Errorf("stream_id = %q, want %q", r.streamID, sid.Canonical())
	}
	if r.fromVersion != 0 || r.toVersion != 1 {
		t.Errorf("versions = [%d,%d), want [0,1)", r.fromVersion, r.toVersion)
	}
	if r.commandID != params.CommandID.String() {
		t.Errorf("command_id = %q, want %q", r.commandID, params.CommandID.String())
	}
	ops, err := es.DecodeDecisionOps(r.ops)
	if err != nil {
		t.Fatalf("decode ops: %v", err)
	}
	if !slices.Equal(ops, params.Constraints) {
		t.Errorf("ops = %+v, want %+v", ops, params.Constraints)
	}
}

// TestAppendRollbackWritesNoDecision pins the same-transaction ordering: a
// claim collision aborts the append, and the decision row the append would
// have written rolls back with it.
func TestAppendRollbackWritesNoDecision(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_dec_rollback"
	claim := es.Claim("email", "rollback@b")

	if _, err := s.Workspace(ws).Append(ctx, es.AppendParams{
		StreamID:        decisionStream(t, "first"),
		ExpectedVersion: 0,
		Events:          eventData("first"),
		Constraints:     []es.ConstraintOp{claim},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if got := len(decisionRows(t, s, ws)); got != 1 {
		t.Fatalf("append_decisions rows after the first append = %d, want 1", got)
	}

	_, err := s.Workspace(ws).Append(ctx, es.AppendParams{
		StreamID:        decisionStream(t, "second"),
		ExpectedVersion: 0,
		Events:          eventData("second"),
		Constraints:     []es.ConstraintOp{claim},
	})
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("colliding append: got %v, want ErrConstraintViolated", err)
	}
	if got := len(decisionRows(t, s, ws)); got != 1 {
		t.Fatalf("append_decisions rows after the rollback = %d, want 1 — the failed append must leave no decision row", got)
	}
}

// TestClaimOnlyAppendWritesDecision pins the zero-event case: a claim-only
// append has a decision worth recording even though it has no events, and
// its span is empty (from == to == ExpectedVersion).
func TestClaimOnlyAppendWritesDecision(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_dec_claim_only"
	sid := streamID(t)

	res, err := s.Workspace(ws).Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Constraints:     []es.ConstraintOp{es.Claim("email", "only@b")},
	})
	if err != nil {
		t.Fatalf("claim-only append: %v", err)
	}
	if res.FromVersion != 0 || res.ToVersion != 0 {
		t.Errorf("append result = [%d,%d), want the empty span [0,0)", res.FromVersion, res.ToVersion)
	}

	rows := decisionRows(t, s, ws)
	if len(rows) != 1 {
		t.Fatalf("append_decisions rows = %d, want 1", len(rows))
	}
	r := rows[0]
	if r.streamID != sid.Canonical() {
		t.Errorf("stream_id = %q, want %q", r.streamID, sid.Canonical())
	}
	if r.fromVersion != 0 || r.toVersion != 0 {
		t.Errorf("versions = [%d,%d), want the empty span [0,0)", r.fromVersion, r.toVersion)
	}
	if r.commandID != uuid.Nil.String() {
		t.Errorf("command_id = %q, want the zero UUID %q when unset", r.commandID, uuid.Nil.String())
	}
	ops, err := es.DecodeDecisionOps(r.ops)
	if err != nil {
		t.Fatalf("decode ops: %v", err)
	}
	if want := []es.ConstraintOp{es.Claim("email", "only@b")}; !slices.Equal(ops, want) {
		t.Errorf("ops = %+v, want %+v", ops, want)
	}

	if n := countPinned(t, s, ws, `SELECT count(*) FROM unique_claims`); n != 1 {
		t.Errorf("unique_claims rows = %d, want 1", n)
	}
	if n := countPinned(t, s, ws, `SELECT count(*) FROM events`); n != 0 {
		t.Errorf("events rows = %d, want 0", n)
	}
}

// TestAppendDecisionsWorkspaceIsolation proves the migration's policy, not
// the adapter's predicates, is what separates decision history: the reads
// below run as a role the policy binds (the pool's login user is a superuser,
// whom PostgreSQL exempts from row security even under FORCE, so a pool-level
// read would pass with the policy dropped). Each workspace sees exactly its
// own row and never the other's.
func TestAppendDecisionsWorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ensureRLSProbeRole(t, ctx, s)

	var super, bypass bool
	if err := s.Pool().QueryRow(ctx,
		`SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = $1`, rlsProbeRole,
	).Scan(&super, &bypass); err != nil {
		t.Fatalf("read %s's role flags: %v", rlsProbeRole, err)
	}
	if super || bypass {
		t.Fatalf("%s: rolsuper=%v rolbypassrls=%v, want false/false — the probe must be a role the policies bind", rlsProbeRole, super, bypass)
	}

	// Migrations issue no grants (spec §5.7), so the probe's SELECT is this
	// test's own, granted as the superuser and revoked on cleanup.
	if _, err := s.Pool().Exec(ctx,
		`GRANT SELECT ON append_decisions TO `+rlsProbeRole); err != nil {
		t.Fatalf("grant %s on append_decisions: %v", rlsProbeRole, err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool().Exec(context.Background(), `REVOKE SELECT ON append_decisions FROM `+rlsProbeRole); err != nil {
			t.Errorf("revoke append_decisions from %s: %v", rlsProbeRole, err)
		}
	})

	const (
		wsA = "ws_dec_rls_a"
		wsB = "ws_dec_rls_b"
	)
	sidA := decisionStream(t, "a")
	sidB := decisionStream(t, "b")
	for _, w := range []struct {
		ws string
		id es.StreamID
	}{{wsA, sidA}, {wsB, sidB}} {
		if _, err := s.Workspace(w.ws).Append(ctx, es.AppendParams{
			StreamID:        w.id,
			ExpectedVersion: 0,
			Events:          eventData(w.ws),
			Constraints:     []es.ConstraintOp{es.Claim("email", w.ws+"@b")},
		}); err != nil {
			t.Fatalf("append under %s: %v", w.ws, err)
		}
	}

	// visible reads the policy-bound table with no workspace_id filter:
	// the policy alone decides.
	visible := func(ws string) []string {
		var streams []string
		err := probePinned(t, ctx, s, ws, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT stream_id FROM append_decisions ORDER BY id`)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var sid string
				if err := rows.Scan(&sid); err != nil {
					return err
				}
				streams = append(streams, sid)
			}
			return rows.Err()
		})
		if err != nil {
			t.Fatalf("read append_decisions as %s: %v", ws, err)
		}
		return streams
	}

	if got := visible(wsA); !slices.Equal(got, []string{sidA.Canonical()}) {
		t.Errorf("%s sees %v, want only %v", wsA, got, sidA.Canonical())
	}
	if got := visible(wsB); !slices.Equal(got, []string{sidB.Canonical()}) {
		t.Errorf("%s sees %v, want only %v", wsB, got, sidB.Canonical())
	}
}
