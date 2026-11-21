package sqlite_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// decisionRow is one append_decisions row, as the raw query below reads it.
// commandID is COALESCEd to "" so a NULL (the adopt backfill) scans without
// a nullable pointer; a live append always writes the UUID string.
type decisionRow struct {
	streamID    string
	fromVersion uint64
	toVersion   uint64
	commandID   string
	ops         []byte
}

// decisionRows reads every append_decisions row in id order. One SQLite
// database is one workspace, so there is no workspace filter to pin.
func decisionRows(t *testing.T, s *sqlite.Store) []decisionRow {
	t.Helper()
	rows, err := s.DB().QueryContext(context.Background(),
		`SELECT stream_id, from_version, to_version, COALESCE(command_id, ''), ops
		   FROM append_decisions ORDER BY id`)
	if err != nil {
		t.Fatalf("read append_decisions: %v", err)
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
		t.Fatalf("append_decisions rows: %v", err)
	}
	return out
}

// TestAppendWritesDecisionRecord pins the append-side write: one append with
// a single Claim records exactly one decision row for its whole span, with
// the command id the events store and the encoded ops the caller supplied.
func TestAppendWritesDecisionRecord(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, err := es.NewStreamID("counter", "decision")
	if err != nil {
		t.Fatal(err)
	}
	cmd := uuid.MustParse("0192f3a0-0000-7000-8000-0000000000d1")

	params := es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventData{{
			TypeURL:       "counter.v1.Incremented",
			SchemaVersion: 1,
			Payload:       []byte("decision"),
		}},
		Constraints: []es.ConstraintOp{es.Claim("email", "a@b")},
		CommandID:   cmd,
	}
	if _, err := s.Append(ctx, params); err != nil {
		t.Fatalf("append: %v", err)
	}

	rows := decisionRows(t, s)
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
	claim := es.Claim("email", "rollback@b")
	newStream := func(id string) es.StreamID {
		sid, err := es.NewStreamID("counter", id)
		if err != nil {
			t.Fatal(err)
		}
		return sid
	}

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        newStream("first"),
		ExpectedVersion: 0,
		Events: []es.EventData{{
			TypeURL:       "counter.v1.Incremented",
			SchemaVersion: 1,
			Payload:       []byte("first"),
		}},
		Constraints: []es.ConstraintOp{claim},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if got := len(decisionRows(t, s)); got != 1 {
		t.Fatalf("append_decisions rows after the first append = %d, want 1", got)
	}

	second := newStream("second")
	_, err := s.Append(ctx, es.AppendParams{
		StreamID:        second,
		ExpectedVersion: 0,
		Events: []es.EventData{{
			TypeURL:       "counter.v1.Incremented",
			SchemaVersion: 1,
			Payload:       []byte("second"),
		}},
		Constraints: []es.ConstraintOp{claim},
	})
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("colliding append: got %v, want ErrConstraintViolated", err)
	}
	if got := len(decisionRows(t, s)); got != 1 {
		t.Fatalf("append_decisions rows after the rollback = %d, want 1 — the failed append must leave no decision row", got)
	}
	// The failed append's events rolled back with its decision row: the
	// decision is written in the same transaction, after applyClaims.
	var events int
	if err := s.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM events WHERE stream_id = ?`, second.Canonical(),
	).Scan(&events); err != nil {
		t.Fatalf("count rolled-back events: %v", err)
	}
	if events != 0 {
		t.Fatalf("events rows for the rolled-back stream = %d, want 0", events)
	}
}

// TestClaimOnlyAppendWritesDecision pins the zero-event case: a claim-only
// append has a decision worth recording even though it has no events, and
// its span is empty (from == to == ExpectedVersion).
func TestClaimOnlyAppendWritesDecision(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, err := es.NewStreamID("counter", "claim-only")
	if err != nil {
		t.Fatal(err)
	}

	res, err := s.Append(ctx, es.AppendParams{
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

	rows := decisionRows(t, s)
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

	var claims, events int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM unique_claims`).Scan(&claims); err != nil {
		t.Fatalf("count unique_claims: %v", err)
	}
	if claims != 1 {
		t.Errorf("unique_claims rows = %d, want 1", claims)
	}
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 0 {
		t.Errorf("events rows = %d, want 0", events)
	}
}
