package sqlite_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// decisionRow is one append_decisions row, as the raw query below reads it.
// commandID is COALESCEd to "" so the scan needs no nullable pointer; a live
// append always writes the UUID string (the zero UUID when unset).
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

// TestAppendWithoutConstraintsWritesNoDecision pins the negative: the record
// is written only for an append that carries at least one constraint op, so a
// plain event append leaves the decision log untouched.
func TestAppendWithoutConstraintsWritesNoDecision(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid, err := es.NewStreamID("counter", "plain")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventData{{
			TypeURL:       "counter.v1.Incremented",
			SchemaVersion: 1,
			Payload:       []byte("plain"),
		}},
	}); err != nil {
		t.Fatalf("append without constraints: %v", err)
	}
	if got := len(decisionRows(t, s)); got != 0 {
		t.Fatalf("append_decisions rows after a zero-constraint append = %d, want 0", got)
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

// decisionStream returns a canonical stream id for the claims-maintenance
// tests.
func decisionStream(t *testing.T, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("counter", id)
	if err != nil {
		t.Fatalf("stream id %q: %v", id, err)
	}
	return sid
}

// appendWithOps appends one event carrying the given constraint ops at the
// stream's current version.
func appendWithOps(t *testing.T, s *sqlite.Store, sid es.StreamID, version uint64, ops ...es.ConstraintOp) {
	t.Helper()
	if _, err := s.Append(context.Background(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events: []es.EventData{{
			TypeURL:       "counter.v1.Incremented",
			SchemaVersion: 1,
			Payload:       []byte("decision"),
		}},
		Constraints: ops,
	}); err != nil {
		t.Fatalf("append %+v at v%d: %v", ops, version, err)
	}
}

// countClaims runs one count query against the store's raw handle.
func countClaims(t *testing.T, s *sqlite.Store, query string) int64 {
	t.Helper()
	var n int64
	if err := s.DB().QueryRowContext(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// TestRebuildClaimsFromDecisions rebuilds a damaged index from the log alone:
// three recorded decisions (claim A, claim B, release A) replay into exactly
// the one claim still held, and a verify of the rebuilt index agrees.
func TestRebuildClaimsFromDecisions(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sidA := decisionStream(t, "a")
	sidB := decisionStream(t, "b")

	appendWithOps(t, s, sidA, 0, es.Claim("email", "rebuild-a@b"))
	appendWithOps(t, s, sidB, 0, es.Claim("email", "rebuild-b@b"))
	appendWithOps(t, s, sidA, 1, es.Release("email", "rebuild-a@b"))

	if got := len(decisionRows(t, s)); got != 3 {
		t.Fatalf("append_decisions rows = %d, want 3", got)
	}
	// Damage the live index directly, as a lost restore would: the rebuild
	// must reconstruct it from append_decisions, not trust what it finds.
	if _, err := s.DB().ExecContext(ctx, `DELETE FROM unique_claims`); err != nil {
		t.Fatalf("delete unique_claims: %v", err)
	}
	if n := countClaims(t, s, `SELECT COUNT(*) FROM unique_claims`); n != 0 {
		t.Fatalf("unique_claims rows before rebuild = %d, want 0", n)
	}

	rep, err := s.RebuildClaims(ctx)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if rep.Decisions != 3 || rep.Claims != 1 {
		t.Errorf("rebuild report = %+v, want {Decisions:3 Claims:1}", rep)
	}

	if sid, found, err := s.LookupClaim(ctx, "email", "rebuild-b@b"); err != nil || !found || sid != sidB.Canonical() {
		t.Errorf("lookup of B = (%q, %v, %v), want (%q, true, nil)", sid, found, err, sidB.Canonical())
	}
	if _, found, err := s.LookupClaim(ctx, "email", "rebuild-a@b"); err != nil || found {
		t.Errorf("lookup of released A = found=%v err=%v, want absent", found, err)
	}

	verify, err := s.VerifyClaims(ctx)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !verify.OK {
		t.Errorf("verify after rebuild = %+v, want OK", verify)
	}
}

// TestRebuildRefusesClaimsWithoutDecisions pins the mandatory guard: live
// claims with no decision history are an inconsistent store — a partial
// restore or rows written outside the append path — and a rebuild must refuse
// rather than delete an index it cannot reconstruct.
func TestRebuildRefusesClaimsWithoutDecisions(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := decisionStream(t, "a")

	appendWithOps(t, s, sid, 0, es.Claim("email", "no-decisions@b"))
	if _, err := s.DB().ExecContext(ctx, `DELETE FROM append_decisions`); err != nil {
		t.Fatalf("delete append_decisions: %v", err)
	}

	rep, err := s.RebuildClaims(ctx)
	if !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Fatalf("rebuild with no decision history: got %v, want ErrClaimsWithoutDecisions", err)
	}
	if rep != (es.ClaimsRebuildReport{}) {
		t.Errorf("rebuild report on a claims-without-decisions state = %+v, want the zero report", rep)
	}
	if _, found, err := s.LookupClaim(ctx, "email", "no-decisions@b"); err != nil || !found {
		t.Fatalf("claim after refused rebuild = found=%v err=%v, want still present", found, err)
	}
}

// TestVerifyRefusesClaimsWithoutDecisions pins the verify-side guard, the
// mirror of RebuildClaims': live claims with no decision history are an
// inconsistent store, not a tamper. Verify must return the zero report and
// ErrClaimsWithoutDecisions rather than inventing a divergence, so the
// runbook's "refusal is exit 2" contract holds.
func TestVerifyRefusesClaimsWithoutDecisions(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := decisionStream(t, "a")

	appendWithOps(t, s, sid, 0, es.Claim("email", "verify-no-decisions@b"))
	if _, err := s.DB().ExecContext(ctx, `DELETE FROM append_decisions`); err != nil {
		t.Fatalf("delete append_decisions: %v", err)
	}

	rep, err := s.VerifyClaims(ctx)
	if !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Fatalf("verify with no decision history: got %v, want ErrClaimsWithoutDecisions", err)
	}
	if rep != (es.ClaimsVerifyReport{}) {
		t.Errorf("verify report on a claims-without-decisions state = %+v, want the zero report", rep)
	}
}

// TestVerifyClaimsReportsDivergence proves the oracle notices an index row
// the log does not license and names it.
func TestVerifyClaimsReportsDivergence(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := decisionStream(t, "a")
	bogus := decisionStream(t, "bogus")

	appendWithOps(t, s, sid, 0, es.Claim("email", "verify@b"))
	if _, err := s.DB().ExecContext(ctx,
		`INSERT INTO unique_claims (scope, value_key, stream_id) VALUES (?,?,?)`,
		"bogus.scope", []byte("bogus"), bogus.Canonical()); err != nil {
		t.Fatalf("insert bogus claim: %v", err)
	}

	rep, err := s.VerifyClaims(ctx)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if rep.OK {
		t.Fatal("verify = OK, want a divergence")
	}
	if rep.FirstDivergence == "" {
		t.Fatal("FirstDivergence is empty, want the bogus scope and stream")
	}
	for _, want := range []string{"bogus.scope", bogus.Canonical()} {
		if !strings.Contains(rep.FirstDivergence, want) {
			t.Errorf("FirstDivergence = %q, want it to name %q", rep.FirstDivergence, want)
		}
	}
}
