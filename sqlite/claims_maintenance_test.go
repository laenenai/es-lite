package sqlite_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// storedOps is one event row's constraint_ops column, as the raw query below
// reads it. A NULL column scans as nil.
type storedOps struct {
	version uint64
	ops     []byte
}

// eventOps reads the constraint_ops column of every event in the stream, in
// version order.
func eventOps(t *testing.T, s *sqlite.Store, sid es.StreamID) []storedOps {
	t.Helper()
	rows, err := s.DB().QueryContext(context.Background(),
		`SELECT version, constraint_ops FROM events WHERE stream_id = ? ORDER BY version`, sid.Canonical())
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	defer rows.Close()
	var out []storedOps
	for rows.Next() {
		var r storedOps
		if err := rows.Scan(&r.version, &r.ops); err != nil {
			t.Fatalf("scan event row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("event rows: %v", err)
	}
	return out
}

// TestAppendWritesConstraintOpsOnFirstEvent pins the append-side shape: one
// append with two events and one Claim writes the encoded ops on the FIRST
// row only, and the returned, ReadStream and ReadAll envelopes carry them on
// that envelope alone.
func TestAppendWritesConstraintOpsOnFirstEvent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "first")
	ops := []es.ConstraintOp{es.Claim("email", "a@b")}

	res, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventData{
			{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("one")},
			{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("two")},
		},
		Constraints: ops,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(res.Envelopes) != 2 {
		t.Fatalf("returned envelopes = %d, want 2", len(res.Envelopes))
	}
	if !slices.Equal(res.Envelopes[0].ConstraintOps, ops) {
		t.Errorf("first returned envelope ops = %+v, want %+v", res.Envelopes[0].ConstraintOps, ops)
	}
	if res.Envelopes[1].ConstraintOps != nil {
		t.Errorf("second returned envelope ops = %+v, want nil", res.Envelopes[1].ConstraintOps)
	}

	rows := eventOps(t, s, sid)
	if len(rows) != 2 {
		t.Fatalf("event rows = %d, want 2", len(rows))
	}
	if rows[0].ops == nil || rows[1].ops != nil {
		t.Fatalf("constraint_ops by version = %v/%v, want encoded/NULL", rows[0].ops, rows[1].ops)
	}
	got, err := es.DecodeConstraintOps(rows[0].ops)
	if err != nil {
		t.Fatalf("decode first row ops: %v", err)
	}
	if !slices.Equal(got, ops) {
		t.Errorf("stored ops = %+v, want %+v", got, ops)
	}

	stream, err := s.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if !slices.Equal(stream[0].ConstraintOps, ops) || stream[1].ConstraintOps != nil {
		t.Errorf("ReadStream ops = %+v / %+v, want first only", stream[0].ConstraintOps, stream[1].ConstraintOps)
	}
	all, err := s.ReadAll(ctx, 0, 10)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(all) != 2 || !slices.Equal(all[0].ConstraintOps, ops) || all[1].ConstraintOps != nil {
		t.Errorf("ReadAll = %+v, want the ops on the first envelope only", all)
	}
}

// TestAppendRollbackWritesNoConstraintOps pins the same-transaction ordering:
// a claim collision aborts the append, and the event row that would have
// carried the ops rolls back with it.
func TestAppendRollbackWritesNoConstraintOps(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	claim := es.Claim("email", "rollback@b")

	first := opsStream(t, "first")
	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        first,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("first")}},
		Constraints:     []es.ConstraintOp{claim},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}

	second := opsStream(t, "second")
	_, err := s.Append(ctx, es.AppendParams{
		StreamID:        second,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("second")}},
		Constraints:     []es.ConstraintOp{claim},
	})
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("colliding append: got %v, want ErrConstraintViolated", err)
	}
	if rows := eventOps(t, s, second); len(rows) != 0 {
		t.Fatalf("events for the rolled-back stream = %d, want 0", len(rows))
	}
	if n := countQuery(t, s, `SELECT COUNT(*) FROM events`); n != 1 {
		t.Fatalf("total events after the rollback = %d, want 1 — the failed append wrote no op-bearing row", n)
	}
}

// TestAppendWithoutConstraintsWritesNoConstraintOps pins the negative: a
// nil-constraints append leaves constraint_ops NULL on EVERY row of the batch,
// not just the first.
func TestAppendWithoutConstraintsWritesNoConstraintOps(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "plain")

	if _, err := s.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events: []es.EventData{
			{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("plain-1")},
			{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("plain-2")},
			{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("plain-3")},
		},
	}); err != nil {
		t.Fatalf("append without constraints: %v", err)
	}
	rows := eventOps(t, s, sid)
	if len(rows) != 3 {
		t.Fatalf("event rows = %d, want 3", len(rows))
	}
	for _, row := range rows {
		if row.ops != nil {
			t.Fatalf("constraint_ops on v%d = %x, want NULL on every row", row.version, row.ops)
		}
	}
}

// TestAppendRejectsConstraintsWithoutEvent pins the semantic core: constraints
// with zero events are refused and write neither a claim nor an event.
func TestAppendRejectsConstraintsWithoutEvent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "claim-only")

	for _, op := range []es.ConstraintOp{
		es.Claim("email", "only@b"),
		es.Release("email", "only@b"),
	} {
		if _, err := s.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Constraints:     []es.ConstraintOp{op},
		}); !errors.Is(err, es.ErrConstraintWithoutEvent) {
			t.Fatalf("zero-event %+v append: got %v, want ErrConstraintWithoutEvent", op, err)
		}
	}
	if n := countQuery(t, s, `SELECT COUNT(*) FROM events`); n != 0 {
		t.Errorf("events rows = %d, want 0", n)
	}
	if n := countQuery(t, s, `SELECT COUNT(*) FROM unique_claims`); n != 0 {
		t.Errorf("unique_claims rows = %d, want 0", n)
	}
}

// TestReadCorruptConstraintOpsFailsLoudly pins the read contract: a corrupt
// blob fails the read, naming the stream and version but never the bytes.
func TestReadCorruptConstraintOpsFailsLoudly(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "corrupt")

	appendWithOps(t, s, sid, 0, es.Claim("email", "a@b"))
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE events SET constraint_ops = ? WHERE stream_id = ? AND version = 1`,
		[]byte("garbage-bytes"), sid.Canonical()); err != nil {
		t.Fatalf("tamper constraint_ops: %v", err)
	}

	_, err := s.ReadStream(ctx, sid, 0, 0)
	if err == nil {
		t.Fatal("ReadStream accepted a corrupt constraint_ops blob")
	}
	for _, want := range []string{sid.Canonical(), "v1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("read error = %q, want it to name %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "garbage-bytes") {
		t.Errorf("read error leaked the blob bytes: %q", err)
	}
}

// TestReadEmptyConstraintOpsFailsLoudly pins the other corrupt shape: a
// zero-length but non-NULL blob (out-of-band SQL; the store never writes one)
// must fail the read as corrupt rather than read as "no ops". The rebuild
// counts it with IS NOT NULL and fails decoding it, so the read must agree.
func TestReadEmptyConstraintOpsFailsLoudly(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "empty")

	appendWithOps(t, s, sid, 0, es.Claim("email", "empty@b"))
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE events SET constraint_ops = X'' WHERE stream_id = ? AND version = 1`, sid.Canonical()); err != nil {
		t.Fatalf("set empty constraint_ops: %v", err)
	}

	if _, err := s.ReadStream(ctx, sid, 0, 0); err == nil {
		t.Fatal("ReadStream accepted a zero-length non-NULL constraint_ops blob")
	} else {
		for _, want := range []string{sid.Canonical(), "v1"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("read error = %q, want it to name %q", err, want)
			}
		}
	}
}

// TestTamperedConstraintOpsDoesNotBreakChain pins the documented behavior: the
// chain attests the event, not the ops, so a decodable tampered ops blob still
// verifies. The index it feeds is trusted infrastructure, like unique_claims.
func TestTamperedConstraintOpsDoesNotBreakChain(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "tamper")

	appendWithOps(t, s, sid, 0, es.Claim("email", "a@b"))
	tampered := es.EncodeConstraintOps([]es.ConstraintOp{es.Claim("email", "different@b")})
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE events SET constraint_ops = ? WHERE stream_id = ? AND version = 1`,
		tampered, sid.Canonical()); err != nil {
		t.Fatalf("tamper constraint_ops: %v", err)
	}

	rep, err := es.VerifyStream(ctx, s, sid)
	if err != nil || !rep.Intact() {
		t.Fatalf("VerifyStream after an ops tamper: intact=%v err=%v, want intact — ops are excluded from the chain", rep.Intact(), err)
	}
}

// TestReleaseRoundTripsAndReplaysNoop pins a Release through the column: it
// decodes from the row and reconstructions as a no-op when it matches nothing.
func TestReleaseRoundTripsAndReplaysNoop(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "release")

	appendWithOps(t, s, sid, 0, es.Release("email", "never-claimed@b"))
	rows := eventOps(t, s, sid)
	if len(rows) != 1 || rows[0].ops == nil {
		t.Fatalf("release row = %+v, want one op-bearing event", rows)
	}
	got, err := es.DecodeConstraintOps(rows[0].ops)
	if err != nil || !slices.Equal(got, []es.ConstraintOp{es.Release("email", "never-claimed@b")}) {
		t.Fatalf("decoded release = %+v (err %v), want the Release", got, err)
	}

	rep, err := s.RebuildClaims(ctx)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if rep.Ops != 1 || rep.Claims != 0 {
		t.Errorf("rebuild report = %+v, want {Ops:1 Claims:0} — a release matching nothing is a no-op", rep)
	}
}

// opsStream returns a canonical stream id for the claims-maintenance tests.
func opsStream(t *testing.T, id string) es.StreamID {
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
			Payload:       []byte("ops"),
		}},
		Constraints: ops,
	}); err != nil {
		t.Fatalf("append %+v at v%d: %v", ops, version, err)
	}
}

// countQuery runs one count query against the store's raw handle.
func countQuery(t *testing.T, s *sqlite.Store, query string) int64 {
	t.Helper()
	var n int64
	if err := s.DB().QueryRowContext(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// TestVerifyClaimsReplaysEventBearingOrder is the replay-order regression for
// the removed advisory lock: every op rides an event-bearing append, and the
// ops must replay in global position order across streams. A claim on A, a
// release on A that matches nothing (still valid and event-bearing), a claim
// on B, then release A and claim A on B verify cleanly at each stage. The
// claim-only race the lock guarded is impossible by construction now —
// ErrConstraintWithoutEvent rejects a constraint-bearing append with no event
// (see TestAppendRejectsConstraintsWithoutEvent) — so replay order is the one
// remaining ordering property to pin.
func TestVerifyClaimsReplaysEventBearingOrder(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sidA := opsStream(t, "order-a")
	sidB := opsStream(t, "order-b")

	claimA := es.Claim("email", "order-a@b")
	appendWithOps(t, s, sidA, 0, claimA)
	appendWithOps(t, s, sidA, 1, es.Release("email", "never-claimed@b"))
	appendWithOps(t, s, sidB, 0, es.Claim("email", "order-b@b"))

	if rep, err := s.VerifyClaims(ctx); err != nil || !rep.OK {
		t.Fatalf("verify after claim/no-op-release/claim = %+v, err %v, want OK", rep, err)
	}
	if sid, found, err := s.LookupClaim(ctx, "email", "order-a@b"); err != nil || !found || sid != sidA.Canonical() {
		t.Fatalf("claim A after the no-op release = (%q, %v, %v), want (%q, true, nil)", sid, found, err, sidA.Canonical())
	}

	appendWithOps(t, s, sidA, 2, es.Release("email", "order-a@b"))
	appendWithOps(t, s, sidB, 1, claimA)

	rep, err := s.VerifyClaims(ctx)
	if err != nil {
		t.Fatalf("verify after release/claim on one stream: %v", err)
	}
	if !rep.OK {
		t.Fatalf("verify after release/claim = %+v, want OK", rep)
	}
	if sid, found, err := s.LookupClaim(ctx, "email", "order-a@b"); err != nil || !found || sid != sidB.Canonical() {
		t.Errorf("claim A after release/claim = (%q, %v, %v), want (%q, true, nil)", sid, found, err, sidB.Canonical())
	}
}

// TestRebuildClaimsFromEventLog rebuilds a damaged index from the log alone:
// three op-bearing events (claim A, claim B, release A) replay into exactly the
// one claim still held, and a verify of the rebuilt index agrees.
func TestRebuildClaimsFromEventLog(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sidA := opsStream(t, "a")
	sidB := opsStream(t, "b")

	appendWithOps(t, s, sidA, 0, es.Claim("email", "rebuild-a@b"))
	appendWithOps(t, s, sidB, 0, es.Claim("email", "rebuild-b@b"))
	appendWithOps(t, s, sidA, 1, es.Release("email", "rebuild-a@b"))

	if got := countQuery(t, s, `SELECT COUNT(*) FROM events WHERE constraint_ops IS NOT NULL`); got != 3 {
		t.Fatalf("op-bearing events = %d, want 3", got)
	}
	// Damage the live index directly, as a lost restore would: the rebuild
	// must reconstruct it from the event log, not trust what it finds.
	if _, err := s.DB().ExecContext(ctx, `DELETE FROM unique_claims`); err != nil {
		t.Fatalf("delete unique_claims: %v", err)
	}
	if n := countQuery(t, s, `SELECT COUNT(*) FROM unique_claims`); n != 0 {
		t.Fatalf("unique_claims rows before rebuild = %d, want 0", n)
	}

	rep, err := s.RebuildClaims(ctx)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if rep.Ops != 3 || rep.Claims != 1 {
		t.Errorf("rebuild report = %+v, want {Ops:3 Claims:1}", rep)
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

// TestRebuildRefusesClaimsWithoutOps pins the mandatory guard: live claims with
// no ops in the event log are an inconsistent store — a partial restore or rows
// written outside the append path — and a rebuild must refuse rather than
// delete an index it cannot reconstruct.
func TestRebuildRefusesClaimsWithoutOps(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "a")

	appendWithOps(t, s, sid, 0, es.Claim("email", "no-ops@b"))
	if _, err := s.DB().ExecContext(ctx, `UPDATE events SET constraint_ops = NULL`); err != nil {
		t.Fatalf("strip constraint_ops: %v", err)
	}

	rep, err := s.RebuildClaims(ctx)
	if !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Fatalf("rebuild with no op history: got %v, want ErrClaimsWithoutDecisions", err)
	}
	if rep != (es.ClaimsRebuildReport{}) {
		t.Errorf("rebuild report on a claims-without-ops state = %+v, want the zero report", rep)
	}
	if _, found, err := s.LookupClaim(ctx, "email", "no-ops@b"); err != nil || !found {
		t.Fatalf("claim after refused rebuild = found=%v err=%v, want still present", found, err)
	}
}

// TestVerifyRefusesClaimsWithoutOps pins the verify-side guard, the mirror of
// RebuildClaims': live claims with no ops in the event log are an inconsistent
// store, not a tamper. Verify must return the zero report and
// ErrClaimsWithoutDecisions rather than inventing a divergence, so the
// runbook's "refusal is exit 2" contract holds.
func TestVerifyRefusesClaimsWithoutOps(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "a")

	appendWithOps(t, s, sid, 0, es.Claim("email", "verify-no-ops@b"))
	if _, err := s.DB().ExecContext(ctx, `UPDATE events SET constraint_ops = NULL`); err != nil {
		t.Fatalf("strip constraint_ops: %v", err)
	}

	rep, err := s.VerifyClaims(ctx)
	if !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Fatalf("verify with no op history: got %v, want ErrClaimsWithoutDecisions", err)
	}
	if rep != (es.ClaimsVerifyReport{}) {
		t.Errorf("verify report on a claims-without-ops state = %+v, want the zero report", rep)
	}
}

// TestVerifyClaimsReportsDivergence proves the oracle notices an index row the
// log does not license and names it.
func TestVerifyClaimsReportsDivergence(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := opsStream(t, "a")
	bogus := opsStream(t, "bogus")

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
