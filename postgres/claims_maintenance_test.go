package postgres_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// opsStream returns a canonical stream id for the ops tests.
func opsStream(t *testing.T, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("counter", id)
	if err != nil {
		t.Fatal(err)
	}
	return sid
}

// pinnedConn acquires a pooled connection with app.workspace_id pinned on the
// session (is_local=false, unlike the store's per-transaction set_config).
// events is FORCE ROW LEVEL SECURITY and the pool's login user is the table
// owner, so a bare pooled query sees rows the way the login user's RLS standing
// decides rather than the way a workspace principal reads them; pinning here
// makes the raw assertions workspace-scoped. The setting dies with the test's
// own pool, which openStore closes in cleanup.
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

// storedOps is one event row's constraint_ops column, as the raw query below
// reads it. A NULL column scans as nil.
type storedOps struct {
	version uint64
	ops     []byte
}

// eventOps reads the constraint_ops column of every event in the stream, in
// version order, under the pinned workspace.
func eventOps(t *testing.T, s *postgres.Store, ws string, sid es.StreamID) []storedOps {
	t.Helper()
	ctx := context.Background()
	rows, err := pinnedConn(t, s, ws).Query(ctx,
		`SELECT version, constraint_ops FROM events WHERE workspace_id = $1 AND stream_id = $2 ORDER BY version`,
		ws, sid.Canonical())
	if err != nil {
		t.Fatalf("read events under %s: %v", ws, err)
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
		t.Fatalf("event rows under %s: %v", ws, err)
	}
	return out
}

// countPinned runs one count over ws with the workspace pinned like eventOps.
func countPinned(t *testing.T, s *postgres.Store, ws, query string) int64 {
	t.Helper()
	var n int64
	if err := pinnedConn(t, s, ws).QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("count under %s: %v", ws, err)
	}
	return n
}

// TestAppendWritesConstraintOpsOnFirstEvent pins the append-side shape: one
// append with two events and one Claim writes the encoded ops on the FIRST
// row only, and the returned, ReadStream and ReadAll envelopes carry them on
// that envelope alone.
func TestAppendWritesConstraintOpsOnFirstEvent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_ops_write"
	sid := streamID(t)
	ops := []es.ConstraintOp{es.Claim("email", "a@b")}

	res, err := s.Workspace(ws).Append(ctx, es.AppendParams{
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

	rows := eventOps(t, s, ws, sid)
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

	stream, err := s.Workspace(ws).ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if !slices.Equal(stream[0].ConstraintOps, ops) || stream[1].ConstraintOps != nil {
		t.Errorf("ReadStream ops = %+v / %+v, want first only", stream[0].ConstraintOps, stream[1].ConstraintOps)
	}
	all, err := s.Workspace(ws).ReadAll(ctx, 0, 10)
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
	const ws = "ws_ops_rollback"
	claim := es.Claim("email", "rollback@b")

	first := opsStream(t, "first")
	if _, err := s.Workspace(ws).Append(ctx, es.AppendParams{
		StreamID:        first,
		ExpectedVersion: 0,
		Events:          eventData("first"),
		Constraints:     []es.ConstraintOp{claim},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}

	second := opsStream(t, "second")
	_, err := s.Workspace(ws).Append(ctx, es.AppendParams{
		StreamID:        second,
		ExpectedVersion: 0,
		Events:          eventData("second"),
		Constraints:     []es.ConstraintOp{claim},
	})
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("colliding append: got %v, want ErrConstraintViolated", err)
	}
	if rows := eventOps(t, s, ws, second); len(rows) != 0 {
		t.Fatalf("events for the rolled-back stream = %d, want 0", len(rows))
	}
	if n := countPinned(t, s, ws, `SELECT count(*) FROM events`); n != 1 {
		t.Fatalf("total events after the rollback = %d, want 1 — the failed append wrote no op-bearing row", n)
	}
}

// TestAppendWithoutConstraintsWritesNoConstraintOps pins the negative: a
// nil-constraints append leaves constraint_ops NULL on EVERY row of the batch,
// not just the first.
func TestAppendWithoutConstraintsWritesNoConstraintOps(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_ops_plain"
	sid := opsStream(t, "plain")

	if _, err := s.Workspace(ws).Append(ctx, es.AppendParams{
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
	rows := eventOps(t, s, ws, sid)
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
	const ws = "ws_ops_no_event"
	sid := opsStream(t, "claim-only")

	for _, op := range []es.ConstraintOp{
		es.Claim("email", "only@b"),
		es.Release("email", "only@b"),
	} {
		if _, err := s.Workspace(ws).Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Constraints:     []es.ConstraintOp{op},
		}); !errors.Is(err, es.ErrConstraintWithoutEvent) {
			t.Fatalf("zero-event %+v append: got %v, want ErrConstraintWithoutEvent", op, err)
		}
	}
	if n := countPinned(t, s, ws, `SELECT count(*) FROM events`); n != 0 {
		t.Errorf("events rows = %d, want 0", n)
	}
	if n := countPinned(t, s, ws, `SELECT count(*) FROM unique_claims`); n != 0 {
		t.Errorf("unique_claims rows = %d, want 0", n)
	}
}

// TestReadCorruptConstraintOpsFailsLoudly pins the read contract: a corrupt
// blob fails the read, naming the stream and version but never the bytes.
func TestReadCorruptConstraintOpsFailsLoudly(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_ops_corrupt"
	sid := opsStream(t, "corrupt")

	appendWithOps(t, s, ws, sid, 0, es.Claim("email", "a@b"))
	if _, err := pinnedConn(t, s, ws).Exec(ctx,
		`UPDATE events SET constraint_ops = $1 WHERE workspace_id = $2 AND stream_id = $3 AND version = 1`,
		[]byte("garbage-bytes"), ws, sid.Canonical()); err != nil {
		t.Fatalf("tamper constraint_ops: %v", err)
	}

	_, err := s.Workspace(ws).ReadStream(ctx, sid, 0, 0)
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

// TestTamperedConstraintOpsDoesNotBreakChain pins the documented behavior: the
// chain attests the event, not the ops, so a decodable tampered ops blob still
// verifies. The index it feeds is trusted infrastructure, like unique_claims.
func TestTamperedConstraintOpsDoesNotBreakChain(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_ops_tamper"
	sid := opsStream(t, "tamper")

	appendWithOps(t, s, ws, sid, 0, es.Claim("email", "a@b"))
	tampered := es.EncodeConstraintOps([]es.ConstraintOp{es.Claim("email", "different@b")})
	if _, err := pinnedConn(t, s, ws).Exec(ctx,
		`UPDATE events SET constraint_ops = $1 WHERE workspace_id = $2 AND stream_id = $3 AND version = 1`,
		tampered, ws, sid.Canonical()); err != nil {
		t.Fatalf("tamper constraint_ops: %v", err)
	}

	rep, err := es.VerifyStream(ctx, s.Workspace(ws), sid)
	if err != nil || !rep.Intact() {
		t.Fatalf("VerifyStream after an ops tamper: intact=%v err=%v, want intact — ops are excluded from the chain", rep.Intact(), err)
	}
}

// TestReleaseRoundTripsAndReplaysNoop pins a Release through the column: it
// decodes from the row and reconstructs as a no-op when it matches nothing.
func TestReleaseRoundTripsAndReplaysNoop(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_ops_release"
	sid := opsStream(t, "release")

	appendWithOps(t, s, ws, sid, 0, es.Release("email", "never-claimed@b"))
	rows := eventOps(t, s, ws, sid)
	if len(rows) != 1 || rows[0].ops == nil {
		t.Fatalf("release row = %+v, want one op-bearing event", rows)
	}
	got, err := es.DecodeConstraintOps(rows[0].ops)
	if err != nil || !slices.Equal(got, []es.ConstraintOp{es.Release("email", "never-claimed@b")}) {
		t.Fatalf("decoded release = %+v (err %v), want the Release", got, err)
	}

	rep, err := s.WorkspaceStore(ws).RebuildClaims(ctx)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if rep.Ops != 1 || rep.Claims != 0 {
		t.Errorf("rebuild report = %+v, want {Ops:1 Claims:0} — a release matching nothing is a no-op", rep)
	}
}

// appendWithOps is the ops tests' shortcut: one event and the given constraint
// ops at the stream's current version.
func appendWithOps(t *testing.T, s *postgres.Store, ws string, sid es.StreamID, version uint64, ops ...es.ConstraintOp) {
	t.Helper()
	if _, err := s.Workspace(ws).Append(context.Background(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events:          eventData("ops"),
		Constraints:     ops,
	}); err != nil {
		t.Fatalf("append %+v at v%d under %s: %v", ops, version, ws, err)
	}
}

// TestRebuildClaimsFromEventLog rebuilds a damaged index from the log alone:
// three op-bearing events (claim A, claim B, release A) replay into exactly the
// one claim still held, and a verify of the rebuilt index agrees.
func TestRebuildClaimsFromEventLog(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_ops_rebuild"
	sidA := opsStream(t, "a")
	sidB := opsStream(t, "b")

	appendWithOps(t, s, ws, sidA, 0, es.Claim("email", "rebuild-a@b"))
	appendWithOps(t, s, ws, sidB, 0, es.Claim("email", "rebuild-b@b"))
	appendWithOps(t, s, ws, sidA, 1, es.Release("email", "rebuild-a@b"))

	if got := countPinned(t, s, ws, `SELECT count(*) FROM events WHERE constraint_ops IS NOT NULL`); got != 3 {
		t.Fatalf("op-bearing events = %d, want 3", got)
	}
	// Damage the live index directly, as a lost restore would: the rebuild
	// must reconstruct it from the event log, not trust what it finds.
	if _, err := pinnedConn(t, s, ws).Exec(ctx,
		`DELETE FROM unique_claims WHERE workspace_id = $1`, ws); err != nil {
		t.Fatalf("delete unique_claims: %v", err)
	}
	if n := countPinned(t, s, ws, `SELECT count(*) FROM unique_claims`); n != 0 {
		t.Fatalf("unique_claims rows before rebuild = %d, want 0", n)
	}

	wsStore := s.WorkspaceStore(ws)
	rep, err := wsStore.RebuildClaims(ctx)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if rep.Ops != 3 || rep.Claims != 1 {
		t.Errorf("rebuild report = %+v, want {Ops:3 Claims:1}", rep)
	}

	if sid, found, err := wsStore.LookupClaim(ctx, "email", "rebuild-b@b"); err != nil || !found || sid != sidB.Canonical() {
		t.Errorf("lookup of B = (%q, %v, %v), want (%q, true, nil)", sid, found, err, sidB.Canonical())
	}
	if _, found, err := wsStore.LookupClaim(ctx, "email", "rebuild-a@b"); err != nil || found {
		t.Errorf("lookup of released A = found=%v err=%v, want absent", found, err)
	}

	verify, err := wsStore.VerifyClaims(ctx)
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
	const ws = "ws_ops_no_ops"
	sid := opsStream(t, "a")

	appendWithOps(t, s, ws, sid, 0, es.Claim("email", "no-ops@b"))
	if _, err := pinnedConn(t, s, ws).Exec(ctx,
		`UPDATE events SET constraint_ops = NULL WHERE workspace_id = $1`, ws); err != nil {
		t.Fatalf("strip constraint_ops: %v", err)
	}

	rep, err := s.WorkspaceStore(ws).RebuildClaims(ctx)
	if !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Fatalf("rebuild with no op history: got %v, want ErrClaimsWithoutDecisions", err)
	}
	if rep != (es.ClaimsRebuildReport{}) {
		t.Errorf("rebuild report on a claims-without-ops state = %+v, want the zero report", rep)
	}
	if _, found, err := s.WorkspaceStore(ws).LookupClaim(ctx, "email", "no-ops@b"); err != nil || !found {
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
	const ws = "ws_ops_verify_no_ops"

	appendWithOps(t, s, ws, opsStream(t, "a"), 0, es.Claim("email", "verify-no-ops@b"))
	if _, err := pinnedConn(t, s, ws).Exec(ctx,
		`UPDATE events SET constraint_ops = NULL WHERE workspace_id = $1`, ws); err != nil {
		t.Fatalf("strip constraint_ops: %v", err)
	}

	rep, err := s.WorkspaceStore(ws).VerifyClaims(ctx)
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
	const ws = "ws_ops_verify"
	sid := opsStream(t, "a")
	bogus := opsStream(t, "bogus")

	appendWithOps(t, s, ws, sid, 0, es.Claim("email", "verify@b"))
	if _, err := pinnedConn(t, s, ws).Exec(ctx,
		`INSERT INTO unique_claims (workspace_id, scope, value_key, stream_id) VALUES ($1,$2,$3,$4)`,
		ws, "bogus.scope", []byte("bogus"), bogus.Canonical()); err != nil {
		t.Fatalf("insert bogus claim: %v", err)
	}

	rep, err := s.WorkspaceStore(ws).VerifyClaims(ctx)
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

// TestClaimWorkspacesUnionsEventsAndClaims pins the workspace discovery for
// `es-verify claims`: it is the union of the event log and the live index
// (ADR 0043 §2.2). A workspace with claim state but no events at all is the
// inconsistent store the audit must surface, not skip, so the union is
// detection — both operations then refuse it with ErrClaimsWithoutDecisions.
func TestClaimWorkspacesUnionsEventsAndClaims(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const (
		claimedWS = "ws_ops_ws_claimed"
		noOpsWS   = "ws_ops_ws_no_ops"
	)
	sid := opsStream(t, "claimed")

	// A genuinely empty database enumerates no workspaces: the zero-row
	// audit still exits 0 because there is nothing to read.
	if empty, err := s.ClaimWorkspaces(ctx); err != nil {
		t.Fatalf("claim workspaces on an empty database: %v", err)
	} else if len(empty) != 0 {
		t.Fatalf("ClaimWorkspaces on an empty database = %v, want none", empty)
	}

	if _, err := s.Workspace(claimedWS).Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          eventData("claimed"),
		Constraints:     []es.ConstraintOp{es.Claim("email", "ws-claimed@b")},
	}); err != nil {
		t.Fatalf("claim-bearing append: %v", err)
	}

	// The inconsistent workspace has a live claim and NO events at all: only
	// the unique_claims arm of the union can discover it.
	if _, err := pinnedConn(t, s, noOpsWS).Exec(ctx,
		`INSERT INTO unique_claims (workspace_id, scope, value_key, stream_id) VALUES ($1,$2,$3,$4)`,
		noOpsWS, "email", []byte("inconsistent@b"), opsStream(t, "no-ops").Canonical()); err != nil {
		t.Fatalf("insert claim with no events: %v", err)
	}
	if n := countPinned(t, s, noOpsWS,
		`SELECT count(*) FROM events WHERE workspace_id = '`+noOpsWS+`'`); n != 0 {
		t.Fatalf("inconsistent workspace events = %d, want 0", n)
	}

	got, err := s.ClaimWorkspaces(ctx)
	if err != nil {
		t.Fatalf("claim workspaces: %v", err)
	}
	for _, ws := range []string{claimedWS, noOpsWS} {
		if !slices.Contains(got, ws) {
			t.Errorf("ClaimWorkspaces = %v, want it to include %s", got, ws)
		}
	}

	wsStore := s.WorkspaceStore(claimedWS)
	rebuild, err := wsStore.RebuildClaims(ctx)
	if err != nil {
		t.Fatalf("rebuild the claimed workspace: %v", err)
	}
	if rebuild.Ops != 1 || rebuild.Claims != 1 {
		t.Errorf("claimed rebuild report = %+v, want {Ops:1 Claims:1}", rebuild)
	}
	verify, err := wsStore.VerifyClaims(ctx)
	if err != nil {
		t.Fatalf("verify the claimed workspace: %v", err)
	}
	if !verify.OK {
		t.Errorf("claimed verify = %+v, want OK", verify)
	}

	// Discovery must surface the inconsistent workspace so its refusal is
	// visible, not skipped: both operations decline to guess or delete.
	if _, err := s.WorkspaceStore(noOpsWS).RebuildClaims(ctx); !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Errorf("rebuild the inconsistent workspace: got %v, want ErrClaimsWithoutDecisions", err)
	}
	if _, err := s.WorkspaceStore(noOpsWS).VerifyClaims(ctx); !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Errorf("verify the inconsistent workspace: got %v, want ErrClaimsWithoutDecisions", err)
	}
}
