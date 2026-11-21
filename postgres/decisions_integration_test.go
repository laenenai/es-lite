package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// decisionRow is one append_decisions row, as the raw queries below read it.
// commandID is COALESCEd to "" so the scan needs no nullable pointer; a live
// append always writes the UUID string (the zero UUID when unset).
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

// appendWithOps is the decision tests' shortcut: one event and the given
// constraint ops at the stream's current version.
func appendWithOps(t *testing.T, s *postgres.Store, ws string, sid es.StreamID, version uint64, ops ...es.ConstraintOp) {
	t.Helper()
	if _, err := s.Workspace(ws).Append(context.Background(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events:          eventData("decision"),
		Constraints:     ops,
	}); err != nil {
		t.Fatalf("append %+v at v%d under %s: %v", ops, version, ws, err)
	}
}

// TestRebuildClaimsFromDecisions rebuilds a damaged index from the log alone:
// three recorded decisions (claim A, claim B, release A) replay into exactly
// the one claim still held, and a verify of the rebuilt index agrees.
func TestRebuildClaimsFromDecisions(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_dec_rebuild"
	sidA := decisionStream(t, "a")
	sidB := decisionStream(t, "b")

	appendWithOps(t, s, ws, sidA, 0, es.Claim("email", "rebuild-a@b"))
	appendWithOps(t, s, ws, sidB, 0, es.Claim("email", "rebuild-b@b"))
	appendWithOps(t, s, ws, sidA, 1, es.Release("email", "rebuild-a@b"))

	if got := len(decisionRows(t, s, ws)); got != 3 {
		t.Fatalf("append_decisions rows = %d, want 3", got)
	}
	// Damage the live index directly, as a lost restore would: the rebuild
	// must reconstruct it from append_decisions, not trust what it finds.
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
	if rep.Decisions != 3 || rep.Claims != 1 {
		t.Errorf("rebuild report = %+v, want {Decisions:3 Claims:1}", rep)
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

// TestRebuildRefusesClaimsWithoutDecisions pins the mandatory guard: live
// claims with no decision history are an inconsistent store — a partial
// restore or rows written outside the append path — and a rebuild must refuse
// rather than delete an index it cannot reconstruct.
func TestRebuildRefusesClaimsWithoutDecisions(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_dec_no_decisions"
	sid := decisionStream(t, "a")

	appendWithOps(t, s, ws, sid, 0, es.Claim("email", "no-decisions@b"))
	if _, err := pinnedConn(t, s, ws).Exec(ctx,
		`DELETE FROM append_decisions WHERE workspace_id = $1`, ws); err != nil {
		t.Fatalf("delete append_decisions: %v", err)
	}

	rep, err := s.WorkspaceStore(ws).RebuildClaims(ctx)
	if !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Fatalf("rebuild with no decision history: got %v, want ErrClaimsWithoutDecisions", err)
	}
	if rep != (es.ClaimsRebuildReport{}) {
		t.Errorf("rebuild report on a claims-without-decisions state = %+v, want the zero report", rep)
	}
	if _, found, err := s.WorkspaceStore(ws).LookupClaim(ctx, "email", "no-decisions@b"); err != nil || !found {
		t.Fatalf("claim after refused rebuild = found=%v err=%v, want still present", found, err)
	}
}

// TestVerifyClaimsReportsDivergence proves the oracle notices an index row
// the log does not license and names it.
func TestVerifyClaimsReportsDivergence(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_dec_verify"
	sid := decisionStream(t, "a")
	bogus := decisionStream(t, "bogus")

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

// TestAppendWithoutConstraintsWritesNoDecision pins the negative: the record
// is written only for an append that carries at least one constraint op, so a
// plain event append leaves the decision log untouched.
func TestAppendWithoutConstraintsWritesNoDecision(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const ws = "ws_dec_no_constraints"

	if _, err := s.Workspace(ws).Append(ctx, es.AppendParams{
		StreamID:        decisionStream(t, "plain"),
		ExpectedVersion: 0,
		Events:          eventData("plain"),
	}); err != nil {
		t.Fatalf("append without constraints: %v", err)
	}
	if got := len(decisionRows(t, s, ws)); got != 0 {
		t.Fatalf("append_decisions rows after a zero-constraint append = %d, want 0", got)
	}
}

// TestClaimWorkspacesIncludesClaimOnly pins the workspace discovery for
// `es-verify claims`: it must not come from ListStreams. A claim-only append
// writes no event, so a workspace whose entire history is claim-only is
// invisible to the stream list — and a rebuild/verify loop derived from it
// would certify a database it never read. ClaimWorkspaces unions the decision
// log with the live index for exactly that case, so an inconsistent
// claims-only workspace is still discovered and its refusal is visible.
func TestClaimWorkspacesIncludesClaimOnly(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const (
		claimOnlyWS    = "ws_dec_ws_claim_only"
		inconsistentWS = "ws_dec_ws_inconsistent"
	)
	sid := decisionStream(t, "claim-only")

	// A genuinely empty database enumerates no workspaces: the zero-row
	// audit still exits 0 because there is nothing to read.
	if empty, err := s.ClaimWorkspaces(ctx); err != nil {
		t.Fatalf("claim workspaces on an empty database: %v", err)
	} else if len(empty) != 0 {
		t.Fatalf("ClaimWorkspaces on an empty database = %v, want none", empty)
	}

	if _, err := s.Workspace(claimOnlyWS).Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Constraints:     []es.ConstraintOp{es.Claim("email", "ws-claim-only@b")},
	}); err != nil {
		t.Fatalf("claim-only append: %v", err)
	}
	if n := countPinned(t, s, claimOnlyWS, `SELECT count(*) FROM events`); n != 0 {
		t.Fatalf("events in %s = %d, want 0 — the claim-only append writes none", claimOnlyWS, n)
	}

	// The union's other arm: an inconsistent workspace holds an index row
	// and no decision history, so only unique_claims can reveal it.
	if _, err := pinnedConn(t, s, inconsistentWS).Exec(ctx,
		`INSERT INTO unique_claims (workspace_id, scope, value_key, stream_id) VALUES ($1,$2,$3,$4)`,
		inconsistentWS, "email", []byte("inconsistent@b"), sid.Canonical()); err != nil {
		t.Fatalf("insert claim-only index row: %v", err)
	}

	got, err := s.ClaimWorkspaces(ctx)
	if err != nil {
		t.Fatalf("claim workspaces: %v", err)
	}
	for _, ws := range []string{claimOnlyWS, inconsistentWS} {
		if !slices.Contains(got, ws) {
			t.Errorf("ClaimWorkspaces = %v, want it to include %s", got, ws)
		}
	}

	wsStore := s.WorkspaceStore(claimOnlyWS)
	rebuild, err := wsStore.RebuildClaims(ctx)
	if err != nil {
		t.Fatalf("rebuild the claim-only workspace: %v", err)
	}
	if rebuild.Decisions != 1 || rebuild.Claims != 1 {
		t.Errorf("claim-only rebuild report = %+v, want {Decisions:1 Claims:1}", rebuild)
	}
	verify, err := wsStore.VerifyClaims(ctx)
	if err != nil {
		t.Fatalf("verify the claim-only workspace: %v", err)
	}
	if !verify.OK {
		t.Errorf("claim-only verify = %+v, want OK", verify)
	}

	// Discovery must surface the inconsistent workspace so its refusal is
	// visible, not skipped: the rebuild declines to guess or delete.
	if _, err := s.WorkspaceStore(inconsistentWS).RebuildClaims(ctx); !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Errorf("rebuild the inconsistent workspace: got %v, want ErrClaimsWithoutDecisions", err)
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
	const ws = "ws_dec_verify_no_decisions"

	appendWithOps(t, s, ws, decisionStream(t, "a"), 0, es.Claim("email", "verify-no-decisions@b"))
	if _, err := pinnedConn(t, s, ws).Exec(ctx,
		`DELETE FROM append_decisions WHERE workspace_id = $1`, ws); err != nil {
		t.Fatalf("delete append_decisions: %v", err)
	}

	rep, err := s.WorkspaceStore(ws).VerifyClaims(ctx)
	if !errors.Is(err, es.ErrClaimsWithoutDecisions) {
		t.Fatalf("verify with no decision history: got %v, want ErrClaimsWithoutDecisions", err)
	}
	if rep != (es.ClaimsVerifyReport{}) {
		t.Errorf("verify report on a claims-without-decisions state = %+v, want the zero report", rep)
	}
}

// TestClaimReplayOrderSurvivesConcurrentRelease pins the advisory lock in
// applyOps (ADR 0043 §3.1). A no-op Release takes no row lock, so without the
// lock two claim-only appends at the same version can commit their effects
// and their decision rows in opposite orders: the live index holds the claim,
// the replay releases it, and a healthy database reports DIVERGED. The lock
// serializes each (workspace, scope, value) pair, so every round either
// release-then-claim or claim-then-release replays exactly.
func TestClaimReplayOrderSurvivesConcurrentRelease(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const (
		ws     = "ws_dec_replay_order"
		rounds = 50
	)
	for i := 0; i < rounds; i++ {
		sid := decisionStream(t, fmt.Sprintf("round-%d", i))
		value := fmt.Sprintf("race-%d@b", i)
		var (
			wg   sync.WaitGroup
			errs [2]error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, errs[0] = s.Workspace(ws).Append(ctx, es.AppendParams{
				StreamID:        sid,
				ExpectedVersion: 0,
				Constraints:     []es.ConstraintOp{es.Release("email", value)},
			})
		}()
		go func() {
			defer wg.Done()
			_, errs[1] = s.Workspace(ws).Append(ctx, es.AppendParams{
				StreamID:        sid,
				ExpectedVersion: 0,
				Constraints:     []es.ConstraintOp{es.Claim("email", value)},
			})
		}()
		wg.Wait()
		for j, err := range errs {
			if err != nil {
				t.Fatalf("round %d append %d: %v", i, j, err)
			}
		}
	}

	rep, err := s.WorkspaceStore(ws).VerifyClaims(ctx)
	if err != nil {
		t.Fatalf("verify after %d concurrent claim/release rounds: %v", rounds, err)
	}
	if !rep.OK {
		t.Fatalf("verify after %d concurrent claim/release rounds = %+v, want OK — the decision id order must match the apply order", rounds, rep)
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
