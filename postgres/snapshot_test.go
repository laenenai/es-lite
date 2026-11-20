package postgres_test

// Integration tests for the Postgres snapshot.Cache adapter (ADR 0042
// §3.2): the round trip preserves every field (Postgres keeps
// microseconds), absent is ok=false with no error, Save is
// last-writer-wins by design — no monotonic guard, a stale row costs at
// most n extra folded events — Purge empties the workspace's rows, and a
// store opened without the table (WithoutAutoMigrate) fails Load and Save
// observably instead of silently reading as empty: the runtime ignores
// these errors, so silence would degrade to permanent full folds with no
// signal.
//
// Workspace isolation has two layers and the tests keep them straight:
// the superuser pool bypasses row security entirely, so the adapter tests
// pin the explicit workspace_id predicate every statement carries, and
// TestSnapshotCacheRLSIsolatesWorkspaces is the only place claiming RLS
// enforcement — as the NOBYPASSRLS probe role the policies bind.

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/snapshot"
)

// snapStream returns a canonical stream id for the snapshot tests.
func snapStream(t *testing.T, id string) string {
	t.Helper()
	sid, err := es.NewStreamID("counter", id)
	if err != nil {
		t.Fatal(err)
	}
	return sid.Canonical()
}

// rawSnapshotCount counts the snapshots rows attributed to ws, reading the
// table directly the way rawPayload does. The pool connects as the
// superuser, who bypasses RLS unconditionally — FORCE binds only the table
// owner — so pinning app.workspace_id here would filter nothing and prove
// nothing; the policy's shape (workspace-only, no bypass arm, ENABLE +
// FORCE) is catalog-asserted by the migration tests. What this probes is
// the row ATTRIBUTION the adapter's own queries key on: the explicit
// workspace_id predicate every es-lite query pairs with RLS.
func rawSnapshotCount(t *testing.T, s *postgres.Store, ws string) int64 {
	t.Helper()
	ctx := context.Background()
	var n int64
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM snapshots WHERE workspace_id = $1`, ws,
	).Scan(&n); err != nil {
		t.Fatalf("count snapshots under %s: %v", ws, err)
	}
	return n
}

func TestSnapshotCacheRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	c := postgres.NewSnapshotCache(s, "ws_snap_rt")
	sid := snapStream(t, "main")

	fixed := time.Date(2026, 11, 20, 10, 0, 0, 123456789, time.UTC)
	want := snapshot.Snapshot{Version: 7, FoldVersion: 2, RecordedAt: fixed, State: []byte{1, 2, 3}}
	if err := c.Save(ctx, sid, want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := c.Load(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v, want ok", ok, err)
	}
	if got.Version != want.Version || got.FoldVersion != want.FoldVersion || !bytes.Equal(got.State, want.State) {
		t.Fatalf("load = (v%d fold%d %q), want (v%d fold%d %q)",
			got.Version, got.FoldVersion, got.State, want.Version, want.FoldVersion, want.State)
	}
	if !got.RecordedAt.Equal(fixed.Truncate(time.Microsecond)) {
		t.Fatalf("recorded_at = %v, want %v (Postgres keeps microseconds)", got.RecordedAt, fixed)
	}
}

func TestSnapshotCacheAbsent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	c := postgres.NewSnapshotCache(s, "ws_snap_absent")

	got, ok, err := c.Load(ctx, snapStream(t, "absent"))
	if ok || err != nil {
		t.Fatalf("absent load: ok=%v err=%v, want false/nil", ok, err)
	}
	if got.Version != 0 || got.FoldVersion != 0 || !got.RecordedAt.IsZero() || len(got.State) != 0 {
		t.Fatalf("absent load returned %+v, want the zero snapshot", got)
	}
}

// TestSnapshotCacheOverwrite pins the deliberate no-guard behaviour (ADR
// 0042 §3.2): the second Save replaces the row even at a LOWER version.
// A monotonic guard would permanently wedge a stream after a log reset,
// because the stale row could never be replaced from a lower version.
func TestSnapshotCacheOverwrite(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	c := postgres.NewSnapshotCache(s, "ws_snap_overwrite")
	sid := snapStream(t, "main")

	v7 := snapshot.Snapshot{Version: 7, FoldVersion: 2, RecordedAt: time.Now().UTC(), State: []byte("seven")}
	if err := c.Save(ctx, sid, v7); err != nil {
		t.Fatalf("save v7: %v", err)
	}
	v5 := snapshot.Snapshot{Version: 5, FoldVersion: 2, RecordedAt: time.Now().UTC(), State: []byte("five")}
	if err := c.Save(ctx, sid, v5); err != nil {
		t.Fatalf("save v5: %v", err)
	}

	got, ok, err := c.Load(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v, want ok", ok, err)
	}
	if got.Version != 5 || string(got.State) != "five" {
		t.Fatalf("load = (v%d %q), want (v5 \"five\") — last-writer-wins must let the lower version through", got.Version, got.State)
	}
}

func TestSnapshotCacheWorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	a := postgres.NewSnapshotCache(s, "ws_a")
	b := postgres.NewSnapshotCache(s, "ws_b")
	sid := snapStream(t, "main")

	if err := a.Save(ctx, sid, snapshot.Snapshot{Version: 3, FoldVersion: 1, RecordedAt: time.Now().UTC(), State: []byte("a-state")}); err != nil {
		t.Fatalf("save via a: %v", err)
	}

	// The explicit workspace_id predicate hides ws_a's row from b's read
	// even for the identical stream id (the superuser pool bypasses RLS;
	// the policy itself is proven by TestSnapshotCacheRLSIsolatesWorkspaces).
	if _, ok, err := b.Load(ctx, sid); ok || err != nil {
		t.Fatalf("b.Load: ok=%v err=%v, want absent — RLS must hide ws_a's row", ok, err)
	}

	// b's purge cannot erase what it cannot see: a's row survives.
	if err := b.Purge(ctx); err != nil {
		t.Fatalf("b.Purge: %v", err)
	}
	got, ok, err := a.Load(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("a.Load after b.Purge: ok=%v err=%v, want the row intact", ok, err)
	}
	if string(got.State) != "a-state" {
		t.Fatalf("a.Load state = %q, want \"a-state\"", got.State)
	}

	// And the boundary holds on disk: a direct count under b's workspace
	// sees none of ws_a's rows, and the row b's purge could not touch is
	// still there, still attributed to a.
	if n := rawSnapshotCount(t, s, "ws_b"); n != 0 {
		t.Fatalf("raw count under ws_b = %d, want 0", n)
	}
	if n := rawSnapshotCount(t, s, "ws_a"); n != 1 {
		t.Fatalf("raw count under ws_a = %d, want 1 — b's purge must not cross the workspace", n)
	}
}

// TestSnapshotCacheRLSIsolatesWorkspaces is the behavioural half of the
// adapter acceptance criterion (ADR 0042 §7): 0013's policy — not the
// adapter's predicates — is what isolates workspaces, and only a role it
// binds can prove it, because the test pool's login user is the
// table-owning SUPERUSER, whom PostgreSQL exempts from row security even
// under FORCE (feed_stores_test.go's probe rationale). Delete the policy
// and every other test in this file still passes; this one must not.
//
// The composition is the suite's established one: ensureRLSProbeRole
// creates the NOBYPASSRLS probe, and the test grants it snapshots locally
// and revokes on cleanup — migrations issue no grants by design (spec
// §5.7), so RLS probes always grant as superuser (dedup_test.go's
// command_dedup grant is the precedent), leaving the role exactly as the
// feed tests expect it. The adapter path runs through a drainStoreAs
// store, every pooled session SET ROLE; the raw probes reuse probePinned,
// which decides with no workspace_id filter anywhere in the statement.
func TestSnapshotCacheRLSIsolatesWorkspaces(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ensureRLSProbeRole(t, ctx, s)

	// The probe must be exactly the shape the policies bind: a superuser
	// or BYPASSRLS probe would pass every assertion below with the policy
	// dropped, proving nothing.
	var super, bypass bool
	if err := s.Pool().QueryRow(ctx,
		`SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = $1`, rlsProbeRole,
	).Scan(&super, &bypass); err != nil {
		t.Fatalf("read %s's role flags: %v", rlsProbeRole, err)
	}
	if super || bypass {
		t.Fatalf("%s: rolsuper=%v rolbypassrls=%v, want false/false — the probe must be a role the policies bind", rlsProbeRole, super, bypass)
	}

	// The probe's reach: schema USAGE is already ensureRLSProbeRole's;
	// the snapshots DML is this test's own grant (the runbook's app line),
	// so refusals below are decided BY the policy, not by a permission
	// wall.
	if _, err := s.Pool().Exec(ctx,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON snapshots TO `+rlsProbeRole); err != nil {
		t.Fatalf("grant %s on snapshots: %v", rlsProbeRole, err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool().Exec(context.Background(), `REVOKE SELECT, INSERT, UPDATE, DELETE ON snapshots FROM `+rlsProbeRole); err != nil {
			t.Errorf("revoke snapshots from %s: %v", rlsProbeRole, err)
		}
	})

	const (
		wsA = "ws_rls_a"
		wsB = "ws_rls_b"
	)

	// The adapter path, as the probe (drainStoreAs: every pooled session
	// SET ROLE, migrations off — the database is already migrated and the
	// probe has no DDL privileges). A missing grant or a policy that
	// refuses the legitimate write fails HERE, loudly.
	probe := drainStoreAs(t, rlsProbeRole)
	a := postgres.NewSnapshotCache(probe, wsA)
	b := postgres.NewSnapshotCache(probe, wsB)
	sid := snapStream(t, "rls")

	if err := a.Save(ctx, sid, snapshot.Snapshot{Version: 3, FoldVersion: 1, RecordedAt: time.Now().UTC(), State: []byte("a-state")}); err != nil {
		t.Fatalf("save via %s as the probe: %v", wsA, err)
	}

	// B's read is absent and B's purge erases nothing: the policy admits
	// neither the row nor its deletion across the workspace line, and a's
	// row survives, still a's.
	if _, ok, err := b.Load(ctx, sid); ok || err != nil {
		t.Fatalf("b.Load: ok=%v err=%v, want absent", ok, err)
	}
	if err := b.Purge(ctx); err != nil {
		t.Fatalf("b.Purge: %v", err)
	}
	if got, ok, err := a.Load(ctx, sid); err != nil || !ok || string(got.State) != "a-state" {
		t.Fatalf("a.Load after b.Purge: ok=%v err=%v state=%q, want the row intact", ok, err, got.State)
	}

	// The policies alone decide, with no workspace_id filter anywhere in
	// the statement (the dedup probe's move): pinned to B the probe counts
	// 0 — drop 0013's policy and this counts 1 — while pinned to A it
	// counts 1, so the probe is proven able to read somewhere. A GUC-name
	// mismatch between the policy and runWorkspace resolves NULL here and
	// refuses everything, failing the pair in either direction.
	for _, tc := range []struct {
		ws   string
		want int64
	}{{wsB, 0}, {wsA, 1}} {
		var n int64
		err := probePinned(t, ctx, s, tc.ws, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT count(*) FROM snapshots`).Scan(&n)
		})
		if err != nil {
			t.Fatalf("raw count pinned to %s: %v", tc.ws, err)
		}
		if n != tc.want {
			t.Fatalf("raw count pinned to %s = %d, want %d — the policy alone must show the pinned workspace only its own rows", tc.ws, n, tc.want)
		}
	}

	// WITH CHECK, behaviourally: pinned to B, writing A's workspace_id is
	// refused at the policy with the row-level-security violation — no
	// adapter Save can land a row outside the GUC's workspace.
	err := probePinned(t, ctx, s, wsB, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO snapshots (workspace_id, stream_id, version, fold_version, recorded_at, state)
			 VALUES ($1, $2, 1, 1, now(), 'x')`,
			wsA, "counter:foreign",
		)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "row-level security") {
		t.Fatalf("cross-workspace insert pinned to %s: got %v, want a row-level security policy violation", wsB, err)
	}
}

func TestSnapshotCachePurge(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	c := postgres.NewSnapshotCache(s, "ws_snap_purge")

	for _, sid := range []string{snapStream(t, "one"), snapStream(t, "two")} {
		snap := snapshot.Snapshot{Version: 1, FoldVersion: 1, RecordedAt: time.Now().UTC(), State: []byte(sid)}
		if err := c.Save(ctx, sid, snap); err != nil {
			t.Fatalf("save %s: %v", sid, err)
		}
	}
	if err := c.Purge(ctx); err != nil {
		t.Fatalf("purge: %v", err)
	}
	for _, sid := range []string{snapStream(t, "one"), snapStream(t, "two")} {
		if _, ok, err := c.Load(ctx, sid); ok || err != nil {
			t.Fatalf("load %s after purge: ok=%v err=%v, want absent", sid, ok, err)
		}
	}
}

// TestSnapshotCacheMissingTableErrors pins the WithoutAutoMigrate
// contract (ADR 0042 §7): a store opened before its migrations fails
// Load and Save observably. The runtime discards these errors, so a
// silent ok=false/nil would degrade the stream to permanent full folds
// with no signal at all.
func TestSnapshotCacheMissingTableErrors(t *testing.T) {
	ctx := context.Background()
	s := openScratchStore(t, postgres.WithoutAutoMigrate())
	c := postgres.NewSnapshotCache(s, "ws_snap_missing")
	sid := snapStream(t, "main")

	if _, ok, err := c.Load(ctx, sid); err == nil {
		t.Fatalf("load on a missing table: ok=%v err=nil, want an error, not a silent miss", ok)
	}
	if err := c.Save(ctx, sid, snapshot.Snapshot{Version: 1, FoldVersion: 1, RecordedAt: time.Now().UTC(), State: []byte("x")}); err == nil {
		t.Fatal("save on a missing table returned nil error, want an error")
	}
}
