package postgres_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/postgres"
)

// Review Focus 6 (spec §5.7): since migration 0005 the events and
// unique_claims bypass arms are role-gated — app.bypass_rls only lifts
// workspace isolation for sessions whose user is a member of es_relay —
// and Drain must fail fast with the typed error instead of silently
// reading nothing (or, pre-0005, silently reading everything: the R11
// hole).
//
// The fix round extends the same gate to the anchor path: Anchor,
// CheckAnchor, ListStreams and ListAnchors all run under bypassTx, the
// same bypass Drain uses, so a non-member session would not merely read
// nothing — Anchor would record a root over zero heads as a real anchor.
// TestAnchorPathFailsFastWithoutRelayRole pins that, with its own probe
// role so the drain test's member-half GRANT (left in place) cannot leak
// into its outsider half, whatever the run order.
//
// The test pool's login user is the container's superuser, and pg_has_role
// is trivially true for superusers, so neither half can run as the login
// user. Each half runs through a Store whose pooled sessions SET ROLE to a
// non-superuser probe — Task 13's pattern, since RLS policies only bind
// non-superusers — which is also how a production relay would connect as
// its own principal in the first place.

const (
	relayRole   = "es_relay"
	drainProbe  = "es_drain_probe"
	anchorProbe = "es_anchor_probe"
)

// ensureDrainRole idempotently creates the non-superuser, non-BYPASSRLS
// probe Drain runs as. Role creation belongs in test setup, never in
// migrations (migrator privileges vary, spec §5.7).
func ensureDrainRole(t *testing.T, ctx context.Context, s *postgres.Store) {
	t.Helper()
	if _, err := s.Pool().Exec(ctx, `DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+drainProbe+`') THEN
				CREATE ROLE `+drainProbe+` NOLOGIN NOSUPERUSER NOBYPASSRLS;
			END IF;
		END $$`); err != nil {
		t.Fatalf("create %s: %v", drainProbe, err)
	}
}

// ensureRelayRole creates es_relay if missing. CI's test:pg setup — and
// the production runbook — create it before anything migrates; the
// idempotent ensure here keeps the test green against a bare container
// too. The suite's shared openStoreWithOptions calls it as well, so
// every superuser drain or anchor test runs with the role present,
// whatever the order.
func ensureRelayRole(t testing.TB, ctx context.Context, s *postgres.Store) {
	t.Helper()
	if _, err := s.Pool().Exec(ctx, `DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+relayRole+`') THEN
				CREATE ROLE `+relayRole+` NOLOGIN NOSUPERUSER NOBYPASSRLS;
			END IF;
		END $$`); err != nil {
		t.Fatalf("create %s: %v", relayRole, err)
	}
}

// ensureAnchorProbe idempotently creates the anchor path's probe — same
// shape as the drain probe: no login, no superuser, no BYPASSRLS.
func ensureAnchorProbe(t *testing.T, ctx context.Context, s *postgres.Store) {
	t.Helper()
	if _, err := s.Pool().Exec(ctx, `DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+anchorProbe+`') THEN
				CREATE ROLE `+anchorProbe+` NOLOGIN NOSUPERUSER NOBYPASSRLS;
			END IF;
		END $$`); err != nil {
		t.Fatalf("create %s: %v", anchorProbe, err)
	}
}

// drainStoreAs returns a Store whose every pooled session runs as role
// (AfterConnect SET ROLE). Migrations are off: the database is already
// migrated, and the probe has no DDL privileges — nor should it.
func drainStoreAs(t *testing.T, role string) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, `SET ROLE `+role); err != nil {
			return err
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	s, err := postgres.OpenWithPool(context.Background(), pool, postgres.WithoutAutoMigrate())
	if err != nil {
		t.Fatalf("open with pool: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// appendUnpublishedEvent appends one event in ws — published_at stays
// NULL until a relay drains it.
func appendUnpublishedEvent(t *testing.T, ctx context.Context, s *postgres.Store, ws string) {
	t.Helper()
	sid, err := es.NewStreamID("drain", "probe-"+ws)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Workspace(ws).Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{{
			TypeURL:       "test.v1.E",
			SchemaVersion: 1,
			Payload:       []byte("x"),
		}},
	}); err != nil {
		t.Fatalf("append in %s: %v", ws, err)
	}
}

// unpublishedCount counts rows no drain has claimed, as the superuser
// (who sees every workspace).
func unpublishedCount(t *testing.T, ctx context.Context, s *postgres.Store) int {
	t.Helper()
	var n int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM events WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatalf("count unpublished: %v", err)
	}
	return n
}

// TestDrainFailsFastWithoutRelayRole is Review Focus 6's both halves: a
// session that is not an es_relay member gets the typed error and reads
// nothing — never a silent empty success — and the same principal,
// granted membership, drains across workspaces as before.
func TestDrainFailsFastWithoutRelayRole(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	ensureRelayRole(t, ctx, s)
	ensureDrainRole(t, ctx, s)
	// The member half below grants es_relay to the probe and leaves it,
	// as a production deployment would. On a long-lived shared test
	// database that leftover grant must not leak into the next run's
	// outsider half (CI's throwaway containers never see it), so reset it
	// here: the outsider half always starts as a non-member.
	if _, err := s.Pool().Exec(ctx, `REVOKE `+relayRole+` FROM `+drainProbe); err != nil {
		t.Fatalf("revoke %s from %s: %v", relayRole, drainProbe, err)
	}
	// Drain's privileges as the probe: the FOR UPDATE SKIP LOCKED claim
	// and the published_at flip need UPDATE, the claim needs SELECT, and
	// naming any of them needs schema USAGE. Nothing more — the bypass
	// must be a policy matter, not a privilege one.
	if _, err := s.Pool().Exec(ctx,
		`GRANT USAGE ON SCHEMA public TO `+drainProbe+`;
		 GRANT SELECT, UPDATE ON events TO `+drainProbe); err != nil {
		t.Fatalf("grant drain privileges: %v", err)
	}

	// Unpublished rows in two workspaces: what a silent empty read would
	// leave behind, and what a working Drain must claim.
	for _, ws := range []string{"ws_drain_a", "ws_drain_b"} {
		appendUnpublishedEvent(t, ctx, s, ws)
	}

	// Not an es_relay member: Drain must fail fast with the typed error
	// before the bypass is set or a single row is claimed.
	outsider := drainStoreAs(t, drainProbe)
	n, err := outsider.Drain(ctx, 10, func(context.Context, []es.Envelope) error { return nil })
	if !errors.Is(err, postgres.ErrRelayRoleMissing) {
		t.Fatalf("drain as a non-member: err=%v, want ErrRelayRoleMissing — never a silent empty success", err)
	}
	if n != 0 {
		t.Fatalf("drain as a non-member claimed %d rows, want 0", n)
	}
	if msg := err.Error(); !strings.Contains(msg, "docs/deploy/README.md") {
		t.Fatalf("the error must point at the runbook, got %q", msg)
	}
	// Zero rows read: both seeded events are still unpublished.
	if got := unpublishedCount(t, ctx, s); got != 2 {
		t.Fatalf("non-member drain left %d unpublished rows, want 2 — it must not read or publish anything", got)
	}
	// The verdict is verified once per Store and stands: a second call
	// fails the same way without re-probing.
	n2, err2 := outsider.Drain(ctx, 10, func(context.Context, []es.Envelope) error { return nil })
	if !errors.Is(err2, postgres.ErrRelayRoleMissing) || n2 != 0 {
		t.Fatalf("second drain as a non-member: n=%d err=%v, want 0/ErrRelayRoleMissing", n2, err2)
	}

	// The same principal, granted es_relay: Drain works as before — the
	// bypass reads across workspaces, publishes and marks. pg_has_role
	// resolves membership live; the fresh Store re-verifies it.
	if _, err := s.Pool().Exec(ctx, `GRANT `+relayRole+` TO `+drainProbe); err != nil {
		t.Fatalf("grant %s to %s: %v", relayRole, drainProbe, err)
	}
	member := drainStoreAs(t, drainProbe)
	var got []es.Envelope
	n, err = member.Drain(ctx, 10, func(_ context.Context, batch []es.Envelope) error {
		got = append(got, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("drain as an es_relay member: %v", err)
	}
	if n != 2 || len(got) != 2 {
		t.Fatalf("member drain claimed=%d delivered=%d, want 2/2", n, len(got))
	}
	workspaces := map[string]bool{}
	for _, e := range got {
		workspaces[e.Workspace] = true
	}
	if len(workspaces) != 2 {
		t.Fatalf("member drain read %d workspaces, want 2 (the bypass reads across workspaces)", len(workspaces))
	}
	if got := unpublishedCount(t, ctx, s); got != 0 {
		t.Fatalf("member drain left %d unpublished rows, want 0", got)
	}
	// Backlog exhausted: the next drain claims nothing.
	n, err = member.Drain(ctx, 10, func(context.Context, []es.Envelope) error { return nil })
	if err != nil || n != 0 {
		t.Fatalf("drain after backlog: n=%d err=%v, want 0/nil", n, err)
	}
}

// TestRelayRoleTransientErrorIsNotLatched is F4's regression test: a
// transient probe failure must not poison the Store. The verdict cache
// used to latch whatever the first probe returned — including the
// context.Canceled of a caller's dead ctx — for the Store's lifetime, so
// one blip failed every later Drain and anchor call even though the role
// existed. A cancelled first Drain must fail (its ctx is dead), but as a
// transient: the next Drain re-probes, decoupled from that dead ctx, and
// succeeds.
func TestRelayRoleTransientErrorIsNotLatched(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	// The login user is a superuser, hence a trivially es_relay member:
	// on a live context the probe can only conclude member=true, so the
	// cancelled ctx stages the transient itself — never a definitive
	// non-membership.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	n, err := s.Drain(cancelled, 10, func(context.Context, []es.Envelope) error { return nil })
	if err == nil {
		t.Fatalf("drain with a cancelled context: n=%d err=nil, want an error", n)
	}
	if errors.Is(err, postgres.ErrRelayRoleMissing) {
		t.Fatalf("drain with a cancelled context: err=%v, want a transient failure — the superuser is a member, so this must not be a definitive non-membership", err)
	}

	// The transient must not be latched: a live-context Drain succeeds,
	// claiming the 0 unpublished rows the freshly truncated store holds.
	n, err = s.Drain(ctx, 10, func(context.Context, []es.Envelope) error { return nil })
	if err != nil {
		t.Fatalf("drain after a transient probe failure: err=%v, want nil — one transient failure must not poison the Store", err)
	}
	if n != 0 {
		t.Fatalf("drain after a transient probe failure claimed %d rows, want 0", n)
	}
}

// TestAnchorPathFailsFastWithoutRelayRole pins the fix round's other half
// of Review Focus 6: the anchor path reads events under the same bypass
// Drain uses (bypassTx), so post-0005 a non-member session would not
// merely read nothing — Anchor would compute a root over zero heads and
// record it as a real anchor (the anchors policy is GUC-only, so the
// write succeeds), and CheckAnchor would report a spurious mismatch. Every
// entry point through bypassTx must fail fast with the same typed error
// and mutate nothing; the same principal granted es_relay anchors and
// verifies across workspaces as before.
func TestAnchorPathFailsFastWithoutRelayRole(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	ensureRelayRole(t, ctx, s)
	ensureAnchorProbe(t, ctx, s)
	// Reset the probe's membership: the member half below grants es_relay
	// and leaves it in place, and on a long-lived shared test database
	// (unlike CI's throwaway containers) that leftover must not leak into
	// the next run's outsider half — it must always start as a non-member.
	if _, err := s.Pool().Exec(ctx, `REVOKE `+relayRole+` FROM `+anchorProbe); err != nil {
		t.Fatalf("revoke %s from %s: %v", relayRole, anchorProbe, err)
	}
	// The anchor path's privileges as the probe: reading events (the log
	// head, stream heads, the stream list), reading/writing anchors,
	// reading log_order (the order root over the per-batch rows, Task 7)
	// and — since the anchor path fences on the horizon (Task 4) —
	// reading the position sequence the fence's first statement reads
	// (Task 16 will relax this to USAGE via pg_sequences). Nothing more
	// — the bypass must stay a policy matter, not a privilege one.
	if _, err := s.Pool().Exec(ctx,
		`GRANT USAGE ON SCHEMA public TO `+anchorProbe+`;
		 GRANT SELECT ON events TO `+anchorProbe+`;
		 GRANT SELECT ON log_order TO `+anchorProbe+`;
		 GRANT SELECT, INSERT ON anchors TO `+anchorProbe+`;
		 GRANT SELECT ON SEQUENCE events_global_position_seq TO `+anchorProbe); err != nil {
		t.Fatalf("grant anchor-path privileges: %v", err)
	}

	// One event in each of two workspaces — what a zero read would hide,
	// and what a working anchor must span.
	for _, ws := range []string{"ws_anchor_a", "ws_anchor_b"} {
		appendUnpublishedEvent(t, ctx, s, ws)
	}
	// A real anchor over both workspaces, as the superuser: what the
	// outsider must refuse to verify, and what the member must reproduce.
	good, err := s.Anchor(ctx, 0)
	if err != nil || good.StreamCount != 2 {
		t.Fatalf("superuser anchor: %+v err=%v, want 2 streams", good, err)
	}

	// Not an es_relay member: every anchor-path entry point fails fast
	// with the typed error — before reading events or writing anchors.
	outsider := drainStoreAs(t, anchorProbe)
	_, err = outsider.Anchor(ctx, 0)
	if !errors.Is(err, postgres.ErrRelayRoleMissing) {
		t.Fatalf("anchor as a non-member: err=%v, want ErrRelayRoleMissing — never a silently wrong anchor", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "docs/deploy/README.md") {
		t.Fatalf("the error must point at the runbook, got %q", msg)
	}
	if err := outsider.CheckAnchor(ctx, good); !errors.Is(err, postgres.ErrRelayRoleMissing) {
		t.Fatalf("check as a non-member: err=%v, want ErrRelayRoleMissing — never a spurious mismatch", err)
	}
	if _, err := outsider.ListStreams(ctx); !errors.Is(err, postgres.ErrRelayRoleMissing) {
		t.Fatalf("list streams as a non-member: err=%v, want ErrRelayRoleMissing", err)
	}
	if _, err := outsider.ListAnchors(ctx); !errors.Is(err, postgres.ErrRelayRoleMissing) {
		t.Fatalf("list anchors as a non-member: err=%v, want ErrRelayRoleMissing", err)
	}
	// The log-order reads the anchor path grew in Task 7 are gated the
	// same way: raw order rows and the deep verify both refuse, never a
	// vacuous root over invisible rows or a verdict over them.
	if _, err := outsider.OrderRows(ctx, 0); !errors.Is(err, postgres.ErrRelayRoleMissing) {
		t.Fatalf("order rows as a non-member: err=%v, want ErrRelayRoleMissing", err)
	}
	if _, err := outsider.VerifyOrder(ctx, 0); !errors.Is(err, postgres.ErrRelayRoleMissing) {
		t.Fatalf("verify order as a non-member: err=%v, want ErrRelayRoleMissing", err)
	}
	// Mutates nothing: only the superuser's anchor exists — a non-member
	// session must not have recorded a root over zero heads behind the
	// typed error's back.
	var n int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM anchors`).Scan(&n); err != nil {
		t.Fatalf("count anchors: %v", err)
	}
	if n != 1 {
		t.Fatalf("non-member anchor path left %d anchor rows, want 1 (nothing written)", n)
	}
	// The verdict is checked once per Store and stands: a second call
	// fails the same way without re-probing.
	if _, err := outsider.ListStreams(ctx); !errors.Is(err, postgres.ErrRelayRoleMissing) {
		t.Fatalf("second call as a non-member: err=%v, want ErrRelayRoleMissing", err)
	}

	// The same principal, granted es_relay: the path works as before —
	// the bypass spans workspaces, and the fresh Store re-verifies.
	if _, err := s.Pool().Exec(ctx, `GRANT `+relayRole+` TO `+anchorProbe); err != nil {
		t.Fatalf("grant %s to %s: %v", relayRole, anchorProbe, err)
	}
	member := drainStoreAs(t, anchorProbe)
	a, err := member.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("anchor as an es_relay member: %v", err)
	}
	if a.StreamCount != 2 || a.UptoGlobalPosition != good.UptoGlobalPosition {
		t.Fatalf("member anchor = %+v, want 2 streams at the superuser's head %d", a, good.UptoGlobalPosition)
	}
	if err := member.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("member check of its own anchor: %v", err)
	}
	if err := member.CheckAnchor(ctx, good); err != nil {
		t.Fatalf("member check of the seeded anchor: %v", err)
	}
	refs, err := member.ListStreams(ctx)
	if err != nil {
		t.Fatalf("member list streams: %v", err)
	}
	workspaces := map[string]bool{}
	for _, ref := range refs {
		workspaces[ref.Workspace] = true
	}
	if len(refs) != 2 || len(workspaces) != 2 {
		t.Fatalf("member list streams = %d refs over %d workspaces, want 2/2 (the bypass spans workspaces)",
			len(refs), len(workspaces))
	}
	list, err := member.ListAnchors(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("member list anchors = %d err=%v, want 2 (the seeded anchor + the member's own)", len(list), err)
	}
}

// TestRelayRoleGatesTheBypassArm pins migration 0005's policy shape
// through pg_policies — not file greps, which would false-positive on
// 0004's comment (the Task 13 review ruling): both events and
// unique_claims keep their bypass arm, but the arm now requires es_relay
// membership. (The feed tables' NO-bypass-arm assertion lives in
// feed_stores_test.go.)
func TestRelayRoleGatesTheBypassArm(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	for _, table := range []string{"events", "unique_claims"} {
		rows, err := s.Pool().Query(ctx, `
			SELECT policyname, qual, with_check FROM pg_policies
			WHERE schemaname = 'public' AND tablename = $1`, table)
		if err != nil {
			t.Fatalf("read policies for %s: %v", table, err)
		}
		var policies []struct{ name, qual, withCheck string }
		for rows.Next() {
			var name, qual, withCheck string
			if err := rows.Scan(&name, &qual, &withCheck); err != nil {
				rows.Close()
				t.Fatalf("scan policy for %s: %v", table, err)
			}
			policies = append(policies, struct{ name, qual, withCheck string }{name, qual, withCheck})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("policies rows for %s: %v", table, err)
		}
		if len(policies) != 1 {
			t.Fatalf("%s: %d policies, want exactly 1", table, len(policies))
		}
		p := policies[0]
		for _, def := range []string{p.qual, p.withCheck} {
			for _, want := range []string{
				"app.workspace_id", // the workspace arm survives
				"app.bypass_rls",   // the bypass arm survives...
				"pg_has_role",      // ...but is now role-gated...
				"es_relay",         // ...on es_relay membership
			} {
				if !strings.Contains(def, want) {
					t.Fatalf("%s: policy %q expression %q lacks %q — migration 0005's role gate is missing",
						table, p.name, def, want)
				}
			}
		}
	}
}
