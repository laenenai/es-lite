package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/projection"
)

// The least-privilege privilege contract (F5): the runbook's grant block
// (docs/deploy/README.md, "Grants: the privilege contract") is enforced
// here, not merely documented. Four probe roles hold EXACTLY their
// principal's runbook grants — no more — and every facet of the flow runs
// through a Store whose pooled sessions SET ROLE to the matching probe
// (drainStoreAs, this suite's established stand-in for a deployment
// connecting as its own login role: current_user is the probe, so RLS
// binds and every privilege check runs against exactly the granted set).
//
// One role per principal, not a single union role: the runbook promises
// each principal's line is sufficient on its own, and a union would let
// one principal's over-grant paper over another's missing grant — exactly
// the drift this test exists to catch. No role gets DELETE or TRUNCATE on
// events; the log is append-only in the privilege model too.
//
// The grant statements in grantLeastPrivilege are byte-consistent with the
// runbook's SQL blocks: the runbook is the contract, this is its
// enforcement. Diff both when either changes.

// The least-privilege probe roles, one per runbook principal.
const (
	lpAppender = "es_lp_appender"
	lpFeed     = "es_lp_feed"
	lpRelay    = "es_lp_relay"
	lpAnchor   = "es_lp_anchor"
)

// The flow's identifiers: one workspace, one stream, one consumed type,
// one subscription key.
const (
	lpWS         = "ws_leastpriv"
	lpType       = "leastpriv.v1.E"
	lpProjection = "leastpriv.probe"
	lpInstance   = "lp-0"
	lpOwner      = "leastpriv-owner"
)

// leastPrivRoles is the probe set, in runbook order.
var leastPrivRoles = []string{lpAppender, lpFeed, lpRelay, lpAnchor}

// ensureLeastPrivRoles creates the four probes idempotently — same shape
// as the drain/anchor probes: no login, no superuser, no BYPASSRLS — and
// drops them again on cleanup, so the test is re-runnable from a cold
// database and leaves the shared review database clean. DROP OWNED BY
// revokes the privileges granted to the role; DROP ROLE removes the role
// and its es_relay membership. Cleanup runs LIFO: after the drainStoreAs
// pools have closed, before the superuser store does.
func ensureLeastPrivRoles(t *testing.T, ctx context.Context, s *postgres.Store) {
	t.Helper()
	for _, role := range leastPrivRoles {
		if _, err := s.Pool().Exec(ctx, `DO $$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+role+`') THEN
					CREATE ROLE `+role+` NOLOGIN NOSUPERUSER NOBYPASSRLS;
				END IF;
			END $$`); err != nil {
			t.Fatalf("create %s: %v", role, err)
		}
	}
	t.Cleanup(func() {
		for _, role := range leastPrivRoles {
			if _, err := s.Pool().Exec(ctx, `DROP OWNED BY `+role); err != nil {
				t.Errorf("drop owned by %s: %v", role, err)
				continue
			}
			if _, err := s.Pool().Exec(ctx, `DROP ROLE `+role); err != nil {
				t.Errorf("drop role %s: %v", role, err)
			}
		}
	})
}

// grantLeastPrivilege issues the runbook's grant block, one principal at a
// time. The statements must stay byte-consistent with
// docs/deploy/README.md's "Grants" section.
func grantLeastPrivilege(t *testing.T, ctx context.Context, s *postgres.Store) {
	t.Helper()
	for _, g := range []struct{ role, grants string }{
		// Appender: draw positions, chain events, attest batch order,
		// apply uniqueness constraints (Release is the only DELETE,
		// on the claims index).
		{lpAppender, `
			GRANT USAGE ON SCHEMA public TO ` + lpAppender + `;
			GRANT USAGE ON SEQUENCE events_global_position_seq TO ` + lpAppender + `;
			GRANT SELECT, INSERT ON events, unique_claims TO ` + lpAppender + `;
			GRANT DELETE ON unique_claims TO ` + lpAppender + `;
			GRANT INSERT ON log_order TO ` + lpAppender},
		// Feed process: the horizon fence (pg_sequences needs USAGE on
		// the sequence, never SELECT) + the feed's own state tables.
		{lpFeed, `
			GRANT USAGE ON SCHEMA public TO ` + lpFeed + `;
			GRANT USAGE ON SEQUENCE events_global_position_seq TO ` + lpFeed + `;
			GRANT SELECT, INSERT, UPDATE, DELETE ON feed_checkpoints, feed_leases, feed_dead_letters TO ` + lpFeed + `;
			GRANT SELECT ON events TO ` + lpFeed},
		// Relay: claim unpublished rows and stamp published_at, the one
		// column it may touch; es_relay is the role-gated bypass.
		{lpRelay, `
			GRANT USAGE ON SCHEMA public TO ` + lpRelay + `;
			GRANT SELECT, UPDATE (published_at) ON events TO ` + lpRelay + `;
			GRANT es_relay TO ` + lpRelay + `;
			GRANT SELECT ON unique_claims, log_order TO ` + lpRelay},
		// Anchor job: heads + log_order under the bypass, anchors rows
		// (RETURNING created_at and the job's ListAnchors verify pass
		// need SELECT too), schema_migrations for ChainedSince, the
		// sequence USAGE the anchor fence's pg_sequences read needs
		// (without it the bound folds to 0 over a non-empty log and
		// the tripwire refuses the anchor with ErrAnchorSequenceMasked),
		// es_relay for the bypass.
		{lpAnchor, `
			GRANT USAGE ON SCHEMA public TO ` + lpAnchor + `;
			GRANT SELECT ON events, log_order, schema_migrations TO ` + lpAnchor + `;
			GRANT SELECT, INSERT ON anchors TO ` + lpAnchor + `;
			GRANT USAGE ON SEQUENCE events_global_position_seq TO ` + lpAnchor + `;
			GRANT es_relay TO ` + lpAnchor},
	} {
		if _, err := s.Pool().Exec(ctx, g.grants); err != nil {
			t.Fatalf("grant %s's runbook privileges: %v", g.role, err)
		}
	}
}

// lpSink hands each delivered envelope to the test on a channel.
type lpSink struct{ out chan<- es.Envelope }

func (s lpSink) Send(_ context.Context, env es.Envelope) error {
	s.out <- env
	return nil
}

// TestLeastPrivilegeEndToEnd runs the deployment's whole flow — append →
// fenced page read → feed subscribe/ack → Drain → Anchor/CheckAnchor —
// with every step executing under a probe role holding exactly its
// principal's runbook grants. A facet failing here means the contract is
// wrong in one direction or the other: the code needs a grant the runbook
// does not document (a runbook bug — fix the docs), or a code path
// reaches past its principal's grants (an over-privilege to disclose).
func TestLeastPrivilegeEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := openStore(t) // PG-gated; truncates the tables, ensures es_relay

	ensureLeastPrivRoles(t, ctx, s)
	grantLeastPrivilege(t, ctx, s)

	sid, err := es.NewStreamID("leastpriv", "main")
	if err != nil {
		t.Fatal(err)
	}

	// Facet 1 — appender: append with a Claim, append with a Release.
	appender := drainStoreAs(t, lpAppender)
	aws := appender.WorkspaceStore(lpWS)
	res1, err := aws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: lpType, SchemaVersion: 1, Payload: []byte("one")}},
		Constraints:     []es.ConstraintOp{es.Claim("email", "lp@example.test")},
	})
	if err != nil {
		t.Fatalf("append under the appender grants: %v", err)
	}
	if res1.ToVersion != 1 || res1.Envelopes[0].GlobalPosition == 0 {
		t.Fatalf("first append result = %+v, want v1 at a real position", res1)
	}
	if _, found, err := aws.LookupClaim(ctx, "email", "lp@example.test"); err != nil || !found {
		t.Fatalf("claim lookup under the appender grants: found=%v err=%v, want found", found, err)
	}
	res2, err := aws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Events:          []es.EventData{{TypeURL: lpType, SchemaVersion: 1, Payload: []byte("two")}},
		Constraints:     []es.ConstraintOp{es.Release("email", "lp@example.test")},
	})
	if err != nil {
		t.Fatalf("append with Release under the appender grants: %v", err)
	}
	if res2.ToVersion != 2 {
		t.Fatalf("second append result = %+v, want v2", res2)
	}
	if _, found, err := aws.LookupClaim(ctx, "email", "lp@example.test"); err != nil || found {
		t.Fatalf("claim after Release under the appender grants: found=%v err=%v, want absent", found, err)
	}
	pos1, pos2 := res1.Envelopes[0].GlobalPosition, res2.Envelopes[0].GlobalPosition

	// Facet 2 — feed process: the fenced page read, then subscribe/ack.
	feedStore := drainStoreAs(t, lpFeed)
	fws := feedStore.WorkspaceStore(lpWS)
	page, _, err := fws.ReadAllBounded(ctx, 0, 100, 0, []string{lpType})
	if err != nil {
		t.Fatalf("fenced page under the feed grants: %v", err)
	}
	if got := positions(page); len(got) != 2 || got[0] != pos1 || got[1] != pos2 {
		t.Fatalf("page under the feed grants = %v, want [%d %d]", got, pos1, pos2)
	}

	srv, err := feed.NewServer(feed.Config{
		Checkpoints: postgres.NewCheckpointStore(feedStore),
		Leases:      postgres.NewLeaseStore(feedStore),
		Logs:        postgres.NewLogs(feedStore),
		// DeadLetters stays nil: this flow has no poison path.
		Poll:     5 * time.Millisecond,
		LeaseTTL: 5 * time.Second,
		Batch:    10,
	})
	if err != nil {
		t.Fatalf("new feed server under the feed grants: %v", err)
	}
	req := feed.Request{
		Scope:      lpWS,
		Projection: lpProjection,
		Instance:   lpInstance,
		Consumer:   "leastpriv-test",
		Owner:      lpOwner,
		Definition: projection.Definition{Name: lpProjection, Consumes: []string{lpType}},
	}
	delivered := make(chan es.Envelope, 8)
	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(subCtx, req, lpSink{delivered}) }()
	var last es.Envelope
	for i, want := range []uint64{pos1, pos2} {
		select {
		case env := <-delivered:
			if env.GlobalPosition != want {
				cancel()
				t.Fatalf("delivered position %d, want %d (event %d)", env.GlobalPosition, want, i)
			}
			last = env
		case <-time.After(10 * time.Second):
			cancel()
			t.Fatalf("feed under the feed grants delivered %d of 2 events past the deadline", i)
		}
	}
	// The fenced ack — an ordinary consumer acknowledging its page.
	if err := srv.Checkpoint(ctx, feed.Ack{
		Scope:      lpWS,
		Projection: lpProjection,
		Instance:   lpInstance,
		Owner:      lpOwner,
		Position:   last.GlobalPosition,
		Definition: req.Definition.Hash(),
	}); err != nil {
		cancel()
		t.Fatalf("ack under the feed grants: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe returned %v, want context.Canceled after the test's cancel", err)
	}
	cp, found, err := postgres.NewCheckpointStore(feedStore).Checkpoint(ctx, lpWS, lpProjection, lpInstance)
	if err != nil || !found || cp.Position != pos2 {
		t.Fatalf("checkpoint under the feed grants = %+v found=%v err=%v, want position %d", cp, found, err, pos2)
	}

	// Facet 3 — relay: drain the backlog across workspaces.
	relay := drainStoreAs(t, lpRelay)
	var relayed []es.Envelope
	n, err := relay.Drain(ctx, 10, func(_ context.Context, batch []es.Envelope) error {
		relayed = append(relayed, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("drain under the relay grants: %v", err)
	}
	if n != 2 || len(relayed) != 2 {
		t.Fatalf("drain under the relay grants claimed=%d delivered=%d, want 2/2", n, len(relayed))
	}
	if got := unpublishedCount(t, ctx, s); got != 0 {
		t.Fatalf("drain under the relay grants left %d unpublished rows, want 0", got)
	}

	// Facet 4 — anchor job: record over the whole settled log and verify.
	anchorJob := drainStoreAs(t, lpAnchor)
	since, err := anchorJob.ChainedSince(ctx)
	if err != nil || since.IsZero() {
		t.Fatalf("ChainedSince under the anchor grants = %v (err %v), want migration 0002's applied_at", since, err)
	}
	a, err := anchorJob.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("anchor under the anchor grants: %v", err)
	}
	if a.StreamCount != 1 || a.OrderCount != 2 {
		t.Fatalf("anchor under the anchor grants = %+v, want 1 stream over 2 order rows", a)
	}
	if a.UptoGlobalPosition < pos2 {
		t.Fatalf("anchor bound %d is below the appended head %d", a.UptoGlobalPosition, pos2)
	}
	if err := anchorJob.CheckAnchor(ctx, a); err != nil {
		t.Fatalf("check under the anchor grants: %v", err)
	}
	// The verify pass's read of the stored anchors (es-verify anchor
	// without a --new flag lists and re-checks every row).
	list, err := anchorJob.ListAnchors(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("list anchors under the anchor grants = %d rows (err %v), want the 1 recorded anchor", len(list), err)
	}
}

// lpAnchorMasked is a fifth probe: the anchor job's runbook grants minus
// the one under test — no USAGE on the position sequence, so the
// pg_sequences view masks last_value to NULL for its sessions.
const lpAnchorMasked = "es_lp_anchor_masked"

// TestAnchorRefusesMaskedSequence pins the masked-sequence tripwire
// (backlog 2026-11-07_anchor-masked-sequence-tripwire, final review
// I2). Under a role without USAGE on the sequence, the fence's
// pg_sequences read folds the whole-log bound to 0 — and over a
// non-empty log that made Anchor silently record an internally
// incoherent anchor: a root over zero heads (heads(0) is empty) with a
// FULL order root (orderRows(0) reads the whole log). The tripwire is
// the contradiction check — a covered row starting above the resolved
// bound cannot exist — so Anchor must fail loudly with the typed error
// naming the sequence grant, and record nothing. The genuine empty-log
// shape must stay silent: a fresh database has no rows above any bound,
// and its anchor is the true genesis anchor (verified forever by the
// empty-bound rule).
func TestAnchorRefusesMaskedSequence(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	if _, err := s.Pool().Exec(ctx, `DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+lpAnchorMasked+`') THEN
				CREATE ROLE `+lpAnchorMasked+` NOLOGIN NOSUPERUSER NOBYPASSRLS;
			END IF;
		END $$`); err != nil {
		t.Fatalf("create %s: %v", lpAnchorMasked, err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool().Exec(ctx, `DROP OWNED BY `+lpAnchorMasked); err != nil {
			t.Errorf("drop owned by %s: %v", lpAnchorMasked, err)
			return
		}
		if _, err := s.Pool().Exec(ctx, `DROP ROLE `+lpAnchorMasked); err != nil {
			t.Errorf("drop role %s: %v", lpAnchorMasked, err)
		}
	})
	if _, err := s.Pool().Exec(ctx, `
		GRANT USAGE ON SCHEMA public TO `+lpAnchorMasked+`;
		GRANT SELECT ON events, log_order, schema_migrations TO `+lpAnchorMasked+`;
		GRANT SELECT, INSERT ON anchors TO `+lpAnchorMasked+`;
		GRANT es_relay TO `+lpAnchorMasked); err != nil {
		t.Fatalf("grant the anchor job's runbook grants minus the sequence USAGE: %v", err)
	}

	maskedJob := drainStoreAs(t, lpAnchorMasked)

	// No false positive on a genuinely empty log: the probe's bound
	// resolves to 0 there because the log IS empty (no events, no
	// attested rows above any bound), and the anchor it records is the
	// true genesis anchor — bound 0, the empty contract.
	genesis, err := maskedJob.Anchor(ctx, 0)
	if err != nil {
		t.Fatalf("anchor over a genuinely empty log under the masked grant: %v — the tripwire must not fire on a true empty bound", err)
	}
	if genesis.UptoGlobalPosition != 0 || genesis.StreamCount != 0 || genesis.OrderCount != 0 {
		t.Fatalf("genesis anchor under the masked grant = %+v, want the empty contract at bound 0", genesis)
	}

	// The log grows — appended by the superuser, so the masking is the
	// probe's alone.
	sid, err := es.NewStreamID("leastpriv", "masked")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WorkspaceStore(lpWS).Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: lpType, SchemaVersion: 1, Payload: []byte("masked")}},
	}); err != nil {
		t.Fatalf("append behind the masked probe's back: %v", err)
	}

	// The masked shape: the probe's next Anchor resolves the bound to 0
	// over the now-non-empty log. The tripwire must refuse it with the
	// typed error — never record the incoherent root over zero heads.
	_, err = maskedJob.Anchor(ctx, 0)
	if !errors.Is(err, postgres.ErrAnchorSequenceMasked) {
		t.Fatalf("anchor under the masked sequence grant: err=%v, want ErrAnchorSequenceMasked — never a silently incoherent anchor", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "events_global_position_seq") {
		t.Fatalf("the tripwire error must name the sequence: %q", msg)
	}

	// Nothing was recorded behind the error's back: the anchors table
	// still holds exactly the genesis anchor.
	list, err := s.ListAnchors(ctx)
	if err != nil || len(list) != 1 || list[0].ID != genesis.ID {
		t.Fatalf("anchors after the refused call = %+v err=%v, want only the genesis anchor %s", list, err, genesis.ID)
	}
}

// TestOpenProbeDoesNotMaskConnectionErrors pins the PG14 floor probe's
// error classification (F33): a server that cannot be reached is a probe
// failure ("version probe"), never a version verdict — a dead DSN must not
// be misreported as "this server is older than PostgreSQL 14". The
// floor branch itself (pg_current_xact_id undefined, SQLSTATE 42883) can
// only fire on a server older than PostgreSQL 13 — the function exists
// from 13, so the 14+ floor is support policy and the probe its tripwire;
// the probe's happy path runs on every Open in this suite.
func TestOpenProbeDoesNotMaskConnectionErrors(t *testing.T) {
	// Port 1 on loopback: nothing listens there, so the pool's first
	// probe statement fails at connect time, bounded by the DSN's
	// connect_timeout.
	_, err := postgres.Open(context.Background(),
		"postgres://postgres@127.0.0.1:1/eslite?sslmode=disable&connect_timeout=2")
	if err == nil {
		t.Fatal("open against a dead endpoint succeeded, want the probe's connection failure")
	}
	if strings.Contains(err.Error(), "PostgreSQL 14") {
		t.Fatalf("a connection failure is misreported as a version floor: %v", err)
	}
	if !strings.Contains(err.Error(), "version probe") {
		t.Fatalf("probe failure = %v, want the version-probe wrap", err)
	}
}

// TestAppendTxReadStreamTxRejectPool pins the DBTX guard (F33):
// AppendTx and ReadStreamTx must refuse a *pgxpool.Pool before any
// statement runs. Observed before the guard, under this suite's superuser
// DSN: both calls returned nil and the pool ran every statement in its own
// autocommit transaction — the append silently committed (its xid draw and
// its insert landed in different transactions, the P1 guarantee gone, a
// mid-append failure half-committed) and the read returned rows with the
// is_local workspace setting already dead. On a non-superuser session the
// same pool append instead fails deep inside on the events RLS WITH
// CHECK — a confusing 42501 naming nothing about the pool — and the read
// silently returns nothing at all.
func TestAppendTxReadStreamTxRejectPool(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_pool_guard")
	sid, err := es.NewStreamID("leastpriv", "pool-guard")
	if err != nil {
		t.Fatal(err)
	}

	_, err = ws.AppendTx(ctx, s.Pool(), es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: lpType, SchemaVersion: 1, Payload: []byte("pool")}},
	})
	const wantAppend = "postgres: AppendTx requires an active transaction, not a pool"
	if err == nil || !strings.Contains(err.Error(), wantAppend) {
		t.Fatalf("AppendTx with a pool: err=%v, want %q", err, wantAppend)
	}

	_, err = ws.ReadStreamTx(ctx, s.Pool(), sid, 0, 0)
	const wantRead = "postgres: ReadStreamTx requires an active transaction, not a pool"
	if err == nil || !strings.Contains(err.Error(), wantRead) {
		t.Fatalf("ReadStreamTx with a pool: err=%v, want %q", err, wantRead)
	}

	// The refused calls left nothing behind: the stream is still unwritten.
	if v, err := ws.CurrentStreamVersion(ctx, sid); err != nil || v != 0 {
		t.Fatalf("stream version after the refused calls = %d (err %v), want 0", v, err)
	}
}
