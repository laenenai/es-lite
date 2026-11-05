package postgres_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/feedtest"
	"github.com/laenenai/es-lite/postgres"
)

// The Postgres feed adapters' tests. The port contracts — the checkpoint,
// lease, log, dead-letter and end-to-end behaviors both backends must
// honor — now live in the shared conformance suite (feedtest, spec §5.6),
// wired below with MultiWorkspace set; what stays here is Postgres-
// specific:
//
//   - the workspace-pinned Logs pin (Open hands back the *Workspace),
//   - the deterministic lease-expiry takeover (the row is pushed into
//     the past with SQL — no timing), and the DB-clock TTL window,
//   - the DB-clock half of the deliberate CreatedAt divergence (the
//     suite asserts presence/reasonableness only),
//   - the RLS policy mechanics: probe-role GUC probes, the events
//     policy binding, WITH CHECK, and the pg_policies shape.
//
// Everything here skips without PG_DSN, like the rest of the Postgres
// suite.

// feedDBClock reads the database's own clock, so timestamp assertions
// compare DB-stamped values against DB time — immune to any skew
// between the test process's clock and the database's.
func feedDBClock(t testing.TB, ctx context.Context, s *postgres.Store) time.Time {
	t.Helper()
	var now time.Time
	if err := s.Pool().QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatalf("read db clock: %v", err)
	}
	return now
}

// expireFeedLease pushes one lease row's expiry into the past with raw
// SQL, the deterministic stand-in for waiting out a real TTL: no sleeps,
// no flaky boundaries. The pool connects as the table-owning superuser,
// so the direct update crosses the RLS the adapters' pinned reads still
// apply (their keys name this test's row).
func expireFeedLease(t *testing.T, ctx context.Context, s *postgres.Store, scope, projection, instance string) {
	t.Helper()
	if _, err := s.Pool().Exec(ctx,
		`UPDATE feed_leases SET expires_at = clock_timestamp() - interval '1 second'
		 WHERE workspace_id = $1 AND projection = $2 AND instance = $3`,
		scope, projection, instance,
	); err != nil {
		t.Fatalf("expire lease %s/%s/%s: %v", scope, projection, instance, err)
	}
}

// feedAppendEvent appends one event to sid in workspace scope at the
// given expected version and returns the committed envelope.
func feedAppendEvent(t testing.TB, ctx context.Context, s *postgres.Store, scope string, sid es.StreamID, version uint64, typeURL string, payload []byte) es.Envelope {
	t.Helper()
	res, err := s.WorkspaceStore(scope).Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events:          []es.EventData{{TypeURL: typeURL, SchemaVersion: 1, Payload: payload}},
	})
	if err != nil {
		t.Fatalf("append %s at v%d in %s: %v", typeURL, version+1, scope, err)
	}
	return res.Envelopes[0]
}

// ---------------------------------------------------------------------------
// The shared conformance suite (spec §5.6)
// ---------------------------------------------------------------------------

// newPostgresFeedStore wires Postgres into the shared feed conformance
// suite (feedtest): every port over one store — freshly truncated, capped
// at 64 payload bytes so the poison contract is exercisable — plus the
// seeding hooks. Postgres isolates workspaces by row-level security, so
// the fixture reports MultiWorkspace: the suite's cross-workspace section
// runs against the real policies.
func newPostgresFeedStore(ctx context.Context, t testing.TB) (feedtest.Store, func()) {
	s := openStoreWithOptions(t, postgres.WithMaxPayloadBytes(64))
	return feedtest.Store{
		Logs:            postgres.NewLogs(s),
		Checkpoints:     postgres.NewCheckpointStore(s),
		Leases:          postgres.NewLeaseStore(s),
		DeadLetters:     postgres.NewDeadLetterStore(s),
		MaxPayloadBytes: 64,
		Append: func(ctx context.Context, t testing.TB, scope string, sid es.StreamID, version uint64, typeURL string, payload []byte) es.Envelope {
			return feedAppendEvent(t, ctx, s, scope, sid, version, typeURL, payload)
		},
		PlantOverCap: func(ctx context.Context, t testing.TB, scope string, sid es.StreamID, version uint64, typeURL string, payload []byte) uint64 {
			return insertPoisonRow(t, ctx, s, scope, sid, version, typeURL, payload)
		},
		ReadDeadLetter: func(ctx context.Context, t testing.TB, scope, projection, instance string, position uint64) (feedtest.DeadLetterRow, bool) {
			var row feedtest.DeadLetterRow
			err := s.Pool().QueryRow(ctx,
				`SELECT type_url, size_bytes, reason, actor, created_at FROM feed_dead_letters
				 WHERE workspace_id = $1 AND projection = $2 AND instance = $3 AND position = $4`,
				scope, projection, instance, int64(position),
			).Scan(&row.TypeURL, &row.SizeBytes, &row.Reason, &row.Actor, &row.CreatedAt)
			if errors.Is(err, pgx.ErrNoRows) {
				return feedtest.DeadLetterRow{}, false
			}
			if err != nil {
				t.Fatalf("postgres: conformance store: read dead letter at %d: %v", position, err)
			}
			return row, true
		},
		CountDeadLetters: func(ctx context.Context, t testing.TB, scope string) int {
			var n int
			if err := s.Pool().QueryRow(ctx,
				`SELECT COUNT(*) FROM feed_dead_letters WHERE workspace_id = $1`, scope,
			).Scan(&n); err != nil {
				t.Fatalf("postgres: conformance store: count dead letters: %v", err)
			}
			return n
		},
	}, func() { s.Close() }
}

// TestPostgresFeedConformance runs the shared feed port conformance suite
// against the Postgres adapters — the same suite sqlite runs — plus, via
// the MultiWorkspace capability, the cross-workspace isolation section.
func TestPostgresFeedConformance(t *testing.T) {
	if os.Getenv("PG_DSN") == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	feedtest.RunAll(t, feedtest.Fixture{
		NewStore:       newPostgresFeedStore,
		MultiWorkspace: true,
	})
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

// TestLogsOpenReturnsTheWorkspaceLog pins feed.Logs on Postgres: Open
// hands back the workspace-pinned *Workspace for the scope — the es.Store
// handle whose fenced ReadAllBounded pages and settled Head implement
// feed.Log — never a log that can see another workspace's events.
func TestLogsOpenReturnsTheWorkspaceLog(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	logs := postgres.NewLogs(s)

	const scopeA, scopeB = "ws_logs_a", "ws_logs_b"
	lg, err := logs.Open(ctx, scopeA)
	if err != nil {
		t.Fatalf("open %s: %v", scopeA, err)
	}
	// The concrete workspace handle: Open must pin the scope, not return
	// some cross-workspace view.
	ws, ok := lg.(*postgres.Workspace)
	if !ok {
		t.Fatalf("Open returned %T, want *postgres.Workspace", lg)
	}
	if ws == nil {
		t.Fatal("Open returned a nil workspace")
	}

	own := feedAppendEvent(t, ctx, s, scopeA, feedStream(t, "logs-a"), 0, "feedtest.v1.E", []byte("own"))
	foreign := feedAppendEvent(t, ctx, s, scopeB, feedStream(t, "logs-b"), 0, "feedtest.v1.E", []byte("foreign"))

	head, err := lg.Head(ctx)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head != own.GlobalPosition {
		t.Fatalf("head under %s = %d, want %d (its own event, not %d from %s)",
			scopeA, head, own.GlobalPosition, foreign.GlobalPosition, scopeB)
	}

	// The other scope opens its own log and sees only its own head.
	lgB, err := logs.Open(ctx, scopeB)
	if err != nil {
		t.Fatalf("open %s: %v", scopeB, err)
	}
	headB, err := lgB.Head(ctx)
	if err != nil {
		t.Fatalf("head %s: %v", scopeB, err)
	}
	if headB != foreign.GlobalPosition {
		t.Fatalf("head under %s = %d, want %d", scopeB, headB, foreign.GlobalPosition)
	}
}

// ---------------------------------------------------------------------------
// LeaseStore: the Postgres-specific pins
// ---------------------------------------------------------------------------

// TestLeaseExpiryTakeoverAndHolderUnexpired pins the expiry half of the
// lease contract without a single sleep: the row's expires_at is pushed
// into the past with raw SQL (the deterministic stand-in for waiting out
// a TTL), after which Holder must report the key free and another owner
// must take the lease over — the same `<=` takeover arm the shared suite
// pins through a real short TTL. The unexpired side (a live row IS a
// holder and DOES fence a second owner) is the suite's assertions.
func TestLeaseExpiryTakeoverAndHolderUnexpired(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	leases := postgres.NewLeaseStore(s)
	const scope, projection, instance = "ws", "p", "live"

	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire owner-a = %v err=%v", ok, err)
	}

	// Expire the row deterministically.
	expireFeedLease(t, ctx, s, scope, projection, instance)

	// An expired row is not a holder, even though it is still in the
	// table: Acquire treats it as free, so Holder must too.
	if _, held, err := leases.Holder(ctx, scope, projection, instance); err != nil || held {
		t.Fatalf("holder after expiry = held=%v err=%v, want free", held, err)
	}

	// Another owner takes the expired lease over.
	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-b", "consumer-b", time.Minute); err != nil || !ok {
		t.Fatalf("acquire owner-b after expiry = %v err=%v, want takeover", ok, err)
	}
	lease, held, err := leases.Holder(ctx, scope, projection, instance)
	if err != nil || !held {
		t.Fatalf("holder after takeover = found=%v err=%v, want the new owner", held, err)
	}
	if lease.Owner != "owner-b" || lease.Consumer != "consumer-b" {
		t.Fatalf("holder after takeover = %+v, want owner-b/consumer-b", lease)
	}
}

// TestLeaseExpiryIsDBClockBound pins the Postgres half of the lease clock
// contract: expiry is computed inside SQL from the database's own clock
// (clock_timestamp() + ttl), never the caller's, so feed replicas whose
// application clocks run skewed never extend a lease into extra fence
// life — and an owner's renewal extends past the original TTL in DB time.
// The shared suite asserts only a plausible window around the test
// clock; this keeps the DB-clock source exact.
func TestLeaseExpiryIsDBClockBound(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	leases := postgres.NewLeaseStore(s)
	const scope, projection, instance = "ws", "p", "live"

	dbBefore := feedDBClock(t, ctx, s)
	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire = %v err=%v", ok, err)
	}
	dbAfter := feedDBClock(t, ctx, s)
	lease, held, err := leases.Holder(ctx, scope, projection, instance)
	if err != nil || !held {
		t.Fatalf("holder = held=%v err=%v", held, err)
	}
	if lease.ExpiresAt.Before(dbBefore) || lease.ExpiresAt.After(dbAfter.Add(time.Minute)) {
		t.Fatalf("holder ExpiresAt = %v, want the DB clock plus the 1m TTL between %v and %v",
			lease.ExpiresAt, dbBefore, dbAfter.Add(time.Minute))
	}

	// The renewal extends only for the owner, and the extension too is
	// DB-clock arithmetic: expires_at lands exactly at the DB clock at
	// the renew plus the 2h TTL.
	if ok, err := leases.Renew(ctx, scope, projection, instance, "owner-b", time.Hour); err != nil {
		t.Fatalf("renew owner-b: %v", err)
	} else if ok {
		t.Fatal("non-owner renewed the lease")
	}
	dbBefore = feedDBClock(t, ctx, s)
	if ok, err := leases.Renew(ctx, scope, projection, instance, "owner-a", 2*time.Hour); err != nil || !ok {
		t.Fatalf("renew owner-a = %v err=%v", ok, err)
	}
	dbAfter = feedDBClock(t, ctx, s)
	lease, held, err = leases.Holder(ctx, scope, projection, instance)
	if err != nil || !held {
		t.Fatalf("holder after renew = held=%v err=%v", held, err)
	}
	if lease.ExpiresAt.Before(dbBefore.Add(2*time.Hour)) || lease.ExpiresAt.After(dbAfter.Add(2*time.Hour)) {
		t.Fatalf("renewed ExpiresAt = %v, want the DB clock plus the 2h TTL between %v and %v",
			lease.ExpiresAt, dbBefore.Add(2*time.Hour), dbAfter.Add(2*time.Hour))
	}
}

// TestCheckpointUpdatedAtIsDBClock pins the Postgres clock contract for
// checkpoint rows: updated_at is the database's own clock at the write
// (clock_timestamp() in SQL), never the writer's — a skewed application
// clock can never place the stamp outside the DB-clock window. The
// shared suite asserts only presence/reasonableness; this keeps the
// clock source exact.
func TestCheckpointUpdatedAtIsDBClock(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	cp := postgres.NewCheckpointStore(s)

	dbBefore := feedDBClock(t, ctx, s)
	if err := cp.Save(ctx, "ws_cp_clock", "p", "live", "", 7); err != nil {
		t.Fatalf("save: %v", err)
	}
	dbAfter := feedDBClock(t, ctx, s)
	checkpoint, found, err := cp.Checkpoint(ctx, "ws_cp_clock", "p", "live")
	if err != nil || !found {
		t.Fatalf("checkpoint = found=%v err=%v, want true/nil", found, err)
	}
	if checkpoint.UpdatedAt.Before(dbBefore) || checkpoint.UpdatedAt.After(dbAfter) {
		t.Fatalf("checkpoint updated_at = %v, want the DB clock between %v and %v (not app time)",
			checkpoint.UpdatedAt, dbBefore, dbAfter)
	}
}

// ---------------------------------------------------------------------------
// DeadLetterStore: the Postgres-specific pin
// ---------------------------------------------------------------------------

// TestDeadLetterCreatedAtIsDBClock pins the Postgres half of the
// deliberate CreatedAt divergence (controller ruling): Record stamps
// clock_timestamp() in SQL and ignores the caller's dl.CreatedAt, so
// every replica's audit trail shares the database's monotone clock. The
// shared suite asserts only presence/reasonableness because the SQLite
// store honors the caller's time instead; each backend keeps its own
// half exact (TestDeadLetterCreatedAtIsCallerSupplied is the other).
func TestDeadLetterCreatedAtIsDBClock(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	dls := postgres.NewDeadLetterStore(s)

	dbBefore := feedDBClock(t, ctx, s)
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: "ws_dl_clock", Projection: "p", Instance: "live",
		Position: 42, Reason: "db-stamped", Actor: "oncall",
		// Deliberately ancient: the DB clock wins, so this must be ignored.
		CreatedAt: time.Unix(1, 0).UTC(),
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	dbAfter := feedDBClock(t, ctx, s)

	var createdAt time.Time
	if err := s.Pool().QueryRow(ctx,
		`SELECT created_at FROM feed_dead_letters WHERE workspace_id = $1 AND position = 42`,
		"ws_dl_clock",
	).Scan(&createdAt); err != nil {
		t.Fatalf("read dead letter: %v", err)
	}
	if createdAt.Before(dbBefore) || createdAt.After(dbAfter) {
		t.Fatalf("created_at = %v, want the DB clock between %v and %v (not the caller's dl.CreatedAt)",
			createdAt, dbBefore, dbAfter)
	}
}

// ---------------------------------------------------------------------------
// RLS policy mechanics (Review Focus 5)
// ---------------------------------------------------------------------------

// The raw-GUC probes below run as a dedicated non-superuser role. Why:
// the test pool connects as the table-owning SUPERUSER, and PostgreSQL
// exempts superusers from row security entirely — even with FORCE ROW
// LEVEL SECURITY, and with no `row_security = force` rescue (PG 17 made
// that GUC a plain boolean). The landed events tests never notice
// because every Workspace query also filters `workspace_id = $1`
// explicitly; to exercise the POLICIES themselves, the probe must run
// as a role they bind. Creating roles in test setup — not in migrations
// — is the pattern spec §5.7 prescribes for CI (Task 15 owns
// migration-side roles).
const rlsProbeRole = "es_rls_probe"

func ensureRLSProbeRole(t *testing.T, ctx context.Context, s *postgres.Store) {
	t.Helper()
	if _, err := s.Pool().Exec(ctx, `DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+rlsProbeRole+`') THEN
				CREATE ROLE `+rlsProbeRole+` NOSUPERUSER NOBYPASSRLS;
			END IF;
		END $$`); err != nil {
		t.Fatalf("create %s: %v", rlsProbeRole, err)
	}
	// The probe reads every feed table and exercises the policies' WITH
	// CHECK on insert; events is included to prove the dead-letter
	// enrichment lookup is policy-bound too.
	if _, err := s.Pool().Exec(ctx,
		`GRANT USAGE ON SCHEMA public TO `+rlsProbeRole+`;
		 GRANT SELECT, INSERT, UPDATE, DELETE ON feed_checkpoints, feed_leases, feed_dead_letters TO `+rlsProbeRole+`;
		 GRANT SELECT ON events TO `+rlsProbeRole); err != nil {
		t.Fatalf("grant %s: %v", rlsProbeRole, err)
	}
}

// probePinned runs fn inside a transaction that is both role-switched
// (SET LOCAL ROLE es_rls_probe — a role the policies bind) and
// workspace-pinned (app.workspace_id = ws, is_local). Nothing outlives
// the transaction. fn's error is returned unwrapped so expected RLS
// violations can be asserted by the caller.
func probePinned(t *testing.T, ctx context.Context, s *postgres.Store, ws string, fn func(tx pgx.Tx) error) error {
	t.Helper()
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin probe: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+rlsProbeRole); err != nil {
		t.Fatalf("set probe role: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.workspace_id', $1, true)`, ws); err != nil {
		t.Fatalf("pin probe to %s: %v", ws, err)
	}
	return fn(tx)
}

// TestFeedStoresRefuseCrossWorkspaceAccess is Review Focus 5's POLICY
// half. The behavioral isolation through the adapters — B's checkpoint
// writes never clobber A's, the same lease key is free in another
// workspace, the dead-letter enrichment never names a foreign event — is
// pinned by the shared conformance suite (the fixture reports
// MultiWorkspace). What only this test can prove is the policies
// themselves:
//
//   - behavior: as a role the policies bind, with no workspace_id
//     filter anywhere (the policy alone decides), rows written under
//     workspace A are invisible under B's GUC and vice versa — on all
//     three tables, in both directions — the dead-letter enrichment
//     lookup over events is policy-bound, and a cross-workspace insert
//     is refused at the policy's WITH CHECK;
//   - schema: pg_policies for the three tables keys on app.workspace_id
//     with NO app.bypass_rls reference, and both RLS flags are set.
func TestFeedStoresRefuseCrossWorkspaceAccess(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	cps := postgres.NewCheckpointStore(s)
	leases := postgres.NewLeaseStore(s)
	dls := postgres.NewDeadLetterStore(s)
	ensureRLSProbeRole(t, ctx, s)

	const (
		wsA, wsB   = "ws_feed_rls_a", "ws_feed_rls_b"
		projection = "p"
		instance   = "live"
	)

	// Rows written under A through the adapters, then rows with the same
	// keys under B: if any feed table keyed its rows without
	// workspace_id, B's writes would clobber A's. (The suite pins the
	// adapter-level read-back; these writes seed the probes below.)
	if err := cps.Save(ctx, wsA, projection, instance, "", 10); err != nil {
		t.Fatalf("save under %s: %v", wsA, err)
	}
	if ok, err := leases.Acquire(ctx, wsA, projection, instance, "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire under %s = %v err=%v, want true/nil", wsA, ok, err)
	}
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: wsA, Projection: projection, Instance: instance,
		Position: 42, Reason: "rls probe", Actor: "probe",
	}); err != nil {
		t.Fatalf("record under %s: %v", wsA, err)
	}
	if err := cps.Save(ctx, wsB, projection, instance, "", 20); err != nil {
		t.Fatalf("save under %s: %v", wsB, err)
	}
	if ok, err := leases.Acquire(ctx, wsB, projection, instance, "owner-b", "consumer-b", time.Minute); err != nil || !ok {
		t.Fatalf("acquire under %s = %v err=%v, want true/nil (the same key in another workspace is free)", wsB, ok, err)
	}
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: wsB, Projection: projection, Instance: instance,
		Position: 43, Reason: "rls probe", Actor: "probe",
	}); err != nil {
		t.Fatalf("record under %s: %v", wsB, err)
	}

	// Raw GUC probes: no workspace_id filter anywhere — the policy alone
	// decides — as the role the policies bind. Under A's GUC only A's
	// rows exist; under B's only B's; in both directions, on all three
	// tables, one row each, and the row seen is that workspace's own.
	for _, table := range []string{"feed_checkpoints", "feed_leases", "feed_dead_letters"} {
		for _, ws := range []string{wsA, wsB} {
			var n int
			if err := probePinned(t, ctx, s, ws, func(tx pgx.Tx) error {
				return tx.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n)
			}); err != nil {
				t.Fatalf("count %s under %s's GUC: %v", table, ws, err)
			}
			if n != 1 {
				t.Fatalf("%s under %s's GUC sees %d rows, want 1 (exactly that workspace's own)", table, ws, n)
			}
		}
	}
	var (
		posA, posB     int64
		dlA, dlB       int64
		ownerA, ownerB string
	)
	if err := probePinned(t, ctx, s, wsA, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT position FROM feed_checkpoints`).Scan(&posA)
	}); err != nil {
		t.Fatalf("read checkpoint under %s: %v", wsA, err)
	}
	if err := probePinned(t, ctx, s, wsB, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT position FROM feed_checkpoints`).Scan(&posB)
	}); err != nil {
		t.Fatalf("read checkpoint under %s: %v", wsB, err)
	}
	if posA != 10 || posB != 20 {
		t.Fatalf("checkpoint rows: under %s = %d, under %s = %d, want 10/20 (each its own)", wsA, posA, wsB, posB)
	}
	if err := probePinned(t, ctx, s, wsA, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT position FROM feed_dead_letters`).Scan(&dlA)
	}); err != nil {
		t.Fatalf("read dead letter under %s: %v", wsA, err)
	}
	if err := probePinned(t, ctx, s, wsB, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT position FROM feed_dead_letters`).Scan(&dlB)
	}); err != nil {
		t.Fatalf("read dead letter under %s: %v", wsB, err)
	}
	if dlA != 42 || dlB != 43 {
		t.Fatalf("dead letter rows: under %s = %d, under %s = %d, want 42/43 (each its own)", wsA, dlA, wsB, dlB)
	}
	if err := probePinned(t, ctx, s, wsA, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT owner FROM feed_leases`).Scan(&ownerA)
	}); err != nil {
		t.Fatalf("read lease under %s: %v", wsA, err)
	}
	if err := probePinned(t, ctx, s, wsB, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT owner FROM feed_leases`).Scan(&ownerB)
	}); err != nil {
		t.Fatalf("read lease under %s: %v", wsB, err)
	}
	if ownerA != "owner-a" || ownerB != "owner-b" {
		t.Fatalf("lease rows: under %s = %q, under %s = %q, want owner-a/owner-b", wsA, ownerA, wsB, ownerB)
	}

	// The dead-letter enrichment lookup is policy-bound over events too:
	// an events row that exists (in B) is invisible under A's GUC when
	// the query carries no explicit workspace filter — the policy alone
	// must hide it. A row in A's own events (appended now) is visible.
	ownEvent := feedAppendEvent(t, ctx, s, wsA, feedStream(t, "rls-own"), 0, "feedtest.v1.E", []byte("x"))
	foreignEvent := feedAppendEvent(t, ctx, s, wsB, feedStream(t, "rls-foreign"), 0, "feedtest.v1.E", []byte("x"))
	for _, probe := range []struct {
		ws   string
		pos  int64
		want int
	}{
		{wsA, int64(ownEvent.GlobalPosition), 1},
		{wsA, int64(foreignEvent.GlobalPosition), 0},
		{wsB, int64(foreignEvent.GlobalPosition), 1},
	} {
		var n int
		if err := probePinned(t, ctx, s, probe.ws, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx,
				`SELECT count(*) FROM events WHERE global_position = $1`, probe.pos,
			).Scan(&n)
		}); err != nil {
			t.Fatalf("events lookup under %s at %d: %v", probe.ws, probe.pos, err)
		}
		if n != probe.want {
			t.Fatalf("events at %d under %s's GUC: %d rows, want %d (the policy must hide foreign workspaces)",
				probe.pos, probe.ws, n, probe.want)
		}
	}

	// WITH CHECK: a cross-workspace insert is refused at the policy —
	// and there is no bypass arm to escape through, because the policy
	// never references one.
	err := probePinned(t, ctx, s, wsA, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO feed_checkpoints (workspace_id, projection, instance, position, definition_hash, updated_at)
			 VALUES ($1, 'p', 'foreign', 99, '', clock_timestamp())`, wsB)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "row-level security") {
		t.Fatalf("cross-workspace insert under %s's GUC: got %v, want a row-level security policy violation", wsA, err)
	}
	err = probePinned(t, ctx, s, wsA, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO feed_checkpoints (workspace_id, projection, instance, position, definition_hash, updated_at)
			 VALUES ($1, 'p', 'own', 99, '', clock_timestamp())`, wsA)
		return err
	})
	if err != nil {
		t.Fatalf("own-workspace insert under %s's GUC refused: %v", wsA, err)
	}

	// Schema: exactly one policy per table, keyed on app.workspace_id,
	// with no app.bypass_rls reference in USING or WITH CHECK — and the
	// RLS flags are ENABLE + FORCE, exactly like events (0001). The rows
	// are drained and closed before any assertion: an open pgx.Rows
	// holds its pool connection, and a t.Fatalf with rows open would
	// wedge Store.Close at cleanup.
	for _, table := range []string{"feed_checkpoints", "feed_leases", "feed_dead_letters"} {
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
		for _, p := range policies {
			for _, def := range []string{p.qual, p.withCheck} {
				if strings.Contains(def, "app.bypass_rls") {
					t.Fatalf("%s: policy %q references app.bypass_rls (%q) — the feed tables must have no bypass arm",
						table, p.name, def)
				}
				if !strings.Contains(def, "app.workspace_id") {
					t.Fatalf("%s: policy %q does not key on app.workspace_id (%q)", table, p.name, def)
				}
			}
		}
		var enabled, forced bool
		if err := s.Pool().QueryRow(ctx,
			`SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::regclass`,
			"public."+table,
		).Scan(&enabled, &forced); err != nil {
			t.Fatalf("read pg_class flags for %s: %v", table, err)
		}
		if !enabled || !forced {
			t.Fatalf("%s: RLS enabled=%v forced=%v, want both true (FORCE keeps the table owner subject)", table, enabled, forced)
		}
	}
}
