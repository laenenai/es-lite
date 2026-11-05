package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/feed"
	"github.com/laenenai/es-lite/postgres"
	"github.com/laenenai/es-lite/projection"
)

// The workspace-scoped feed adapters (spec §5.6): checkpoint, lease,
// dead-letter and Logs behavior over a real Postgres — the mirror of
// sqlite/feed_test.go's adapter tests, plus the RLS isolation the
// Postgres tables add (Review Focus 5). Everything here skips without
// PG_DSN, like the rest of the Postgres suite.

// feedDBClock reads the database's own clock, so timestamp assertions
// compare DB-stamped values against DB time — immune to any skew
// between the test process's clock and the database's.
func feedDBClock(t *testing.T, ctx context.Context, s *postgres.Store) time.Time {
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
func feedAppendEvent(t *testing.T, ctx context.Context, s *postgres.Store, scope string, sid es.StreamID, version uint64, typeURL string, payload []byte) es.Envelope {
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
// CheckpointStore
// ---------------------------------------------------------------------------

func TestCheckpointLoadAbsentSaveAndNoRewind(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	cp := postgres.NewCheckpointStore(s)

	pos, err := cp.Load(ctx, "ws", "p", "live")
	if err != nil {
		t.Fatalf("load absent: %v", err)
	}
	if pos != 0 {
		t.Fatalf("absent load = %d, want 0", pos)
	}
	if _, found, err := cp.Checkpoint(ctx, "ws", "p", "live"); err != nil || found {
		t.Fatalf("absent checkpoint = found=%v err=%v, want false/nil", found, err)
	}

	if err := cp.Save(ctx, "ws", "p", "live", "", 5); err != nil {
		t.Fatalf("save: %v", err)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
		t.Fatalf("load = %d, want 5", pos)
	}

	// Save never rewinds: GREATEST in SQL keeps the higher position.
	if err := cp.Save(ctx, "ws", "p", "live", "", 3); err != nil {
		t.Fatalf("save lower: %v", err)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
		t.Fatalf("load after lower save = %d, want 5 (monotonic)", pos)
	}

	// A different instance is a different row.
	if err := cp.Save(ctx, "ws", "p", "other", "", 2); err != nil {
		t.Fatalf("save other: %v", err)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "other"); pos != 2 {
		t.Fatalf("other instance = %d, want 2", pos)
	}
	if pos, _ = cp.Load(ctx, "ws", "p", "live"); pos != 5 {
		t.Fatalf("live instance = %d, want 5", pos)
	}
}

func TestCheckpointResetDeleteAndTimestamp(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	cp := postgres.NewCheckpointStore(s)

	// updated_at is the database's clock (clock_timestamp() in SQL), not
	// the writer's: bracket the write with two reads of the DB's own
	// clock and the stamp must fall between them. A skewed application
	// clock could never place the value outside this window.
	dbBefore := feedDBClock(t, ctx, s)
	if err := cp.Save(ctx, "ws", "p", "live", "", 7); err != nil {
		t.Fatalf("save: %v", err)
	}
	dbAfter := feedDBClock(t, ctx, s)
	checkpoint, found, err := cp.Checkpoint(ctx, "ws", "p", "live")
	if err != nil || !found {
		t.Fatalf("checkpoint = found=%v err=%v, want true/nil", found, err)
	}
	if checkpoint.Position != 7 {
		t.Fatalf("checkpoint position = %d, want 7", checkpoint.Position)
	}
	if checkpoint.UpdatedAt.Before(dbBefore) || checkpoint.UpdatedAt.After(dbAfter) {
		t.Fatalf("checkpoint updated_at = %v, want the DB clock between %v and %v (not app time)",
			checkpoint.UpdatedAt, dbBefore, dbAfter)
	}

	// Reset overwrites, backwards included.
	if err := cp.Reset(ctx, "ws", "p", "live", "", 1); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if pos, _ := cp.Load(ctx, "ws", "p", "live"); pos != 1 {
		t.Fatalf("load after reset = %d, want 1", pos)
	}
	if checkpoint, _, _ = cp.Checkpoint(ctx, "ws", "p", "live"); checkpoint.Position != 1 {
		t.Fatalf("checkpoint after reset = %d, want 1", checkpoint.Position)
	}

	// Delete removes exactly that row.
	if err := cp.Save(ctx, "ws", "p", "other", "", 4); err != nil {
		t.Fatalf("save other: %v", err)
	}
	if err := cp.Delete(ctx, "ws", "p", "live"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, found, err := cp.Checkpoint(ctx, "ws", "p", "live"); err != nil || found {
		t.Fatalf("checkpoint after delete = found=%v err=%v, want false/nil", found, err)
	}
	if pos, _ := cp.Load(ctx, "ws", "p", "other"); pos != 4 {
		t.Fatalf("other instance after delete = %d, want 4", pos)
	}
	// Deleting again is a no-op.
	if err := cp.Delete(ctx, "ws", "p", "live"); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
}

// The definition fingerprint round-trips through Checkpoint, and an
// empty definition never clears a recorded one: Save and Reset both
// preserve the stored hash when none is supplied — an ack that carries
// no identity, and a position-only advance like Skip, must not
// re-identify a checkpoint — while a non-empty definition becomes the
// checkpoint's new identity (a reset under a changed definition).
func TestCheckpointDefinitionHashRecordedAndPreserved(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	cp := postgres.NewCheckpointStore(s)

	h1 := projection.Definition{Name: "p", Consumes: []string{"a.v1.A"}}.Hash()
	h2 := projection.Definition{Name: "p", Consumes: []string{"a.v1.A", "b.v1.B"}}.Hash()

	// A save with no definition records no fingerprint.
	if err := cp.Save(ctx, "ws", "p", "live", "", 5); err != nil {
		t.Fatalf("save without definition: %v", err)
	}
	got, found, err := cp.Checkpoint(ctx, "ws", "p", "live")
	if err != nil || !found || got.Position != 5 || got.DefinitionHash != "" {
		t.Fatalf("checkpoint = %+v found=%v err=%v, want position 5 and no hash", got, found, err)
	}

	// A definition-carrying save records the fingerprint.
	if err := cp.Save(ctx, "ws", "p", "live", h1, 6); err != nil {
		t.Fatalf("save with definition: %v", err)
	}
	if got, _, _ := cp.Checkpoint(ctx, "ws", "p", "live"); got.DefinitionHash != h1 || got.Position != 6 {
		t.Fatalf("checkpoint = %+v, want position 6 under hash %q", got, h1)
	}

	// An empty definition preserves the recorded hash on Save ...
	if err := cp.Save(ctx, "ws", "p", "live", "", 7); err != nil {
		t.Fatalf("save without definition: %v", err)
	}
	if got, _, _ := cp.Checkpoint(ctx, "ws", "p", "live"); got.Position != 7 || got.DefinitionHash != h1 {
		t.Fatalf("checkpoint = %+v, want position 7 with the hash preserved", got)
	}

	// ... and on Reset, which may rewind but still must not clear it.
	if err := cp.Reset(ctx, "ws", "p", "live", "", 1); err != nil {
		t.Fatalf("reset without definition: %v", err)
	}
	if got, _, _ := cp.Checkpoint(ctx, "ws", "p", "live"); got.Position != 1 || got.DefinitionHash != h1 {
		t.Fatalf("checkpoint = %+v, want position 1 with the hash preserved", got)
	}

	// A reset under a new definition re-identifies the checkpoint.
	if err := cp.Reset(ctx, "ws", "p", "live", h2, 0); err != nil {
		t.Fatalf("reset with definition: %v", err)
	}
	if got, _, _ := cp.Checkpoint(ctx, "ws", "p", "live"); got.Position != 0 || got.DefinitionHash != h2 {
		t.Fatalf("checkpoint = %+v, want position 0 under the new hash", got)
	}
}

// ---------------------------------------------------------------------------
// LeaseStore
// ---------------------------------------------------------------------------

func TestLeaseExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	leases := postgres.NewLeaseStore(s)
	const scope, projection, instance = "ws", "p", "live"

	ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", time.Minute)
	if err != nil {
		t.Fatalf("acquire owner-a: %v", err)
	}
	if !ok {
		t.Fatal("owner-a lost a free lease")
	}

	ok, err = leases.Acquire(ctx, scope, projection, instance, "owner-b", "consumer-b", time.Minute)
	if err != nil {
		t.Fatalf("acquire owner-b: %v", err)
	}
	if ok {
		t.Fatal("owner-b took a lease owner-a still holds; want exactly one winner")
	}

	lease, held, err := leases.Holder(ctx, scope, projection, instance)
	if err != nil {
		t.Fatalf("holder: %v", err)
	}
	if !held || lease.Owner != "owner-a" || lease.Consumer != "consumer-a" {
		t.Fatalf("holder = %+v held=%v, want owner-a/consumer-a", lease, held)
	}
	// The expiry is absolute DB time ttl from the acquire: it must still
	// be in the future of the DB's own clock, and no further than ttl
	// past a clock read taken after the acquire.
	dbNow := feedDBClock(t, ctx, s)
	if !lease.ExpiresAt.After(dbNow) {
		t.Fatalf("holder ExpiresAt %v is not in the future of the DB clock %v", lease.ExpiresAt, dbNow)
	}
	if lease.ExpiresAt.After(dbNow.Add(time.Minute + 5*time.Second)) {
		t.Fatalf("holder ExpiresAt %v is more than the 1m TTL past %v", lease.ExpiresAt, dbNow)
	}
}

func TestLeaseReacquireBySameOwnerUpdatesConsumer(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	leases := postgres.NewLeaseStore(s)
	const scope, projection, instance = "ws", "p", "live"

	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "pod-a", time.Minute); err != nil || !ok {
		t.Fatalf("first acquire = %v err=%v, want true/nil", ok, err)
	}
	// A reconnect by the same owner must succeed and refresh the consumer
	// identity Status reports.
	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "pod-b", time.Minute); err != nil || !ok {
		t.Fatalf("same-owner acquire = %v err=%v, want true/nil", ok, err)
	}
	lease, held, err := leases.Holder(ctx, scope, projection, instance)
	if err != nil || !held {
		t.Fatalf("holder = found=%v err=%v, want held", held, err)
	}
	if lease.Owner != "owner-a" || lease.Consumer != "pod-b" {
		t.Fatalf("holder = %+v, want owner-a/pod-b", lease)
	}
}

func TestLeaseRenewByOwnerAndNonOwner(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	leases := postgres.NewLeaseStore(s)
	const scope, projection, instance = "ws", "p", "live"

	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire owner-a = %v err=%v", ok, err)
	}
	if ok, err := leases.Renew(ctx, scope, projection, instance, "owner-b", time.Minute); err != nil {
		t.Fatalf("renew owner-b: %v", err)
	} else if ok {
		t.Fatal("non-owner renewed the lease")
	}
	if ok, err := leases.Renew(ctx, scope, projection, instance, "owner-a", 2*time.Hour); err != nil {
		t.Fatalf("renew owner-a: %v", err)
	} else if !ok {
		t.Fatal("owner failed to renew its own lease")
	}

	// The renewal extended the expiry: the holder now reports a further
	// deadline than the 1m TTL the acquire set — no sleeps needed.
	lease, held, err := leases.Holder(ctx, scope, projection, instance)
	if err != nil || !held {
		t.Fatalf("holder after renew = found=%v err=%v, want held", held, err)
	}
	dbNow := feedDBClock(t, ctx, s)
	if !lease.ExpiresAt.After(dbNow.Add(time.Minute + 30*time.Second)) {
		t.Fatalf("renewed ExpiresAt %v did not extend past the original TTL (DB clock %v)", lease.ExpiresAt, dbNow)
	}
	if lease.Owner != "owner-a" {
		t.Fatalf("holder owner = %q, want owner-a", lease.Owner)
	}
}

// TestLeaseExpiryTakeoverAndHolderUnexpired pins the expiry half of the
// lease contract without a single sleep: the row's expires_at is pushed
// into the past with raw SQL (the deterministic stand-in for waiting out
// a TTL), after which Holder must report the key free and another owner
// must take the lease over — the same `<=` takeover arm the SQLite
// adapter pins. The unexpired side (a live row IS a holder and DOES
// fence a second owner) is the other tests' assertions.
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

func TestLeaseReleaseByOwnerAndNonOwner(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	leases := postgres.NewLeaseStore(s)
	const scope, projection, instance = "ws", "p", "live"

	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire = %v err=%v", ok, err)
	}

	// A non-owner's release is a no-op, not an error.
	if err := leases.Release(ctx, scope, projection, instance, "owner-b"); err != nil {
		t.Fatalf("release owner-b: %v", err)
	}
	if _, held, err := leases.Holder(ctx, scope, projection, instance); err != nil || !held {
		t.Fatalf("holder after non-owner release = held=%v err=%v, want still held", held, err)
	}

	if err := leases.Release(ctx, scope, projection, instance, "owner-a"); err != nil {
		t.Fatalf("release owner-a: %v", err)
	}
	if _, held, err := leases.Holder(ctx, scope, projection, instance); err != nil || held {
		t.Fatalf("holder after owner release = held=%v err=%v, want free", held, err)
	}
	// Releasing a free key is a no-op, and the next owner can take it.
	if err := leases.Release(ctx, scope, projection, instance, "owner-a"); err != nil {
		t.Fatalf("release free key: %v", err)
	}
	if ok, err := leases.Acquire(ctx, scope, projection, instance, "owner-b", "consumer-b", time.Minute); err != nil || !ok {
		t.Fatalf("acquire after release = %v err=%v, want true/nil", ok, err)
	}
}

// The subscription key is the (scope, projection, instance) triple, here
// three columns — distinct triples must stay distinct rows even when a
// part contains the separator-like "/" — ("ws","a/b","c") and ("ws","a",
// "b/c") are different subscriptions, never one lease.
func TestLeaseDistinctTriplesNeverCollide(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	leases := postgres.NewLeaseStore(s)

	if ok, err := leases.Acquire(ctx, "ws", "a/b", "c", "owner-a", "consumer-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire = %v err=%v, want true/nil", ok, err)
	}
	// The segment-shifted triple is a different subscription: another owner
	// can hold it alongside the first.
	if ok, err := leases.Acquire(ctx, "ws", "a", "b/c", "owner-b", "consumer-b", time.Minute); err != nil || !ok {
		t.Fatalf("acquire of the shifted triple = %v err=%v, want true (distinct triples never collide)", ok, err)
	}
	// The original triple is still fenced by owner-a.
	if ok, err := leases.Acquire(ctx, "ws", "a/b", "c", "owner-b", "consumer-b", time.Minute); err != nil || ok {
		t.Fatalf("acquire of the held triple = %v err=%v, want false (still held by owner-a)", ok, err)
	}
}

// ---------------------------------------------------------------------------
// DeadLetterStore
// ---------------------------------------------------------------------------

// TestDeadLetterRecordInsertOrReplace pins the store contract, mirroring
// the SQLite adapter test: one pointer row per skipped position,
// replaced (not duplicated) on a repeat skip of the same key; the row is
// enriched with the skipped event's type and size when the caller — the
// feed core — cannot know them (metadata only: an over-cap poison row's
// 4096-byte payload is never scanned, exactly the production poison
// path); a caller-supplied type passes through untouched; a position
// with no event row records the empty type and zero size; and a position
// belonging to ANOTHER workspace names nothing — the enrichment is
// workspace-pinned. created_at is the database's clock, not the
// caller's timestamp.
func TestDeadLetterRecordInsertOrReplace(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	dls := postgres.NewDeadLetterStore(s)
	const (
		scope      = "ws_dl"
		other      = "ws_dl_other"
		projection = "p"
		instance   = "live"
		typ        = "feedtest.v1.Happened"
	)

	// The poison fixture: an over-cap events row enters via raw SQL (the
	// only way one exists — the append cap refuses it), in this scope.
	poisonGP := insertPoisonRow(t, ctx, s, scope, feedStream(t, "dl-poison"), 1, typ, bytes.Repeat([]byte("p"), 4096))
	// A position that belongs to another workspace: the enrichment must
	// not name it, even though the row exists in the same database.
	foreign := feedAppendEvent(t, ctx, s, other, feedStream(t, "dl-foreign"), 0, typ, []byte("12345"))

	type row struct {
		typeURL   string
		size      int
		reason    string
		actor     string
		createdAt time.Time
	}
	read := func(position uint64) row {
		t.Helper()
		var r row
		err := s.Pool().QueryRow(ctx,
			`SELECT type_url, size_bytes, reason, actor, created_at FROM feed_dead_letters
			 WHERE workspace_id = $1 AND projection = $2 AND instance = $3 AND position = $4`,
			scope, projection, instance, int64(position),
		).Scan(&r.typeURL, &r.size, &r.reason, &r.actor, &r.createdAt)
		if err != nil {
			t.Fatalf("read dead letter at %d: %v", position, err)
		}
		return r
	}
	count := func() int {
		t.Helper()
		var n int
		if err := s.Pool().QueryRow(ctx,
			`SELECT COUNT(*) FROM feed_dead_letters WHERE workspace_id = $1`, scope,
		).Scan(&n); err != nil {
			t.Fatalf("count dead letters: %v", err)
		}
		return n
	}

	// The core delivers a pointer — no type, no size. The store names the
	// event from the log it shares a database with, metadata only.
	dbBefore := feedDBClock(t, ctx, s)
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: scope, Projection: projection, Instance: instance,
		Position: poisonGP, Reason: "poison: over cap", Actor: "oncall",
		// Deliberately ancient: the DB clock wins, so this must be ignored.
		CreatedAt: time.Unix(1, 0).UTC(),
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	dbAfter := feedDBClock(t, ctx, s)
	if r := read(poisonGP); r.typeURL != typ || r.size != 4096 ||
		r.reason != "poison: over cap" || r.actor != "oncall" {
		t.Fatalf("recorded row = %+v, want the poison named (%q, 4096 bytes) with the audit fields", r, typ)
	}
	if r := read(poisonGP); r.createdAt.Before(dbBefore) || r.createdAt.After(dbAfter) {
		t.Fatalf("created_at = %v, want the DB clock between %v and %v (not the caller's dl.CreatedAt)",
			r.createdAt, dbBefore, dbAfter)
	}

	// A repeat skip of the same position replaces the row: exactly one
	// row, the latest reason, actor and timestamp.
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: scope, Projection: projection, Instance: instance,
		Position: poisonGP, Reason: "triaged: cap raised", Actor: "oncall-2",
	}); err != nil {
		t.Fatalf("re-record: %v", err)
	}
	if n := count(); n != 1 {
		t.Fatalf("rows after the repeat = %d, want 1 (replaced, never duplicated)", n)
	}
	if r := read(poisonGP); r.reason != "triaged: cap raised" || r.actor != "oncall-2" ||
		r.typeURL != typ || r.size != 4096 {
		t.Fatalf("replaced row = %+v, want the latest audit fields over the same named event", r)
	}

	// A caller-supplied type and size pass through: the enrichment keeps a
	// complete dead letter as delivered.
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: scope, Projection: projection, Instance: instance,
		Position: poisonGP, TypeURL: "custom.v1.Typed", SizeBytes: 99,
		Reason: "named by the caller", Actor: "oncall-3",
	}); err != nil {
		t.Fatalf("record with a type: %v", err)
	}
	if r := read(poisonGP); r.typeURL != "custom.v1.Typed" || r.size != 99 {
		t.Fatalf("row with a caller-supplied type = %+v, want the caller's type/size kept", r)
	}

	// A position with no event row records the empty type and zero size:
	// the pointer still lands, Skip does not require the position to exist.
	const absent = uint64(1) << 40
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: scope, Projection: projection, Instance: instance,
		Position: absent, Reason: "skipped past a restored log", Actor: "oncall",
	}); err != nil {
		t.Fatalf("record an absent position: %v", err)
	}
	if r := read(absent); r.typeURL != "" || r.size != 0 {
		t.Fatalf("absent-position row = %+v, want the empty type and zero size", r)
	}

	// A position whose event belongs to another workspace names nothing:
	// the enrichment read is workspace-pinned (explicit workspace_id
	// filter and RLS), so the pointer records ''/0 rather than naming a
	// foreign tenant's event.
	if err := dls.Record(ctx, feed.DeadLetter{
		Scope: scope, Projection: projection, Instance: instance,
		Position: foreign.GlobalPosition, Reason: "probe", Actor: "oncall",
	}); err != nil {
		t.Fatalf("record a foreign-workspace position: %v", err)
	}
	if r := read(foreign.GlobalPosition); r.typeURL != "" || r.size != 0 {
		t.Fatalf("foreign-position row = %+v, want ''/0 — the enrichment must not see across workspaces", r)
	}

	if n := count(); n != 3 {
		t.Fatalf("rows = %d, want 3 (the replaced key plus the absent and foreign positions)", n)
	}
}

// ---------------------------------------------------------------------------
// Cross-workspace isolation (Review Focus 5)
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

// TestFeedStoresRefuseCrossWorkspaceAccess is Review Focus 5. The feed
// tables' policies have no bypass arm; this proves both halves:
//
//   - behavior: rows written under workspace A are invisible under B's
//     GUC and vice versa — on all three tables, in both directions,
//     through the adapters AND through raw GUC-pinned queries with no
//     workspace_id filter (where the policy alone decides), as a role
//     the policies bind;
//   - schema: pg_policies for the three tables keys on app.workspace_id
//     with NO app.bypass_rls reference, and both RLS flags are set.
//
// It also proves the policies' WITH CHECK: a cross-workspace insert is
// refused at the policy, and that the dead-letter enrichment lookup
// over events is policy-bound as well.
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
	// workspace_id, B's writes would clobber A's.
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

	// Adapter level: each scope's store sees only its own rows, and A's
	// rows survived B's writes to the same keys.
	if pos, err := cps.Load(ctx, wsA, projection, instance); err != nil || pos != 10 {
		t.Fatalf("load under %s = %d err=%v, want 10 (B's write must not clobber it)", wsA, pos, err)
	}
	if pos, err := cps.Load(ctx, wsB, projection, instance); err != nil || pos != 20 {
		t.Fatalf("load under %s = %d err=%v, want 20 — A's row leaked", wsB, pos, err)
	}
	leaseA, held, err := leases.Holder(ctx, wsA, projection, instance)
	if err != nil || !held || leaseA.Owner != "owner-a" {
		t.Fatalf("holder under %s = %+v held=%v err=%v, want owner-a", wsA, leaseA, held, err)
	}
	leaseB, held, err := leases.Holder(ctx, wsB, projection, instance)
	if err != nil || !held || leaseB.Owner != "owner-b" {
		t.Fatalf("holder under %s = %+v held=%v err=%v, want owner-b — A's lease leaked", wsB, leaseB, held, err)
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
	err = probePinned(t, ctx, s, wsA, func(tx pgx.Tx) error {
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

// ---------------------------------------------------------------------------
// End to end: the adapters under the real feed core
// ---------------------------------------------------------------------------

// feedCancelSink records events, acks the final delivered position
// through ack (while the stream still holds its lease —
// acknowledgements are fenced on it), and cancels its subscription once
// want events have arrived.
type feedCancelSink struct {
	mu     sync.Mutex
	events []es.Envelope
	want   int
	cancel context.CancelFunc
	ack    func(ctx context.Context, position uint64) error
}

func (c *feedCancelSink) Send(ctx context.Context, env es.Envelope) error {
	c.mu.Lock()
	c.events = append(c.events, env)
	n := len(c.events)
	c.mu.Unlock()
	if n >= c.want {
		if c.ack != nil {
			if err := c.ack(ctx, env.GlobalPosition); err != nil {
				return fmt.Errorf("ack at %d: %w", env.GlobalPosition, err)
			}
		}
		c.cancel()
	}
	return nil
}

func (c *feedCancelSink) got() []es.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]es.Envelope(nil), c.events...)
}

// feedAckingSink records events and acknowledges every delivered
// position through ack while the stream still holds its lease —
// acknowledgements are fenced on the lease, so this is the only moment
// an ack can land.
type feedAckingSink struct {
	mu     sync.Mutex
	events []es.Envelope
	ack    func(ctx context.Context, position uint64) error
}

func (a *feedAckingSink) Send(ctx context.Context, env es.Envelope) error {
	a.mu.Lock()
	a.events = append(a.events, env)
	a.mu.Unlock()
	if a.ack != nil {
		if err := a.ack(ctx, env.GlobalPosition); err != nil {
			return fmt.Errorf("ack at %d: %w", env.GlobalPosition, err)
		}
	}
	return nil
}

func (a *feedAckingSink) got() []es.Envelope {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]es.Envelope(nil), a.events...)
}

// newPGFeedServer wires a feed.Server over the real Postgres adapters.
func newPGFeedServer(t *testing.T, s *postgres.Store) *feed.Server {
	t.Helper()
	srv, err := feed.NewServer(feed.Config{
		Logs:        postgres.NewLogs(s),
		Checkpoints: postgres.NewCheckpointStore(s),
		Leases:      postgres.NewLeaseStore(s),
		DeadLetters: postgres.NewDeadLetterStore(s),
		Poll:        10 * time.Millisecond,
		Batch:       10,
		LeaseTTL:    time.Minute,
		RenewEvery:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return srv
}

// TestFeedEndToEndResumesFromCheckpoint runs the feed core over the
// Postgres adapters: a subscription delivers only its definition's
// consumed types, the fenced ack lands a durable checkpoint in
// feed_checkpoints, and a second subscription resumes from it — exactly
// the events the log holds after the checkpoint, never anything before.
func TestFeedEndToEndResumesFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const scope = "ws_feed_e2e"
	sid := feedStream(t, "feed-e2e")

	const (
		consumed = "feedtest.v1.Consumed"
		ignored  = "feedtest.v1.Ignored"
	)
	res, err := s.WorkspaceStore(scope).Append(ctx, es.AppendParams{
		StreamID: sid,
		Events: []es.EventData{
			{TypeURL: consumed, SchemaVersion: 1, Payload: []byte("a1")},
			{TypeURL: ignored, SchemaVersion: 1, Payload: []byte("b1")},
			{TypeURL: consumed, SchemaVersion: 1, Payload: []byte("a2")},
		},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	srv := newPGFeedServer(t, s)
	req := feed.Request{
		Scope:      scope,
		Projection: "p",
		Instance:   "live",
		Consumer:   "test",
		// Caller-supplied owner: the fenced ack below must name it.
		Owner:      "e2e-owner",
		Definition: projection.Definition{Name: "p", Consumes: []string{consumed}},
	}

	// First subscription: collect the two consumed events, ack under the
	// definition, then cancel.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	sink1 := &feedCancelSink{want: 2, cancel: cancel1, ack: func(ctx context.Context, pos uint64) error {
		return srv.Checkpoint(ctx, feed.Ack{
			Scope: req.Scope, Projection: req.Projection, Instance: req.Instance,
			Owner: req.Owner, Position: pos,
			// The ack records the subscription's fingerprint, so the
			// resumed subscription crosses with a recorded identity.
			Definition: req.Definition.Hash(),
		})
	}}
	if err := srv.Subscribe(ctx1, req, sink1); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe 1: %v", err)
	}

	first := sink1.got()
	if len(first) != 2 {
		t.Fatalf("first subscription got %d events, want 2: %+v", len(first), first)
	}
	if first[0].GlobalPosition != res.Envelopes[0].GlobalPosition ||
		first[1].GlobalPosition != res.Envelopes[2].GlobalPosition {
		t.Fatalf("first subscription positions = %d,%d, want %d,%d (the consumed events, in order)",
			first[0].GlobalPosition, first[1].GlobalPosition,
			res.Envelopes[0].GlobalPosition, res.Envelopes[2].GlobalPosition)
	}
	acked := first[len(first)-1].GlobalPosition

	// The checkpoint the acks wrote is durable in Postgres.
	cp, found, err := postgres.NewCheckpointStore(s).Checkpoint(ctx, req.Scope, req.Projection, req.Instance)
	if err != nil || !found {
		t.Fatalf("checkpoint after the acks = found %v err %v, want found", found, err)
	}
	if cp.Position != acked {
		t.Fatalf("checkpoint = %d, want the acked %d", cp.Position, acked)
	}

	// Append one more consumed event; the resumed subscription delivers
	// exactly it — never the events before the checkpoint.
	next := feedAppendEvent(t, ctx, s, scope, sid, 3, consumed, []byte("a3"))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	sink2 := &feedCancelSink{want: 1, cancel: cancel2}
	if err := srv.Subscribe(ctx2, req, sink2); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe 2: %v", err)
	}
	second := sink2.got()
	if len(second) != 1 {
		t.Fatalf("resumed subscription got %d events, want 1: %+v", len(second), second)
	}
	if second[0].GlobalPosition != next.GlobalPosition {
		t.Fatalf("resumed event position = %d, want %d (after checkpoint %d)",
			second[0].GlobalPosition, next.GlobalPosition, acked)
	}
}

// TestSkipResumesPastPoison walks the whole poison runbook end to end on
// Postgres, mirroring the SQLite e2e: the over-cap row (inserted past
// the append cap by raw SQL, the only way such a row can exist) kills a
// live subscription with *feed.EventTooLargeError; the operator
// deliberately skips past it; the dead letter lands in feed_dead_letters
// as a complete pointer — the store names the event's type and size from
// the log without ever scanning its payload — and the resubscription
// delivers exactly the events after the poison position, never the
// poison itself. The skipped subscription's definition fingerprint
// survives the skip.
func TestSkipResumesPastPoison(t *testing.T) {
	ctx := context.Background()
	s := openStoreWithOptions(t, postgres.WithMaxPayloadBytes(64))
	const scope = "ws_skip_e2e"
	const typ = "feedtest.v1.Poison"

	// Global order: before < poison < after1 < after2. The poison row
	// enters via raw SQL because the append cap refuses it.
	before := feedAppendEvent(t, ctx, s, scope, feedStream(t, "poison-e2e-a"), 0, typ, []byte("ok"))
	poisonGP := insertPoisonRow(t, ctx, s, scope, feedStream(t, "poison-e2e"), 1, typ, bytes.Repeat([]byte("p"), 4096))
	after1 := feedAppendEvent(t, ctx, s, scope, feedStream(t, "poison-e2e-b"), 0, typ, []byte("after-1"))
	after2 := feedAppendEvent(t, ctx, s, scope, feedStream(t, "poison-e2e-b"), 1, typ, []byte("after-2"))
	if !(before.GlobalPosition < poisonGP && poisonGP < after1.GlobalPosition && after1.GlobalPosition < after2.GlobalPosition) {
		t.Fatalf("insert order: before=%d poison=%d after=%d,%d, want the poison between the normal rows",
			before.GlobalPosition, poisonGP, after1.GlobalPosition, after2.GlobalPosition)
	}

	srv := newPGFeedServer(t, s)
	req := feed.Request{
		Scope:      scope,
		Projection: "p",
		Instance:   "live",
		Consumer:   "test",
		Owner:      "skip-e2e-owner",
		Definition: projection.Definition{Name: "p", Consumes: []string{typ}},
	}

	// The live subscription delivers the event before the poison, acks it
	// under the definition, then dies on the poison row — never
	// materialized, never delivered.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	sink := &feedAckingSink{ack: func(ctx context.Context, pos uint64) error {
		return srv.Checkpoint(ctx, feed.Ack{
			Scope: req.Scope, Projection: req.Projection, Instance: req.Instance,
			Owner: req.Owner, Position: pos, Definition: req.Definition.Hash(),
		})
	}}
	err := srv.Subscribe(ctx1, req, sink)
	var tooLarge *feed.EventTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("subscribe error = %v (%T), want *feed.EventTooLargeError", err, err)
	}
	if tooLarge.Position != poisonGP || tooLarge.TypeURL != typ ||
		tooLarge.Size != 4096 || tooLarge.Limit != 64 {
		t.Fatalf("poison error = %+v, want position %d of type %q, 4096 bytes over the 64-byte cap",
			tooLarge, poisonGP, typ)
	}
	if got := sink.got(); len(got) != 1 || got[0].GlobalPosition != before.GlobalPosition {
		t.Fatalf("delivered before the poison = %v, want only the event at %d (the poison never delivered)",
			positions(got), before.GlobalPosition)
	}

	// The operator skips past the poison. No force: the poison error
	// killed the stream and released its lease, so the fence is free.
	dbBefore := feedDBClock(t, ctx, s)
	reason := "poison: 4096-byte payload over the 64-byte store cap"
	if err := srv.Skip(ctx, feed.SkipRequest{
		Scope: req.Scope, Projection: req.Projection, Instance: req.Instance,
		Position: poisonGP, Actor: "oncall@example.com", Reason: reason,
	}); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	dbAfter := feedDBClock(t, ctx, s)

	// The dead letter landed as a complete pointer: the store named the
	// event's type and size from the log (metadata only — the 4096-byte
	// payload was never scanned), and the row carries the operator's
	// reason and actor with the DB clock's created_at.
	var (
		typeURL     string
		size        int
		dlReason    string
		dlActor     string
		dlCreatedAt time.Time
	)
	if err := s.Pool().QueryRow(ctx,
		`SELECT type_url, size_bytes, reason, actor, created_at FROM feed_dead_letters
		 WHERE workspace_id = $1 AND projection = $2 AND instance = $3 AND position = $4`,
		req.Scope, req.Projection, req.Instance, int64(poisonGP),
	).Scan(&typeURL, &size, &dlReason, &dlActor, &dlCreatedAt); err != nil {
		t.Fatalf("read the landed dead letter: %v", err)
	}
	if typeURL != typ || size != 4096 || dlReason != reason || dlActor != "oncall@example.com" {
		t.Fatalf("dead letter row = (%q, %d, %q, %q), want the poison named with the audit fields",
			typeURL, size, dlReason, dlActor)
	}
	if dlCreatedAt.Before(dbBefore) || dlCreatedAt.After(dbAfter) {
		t.Fatalf("dead letter created_at = %v, want the DB clock between %v and %v", dlCreatedAt, dbBefore, dbAfter)
	}

	// The checkpoint advanced to the poison position with the ack's
	// definition fingerprint preserved — a skip is a position advance,
	// never a redefinition.
	cp, found, err := postgres.NewCheckpointStore(s).Checkpoint(ctx, req.Scope, req.Projection, req.Instance)
	if err != nil || !found {
		t.Fatalf("checkpoint after the skip = found %v err %v", found, err)
	}
	if cp.Position != poisonGP {
		t.Fatalf("checkpoint = %d after the skip, want the poison position %d", cp.Position, poisonGP)
	}
	if cp.DefinitionHash != req.Definition.Hash() {
		t.Fatalf("definition hash = %q after the skip, want the acked %q preserved", cp.DefinitionHash, req.Definition.Hash())
	}

	// The resubscription delivers exactly the events after the poison —
	// never the poison itself.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	sink2 := &feedCancelSink{want: 2, cancel: cancel2}
	if err := srv.Subscribe(ctx2, req, sink2); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("resubscribe: %v", err)
	}
	got := sink2.got()
	want := []uint64{after1.GlobalPosition, after2.GlobalPosition}
	if !slices.Equal(positions(got), want) {
		t.Fatalf("resumed positions = %v, want %v (the events after the poison, never the poison itself)",
			positions(got), want)
	}
}
