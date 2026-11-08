package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/examples/counter"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/postgres"
)

// Integration tests against a real Postgres. Skip unless PG_DSN is set:
//
//	docker run -d --rm --name pg -p 5433:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=eslite postgres:17
//	PG_DSN='postgres://postgres:pw@127.0.0.1:5433/eslite?sslmode=disable' go test ./postgres/...
func openStore(t testing.TB) *postgres.Store {
	t.Helper()
	return openStoreWithOptions(t)
}

// openStoreWithOptions is openStore applying Open options, for tests that
// configure the store (WithMaxPayloadBytes, WithoutAutoMigrate, ...).
func openStoreWithOptions(t testing.TB, opts ...postgres.Option) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("set PG_DSN to run the Postgres integration tests")
	}
	ctx := context.Background()
	s, err := postgres.Open(ctx, dsn, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// The RLS bypass is role-gated (migration 0005): every Drain or
	// anchor-path call probes es_relay membership, and even the
	// superuser's probe needs the role to exist. The suite's shared
	// setup ensures it idempotently — role creation belongs to test
	// setup, never migrations (spec §5.7) — so the suite is green
	// against a bare container, whatever the test order (chain_test's
	// anchor tests run before drain_test's own ensure).
	ensureRelayRole(t, ctx, s)
	// Isolate each test: tests run sequentially, so a truncate is safe and
	// keeps the shared partitioned tables clean. log_order is truncated
	// with events — its rows are written by the same appends, and a stale
	// row would dispute the next test's order roots and deep verifies.
	// command_dedup joins the list for the same reason: a dedup Save must
	// be free to create its row, and the dedup tests' first-record-wins
	// and per-workspace-count verdicts must not inherit a stale row from
	// a previous run.
	// The bare checkpoints table is gone (0008: dead on Postgres — only
	// SQLite implements delivery.Checkpoints), so it is no longer
	// truncated here.
	if _, err := s.Pool().Exec(ctx, `TRUNCATE events, log_order, unique_claims, anchors, feed_checkpoints, feed_leases, feed_dead_letters, command_dedup`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func streamID(t *testing.T) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID("counter", "main")
	if err != nil {
		t.Fatal(err)
	}
	return sid
}

func TestAppendFoldTimeTravel(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	rt := aggregate.NewRuntime(s.Workspace("ws_a"), counter.Decider, counterv1.EventCodec{})
	sid := streamID(t)

	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 5}), es.Meta{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 20}), es.Meta{}); err != nil {
		t.Fatalf("increment: %v", err)
	}

	state, version, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetCount() != 25 || version != 2 {
		t.Fatalf("count=%d version=%d, want 25/2", state.GetCount(), version)
	}

	past, err := rt.LoadAsOfVersion(ctx, sid, 1)
	if err != nil {
		t.Fatalf("as-of-version: %v", err)
	}
	if past.GetCount() != 5 {
		t.Fatalf("as-of v1: count=%d, want 5", past.GetCount())
	}
}

func TestWorkspaceIsolationRLS(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := streamID(t)

	rtA := aggregate.NewRuntime(s.Workspace("ws_a"), counter.Decider, counterv1.EventCodec{})
	if _, err := rtA.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 10, Initial: 1}), es.Meta{}); err != nil {
		t.Fatalf("init in ws_a: %v", err)
	}

	// A different workspace sees the stream as never-written for the same
	// StreamID: RLS blocks cross-workspace reads even with the identical
	// id, so ws_b's read reports ErrStreamNotFound, not ws_a's events.
	_, err := s.Workspace("ws_b").ReadStream(ctx, sid, 0, 0)
	if !errors.Is(err, es.ErrStreamNotFound) {
		t.Fatalf("read ws_b: got %v, want ErrStreamNotFound — RLS must hide ws_a's rows", err)
	}

	// ws_a still sees its own event.
	evs, err := s.Workspace("ws_a").ReadStream(ctx, sid, 0, 0)
	if err != nil || len(evs) != 1 {
		t.Fatalf("ws_a read: len=%d err=%v, want 1", len(evs), err)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_a")
	sid := streamID(t)

	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	// Stale expected version loses.
	_, err := ws.Append(ctx, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("y")}},
	})
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("stale append: got %v, want ErrConflict", err)
	}
}

// TestDuplicateEventIDIsNotConflict pins the F25 classification: appending
// an EventID the log already stores — on ANOTHER stream, so no version is
// contested — fails with es.ErrDuplicateEventID, which is not
// es.ErrConflict. A caller that retried on ErrConflict would loop forever:
// the event is already in the log and the append can never win.
func TestDuplicateEventIDIsNotConflict(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_dup")
	sidA, _ := es.NewStreamID("thing", "a")
	sidB, _ := es.NewStreamID("thing", "b")
	id := uuid.MustParse("11111111-1111-7111-8111-111111111111")

	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sidA,
		ExpectedVersion: 0,
		Events:          []es.EventData{{EventID: id, TypeURL: "thing.v1.Created", SchemaVersion: 1, Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}

	// The same EventID on a different stream: both appends expect v0 of a
	// fresh stream, so no version is contested — this is the
	// (workspace_id, event_id) unique constraint, not the pkey backstop.
	_, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sidB,
		ExpectedVersion: 0,
		Events:          []es.EventData{{EventID: id, TypeURL: "thing.v1.Created", SchemaVersion: 1, Payload: []byte("y")}},
	})
	if !errors.Is(err, es.ErrDuplicateEventID) {
		t.Fatalf("duplicate event id append: got %v, want ErrDuplicateEventID", err)
	}
	if errors.Is(err, es.ErrConflict) {
		t.Fatalf("duplicate event id append misrouted to ErrConflict: %v", err)
	}

	// The rejected append wrote nothing: stream B is still unwritten.
	if v, err := ws.CurrentStreamVersion(ctx, sidB); err != nil || v != 0 {
		t.Fatalf("stream B version after rejected append = %d (err %v), want 0", v, err)
	}
}

// TestScanRejectsCorruptEventID pins the F34 read-side contract: a stored
// audit-UUID column (event_id, correlation_id, causation_id, command_id)
// that is not a UUID fails the read instead of being silently zeroed into
// a forged envelope, and the error names the offending column and the
// value's LENGTH only — the corrupt bytes themselves must never leak into
// logs.
func TestScanRejectsCorruptEventID(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.WorkspaceStore("ws_scan")

	for i, col := range []string{"event_id", "correlation_id", "causation_id", "command_id"} {
		t.Run(col, func(t *testing.T) {
			sid, err := es.NewStreamID("thing", "corrupt-"+col)
			if err != nil {
				t.Fatal(err)
			}
			// Each row gets a distinct valid EventID (the unique
			// constraint would otherwise reject the seed itself) and a
			// corrupt value in exactly one audit column.
			good := fmt.Sprintf("11111111-1111-4111-8111-%012d", i)
			bad := "corrupt-" + col
			cols := map[string]string{
				"event_id":       good,
				"correlation_id": good,
				"causation_id":   good,
				"command_id":     good,
			}
			cols[col] = bad
			if _, err := s.Pool().Exec(ctx,
				`INSERT INTO events (workspace_id, event_id, stream_type, stream_id, version,
				   type_url, schema_version, occurred_at, recorded_at,
				   correlation_id, causation_id, command_id,
				   actor_type, actor_id, actor_principal, payload, prev_hash, hash)
				 VALUES ('ws_scan', $1, 'thing', $2, 1,
				   'thing.v1.Corrupt', 1, now(), now(),
				   $3, $4, $5,
				   'thing', 'seed', 'thing:seed', 'x', '', '')`,
				cols["event_id"], sid.Canonical(), cols["correlation_id"], cols["causation_id"], cols["command_id"],
			); err != nil {
				t.Fatalf("seed corrupt row: %v", err)
			}

			_, rerr := ws.ReadStream(ctx, sid, 0, 0)
			if rerr == nil {
				t.Fatalf("read of a corrupt %s succeeded: want the read to fail", col)
			}
			msg := rerr.Error()
			if !strings.Contains(msg, col) {
				t.Fatalf("error %q does not name the offending column %q", msg, col)
			}
			if !strings.Contains(msg, fmt.Sprintf("length %d", len(bad))) {
				t.Fatalf("error %q does not report the corrupt value's length %d", msg, len(bad))
			}
			if strings.Contains(msg, bad) {
				t.Fatalf("error %q leaks the corrupt value's bytes", msg)
			}
		})
	}
}

// TestOpaquePayloadRoundTrip proves es-lite stores payloads verbatim (ADR
// 0025): what goes on disk is exactly what was appended (no crypto), and reads
// echo the same bytes back.
func TestOpaquePayloadRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_secret")
	sid := streamID(t)

	payload := []byte("client-sealed-opaque-bytes-abc123")
	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID: sid, ExpectedVersion: 0,
		Events: []es.EventData{{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: payload}},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// The raw column holds exactly the bytes given — es-lite adds no crypto.
	raw := rawPayload(t, s, "ws_secret", sid.Canonical(), 1)
	if !bytes.Equal(raw, payload) {
		t.Fatalf("stored payload = %q, want verbatim %q", raw, payload)
	}

	// A normal read echoes the same bytes back.
	evs, err := ws.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(evs) != 1 || !bytes.Equal(evs[0].Payload, payload) {
		t.Fatalf("read payload mismatch: %v", evs)
	}
}

func TestDrainClaimDelivery(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := streamID(t)

	// Events across two workspaces.
	for _, ws := range []string{"ws_a", "ws_b"} {
		rt := aggregate.NewRuntime(s.Workspace(ws), counter.Decider, counterv1.EventCodec{})
		if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Init{Min: 0, Max: 100, Initial: 0}), es.Meta{}); err != nil {
			t.Fatalf("init %s: %v", ws, err)
		}
		if _, err := rt.Handle(ctx, sid, counterv1.Command(&counterv1.Increment{By: 3}), es.Meta{}); err != nil {
			t.Fatalf("inc %s: %v", ws, err)
		}
	}

	var got []es.Envelope
	n, err := s.Drain(ctx, 100, func(ctx context.Context, batch []es.Envelope) error {
		got = append(got, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 4 || len(got) != 4 {
		t.Fatalf("drain claimed=%d delivered=%d, want 4/4", n, len(got))
	}
	// Global order is monotonic across workspaces.
	for i := 1; i < len(got); i++ {
		if got[i].GlobalPosition <= got[i-1].GlobalPosition {
			t.Fatalf("global_position not increasing at %d", i)
		}
	}
	// Everything is now published: a second drain claims nothing.
	n2, err := s.Drain(ctx, 100, func(context.Context, []es.Envelope) error { return nil })
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second drain claimed %d, want 0", n2)
	}
}

func TestUniqueClaimsWorkspaceScoped(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	appendClaim := func(ws, stream string, expected uint64, ops ...es.ConstraintOp) error {
		sid, _ := es.NewStreamID("thing", stream)
		_, err := s.Workspace(ws).Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: expected,
			Events:          []es.EventData{{TypeURL: "test.v1.E", SchemaVersion: 1, Payload: []byte("x")}},
			Constraints:     ops,
		})
		return err
	}

	// Uniqueness is per-workspace (PK includes workspace_id): the same value in
	// two workspaces is independent even though es-lite stores it verbatim.
	if err := appendClaim("ws1", "a", 0, es.Claim("email", "x@y.com")); err != nil {
		t.Fatalf("ws1 claim: %v", err)
	}
	if err := appendClaim("ws2", "a", 0, es.Claim("email", "x@y.com")); err != nil {
		t.Fatalf("same value in ws2 must be independent: %v", err)
	}
	// Within ws1 it collides.
	if err := appendClaim("ws1", "b", 0, es.Claim("email", "x@y.com")); !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("intra-workspace conflict: got %v, want ErrConstraintViolated", err)
	}

	// es-lite stores the value opaquely — the bytes given, verbatim (ADR 0025).
	// A real caller hashes personal data before it reaches the store; here the raw
	// value flows through, so we see it verbatim.
	vk := rawValueKey(t, s, "ws1", "email")
	if !bytes.Equal(vk, []byte("x@y.com")) {
		t.Fatalf("value_key = %q, want verbatim %q", vk, "x@y.com")
	}
}

// rawValueKey reads one stored value_key for (workspace, scope), bypassing RLS.
func rawValueKey(t *testing.T, s *postgres.Store, ws, scope string) []byte {
	t.Helper()
	ctx := context.Background()
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('app.bypass_rls', 'on', true)`); err != nil {
		t.Fatal(err)
	}
	var vk []byte
	if err := tx.QueryRow(ctx,
		`SELECT value_key FROM unique_claims WHERE workspace_id=$1 AND scope=$2 LIMIT 1`, ws, scope,
	).Scan(&vk); err != nil {
		t.Fatalf("raw value_key: %v", err)
	}
	return vk
}

func TestMigrateIdempotentAndConcurrent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t) // auto-migrated on Open

	var v int
	if err := s.Pool().QueryRow(ctx, `SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatalf("schema_migrations: %v", err)
	}
	// Every embedded migration is applied on Open; 0010 (command_dedup)
	// made it ten.
	if v != 10 {
		t.Fatalf("applied version = %d, want 10", v)
	}

	// Concurrent migrators serialize on the advisory lock and all no-op safely.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.Migrate(ctx) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent migrate: %v", err)
		}
	}
}

// rawPayload reads the stored (encrypted) payload bytes, bypassing RLS and
// decryption, to prove what is actually on disk.
func rawPayload(t *testing.T, s *postgres.Store, ws, canonicalStream string, version int) []byte {
	t.Helper()
	ctx := context.Background()
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('app.bypass_rls', 'on', true)`); err != nil {
		t.Fatal(err)
	}
	var p []byte
	if err := tx.QueryRow(ctx,
		`SELECT payload FROM events WHERE workspace_id=$1 AND stream_id=$2 AND version=$3`,
		ws, canonicalStream, version,
	).Scan(&p); err != nil {
		t.Fatalf("raw payload: %v", err)
	}
	return p
}

func TestEmptyAppendChecksVersionAndAppliesConstraints(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_empty")
	sid, _ := es.NewStreamID("thing", "main")

	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: "thing.v1.Created", SchemaVersion: 1, Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if _, err := ws.Append(ctx, es.AppendParams{StreamID: sid, ExpectedVersion: 0}); !errors.Is(err, es.ErrConflict) {
		t.Fatalf("stale empty append error = %v, want ErrConflict", err)
	}
	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Constraints:     []es.ConstraintOp{es.Claim("email", "a@b.com")},
	}); err != nil {
		t.Fatalf("claim-only append: %v", err)
	}
	if _, found, err := ws.LookupClaim(ctx, "email", "a@b.com"); err != nil || !found {
		t.Fatalf("claim lookup found=%v err=%v, want found", found, err)
	}
}

// TestAppendRejectsOversizedPayload pins the append-time payload cap
// (WithMaxPayloadBytes): an over-cap batch is rejected whole with
// es.ErrPayloadTooLarge before anything is inserted — on both the
// self-transacting Append path and the caller-owned AppendTx path — 0
// selects the 8 MiB default, a negative value lifts the cap, and the
// wrapped error carries stream, version, type URL and size — never the
// payload bytes.
func TestAppendRejectsOversizedPayload(t *testing.T) {
	ctx := context.Background()

	t.Run("over cap is rejected before any insert", func(t *testing.T) {
		s := openStoreWithOptions(t, postgres.WithMaxPayloadBytes(64))
		ws := s.WorkspaceStore("ws_cap")
		sid := streamID(t)

		// Seed one event so the log has a non-zero baseline to compare
		// against after the rejection.
		if _, err := ws.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Events:          []es.EventData{{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: []byte("x")}},
		}); err != nil {
			t.Fatalf("seed append: %v", err)
		}
		head, err := ws.Head(ctx)
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		rawHead, err := ws.RawHead(ctx)
		if err != nil {
			t.Fatalf("raw head: %v", err)
		}
		if head == 0 || rawHead == 0 {
			t.Fatalf("head=%d rawHead=%d, want a non-zero baseline", head, rawHead)
		}

		payload := bytes.Repeat([]byte("p"), 100)
		_, err = ws.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 1,
			Events:          []es.EventData{{TypeURL: "counter.v1.Oversized", SchemaVersion: 1, Payload: payload}},
		})
		if !errors.Is(err, es.ErrPayloadTooLarge) {
			t.Fatalf("oversized append: got %v, want ErrPayloadTooLarge", err)
		}
		msg := err.Error()
		if !strings.Contains(msg, "counter.v1.Oversized") || !strings.Contains(msg, "100") {
			t.Fatalf("error %q lacks the type URL or the size", msg)
		}
		if strings.Contains(msg, string(payload)) {
			t.Fatal("the error leaks the payload bytes")
		}

		// Nothing was appended: the stream version and both heads are
		// exactly what they were before the rejected append.
		if v, err := ws.CurrentStreamVersion(ctx, sid); err != nil || v != 1 {
			t.Fatalf("stream version after rejected append = %d (err %v), want 1", v, err)
		}
		if h, err := ws.Head(ctx); err != nil || h != head {
			t.Fatalf("head after rejected append = %d (err %v), want %d", h, err, head)
		}
		if h, err := ws.RawHead(ctx); err != nil || h != rawHead {
			t.Fatalf("raw head after rejected append = %d (err %v), want %d", h, err, rawHead)
		}
	})

	// The cap also guards a caller-owned transaction: AppendTx rejects the
	// batch before any insert, leaving the caller's transaction usable.
	t.Run("caller-owned transaction is rejected too", func(t *testing.T) {
		s := openStoreWithOptions(t, postgres.WithMaxPayloadBytes(64))
		ws := s.WorkspaceStore("ws_cap_tx")
		sid := streamID(t)

		tx, err := s.Pool().Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback(ctx)
		narrow := narrowTx{tx}

		_, err = ws.AppendTx(ctx, narrow, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Events:          []es.EventData{{TypeURL: "counter.v1.Oversized", SchemaVersion: 1, Payload: bytes.Repeat([]byte("p"), 100)}},
		})
		if !errors.Is(err, es.ErrPayloadTooLarge) {
			t.Fatalf("oversized AppendTx: got %v, want ErrPayloadTooLarge", err)
		}
		if rows, err := ws.ReadStreamTx(ctx, narrow, sid, 0, 0); err != nil || len(rows) != 0 {
			t.Fatalf("rows inside tx = %d (err %v), want 0 — nothing was inserted", len(rows), err)
		}
	})

	// A negative option lifts the cap: even a payload over the 8 MiB
	// default appends.
	t.Run("negative option means unlimited", func(t *testing.T) {
		s := openStoreWithOptions(t, postgres.WithMaxPayloadBytes(-1))
		ws := s.WorkspaceStore("ws_cap_unlimited")
		sid := streamID(t)
		if _, err := ws.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Events:          []es.EventData{{TypeURL: "counter.v1.Oversized", SchemaVersion: 1, Payload: bytes.Repeat([]byte("p"), 8<<20+1)}},
		}); err != nil {
			t.Fatalf("unlimited append: %v", err)
		}
	})

	// Zero selects the default: 8 MiB exactly still fits, one byte over is
	// rejected.
	t.Run("zero option means the 8 MiB default", func(t *testing.T) {
		s := openStoreWithOptions(t, postgres.WithMaxPayloadBytes(0))
		ws := s.WorkspaceStore("ws_cap_default")
		sid := streamID(t)
		_, err := ws.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Events:          []es.EventData{{TypeURL: "counter.v1.Oversized", SchemaVersion: 1, Payload: bytes.Repeat([]byte("p"), 8<<20+1)}},
		})
		if !errors.Is(err, es.ErrPayloadTooLarge) {
			t.Fatalf("over-default append: got %v, want ErrPayloadTooLarge", err)
		}
		if _, err := ws.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: 0,
			Events:          []es.EventData{{TypeURL: "counter.v1.MaxSize", SchemaVersion: 1, Payload: bytes.Repeat([]byte("p"), 8<<20)}},
		}); err != nil {
			t.Fatalf("at-cap append: %v", err)
		}
	})
}

// TestReadStreamStreamNotFound pins the ReadStream not-found contract: a
// read from the beginning of a stream that was never written reports
// es.ErrStreamNotFound (naming the stream) instead of a silent empty
// slice — whatever the upper bound, since from the start of the stream an
// empty result means no rows exist at all.
func TestReadStreamStreamNotFound(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_nf")
	sid := streamID(t)

	for _, tc := range []struct {
		name string
		to   uint64
	}{
		{"whole stream", 0},
		{"bounded prefix", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ws.ReadStream(ctx, sid, 0, tc.to)
			if !errors.Is(err, es.ErrStreamNotFound) {
				t.Fatalf("ReadStream(0, %d) on a never-written stream: got %v, want ErrStreamNotFound", tc.to, err)
			}
			if !strings.Contains(err.Error(), sid.Canonical()) {
				t.Fatalf("error %q does not name the stream %s", err, sid.Canonical())
			}
		})
	}

	// The caller-owned-transaction read keeps the plain empty result: the
	// not-found contract is ReadStream's, not the Tx join's.
	t.Run("ReadStreamTx is unaffected", func(t *testing.T) {
		tx, err := s.Pool().Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		envs, err := s.WorkspaceStore("ws_nf").ReadStreamTx(ctx, narrowTx{tx}, sid, 0, 0)
		if err != nil || len(envs) != 0 {
			t.Fatalf("ReadStreamTx on a never-written stream: envs=%d err=%v, want (nil, nil)", len(envs), err)
		}
	})
}

// TestReadStreamEmptyRangeStillNil pins the other half of the contract: an
// empty range is not "stream not found". Reads with fromVersion > 0, and
// reads from 0 of a stream that exists, return (nil, nil).
func TestReadStreamEmptyRangeStillNil(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_range")
	sid := streamID(t)

	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 0,
		Events:          []es.EventData{{TypeURL: "counter.v1.Initialized", SchemaVersion: 1, Payload: []byte("x")}},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// An existing stream read from its head: an empty range, not a
	// missing stream.
	current, err := ws.CurrentStreamVersion(ctx, sid)
	if err != nil {
		t.Fatalf("current version: %v", err)
	}
	envs, err := ws.ReadStream(ctx, sid, current, 0)
	if err != nil || len(envs) != 0 {
		t.Fatalf("read from head v%d: envs=%d err=%v, want (nil, nil)", current, len(envs), err)
	}

	// Head truncation staged out-of-band (the F2 threat model), bypassing
	// RLS the way the raw-payload helpers do: the stream still has rows
	// above the bound, so a bounded read from 0 that matches nothing is an
	// empty range, not a missing stream.
	if _, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: 1,
		Events:          []es.EventData{{TypeURL: "counter.v1.Incremented", SchemaVersion: 1, Payload: []byte("y")}},
	}); err != nil {
		t.Fatalf("append v2: %v", err)
	}
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.bypass_rls', 'on', true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM events WHERE workspace_id=$1 AND stream_id=$2 AND version <= 1`, "ws_range", sid.Canonical(),
	); err != nil {
		t.Fatalf("stage truncation: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	envs, err = ws.ReadStream(ctx, sid, 0, 1)
	if err != nil || len(envs) != 0 {
		t.Fatalf("bounded read of a head-truncated stream: envs=%d err=%v, want (nil, nil)", len(envs), err)
	}
	// The whole-stream read still sees the survivor: the stream exists.
	envs, err = ws.ReadStream(ctx, sid, 0, 0)
	if err != nil || len(envs) != 1 || envs[0].Version != 2 {
		t.Fatalf("whole read after truncation: envs=%d err=%v, want the v2 survivor alone", len(envs), err)
	}
}
