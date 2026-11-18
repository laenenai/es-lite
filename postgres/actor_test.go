package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/laenenai/es-lite/es"
)

// The delegated actor (ADR 0041) on the Postgres backend: the three
// nullable via_* columns migration 0012 adds, the write mapping, the
// strict read reconstruction and the all-or-nothing CHECK. Via is an
// audit fact, not telemetry — unlike the span context (0011) a read
// that finds a half-recorded delegation must fail, and the append path
// refuses one before anything is written. Everything here skips
// without PG_DSN, like the rest of the Postgres suite.

// viaTypeURL is this file's event type.
const viaTypeURL = "delegation.v1.E"

// appendWithActor appends one viaTypeURL event to sid at the given
// expected version, carrying actor as the appending actor.
func appendWithActor(t testing.TB, ctx context.Context, ws es.Store, sid es.StreamID, version uint64, actor es.Actor) es.Envelope {
	t.Helper()
	res, err := ws.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Actor:           actor,
		Events:          []es.EventData{{TypeURL: viaTypeURL, SchemaVersion: 1, Payload: []byte("p")}},
	})
	if err != nil {
		t.Fatalf("append at v%d: %v", version+1, err)
	}
	return res.Envelopes[0]
}

// TestDelegatedActorRoundTrip pins the write and read mapping: an
// append carrying a delegated actor comes back with the same actor —
// accountable subject and Via both — on the stream read and the
// global-order read, and the envelope read back still verifies against
// its chain hash, which attests the Via (ADR 0041 §3.2): a store that
// dropped or altered the delegation on the way through would break the
// hash here, not just the field.
func TestDelegatedActorRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_via_rt")
	sid := streamID(t)
	actor := es.Actor{Type: "user", ID: "alice", Via: es.ActorRef{Type: "service", ID: "agent-runner"}}

	env := appendWithActor(t, ctx, ws, sid, 0, actor)
	if env.Actor != actor {
		t.Fatalf("append result carries %+v, want %+v", env.Actor, actor)
	}

	stream, err := ws.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(stream) != 1 {
		t.Fatalf("stream has %d events, want 1", len(stream))
	}
	if stream[0].Actor != actor {
		t.Fatalf("ReadStream carries %+v, want %+v", stream[0].Actor, actor)
	}
	if h := es.EnvelopeHash(stream[0]); !bytes.Equal(h[:], stream[0].Hash) {
		t.Fatal("ReadStream envelope fails its chain hash — the persisted Via does not match what was attested")
	}

	all, err := ws.ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("log has %d events, want 1", len(all))
	}
	if all[0].Actor != actor {
		t.Fatalf("ReadAll carries %+v, want %+v", all[0].Actor, actor)
	}
}

// TestDelegatedActorAbsentReadsZero pins the absent shape: an append
// with a plain actor — no delegation — reads back with a zero Via and
// the accountable actor unchanged.
func TestDelegatedActorAbsentReadsZero(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_via_absent")
	sid := streamID(t)
	actor := es.Actor{Type: "service", ID: "billing"}

	appendWithActor(t, ctx, ws, sid, 0, actor)

	envs, err := ws.ReadStream(ctx, sid, 0, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("stream has %d events, want 1", len(envs))
	}
	if !envs[0].Actor.Via.IsZero() {
		t.Fatalf("Via = %+v, want the zero ActorRef", envs[0].Actor.Via)
	}
	if envs[0].Actor != actor {
		t.Fatalf("actor = %+v, want %+v with no Via", envs[0].Actor, actor)
	}
}

// TestDelegatedActorPartialRejected pins the append-time guard
// (ADR 0041 §3.4): a Via that is not complete — or a Via with nobody
// to account for — is rejected with es.ErrInvalidDelegation on both
// Append and AppendTx, before anything is written, so the stream's
// version does not move.
func TestDelegatedActorPartialRejected(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	ws := s.Workspace("ws_via_partial")
	wstx := s.WorkspaceStore("ws_via_partial")
	sid := streamID(t)

	for name, actor := range map[string]es.Actor{
		"via type only":   {Type: "user", ID: "alice", Via: es.ActorRef{Type: "service"}},
		"via id only":     {Type: "user", ID: "alice", Via: es.ActorRef{ID: "agent-runner"}},
		"via, zero actor": {Via: es.ActorRef{Type: "service", ID: "agent-runner"}},
	} {
		_, err := ws.Append(ctx, es.AppendParams{
			StreamID: sid,
			Actor:    actor,
			Events:   []es.EventData{{TypeURL: viaTypeURL, SchemaVersion: 1, Payload: []byte("p")}},
		})
		if !errors.Is(err, es.ErrInvalidDelegation) {
			t.Fatalf("%s: append: error = %v, want es.ErrInvalidDelegation", name, err)
		}

		tx, err := s.Pool().Begin(ctx)
		if err != nil {
			t.Fatalf("%s: begin: %v", name, err)
		}
		_, err = wstx.AppendTx(ctx, narrowTx{tx}, es.AppendParams{
			StreamID: sid,
			Actor:    actor,
			Events:   []es.EventData{{TypeURL: viaTypeURL, SchemaVersion: 1, Payload: []byte("p")}},
		})
		if !errors.Is(err, es.ErrInvalidDelegation) {
			tx.Rollback(ctx)
			t.Fatalf("%s: append tx: error = %v, want es.ErrInvalidDelegation", name, err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("%s: rollback: %v", name, err)
		}
	}

	if v, err := ws.CurrentStreamVersion(ctx, sid); err != nil || v != 0 {
		t.Fatalf("stream version after the rejected appends = %d (err %v), want 0", v, err)
	}
}

// TestDelegatedActorCheckRejectsRawPartial pins the database-side
// guard: even a raw INSERT that bypasses the store cannot store a half
// delegation. The column list mirrors TestSpanContextCheckRejectsRawPartial
// plus via_type; via_type set with via_id and via_principal NULL trips
// migration 0012's all-or-nothing CHECK — events_via_complete — as
// SQLSTATE 23514.
func TestDelegatedActorCheckRejectsRawPartial(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	sid := streamID(t)

	_, err := s.Pool().Exec(ctx,
		`INSERT INTO events (
			workspace_id, event_id, stream_type, stream_id, version,
			type_url, schema_version, occurred_at, recorded_at,
			correlation_id, causation_id, command_id,
			actor_type, actor_id, actor_principal, payload,
			via_type
		) VALUES ($1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11,$12, $13,$14,$15,$16, $17)`,
		"ws_via_raw", "11111111-1111-7111-8111-111111111111", sid.Type, sid.Canonical(), 1,
		viaTypeURL, 1, time.Now(), time.Now(),
		"", "", "",
		sid.Type, "raw", "conformance:raw", []byte("p"),
		"service",
	)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" || // check_violation
		pgErr.ConstraintName != "events_via_complete" {
		t.Fatalf("raw partial insert: error = %v, want SQLSTATE 23514 (check_violation) on constraint events_via_complete", err)
	}
}

// TestDelegatedActorViaIndexUsed pins the reverse audit query's index
// (ADR 0041 §5): the planner must reach events_via_idx for the
// by-via-principal lookup ("everything service S did, across users").
// The events table is HASH-partitioned, so a plan names the
// per-partition child index, not the parent — the children are
// resolved from the catalog and the plan must use one of them, the
// same assert-by-shape rationale as TestFeedIndexesExist.
func TestDelegatedActorViaIndexUsed(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	rows, err := s.Pool().Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'events_via_idx'::regclass`)
	if err != nil {
		t.Fatalf("list events_via_idx children: %v", err)
	}
	var children []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			t.Fatalf("scan child index: %v", err)
		}
		children = append(children, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("list events_via_idx children: %v", err)
	}
	if len(children) == 0 {
		t.Fatal("events_via_idx has no partition child indexes")
	}

	var plan string
	if err := s.Pool().QueryRow(ctx,
		`EXPLAIN (COSTS OFF) SELECT global_position FROM events
		  WHERE workspace_id = $1 AND via_principal = $2`,
		"ws_via_idx", "service:agent-runner",
	).Scan(&plan); err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !slices.ContainsFunc(children, func(name string) bool { return strings.Contains(plan, name) }) {
		t.Fatalf("plan for the via-principal lookup does not use events_via_idx (checked children %v):\n%s", children, plan)
	}
}
